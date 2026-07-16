//! The REST surface: a driving adapter.
//!
//! Handlers translate HTTP into calls on the repositories and back. They own
//! no scheduling logic -- in particular, schedule validation is delegated to
//! `Schedule::interval` rather than re-checked here, because validation that
//! exists in two places drifts and the copy in the handler is the one that
//! gets forgotten.

use adapter_postgres::{PgJobRepository, PgRunRepository};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use scheduler_domain::{
    DomainError, Job, JobId, JobRepository, RunId, RunRepository, RunState, Schedule,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub jobs: PgJobRepository,
    pub runs: PgRunRepository,
    pub pool: PgPool,
}

impl AppState {
    pub fn new(pool: PgPool) -> Self {
        Self {
            jobs: PgJobRepository { pool: pool.clone() },
            runs: PgRunRepository { pool: pool.clone() },
            pool,
        }
    }
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/jobs", post(create_job))
        .route("/jobs/{id}", get(get_job))
        .route("/runs/{id}", get(get_run))
        .with_state(state)
}

/// Liveness only. Deliberately does **not** touch the database.
///
/// A liveness probe that fails when Postgres blips makes Kubernetes restart a
/// perfectly healthy process, turning a brief dependency outage into a restart
/// storm across every replica at once. Readiness is where dependency health
/// belongs: it removes the pod from the load balancer without killing it.
async fn health() -> StatusCode {
    StatusCode::OK
}

async fn ready(State(st): State<AppState>) -> StatusCode {
    match sqlx::query("SELECT 1").execute(&st.pool).await {
        Ok(_) => StatusCode::OK,
        Err(e) => {
            tracing::warn!(error = %e, "readiness check failed");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

/// Wire representation of a schedule. Tagged, so an unknown variant is a
/// deserialization error (400) rather than a silently-defaulted schedule.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScheduleBody {
    Interval {
        every_secs: i64,
    },
    OneShot {
        #[serde(with = "time::serde::rfc3339")]
        at: OffsetDateTime,
    },
}

#[derive(Debug, Deserialize)]
pub struct CreateJobBody {
    pub tenant: String,
    pub target: String,
    pub schedule: ScheduleBody,
}

#[derive(Debug, Serialize)]
pub struct CreatedJob {
    pub id: Uuid,
}

#[derive(Debug, Serialize)]
pub struct JobView {
    pub id: Uuid,
    pub tenant: String,
    pub target: String,
    pub schedule: ScheduleView,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScheduleView {
    Interval {
        every_secs: i64,
    },
    OneShot {
        #[serde(with = "time::serde::rfc3339")]
        at: OffsetDateTime,
    },
}

#[derive(Debug, Serialize)]
pub struct RunView {
    pub id: Uuid,
    pub job_id: Uuid,
    pub state: String,
    pub attempt: i32,
    #[serde(with = "time::serde::rfc3339")]
    pub scheduled_at: OffsetDateTime,
}

/// The single place `DomainError` becomes a status code.
///
/// Mapping per-handler is how a client error leaks out as a 500: one handler
/// remembers that `Invalid` means 400 and the next one does not.
struct ApiError(DomainError);

impl From<DomainError> for ApiError {
    fn from(e: DomainError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            DomainError::NotFound => StatusCode::NOT_FOUND,
            DomainError::Invalid(_) => StatusCode::BAD_REQUEST,
            DomainError::Storage(_) | DomainError::Publish(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        // Internal failures are logged but not echoed: a storage error string
        // can carry connection details and table names. Client errors are safe
        // to return because the client caused them.
        let body = match &self.0 {
            DomainError::Storage(_) | DomainError::Publish(_) => {
                tracing::error!(error = %self.0, "request failed");
                "internal error".to_string()
            }
            other => other.to_string(),
        };

        (status, Json(serde_json::json!({ "error": body }))).into_response()
    }
}

async fn create_job(
    State(st): State<AppState>,
    Json(body): Json<CreateJobBody>,
) -> Result<(StatusCode, Json<CreatedJob>), ApiError> {
    // Validation lives in the domain constructor, not here. `Schedule::interval`
    // already rejects a non-positive period; duplicating that check in the
    // handler would give two rules that can disagree.
    let schedule = match body.schedule {
        ScheduleBody::Interval { every_secs } => Schedule::interval(every_secs)?,
        ScheduleBody::OneShot { at } => Schedule::OneShot { at },
    };

    // Through the domain constructor, not a struct literal. The literal only
    // validated the schedule, so an empty tenant and an empty target were both
    // accepted: a job with no target is materialized forever and can never be
    // delivered, and an empty tenant flows into the per-tenant NATS subject.
    //
    // `created_at` is read from the system clock here rather than from the
    // `Clock` port. The port exists so *scheduling* logic is testable without
    // real time; this is a request-time stamp taken at the edge, and no clock
    // is threaded into this adapter. It is not inert metadata, though — it
    // anchors the job's interval grid (see `scheduler_domain::Job::created_at`),
    // so it is set once, here, and never re-stamped.
    let job = Job::new(
        JobId(Uuid::new_v4()),
        body.tenant,
        schedule,
        body.target,
        OffsetDateTime::now_utc(),
    )?;
    st.jobs.insert(&job).await?;

    Ok((StatusCode::CREATED, Json(CreatedJob { id: job.id.0 })))
}

async fn get_job(
    State(st): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JobView>, ApiError> {
    let job = st.jobs.get(JobId(id)).await?;
    Ok(Json(JobView {
        id: job.id.0,
        tenant: job.tenant.0,
        target: job.target,
        schedule: match job.schedule {
            Schedule::Interval { every_secs } => ScheduleView::Interval { every_secs },
            Schedule::OneShot { at } => ScheduleView::OneShot { at },
        },
    }))
}

async fn get_run(
    State(st): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<RunView>, ApiError> {
    let run = st.runs.get(RunId(id)).await?;
    Ok(Json(RunView {
        id: run.id.0,
        job_id: run.job_id.0,
        state: state_name(run.state).to_string(),
        attempt: run.attempt,
        scheduled_at: run.scheduled_at,
    }))
}

fn state_name(s: RunState) -> &'static str {
    match s {
        RunState::Pending => "pending",
        RunState::Claimed => "claimed",
        RunState::Running => "running",
        RunState::Succeeded => "succeeded",
        RunState::Failed => "failed",
        RunState::Dead => "dead",
    }
}
