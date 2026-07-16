//! gRPC driving adapter.
//!
//! A peer of `api-rest` and `api-graphql` over the same ports. As with those,
//! every write goes through the domain constructors rather than building
//! values directly — a third transport is a third chance to make the same
//! validation mistake, and the Phase 2b review found exactly that on the
//! first one.

use crate::pb::scheduler_server::Scheduler;
use crate::pb::{
    CreateJobRequest, CreateJobResponse, GetJobRequest, GetJobResponse, GetRunRequest,
    GetRunResponse, job,
};
use scheduler_domain::{
    DomainError, Job, JobId, JobRepository, RunId, RunRepository, RunState, Schedule,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// The single place `DomainError` becomes a `Status`.
///
/// Mapping per-RPC is how a client error eventually surfaces as `internal`:
/// one handler remembers that `Invalid` is `invalid_argument` and the next
/// does not. This mirrors the REST adapter's single `IntoResponse` impl.
fn to_status(e: DomainError) -> Status {
    match e {
        DomainError::NotFound => Status::not_found("not found"),
        // Safe to echo: the client caused it.
        DomainError::Invalid(m) => Status::invalid_argument(m),
        // Storage/publish detail carries connection strings and table names,
        // so it is logged rather than returned.
        other => {
            tracing::error!(error = %other, "grpc request failed");
            Status::internal("internal error")
        }
    }
}

fn parse_id(raw: &str, what: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(raw)
        .map_err(|_| Status::invalid_argument(format!("{what} must be a UUID, got {raw:?}")))
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

fn rfc3339(t: time::OffsetDateTime) -> String {
    t.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

pub struct SchedulerService<J: JobRepository, R: RunRepository> {
    jobs: J,
    runs: R,
}

impl<J: JobRepository, R: RunRepository> SchedulerService<J, R> {
    pub fn new(jobs: J, runs: R) -> Self {
        Self { jobs, runs }
    }
}

#[tonic::async_trait]
impl<J, R> Scheduler for SchedulerService<J, R>
where
    J: JobRepository + 'static,
    R: RunRepository + 'static,
{
    async fn create_job(
        &self,
        request: Request<CreateJobRequest>,
    ) -> Result<Response<CreateJobResponse>, Status> {
        let req = request.into_inner();

        // Through the domain constructors: `Schedule::interval` bounds the
        // period (an unbounded one overflowed and stalled the engine), and
        // `Job::new` rejects an empty tenant or target.
        //
        // `created_at` comes from the system clock at the edge, as in the REST
        // adapter, and anchors the job's interval grid — see
        // `scheduler_domain::Job::created_at`.
        let schedule = Schedule::interval(req.every_secs).map_err(to_status)?;
        let job = Job::new(
            JobId(Uuid::new_v4()),
            req.tenant,
            schedule,
            req.target,
            time::OffsetDateTime::now_utc(),
        )
        .map_err(to_status)?;

        self.jobs.insert(&job).await.map_err(to_status)?;

        Ok(Response::new(CreateJobResponse {
            id: job.id.0.to_string(),
        }))
    }

    async fn get_job(
        &self,
        request: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        let id = parse_id(&request.into_inner().id, "job id")?;
        let job = self.jobs.get(JobId(id)).await.map_err(to_status)?;

        Ok(Response::new(GetJobResponse {
            job: Some(crate::pb::Job {
                id: job.id.0.to_string(),
                tenant: job.tenant.0,
                target: job.target,
                schedule: Some(match job.schedule {
                    Schedule::Interval { every_secs } => job::Schedule::EverySecs(every_secs),
                    Schedule::OneShot { at } => job::Schedule::FireAt(rfc3339(at)),
                }),
            }),
        }))
    }

    async fn get_run(
        &self,
        request: Request<GetRunRequest>,
    ) -> Result<Response<GetRunResponse>, Status> {
        let id = parse_id(&request.into_inner().id, "run id")?;
        let run = self.runs.get(RunId(id)).await.map_err(to_status)?;

        Ok(Response::new(GetRunResponse {
            run: Some(crate::pb::Run {
                id: run.id.0.to_string(),
                job_id: run.job_id.0.to_string(),
                state: state_name(run.state).to_string(),
                attempt: run.attempt,
                scheduled_at: rfc3339(run.scheduled_at),
            }),
        }))
    }
}
