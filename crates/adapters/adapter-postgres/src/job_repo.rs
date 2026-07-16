use scheduler_domain::{DomainError, DomainResult, Job, JobId, JobRepository, Schedule, TenantId};
use sqlx::{PgPool, Row};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone)]
pub struct PgJobRepository {
    pub pool: PgPool,
}

/// The column projection every query in this module selects. Kept in one
/// place so `row_to_job` can rely on it -- the same convention `run_repo.rs`
/// follows.
/// `created_at` is in the projection because it is scheduling input, not just
/// audit metadata: it anchors the job's interval grid (see
/// `scheduler_domain::Job::created_at`). Dropping it from a query would hand
/// the materializer a job whose grid it cannot reconstruct.
const JOB_COLUMNS: &str = "id, tenant, target, kind, every_secs, fire_at, created_at";

/// Maps the persisted discriminant plus payload columns back to a `Schedule`.
///
/// A row that does not match its own `kind` is a handled error rather than a
/// panic or a silent default. The `jobs_schedule_shape` CHECK constraint (see
/// `migrations/0003_jobs.sql`) should make this unreachable, but the mapper
/// cannot assume the constraint survived every future migration, and
/// panicking in the engine's materialize loop over one bad row would take the
/// whole engine down.
fn row_to_job(row: &sqlx::postgres::PgRow) -> DomainResult<Job> {
    let kind: &str = row.get("kind");
    let schedule = match kind {
        "interval" => {
            let every_secs: Option<i64> = row.get("every_secs");
            let every_secs = every_secs
                .ok_or_else(|| DomainError::Storage("interval job with NULL every_secs".into()))?;
            // Goes through the validating constructor rather than building the
            // variant directly: a non-positive period read back from storage is
            // just as dangerous to the materializer as one supplied by a caller.
            Schedule::interval(every_secs)?
        }
        "oneshot" => {
            let fire_at: Option<OffsetDateTime> = row.get("fire_at");
            let at = fire_at
                .ok_or_else(|| DomainError::Storage("oneshot job with NULL fire_at".into()))?;
            Schedule::OneShot { at }
        }
        other => {
            return Err(DomainError::Storage(format!(
                "corrupt jobs.kind value {other:?}"
            )));
        }
    };

    Ok(Job {
        id: JobId(row.get::<Uuid, _>("id")),
        tenant: TenantId(row.get::<String, _>("tenant")),
        schedule,
        target: row.get::<String, _>("target"),
        created_at: row.get::<OffsetDateTime, _>("created_at"),
    })
}

/// Splits a `Schedule` into the discriminant and the two nullable payload
/// columns. The inverse of the `match` in `row_to_job`; keeping them adjacent
/// is what stops them drifting.
fn schedule_columns(s: &Schedule) -> (&'static str, Option<i64>, Option<OffsetDateTime>) {
    match s {
        Schedule::Interval { every_secs } => ("interval", Some(*every_secs), None),
        Schedule::OneShot { at } => ("oneshot", None, Some(*at)),
    }
}

impl JobRepository for PgJobRepository {
    fn insert(&self, job: &Job) -> impl std::future::Future<Output = DomainResult<()>> + Send {
        let pool = self.pool.clone();
        let job = job.clone();
        async move {
            let (kind, every_secs, fire_at) = schedule_columns(&job.schedule);
            // `created_at` is bound explicitly rather than left to the column's
            // `DEFAULT now()`. It anchors the interval grid, so the value the
            // caller validated and the value the materializer later reads back
            // have to be the same instant. Letting the default supply it would
            // put a job's grid microseconds away from where the API said it
            // was, and would silently re-phase a job if a row were ever
            // re-inserted.
            sqlx::query(
                "INSERT INTO jobs (id, tenant, target, kind, every_secs, fire_at, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(job.id.0)
            .bind(&job.tenant.0)
            .bind(&job.target)
            .bind(kind)
            .bind(every_secs)
            .bind(fire_at)
            .bind(job.created_at)
            .execute(&pool)
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))?;
            Ok(())
        }
    }

    fn get(&self, id: JobId) -> impl std::future::Future<Output = DomainResult<Job>> + Send {
        let pool = self.pool.clone();
        async move {
            let row = sqlx::query(&format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = $1"))
                .bind(id.0)
                .fetch_optional(&pool)
                .await
                .map_err(|e| DomainError::Storage(e.to_string()))?
                .ok_or(DomainError::NotFound)?;
            row_to_job(&row)
        }
    }

    fn list_active(
        &self,
        limit: i64,
    ) -> impl std::future::Future<Output = DomainResult<Vec<Job>>> + Send {
        let pool = self.pool.clone();
        async move {
            // Ordered by `created_at` so paging is stable and the oldest jobs
            // are never starved by a steady stream of new ones -- the same
            // fairness argument as the claim's `ORDER BY scheduled_at`.
            let rows = sqlx::query(&format!(
                "SELECT {JOB_COLUMNS} FROM jobs
                 WHERE active
                 ORDER BY created_at
                 LIMIT $1"
            ))
            .bind(limit)
            .fetch_all(&pool)
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))?;
            rows.iter().map(row_to_job).collect()
        }
    }
}
