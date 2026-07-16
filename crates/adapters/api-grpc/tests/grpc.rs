//! Drives the service implementation directly rather than over a socket.
//!
//! The trait methods are the unit of behaviour here; binding a port would add
//! a listener, a client connection and a shutdown to every test without
//! testing anything tonic does not already test itself. This is the same
//! reasoning that has the REST tests use `oneshot`.

use api_grpc::SchedulerService;
use api_grpc::pb::scheduler_server::Scheduler;
use api_grpc::pb::{CreateJobRequest, GetJobRequest, GetRunRequest, job};
use scheduler_application::testing::{InMemoryJobs, InMemoryRuns};
use scheduler_domain::*;
use tonic::{Code, Request};
use uuid::Uuid;

fn service() -> SchedulerService<InMemoryJobs, InMemoryRuns> {
    SchedulerService::new(InMemoryJobs::new(), InMemoryRuns::new())
}

fn create(tenant: &str, target: &str, every_secs: i64) -> Request<CreateJobRequest> {
    Request::new(CreateJobRequest {
        tenant: tenant.into(),
        target: target.into(),
        every_secs,
    })
}

/// Repositories whose reads and writes always fail, so the storage error path
/// can be observed. The message is deliberately shaped like a real sqlx error:
/// those carry the host, port, database user and table name.
///
/// The same fixture shape as `FailingRuns` in the GraphQL tests -- the rule is
/// a transport-wide one, so the fixture that pins it is too.
const LEAKY_MESSAGE: &str = "error returned from database: relation \"job_runs\" \
     does not exist (host=db.internal port=5432 user=scheduler)";

#[derive(Clone)]
struct FailingJobs;

impl JobRepository for FailingJobs {
    async fn insert(&self, _job: &Job) -> DomainResult<()> {
        Err(DomainError::Storage(LEAKY_MESSAGE.into()))
    }
    async fn get(&self, _id: JobId) -> DomainResult<Job> {
        Err(DomainError::Storage(LEAKY_MESSAGE.into()))
    }
    async fn list_active(&self, _limit: i64) -> DomainResult<Vec<Job>> {
        Err(DomainError::Storage(LEAKY_MESSAGE.into()))
    }
}

#[derive(Clone)]
struct FailingRuns;

impl RunRepository for FailingRuns {
    async fn insert_runs(&self, _runs: &[JobRun]) -> DomainResult<()> {
        Ok(())
    }
    async fn claim_due(
        &self,
        _now: time::OffsetDateTime,
        _limit: i64,
        _owner: &str,
        _per_tenant_cap: i64,
    ) -> DomainResult<scheduler_domain::ClaimOutcome> {
        Ok(scheduler_domain::ClaimOutcome::default())
    }
    async fn claim_ids(
        &self,
        _ids: &[RunId],
        _now: time::OffsetDateTime,
        _owner: &str,
        _per_tenant_cap: i64,
    ) -> DomainResult<scheduler_domain::ClaimOutcome> {
        Ok(scheduler_domain::ClaimOutcome::default())
    }
    async fn release(&self, _ids: &[RunId]) -> DomainResult<()> {
        Ok(())
    }
    async fn reclaim_expired(
        &self,
        _now: time::OffsetDateTime,
        _limit: i64,
    ) -> DomainResult<Vec<RunId>> {
        Ok(Vec::new())
    }
    async fn complete(&self, _id: RunId, _outcome: RunState) -> DomainResult<bool> {
        Ok(false)
    }
    async fn runs_for_jobs(
        &self,
        _job_ids: &[JobId],
        _before: time::OffsetDateTime,
        _limit_per_job: i64,
    ) -> DomainResult<Vec<JobRun>> {
        Ok(Vec::new())
    }
    async fn get(&self, _id: RunId) -> DomainResult<JobRun> {
        Err(DomainError::Storage(LEAKY_MESSAGE.into()))
    }
}

fn assert_leaks_nothing(status: &tonic::Status) {
    for secret in ["db.internal", "job_runs", "5432", "user=scheduler"] {
        assert!(
            !status.message().contains(secret),
            "storage detail {secret:?} leaked to the client: {}",
            status.message()
        );
    }
}

/// A `Status` message goes straight to the client, so a storage failure must
/// not carry its detail into one. A sqlx error names the host, port, database
/// user and table.
///
/// REST and GraphQL both had this test; gRPC did not, because the Phase 2c
/// scrubbing fix covered two of the three surfaces. Replacing
/// `Status::internal("internal error")` with `Status::internal(other.to_string())`
/// passed the entire suite. Found by an independent reviewer's mutation.
#[tokio::test]
async fn storage_errors_are_scrubbed_from_get_run() {
    let svc = SchedulerService::new(InMemoryJobs::new(), FailingRuns);

    let status = svc
        .get_run(Request::new(GetRunRequest {
            id: Uuid::new_v4().to_string(),
        }))
        .await
        .expect_err("a storage failure must surface as an error");

    assert_eq!(
        status.code(),
        Code::Internal,
        "a storage failure is not the client's mistake: {status:?}"
    );
    assert_leaks_nothing(&status);
}

/// The same rule on the job paths -- a read and a write, both through
/// `to_status`, so neither branch can regress on its own.
#[tokio::test]
async fn storage_errors_are_scrubbed_from_the_job_rpcs() {
    let svc = SchedulerService::new(FailingJobs, FailingRuns);

    let read = svc
        .get_job(Request::new(GetJobRequest {
            id: Uuid::new_v4().to_string(),
        }))
        .await
        .expect_err("a storage failure must surface as an error");
    assert_eq!(read.code(), Code::Internal, "got {read:?}");
    assert_leaks_nothing(&read);

    let write = svc
        .create_job(create("acme", "http://svc/run", 60))
        .await
        .expect_err("a storage failure must surface as an error");
    assert_eq!(write.code(), Code::Internal, "got {write:?}");
    assert_leaks_nothing(&write);
}

#[tokio::test]
async fn create_job_returns_an_id_and_the_job_is_readable() {
    let svc = service();

    let created = svc
        .create_job(create("acme", "http://svc/run", 60))
        .await
        .expect("create must succeed")
        .into_inner();
    Uuid::parse_str(&created.id).expect("id must be a UUID");

    let got = svc
        .get_job(Request::new(GetJobRequest {
            id: created.id.clone(),
        }))
        .await
        .expect("get must succeed")
        .into_inner()
        .job
        .expect("job must be present");

    assert_eq!(got.id, created.id);
    assert_eq!(got.tenant, "acme");
    assert_eq!(got.target, "http://svc/run");
    assert_eq!(got.schedule, Some(job::Schedule::EverySecs(60)));
}

/// An absent job is `not_found`, not `internal`. Collapsing them makes every
/// client treat a normal miss as an outage.
#[tokio::test]
async fn get_unknown_job_is_status_not_found() {
    let svc = service();
    let status = svc
        .get_job(Request::new(GetJobRequest {
            id: Uuid::new_v4().to_string(),
        }))
        .await
        .expect_err("must be an error");
    assert_eq!(status.code(), Code::NotFound, "got {status:?}");
}

#[tokio::test]
async fn get_unknown_run_is_status_not_found() {
    let svc = service();
    let status = svc
        .get_run(Request::new(GetRunRequest {
            id: Uuid::new_v4().to_string(),
        }))
        .await
        .expect_err("must be an error");
    assert_eq!(status.code(), Code::NotFound, "got {status:?}");
}

/// A malformed id is the client's mistake, so it must be `invalid_argument`
/// rather than surfacing as an internal failure.
#[tokio::test]
async fn malformed_id_is_invalid_argument() {
    let svc = service();
    let status = svc
        .get_job(Request::new(GetJobRequest {
            id: "not-a-uuid".into(),
        }))
        .await
        .expect_err("must be an error");
    assert_eq!(status.code(), Code::InvalidArgument, "got {status:?}");
}

/// A third transport is a third chance to make the same validation mistake.
/// The Phase 2b review found the REST surface accepting every one of these,
/// and the overflowing interval permanently stalled the engine.
#[tokio::test]
async fn create_job_rejects_invalid_input() {
    let cases = [
        (0i64, "acme", "http://x", "a zero interval"),
        (-1, "acme", "http://x", "a negative interval"),
        (
            1_000_000_000_000,
            "acme",
            "http://x",
            "an interval that would overflow time",
        ),
        (i64::MAX, "acme", "http://x", "i64::MAX"),
        (60, "", "http://x", "an empty tenant"),
        (60, "   ", "http://x", "a whitespace tenant"),
        (60, "acme", "", "an empty target"),
    ];

    for (every_secs, tenant, target, what) in cases {
        let jobs = InMemoryJobs::new();
        let svc = SchedulerService::new(jobs.clone(), InMemoryRuns::new());

        let status = svc
            .create_job(create(tenant, target, every_secs))
            .await
            .expect_err(&format!("{what} must be rejected"));

        assert_eq!(
            status.code(),
            Code::InvalidArgument,
            "{what} must be invalid_argument, got {status:?}"
        );
        assert!(
            jobs.list_active(10).await.unwrap().is_empty(),
            "{what} must not persist a job"
        );
    }
}

/// A one-shot schedule survives the round trip through the proto's oneof.
#[tokio::test]
async fn oneshot_schedule_round_trips_as_rfc3339() {
    let jobs = InMemoryJobs::new();
    let at = time::macros::datetime!(2026-08-01 09:00:00 UTC);
    let stored = Job::new(
        JobId(Uuid::new_v4()),
        "acme",
        Schedule::OneShot { at },
        "http://svc/run",
        time::macros::datetime!(2026-07-19 10:00:00 UTC),
    )
    .unwrap();
    jobs.insert(&stored).await.unwrap();

    let svc = SchedulerService::new(jobs, InMemoryRuns::new());
    let got = svc
        .get_job(Request::new(GetJobRequest {
            id: stored.id.0.to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .job
        .unwrap();

    assert_eq!(
        got.schedule,
        Some(job::Schedule::FireAt("2026-08-01T09:00:00Z".into())),
        "timestamps use RFC 3339, the same encoding as the NATS wire format"
    );
}

#[tokio::test]
async fn get_run_reports_state_and_attempt() {
    let runs = InMemoryRuns::new();
    let run = JobRun {
        id: RunId(Uuid::new_v4()),
        job_id: JobId(Uuid::new_v4()),
        tenant: TenantId("acme".into()),
        scheduled_at: time::macros::datetime!(2026-07-19 10:00:00 UTC),
        state: RunState::Claimed,
        attempt: 2,
    };
    runs.seed(vec![run.clone()]).await;

    let svc = SchedulerService::new(InMemoryJobs::new(), runs);
    let got = svc
        .get_run(Request::new(GetRunRequest {
            id: run.id.0.to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .run
        .unwrap();

    assert_eq!(got.state, "claimed");
    assert_eq!(got.attempt, 2);
    assert_eq!(got.scheduled_at, "2026-07-19T10:00:00Z");
    assert_eq!(got.job_id, run.job_id.0.to_string());
}
