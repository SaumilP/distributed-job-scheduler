use adapter_postgres::PgJobRepository;
use scheduler_domain::*;
use uuid::Uuid;

mod support;

/// A whole-second `created_at` on purpose: Postgres `TIMESTAMPTZ` keeps
/// microseconds, so a nanosecond-precision instant would not survive the round
/// trip and `interval_job_round_trips` would fail for a reason that has nothing
/// to do with what it is testing.
const CREATED_AT: time::OffsetDateTime = time::macros::datetime!(2026-07-19 10:00:00 UTC);

fn interval_job(secs: i64) -> Job {
    Job {
        id: JobId(Uuid::new_v4()),
        tenant: TenantId("acme".into()),
        schedule: Schedule::Interval { every_secs: secs },
        target: "http://svc/run".into(),
        created_at: CREATED_AT,
    }
}

#[tokio::test]
async fn interval_job_round_trips() {
    let pool = support::pg_pool().await;
    let repo = PgJobRepository { pool: pool.clone() };
    let job = interval_job(60);
    repo.insert(&job).await.unwrap();
    let got = repo.get(job.id).await.unwrap();
    assert_eq!(got, job, "a job must survive the round trip unchanged");
}

#[tokio::test]
async fn oneshot_job_round_trips() {
    let pool = support::pg_pool().await;
    let repo = PgJobRepository { pool: pool.clone() };
    let job = Job {
        schedule: Schedule::OneShot {
            at: time::macros::datetime!(2026-08-01 09:00:00 UTC),
        },
        ..interval_job(60)
    };
    repo.insert(&job).await.unwrap();
    assert_eq!(repo.get(job.id).await.unwrap(), job);
}

#[tokio::test]
async fn get_missing_job_is_not_found() {
    let pool = support::pg_pool().await;
    let repo = PgJobRepository { pool: pool.clone() };
    let err = repo.get(JobId(Uuid::new_v4())).await.unwrap_err();
    assert!(matches!(err, DomainError::NotFound), "got {err:?}");
}

#[tokio::test]
async fn list_active_returns_inserted_jobs_and_honors_limit() {
    let pool = support::pg_pool().await;
    let repo = PgJobRepository { pool: pool.clone() };
    for _ in 0..3 {
        repo.insert(&interval_job(60)).await.unwrap();
    }
    assert_eq!(repo.list_active(10).await.unwrap().len(), 3);
    assert_eq!(
        repo.list_active(2).await.unwrap().len(),
        2,
        "limit must be honored"
    );
}

/// The engine materializes only for active jobs. A deactivated job must stop
/// producing runs -- otherwise "pause a job" is unimplementable.
#[tokio::test]
async fn list_active_excludes_deactivated_jobs() {
    let pool = support::pg_pool().await;
    let repo = PgJobRepository { pool: pool.clone() };
    let job = interval_job(60);
    repo.insert(&job).await.unwrap();
    sqlx::query("UPDATE jobs SET active = FALSE WHERE id = $1")
        .bind(job.id.0)
        .execute(&*pool)
        .await
        .unwrap();
    assert!(repo.list_active(10).await.unwrap().is_empty());
}

/// The shape constraint must be enforced by the database, not merely by the
/// mapper. Nothing stops a future writer from bypassing the repository.
#[tokio::test]
async fn schedule_shape_constraint_rejects_a_malformed_row() {
    let pool = support::pg_pool().await;
    let err = sqlx::query(
        "INSERT INTO jobs (id, tenant, target, kind, every_secs, fire_at)
         VALUES ($1, 't', 'x', 'interval', NULL, NULL)",
    )
    .bind(Uuid::new_v4())
    .execute(&*pool)
    .await
    .unwrap_err();
    assert!(err.to_string().contains("jobs_schedule_shape"), "got {err}");
}

/// `Schedule::interval` rejects a non-positive period in the domain because a
/// non-advancing interval makes the materializer loop forever. The database
/// must refuse it too, for rows written by anything but the repository.
///
/// Migration 0004 folded this into `jobs_interval_bounded`, which now enforces
/// both ends of the range.
#[tokio::test]
async fn interval_constraint_rejects_a_zero_period() {
    let pool = support::pg_pool().await;
    let err = sqlx::query(
        "INSERT INTO jobs (id, tenant, target, kind, every_secs)
         VALUES ($1, 't', 'x', 'interval', 0)",
    )
    .bind(Uuid::new_v4())
    .execute(&*pool)
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("jobs_interval_bounded"),
        "got {err}"
    );
}

/// An unrecognized `kind` is a handled storage error, not a panic and not a
/// silent default -- the same rule `state_from_str` follows for runs.
#[tokio::test]
async fn unrecognized_kind_is_rejected_by_the_database() {
    let pool = support::pg_pool().await;
    let err = sqlx::query(
        "INSERT INTO jobs (id, tenant, target, kind, every_secs)
         VALUES ($1, 't', 'x', 'weekly', 60)",
    )
    .bind(Uuid::new_v4())
    .execute(&*pool)
    .await
    .unwrap_err();
    assert!(err.to_string().contains("jobs_kind_check"), "got {err}");
}

/// The database must refuse an interval the domain would refuse, because rows
/// can be written by things other than the repository and a poisoned row
/// survives every restart.
#[tokio::test]
async fn interval_upper_bound_constraint_rejects_an_overflowing_period() {
    let pool = support::pg_pool().await;
    let err = sqlx::query(
        "INSERT INTO jobs (id, tenant, target, kind, every_secs)
         VALUES ($1, 't', 'x', 'interval', $2)",
    )
    .bind(Uuid::new_v4())
    .bind(1_000_000_000_000i64)
    .execute(&*pool)
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("jobs_interval_bounded"),
        "got {err}"
    );
}

/// The domain's bound and the database's must agree: a period the domain
/// accepts must be storable.
#[tokio::test]
async fn the_domain_maximum_interval_is_storable() {
    let pool = support::pg_pool().await;
    let repo = PgJobRepository { pool: pool.clone() };
    let job = Job {
        schedule: Schedule::interval(scheduler_domain::MAX_INTERVAL_SECS).unwrap(),
        ..interval_job(60)
    };
    repo.insert(&job)
        .await
        .expect("the domain maximum must be storable");
    assert_eq!(repo.get(job.id).await.unwrap(), job);
}
