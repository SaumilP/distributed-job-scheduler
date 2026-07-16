use adapter_postgres::PgRunRepository;
use scheduler_application::testing::{FixedClock, RecordingPublisher};
use scheduler_application::{ClaimAndDispatch, MaterializeDueRuns};
use scheduler_domain::*;
use time::OffsetDateTime;
use uuid::Uuid;

mod support;

#[tokio::test]
async fn use_case_claims_and_dispatches_over_postgres() {
    let pool = support::pg_pool().await;

    let repo = PgRunRepository { pool: pool.clone() };
    let fixed_now = OffsetDateTime::now_utc();
    let runs: Vec<JobRun> = (0..5)
        .map(|_| JobRun {
            id: RunId(Uuid::new_v4()),
            job_id: JobId(Uuid::new_v4()),
            tenant: TenantId("t1".into()),
            scheduled_at: fixed_now - time::Duration::seconds(1),
            state: RunState::Pending,
            attempt: 0,
        })
        .collect();
    repo.insert_runs(&runs).await.unwrap();

    let publisher = RecordingPublisher::new();
    let uc = ClaimAndDispatch {
        runs: repo,
        publisher: publisher.clone(),
        clock: FixedClock(fixed_now),
        batch: 10,
        owner: "engine-1".into(),
        in_flight: Default::default(),
        metrics: std::sync::Arc::new(scheduler_application::testing::NoopMetrics),
        per_tenant_cap: 0,
    };

    let n = uc.run().await.unwrap();
    assert_eq!(n, 5);
    assert_eq!(publisher.count().await, 5);

    // A second run must claim nothing: the claimed state actually persisted
    // (rather than e.g. the first claim's UPDATE having been rolled back or
    // never committed), so the same rows are not claimable again.
    let n2 = uc.run().await.unwrap();
    assert_eq!(n2, 0);
}

/// **The storage-side half of the materializer regression.**
///
/// The engine and application tests assert that a moving clock keeps proposing
/// the *same instants*. This asserts the thing that actually matters
/// operationally: that those repeats collapse into no new rows. It has to run
/// against Postgres, because `ON CONFLICT DO NOTHING` and
/// `UNIQUE (job_id, scheduled_at)` are the mechanism, and the in-memory fake
/// deliberately enforces neither.
///
/// The materializer used to anchor its grid on `clock.now()`. Every tick then
/// proposed instants that collided with nothing, so the constraint absorbed
/// nothing and the table grew by a full horizon per poll. Measured on the demo
/// stack: 888 rows for one 5-second job in about two minutes. This is that
/// measurement, shrunk to a test.
///
/// 12 ticks, clock +1s each, 5s period, 60s horizon:
///   - correct: 12 rows after the first tick, still 12 at the end (12s of
///     movement sweeps 2 new grid points in, so 14).
///   - the bug: 12 new rows on every tick, 144 total.
#[tokio::test]
async fn materialize_over_a_moving_clock_leaves_the_row_count_stable() {
    const PERIOD: i64 = 5;
    const HORIZON: i64 = 60;
    const TICKS: i64 = 12;

    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };

    // Whole seconds: Postgres TIMESTAMPTZ keeps microseconds, and an anchor
    // with nanoseconds would make the grid points read back slightly shifted.
    let start = OffsetDateTime::now_utc().replace_nanosecond(0).unwrap();

    // Anchored off the tick boundary so a cursor-anchored implementation
    // cannot coincidentally agree with a job-anchored one.
    let job = Job {
        id: JobId(Uuid::new_v4()),
        tenant: TenantId("t1".into()),
        schedule: Schedule::Interval { every_secs: PERIOD },
        target: "http://svc/run".into(),
        created_at: start - time::Duration::seconds(3),
    };

    let row_count = async |pool: &sqlx::PgPool| -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM job_runs WHERE job_id = $1")
            .bind(job.id.0)
            .fetch_one(pool)
            .await
            .unwrap()
    };

    let mut after_first_tick = 0i64;

    for tick in 0..TICKS {
        let uc = MaterializeDueRuns {
            runs: repo.clone(),
            clock: FixedClock(start + time::Duration::seconds(tick)),
            horizon_secs: HORIZON,
            metrics: std::sync::Arc::new(scheduler_application::testing::NoopMetrics),
        };
        let proposed = uc.run(&job).await.unwrap();

        // Every tick still proposes a full horizon -- so the assertion below is
        // about duplicates being absorbed, not about the materializer having
        // quietly stopped producing.
        assert_eq!(
            proposed.len(),
            (HORIZON / PERIOD) as usize,
            "tick {tick} proposed {} runs, expected a full horizon",
            proposed.len()
        );

        if tick == 0 {
            after_first_tick = row_count(&pool).await;
            assert_eq!(after_first_tick, HORIZON / PERIOD);
        }
    }

    let stored = row_count(&pool).await;
    let swept = TICKS / PERIOD; // new grid points the moving clock exposed

    assert_eq!(
        stored,
        after_first_tick + swept,
        "after {TICKS} ticks the table holds {stored} rows; it must grow only as \
         the clock sweeps new grid points into the horizon ({swept} of them), not \
         once per tick. The cursor-anchored bug produced {}.",
        TICKS * (HORIZON / PERIOD)
    );

    // Every stored instant is on the job's grid, which is what makes the
    // unique constraint able to collide at all.
    let instants: Vec<OffsetDateTime> =
        sqlx::query_scalar("SELECT scheduled_at FROM job_runs WHERE job_id = $1")
            .bind(job.id.0)
            .fetch_all(&*pool)
            .await
            .unwrap();
    for at in instants {
        assert_eq!(
            (at - job.created_at).whole_seconds() % PERIOD,
            0,
            "{at} is not on the grid anchored at {}",
            job.created_at
        );
    }
}
