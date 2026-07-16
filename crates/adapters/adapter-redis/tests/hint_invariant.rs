//! The one invariant that makes the Redis index safe: **it is a hint, Postgres
//! is truth.** Wires a real Postgres alongside a real Redis and asserts the
//! three ways that plays out — the hot path agrees with the scan, a wiped index
//! loses nothing, and a stale hint is never claimed.

use adapter_postgres::{PgRunRepository, run_migrations};
use adapter_redis::RedisDueIndex;
use scheduler_domain::{JobId, JobRun, RunId, RunRepository, RunState, TenantId};
use sqlx::postgres::PgPoolOptions;
use time::OffsetDateTime;
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

/// One shared Postgres and one shared Redis per binary; serialize so each test
/// truncates/flushes to a clean slate without racing another.
static GATE: Mutex<()> = Mutex::const_new(());

struct Fixture {
    _gate: MutexGuard<'static, ()>,
    repo: PgRunRepository,
    index: RedisDueIndex,
}

async fn setup() -> Fixture {
    let gate = GATE.lock().await;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&test_support::postgres_url(test_support::postgres_port()))
        .await
        .expect("connect postgres");
    run_migrations(&pool).await.expect("migrations");
    sqlx::query("TRUNCATE job_runs")
        .execute(&pool)
        .await
        .expect("truncate");

    let redis_url = test_support::redis_url(test_support::redis_port());
    let client = redis::Client::open(redis_url.clone()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    let index = RedisDueIndex::connect(&redis_url).await.unwrap();

    Fixture {
        _gate: gate,
        repo: PgRunRepository { pool },
        index,
    }
}

fn due_runs(n: usize, now: OffsetDateTime) -> Vec<JobRun> {
    (0..n)
        .map(|i| JobRun {
            id: RunId(Uuid::new_v4()),
            job_id: JobId(Uuid::new_v4()),
            tenant: TenantId("t1".into()),
            scheduled_at: now - time::Duration::seconds((n - i) as i64),
            state: RunState::Pending,
            attempt: 0,
        })
        .collect()
}

/// (a) The hot path — `pop_due` then `claim_ids` — claims exactly the due runs,
/// the same set the index-free scan would.
#[tokio::test]
async fn hot_path_claims_the_same_runs_as_the_scan() {
    let fx = setup().await;
    let now = OffsetDateTime::now_utc();
    let runs = due_runs(5, now);
    fx.repo.insert_runs(&runs).await.unwrap();
    fx.index.push(&runs).await.unwrap();

    let ids = fx.index.pop_due(now, 100).await.unwrap();
    let outcome = fx.repo.claim_ids(&ids, now, "engine-1", 0).await.unwrap();
    assert_eq!(
        outcome.claimed.len(),
        5,
        "the hot path must claim every due run"
    );
}

/// (b) Wiping the index loses nothing: the Postgres scan still claims every due
/// run, because the runs live in Postgres and the index only pointed at them.
#[tokio::test]
async fn a_wiped_index_loses_no_runs() {
    let fx = setup().await;
    let now = OffsetDateTime::now_utc();
    let runs = due_runs(4, now);
    fx.repo.insert_runs(&runs).await.unwrap();
    fx.index.push(&runs).await.unwrap();

    // Catastrophe: the whole index is gone.
    let client = redis::Client::open(test_support::redis_url(test_support::redis_port())).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    assert_eq!(fx.index.len().await.unwrap(), 0, "the index is wiped");

    // The scan path recovers everything — nothing was lost with the hint.
    let outcome = fx.repo.claim_due(now, 100, "engine-1", 0).await.unwrap();
    assert_eq!(
        outcome.claimed.len(),
        4,
        "a wiped index must not lose a single run"
    );
}

/// (c) A stale hint — an id the index still holds but that Postgres has already
/// claimed — is never re-claimed through the hot path.
#[tokio::test]
async fn a_stale_hint_is_never_reclaimed() {
    let fx = setup().await;
    let now = OffsetDateTime::now_utc();
    let runs = due_runs(2, now);
    fx.repo.insert_runs(&runs).await.unwrap();
    fx.index.push(&runs).await.unwrap();

    // Claim run 0 out of band, so the index's copy of its id is now stale.
    let claimed = fx
        .repo
        .claim_ids(&[runs[0].id], now, "engine-1", 0)
        .await
        .unwrap();
    assert_eq!(claimed.claimed.len(), 1);

    // Pop from the index (still holds both ids) and claim: only the run that is
    // still pending comes back; the stale one is dropped by Postgres.
    let ids = fx.index.pop_due(now, 100).await.unwrap();
    let outcome = fx.repo.claim_ids(&ids, now, "engine-2", 0).await.unwrap();
    let got: Vec<RunId> = outcome.claimed.iter().map(|r| r.id).collect();
    assert_eq!(
        got,
        vec![runs[1].id],
        "the stale hint must not be re-claimed"
    );

    // Postgres, not the index, is the record: run 0 is claimed exactly once.
    assert_eq!(
        fx.repo.get(runs[0].id).await.unwrap().state,
        RunState::Claimed
    );
}
