//! `RedisDueIndex` against a real Redis: due ordering, destructive pop, limit.

use adapter_redis::RedisDueIndex;
use scheduler_domain::{JobId, JobRun, RunId, RunState, TenantId};
use time::OffsetDateTime;
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

/// Serializes the tests, which share one container and one `sched:due` key.
static GATE: Mutex<()> = Mutex::const_new(());

/// A gate guard (hold it for the whole test) plus an index over a freshly
/// flushed database.
async fn fresh() -> (MutexGuard<'static, ()>, RedisDueIndex) {
    let gate = GATE.lock().await;
    let url = test_support::redis_url(test_support::redis_port());
    let client = redis::Client::open(url.clone()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    let index = RedisDueIndex::connect(&url).await.unwrap();
    (gate, index)
}

fn run_at(scheduled_at: OffsetDateTime) -> JobRun {
    JobRun {
        id: RunId(Uuid::new_v4()),
        job_id: JobId(Uuid::new_v4()),
        tenant: TenantId("t1".into()),
        scheduled_at,
        state: RunState::Pending,
        attempt: 0,
    }
}

#[tokio::test]
async fn pop_returns_due_ids_oldest_first_and_leaves_the_future() {
    let (_gate, index) = fresh().await;
    let now = OffsetDateTime::now_utc();

    let a = run_at(now - time::Duration::seconds(30));
    let b = run_at(now - time::Duration::seconds(20));
    let c = run_at(now - time::Duration::seconds(10));
    let future = run_at(now + time::Duration::seconds(100));
    // Pushed out of order; the score, not the push order, decides.
    index
        .push(&[c.clone(), a.clone(), future.clone(), b.clone()])
        .await
        .unwrap();

    let popped = index.pop_due(now, 10).await.unwrap();
    assert_eq!(
        popped,
        vec![a.id, b.id, c.id],
        "due ids must come back oldest-score first, and the future one excluded"
    );
    assert_eq!(
        index.len().await.unwrap(),
        1,
        "the future run stays indexed"
    );
}

#[tokio::test]
async fn pop_is_destructive() {
    let (_gate, index) = fresh().await;
    let now = OffsetDateTime::now_utc();
    index
        .push(&[
            run_at(now - time::Duration::seconds(5)),
            run_at(now - time::Duration::seconds(4)),
        ])
        .await
        .unwrap();

    assert_eq!(index.pop_due(now, 10).await.unwrap().len(), 2);
    assert!(
        index.pop_due(now, 10).await.unwrap().is_empty(),
        "a popped id must not come back"
    );
}

#[tokio::test]
async fn pop_respects_the_limit() {
    let (_gate, index) = fresh().await;
    let now = OffsetDateTime::now_utc();
    let runs: Vec<JobRun> = (0..5)
        .map(|i| run_at(now - time::Duration::seconds(50 - i)))
        .collect();
    index.push(&runs).await.unwrap();

    assert_eq!(index.pop_due(now, 2).await.unwrap().len(), 2);
    assert_eq!(
        index.len().await.unwrap(),
        3,
        "the rest stay for the next pop"
    );
}
