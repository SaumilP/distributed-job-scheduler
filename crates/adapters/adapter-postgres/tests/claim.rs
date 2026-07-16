use adapter_postgres::PgRunRepository;
use scheduler_domain::*;
use sqlx::Row;
use std::collections::HashSet;
use std::time::Duration;
use time::OffsetDateTime;
use uuid::Uuid;

mod support;

fn seed_runs(n: usize, now: OffsetDateTime) -> Vec<JobRun> {
    (0..n)
        .map(|_| JobRun {
            id: RunId(Uuid::new_v4()),
            job_id: JobId(Uuid::new_v4()),
            tenant: TenantId("t1".into()),
            scheduled_at: now - time::Duration::seconds(10),
            state: RunState::Pending,
            attempt: 0,
        })
        .collect()
}

/// Like `seed_runs`, but with a distinct, strictly increasing `scheduled_at`
/// per row (row `i` at `now - (n - i)` seconds), so `ORDER BY scheduled_at` is
/// a total order over the seeded rows rather than a tie among identical
/// timestamps. All rows are still in the past (due), just not simultaneously.
fn seed_runs_staggered(n: usize, now: OffsetDateTime) -> Vec<JobRun> {
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

/// The primary guarantee for `claim_due`: rows locked (but not yet committed)
/// by another transaction must be skipped, not waited on. This is deterministic
/// (no reliance on two racing UPDATEs happening to interleave) -- a fully
/// serialized run cannot pass it, because the held 50 ids are asserted to be
/// exactly the ones NOT returned by the claim.
///
/// Rows are seeded with staggered `scheduled_at` values (via
/// `seed_runs_staggered`) rather than an identical timestamp, so `ORDER BY
/// scheduled_at` is a total order and the "first 50" locked by the holder are
/// guaranteed by construction to be disjoint from the "next 50" the claim can
/// see -- not merely by scan-order tie-break stability.
///
/// If `FOR UPDATE SKIP LOCKED` were removed from the claim query, `claim_due`
/// would instead block on the still-open holding transaction's row locks and
/// this test would hang until the `tokio::time::timeout` below fires, failing
/// with a clear timeout error rather than silently passing.
#[tokio::test]
async fn claim_skips_rows_locked_by_another_transaction() {
    let pool = support::pg_pool().await;

    let now = OffsetDateTime::now_utc();
    let repo = PgRunRepository { pool: pool.clone() };
    let runs = seed_runs_staggered(100, now);
    repo.insert_runs(&runs).await.unwrap();

    // On a separate connection, open an explicit transaction and lock 50 of the
    // 100 due rows without committing. Those locks must remain held while the
    // claim below runs.
    let mut holder = pool.begin().await.unwrap();
    let held_rows = sqlx::query(
        "SELECT id FROM job_runs WHERE state = 'pending' ORDER BY scheduled_at LIMIT 50 FOR UPDATE",
    )
    .fetch_all(&mut *holder)
    .await
    .unwrap();
    let held_ids: HashSet<Uuid> = held_rows.iter().map(|r| r.get::<Uuid, _>("id")).collect();
    assert_eq!(held_ids.len(), 50, "expected to lock exactly 50 rows");

    // From the pool (a different connection), claim against the full 100.
    // With SKIP LOCKED this returns immediately with the 50 unlocked rows.
    // Without it, this call blocks on the held locks until the holder commits
    // or rolls back -- which we deliberately never do before the timeout.
    let claimed = tokio::time::timeout(Duration::from_secs(10), repo.claim_due(now, 100, "c2", 0))
        .await
        .expect("claim_due blocked -- SKIP LOCKED is not taking effect")
        .expect("claim_due returned an error");

    assert_eq!(
        claimed.len(),
        50,
        "expected to claim exactly the 50 unlocked rows"
    );
    let claimed_ids: HashSet<Uuid> = claimed.iter().map(|r| r.id.0).collect();
    assert!(
        claimed_ids.is_disjoint(&held_ids),
        "claim must not touch rows locked by the still-open holding transaction"
    );
    assert_eq!(
        claimed_ids.len() + held_ids.len(),
        100,
        "claimed + held should account for all 100 seeded rows"
    );

    // Release the held locks and let the test end cleanly.
    holder.rollback().await.unwrap();
}

/// Secondary smoke test: two concurrent claimers should never double-claim the
/// same run. This is not the primary guarantee (see
/// `claim_skips_rows_locked_by_another_transaction` above) since a fully
/// serialized execution also satisfies these assertions, but it's a cheap
/// sanity check under real concurrency.
#[tokio::test]
async fn two_claimers_get_disjoint_batches() {
    let pool = support::pg_pool().await;

    // Seed 100 due runs.
    let now = OffsetDateTime::now_utc();
    let repo = PgRunRepository { pool: pool.clone() };
    let runs = seed_runs(100, now);
    repo.insert_runs(&runs).await.unwrap();

    // Two concurrent claimers, batch 60 each. Total due = 100.
    let r1 = repo.clone();
    let r2 = repo.clone();
    let (a, b) = tokio::join!(
        async move { r1.claim_due(now, 60, "c1", 0).await.unwrap() },
        async move { r2.claim_due(now, 60, "c2", 0).await.unwrap() },
    );

    // Disjoint: no run id claimed by both.
    let ids_a: HashSet<_> = a.iter().map(|r| r.id).collect();
    let ids_b: HashSet<_> = b.iter().map(|r| r.id).collect();
    assert!(ids_a.is_disjoint(&ids_b), "claims overlapped");
    // Together they claim exactly the 100 due runs (60 + 40, order not guaranteed).
    assert_eq!(a.len() + b.len(), 100);
}

/// `get()` must report the real persisted state, not a hardcoded value.
/// Regression test for a bug where `get()` always returned `RunState::Pending`
/// regardless of the row's actual `state` column.
#[tokio::test]
async fn get_reflects_claimed_state_after_claim() {
    let pool = support::pg_pool().await;

    let now = OffsetDateTime::now_utc();
    let repo = PgRunRepository { pool: pool.clone() };
    let runs = seed_runs(1, now);
    let run_id = runs[0].id;
    repo.insert_runs(&runs).await.unwrap();

    let before = repo.get(run_id).await.unwrap();
    assert_eq!(before.state, RunState::Pending);

    let claimed = repo.claim_due(now, 1, "c1", 0).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, run_id);
    assert_eq!(claimed[0].state, RunState::Claimed);

    let after = repo.get(run_id).await.unwrap();
    assert_eq!(
        after.state,
        RunState::Claimed,
        "get() must reflect the real persisted state"
    );
}

/// The `state` column carries a CHECK constraint restricting it to the six
/// known `RunState` values. A raw INSERT attempting to persist an
/// unrecognized value must be rejected by Postgres, not silently accepted
/// (which would later be misread back as `Pending` -- see
/// `state_from_str` in `src/run_repo.rs`).
#[tokio::test]
async fn check_constraint_rejects_unrecognized_state() {
    let pool = support::pg_pool().await;

    let now = OffsetDateTime::now_utc();
    let result = sqlx::query(
        "INSERT INTO job_runs (id, job_id, tenant, scheduled_at, state, attempt)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind("t1")
    .bind(now)
    .bind("bogus")
    .bind(0i32)
    .execute(&*pool)
    .await;

    let err = result.expect_err("inserting an unrecognized state value must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("job_runs_state_check") || msg.to_lowercase().contains("check constraint"),
        "expected a CHECK-constraint violation, got: {msg}"
    );
}

/// `claim_due` sets `lease_expires_at` from the caller-supplied `now` plus
/// [`LEASE_SECS`], not from SQL `now()`. This closes the loop on the bug where
/// the query derived the lease from the database's wall clock instead of the
/// injected clock: a skewed `now` (years away from real wall time) must produce
/// a `lease_expires_at` that tracks the skew, not real time. If the query were
/// reverted to `now() + interval '...'`, the first assertion below would fail
/// because the persisted lease would sit near real wall-clock time instead of
/// near `skewed_now + LEASE_SECS`.
///
/// Written against the constant rather than a literal. It was a literal `30`,
/// and re-tuning `LEASE_SECS` in Phase 3a Task 3 failed it for a reason that
/// had nothing to do with the property it guards.
#[tokio::test]
async fn lease_expiry_derives_from_injected_clock_not_wall_clock() {
    let pool = support::pg_pool().await;

    let repo = PgRunRepository { pool: pool.clone() };

    // Deliberately skewed "now" -- years in the past, far from real wall-clock
    // time. `scheduled_at` is set earlier still so the row remains due.
    let skewed_now = time::macros::datetime!(2020-01-01 0:00:00 UTC);

    let run_id = RunId(Uuid::new_v4());
    let run = JobRun {
        id: run_id,
        job_id: JobId(Uuid::new_v4()),
        tenant: TenantId("t1".into()),
        scheduled_at: skewed_now - time::Duration::seconds(10),
        state: RunState::Pending,
        attempt: 0,
    };
    repo.insert_runs(&[run]).await.unwrap();

    let claimed = repo.claim_due(skewed_now, 1, "c1", 0).await.unwrap();
    assert_eq!(claimed.len(), 1, "the seeded due row must be claimed");

    let row = sqlx::query("SELECT lease_expires_at FROM job_runs WHERE id = $1")
        .bind(run_id.0)
        .fetch_one(&*pool)
        .await
        .unwrap();
    let lease_expires_at: OffsetDateTime = row.get("lease_expires_at");

    let expected = skewed_now + time::Duration::seconds(LEASE_SECS);
    let diff_from_expected = (lease_expires_at - expected).abs();
    assert!(
        diff_from_expected < time::Duration::seconds(2),
        "lease_expires_at ({lease_expires_at}) should be within 2s of \
         skewed_now + LEASE_SECS ({expected}), actual diff = {diff_from_expected}"
    );

    let real_now = OffsetDateTime::now_utc();
    let diff_from_real = (lease_expires_at - real_now).abs();
    assert!(
        diff_from_real > time::Duration::days(365),
        "lease_expires_at ({lease_expires_at}) should NOT be near real wall-clock now() \
         ({real_now}) -- this would indicate the query reverted to SQL now() instead of \
         the injected clock"
    );
}

/// Exercises the full claim predicate -- `state = 'pending' AND scheduled_at
/// <= $2` -- rather than just the state or just the due-window in isolation.
/// Every other test in this file seeds only past-dated Pending rows, so
/// deleting either filter from the query would still pass the whole suite;
/// this test seeds three distinct cohorts specifically so that removing
/// either clause changes which row is returned.
#[tokio::test]
async fn claim_respects_due_window_and_state_filter() {
    let pool = support::pg_pool().await;

    let now = OffsetDateTime::now_utc();
    let repo = PgRunRepository { pool: pool.clone() };

    // (a) due Pending: should be claimed.
    let due_pending = JobRun {
        id: RunId(Uuid::new_v4()),
        job_id: JobId(Uuid::new_v4()),
        tenant: TenantId("t1".into()),
        scheduled_at: now - time::Duration::seconds(10),
        state: RunState::Pending,
        attempt: 0,
    };
    // (b) future Pending: not yet due, must not be claimed.
    let future_pending = JobRun {
        id: RunId(Uuid::new_v4()),
        job_id: JobId(Uuid::new_v4()),
        tenant: TenantId("t1".into()),
        scheduled_at: now + time::Duration::seconds(60),
        state: RunState::Pending,
        attempt: 0,
    };
    // (c) already-Claimed, and also due by scheduled_at: wrong state, must
    // not be claimed again.
    let already_claimed = JobRun {
        id: RunId(Uuid::new_v4()),
        job_id: JobId(Uuid::new_v4()),
        tenant: TenantId("t1".into()),
        scheduled_at: now - time::Duration::seconds(10),
        state: RunState::Claimed,
        attempt: 0,
    };

    repo.insert_runs(&[
        due_pending.clone(),
        future_pending.clone(),
        already_claimed.clone(),
    ])
    .await
    .unwrap();

    let claimed = repo.claim_due(now, 10, "c1", 0).await.unwrap();

    assert_eq!(
        claimed.len(),
        1,
        "expected exactly one run to satisfy both the state and due-window filters"
    );
    assert_eq!(
        claimed[0].id, due_pending.id,
        "the claimed run must be the due Pending one, not the future or already-claimed rows"
    );
}

/// Asserts `LIMIT $3` is actually honored, not merely present in the SQL
/// text: seeds more due Pending rows than the requested limit and checks the
/// claim returns exactly `limit` of them.
#[tokio::test]
async fn claim_respects_limit() {
    let pool = support::pg_pool().await;

    let now = OffsetDateTime::now_utc();
    let repo = PgRunRepository { pool: pool.clone() };
    let runs = seed_runs(10, now);
    repo.insert_runs(&runs).await.unwrap();

    let claimed = repo.claim_due(now, 3, "c1", 0).await.unwrap();

    assert_eq!(claimed.len(), 3, "claim_due must honor the requested limit");
}

/// The idempotency half of "at-least-once + idempotency = effectively-once".
///
/// Two materializer passes over an overlapping horizon propose the same
/// logical run -- same `job_id`, same `scheduled_at` -- with different
/// surrogate `id`s. The second proposal must be absorbed silently: no error
/// surfaced to the caller, and no second row.
#[tokio::test]
async fn insert_runs_is_idempotent_on_job_and_scheduled_at() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();
    let job_id = JobId(Uuid::new_v4());
    let mk = |id| JobRun {
        id: RunId(id),
        job_id,
        tenant: TenantId("t1".into()),
        scheduled_at: now, // same (job_id, scheduled_at) both times
        state: RunState::Pending,
        attempt: 0,
    };

    let first_id = Uuid::new_v4();
    repo.insert_runs(&[mk(first_id)]).await.unwrap();
    // Second insert of the SAME logical run must not error and must not duplicate.
    repo.insert_runs(&[mk(Uuid::new_v4())]).await.unwrap();

    let ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM job_runs WHERE job_id = $1")
        .bind(job_id.0)
        .fetch_all(&*pool)
        .await
        .unwrap();
    assert_eq!(
        ids,
        vec![first_id],
        "duplicate materialization must leave the original row -- and its id -- untouched"
    );
}

/// A batch that partially conflicts must not be rejected wholesale.
///
/// This is the realistic materializer shape: successive passes overlap at the
/// boundary. If the conflicting row aborted the statement, every non-conflicting
/// run in the same batch would be lost and the schedule would develop holes.
#[tokio::test]
async fn insert_runs_partial_batch_conflict_still_inserts_the_rest() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();
    let job_id = JobId(Uuid::new_v4());
    let mk = |secs: i64| JobRun {
        id: RunId(Uuid::new_v4()),
        job_id,
        tenant: TenantId("t1".into()),
        scheduled_at: now + time::Duration::seconds(secs),
        state: RunState::Pending,
        attempt: 0,
    };

    repo.insert_runs(&[mk(10)]).await.unwrap();
    // Batch overlaps the existing run at +10 and adds two new ones.
    repo.insert_runs(&[mk(10), mk(20), mk(30)]).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM job_runs WHERE job_id = $1")
        .bind(job_id.0)
        .fetch_one(&*pool)
        .await
        .unwrap();
    assert_eq!(
        count, 3,
        "conflicting row skipped, non-conflicting rows inserted"
    );
}

/// Pins `ON CONFLICT ... DO NOTHING` -- specifically the *action*, not merely
/// its presence.
///
/// The two idempotency tests above both survive a mutation to
/// `DO UPDATE SET state = 'pending', attempt = 0`: one asserts on the
/// surviving `id` (which an upsert preserves) and the other on `count(*)`
/// (which an upsert also satisfies). This asserts the thing the code comment
/// actually claims: a duplicate materialization must not drag an in-flight
/// run backwards.
///
/// Without it: run is claimed and dispatched, the materializer's next
/// overlapping horizon re-proposes it, the row resets to pending, and the
/// next tick claims and publishes it again -- a duplicate execution created
/// by the mechanism meant to prevent one.
#[tokio::test]
async fn conflicting_insert_does_not_resurrect_a_claimed_run() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();
    let job_id = JobId(Uuid::new_v4());
    let scheduled_at = now - time::Duration::seconds(5);

    let original = JobRun {
        id: RunId(Uuid::new_v4()),
        job_id,
        tenant: TenantId("t1".into()),
        scheduled_at,
        state: RunState::Pending,
        attempt: 0,
    };
    repo.insert_runs(&[original.clone()]).await.unwrap();
    let claimed = repo.claim_due(now, 10, "engine-1", 0).await.unwrap();
    assert_eq!(claimed.len(), 1);

    // A later materializer pass re-proposes the same logical run.
    repo.insert_runs(&[JobRun {
        id: RunId(Uuid::new_v4()),
        ..original.clone()
    }])
    .await
    .unwrap();

    let after = repo.get(original.id).await.unwrap();
    assert_eq!(
        after.state,
        RunState::Claimed,
        "a duplicate materialization must not reset a claimed run to pending"
    );
    assert_eq!(after.attempt, 1, "attempt must not be reset either");

    // And it must not become claimable a second time.
    let again = repo.claim_due(now, 10, "engine-1", 0).await.unwrap();
    assert!(
        again.is_empty(),
        "resurrected run was claimed twice -- duplicate execution"
    );
}

/// Pins `ORDER BY scheduled_at`.
///
/// `claim_respects_limit` seeds identical timestamps and asserts only on the
/// count, so reversing the ordering to DESC passes it. A reversed order
/// drains a backlog newest-first and starves the oldest overdue runs
/// indefinitely -- a liveness failure that ships green.
#[tokio::test]
async fn claim_returns_oldest_runs_first() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    // Staggered so `scheduled_at` is a total order: row i is due at now-(5-i).
    let runs = seed_runs_staggered(5, now);
    repo.insert_runs(&runs).await.unwrap();

    let claimed = repo.claim_due(now, 2, "engine-1", 0).await.unwrap();

    // Asserts on the *set*, not the sequence. `ORDER BY scheduled_at` sits in
    // the sub-select that picks which rows to lock; `RETURNING` makes no
    // promise about the order it emits them in, and observably does not
    // preserve it. The guarantee being pinned here is "the oldest due runs
    // are the ones claimed", which is what prevents backlog starvation --
    // not "the returned vector is sorted", which the SQL never claimed.
    let expected: HashSet<RunId> = runs.iter().take(2).map(|r| r.id).collect();
    let got: HashSet<RunId> = claimed.iter().map(|r| r.id).collect();
    assert_eq!(got, expected, "claim must take the two oldest due runs");
}

/// Pins `attempt = attempt + 1` in the claim.
///
/// Nothing asserted `attempt` after a claim, so deleting the increment
/// shipped green. Retry accounting silently stopping matters as soon as
/// `claim_due` keys its max-attempts -> `dead` transition on this column.
#[tokio::test]
async fn claim_increments_attempt() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let runs = seed_runs(1, now);
    repo.insert_runs(&runs).await.unwrap();
    let id = runs[0].id;

    let claimed = repo.claim_due(now, 10, "engine-1", 0).await.unwrap();
    assert_eq!(claimed[0].attempt, 1, "first claim must record attempt 1");

    repo.release(&[id]).await.unwrap();
    let reclaimed = repo.claim_due(now, 10, "engine-1", 0).await.unwrap();
    assert_eq!(
        reclaimed[0].attempt, 2,
        "a re-claimed run must count the second attempt"
    );
}

/// `release` is the compensating half of claim-then-publish: without it, a
/// run whose publish failed stays `Claimed` forever and is never retried.
#[tokio::test]
async fn release_returns_a_claimed_run_to_pending_and_clears_the_lease() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let runs = seed_runs(1, now);
    repo.insert_runs(&runs).await.unwrap();
    let id = runs[0].id;
    repo.claim_due(now, 10, "engine-1", 0).await.unwrap();

    repo.release(&[id]).await.unwrap();

    assert_eq!(repo.get(id).await.unwrap().state, RunState::Pending);

    let row = sqlx::query("SELECT lease_owner, lease_expires_at FROM job_runs WHERE id = $1")
        .bind(id.0)
        .fetch_one(&*pool)
        .await
        .unwrap();
    assert!(
        row.get::<Option<String>, _>("lease_owner").is_none(),
        "a released run must not still name an owner"
    );
    assert!(
        row.get::<Option<OffsetDateTime>, _>("lease_expires_at")
            .is_none()
    );

    // The whole point: it can be claimed again.
    let again = repo.claim_due(now, 10, "engine-2", 0).await.unwrap();
    assert_eq!(again.len(), 1, "released run must be claimable again");
}

/// A release races the worker it is compensating for, so it must be scoped to
/// rows still in `claimed`. Dragging a running or finished run back to
/// pending would schedule a duplicate execution of work already underway.
#[tokio::test]
async fn release_does_not_drag_a_progressed_run_backwards() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let runs = seed_runs(1, now);
    repo.insert_runs(&runs).await.unwrap();
    let id = runs[0].id;
    repo.claim_due(now, 10, "engine-1", 0).await.unwrap();

    // The worker got there first and is already executing.
    sqlx::query("UPDATE job_runs SET state = 'running' WHERE id = $1")
        .bind(id.0)
        .execute(&*pool)
        .await
        .unwrap();

    repo.release(&[id]).await.unwrap();

    assert_eq!(
        repo.get(id).await.unwrap().state,
        RunState::Running,
        "release must not reset a run that has already progressed"
    );
}

#[tokio::test]
async fn complete_transitions_a_claimed_run_to_succeeded() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();
    let runs = seed_runs(1, now);
    repo.insert_runs(&runs).await.unwrap();
    repo.claim_due(now, 10, "engine-1", 0).await.unwrap();

    let did = repo
        .complete(runs[0].id, RunState::Succeeded)
        .await
        .unwrap();

    assert!(
        did,
        "the first completion must report that it performed the transition"
    );
    assert_eq!(
        repo.get(runs[0].id).await.unwrap().state,
        RunState::Succeeded
    );
}

/// The duplicate-suppression primitive. A redelivered message must be
/// recognizable as already-done, or "at-least-once + idempotency" has no
/// mechanism behind it -- only a slogan.
#[tokio::test]
async fn completing_an_already_terminal_run_reports_no_transition() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();
    let runs = seed_runs(1, now);
    repo.insert_runs(&runs).await.unwrap();
    repo.claim_due(now, 10, "engine-1", 0).await.unwrap();
    repo.complete(runs[0].id, RunState::Succeeded)
        .await
        .unwrap();

    let did = repo
        .complete(runs[0].id, RunState::Succeeded)
        .await
        .unwrap();

    assert!(
        !did,
        "a second completion must report that it changed nothing"
    );
    assert_eq!(
        repo.get(runs[0].id).await.unwrap().state,
        RunState::Succeeded
    );
}

/// A late `Failed` must not bury a recorded `Succeeded`. Redelivery means the
/// same run can be reported twice with different outcomes; first terminal
/// write wins.
#[tokio::test]
async fn complete_does_not_overwrite_a_terminal_state() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();
    let runs = seed_runs(1, now);
    repo.insert_runs(&runs).await.unwrap();
    repo.claim_due(now, 10, "engine-1", 0).await.unwrap();
    repo.complete(runs[0].id, RunState::Succeeded)
        .await
        .unwrap();

    let did = repo.complete(runs[0].id, RunState::Failed).await.unwrap();

    assert!(!did);
    assert_eq!(
        repo.get(runs[0].id).await.unwrap().state,
        RunState::Succeeded
    );
}

/// Only terminal outcomes are legal. `complete(id, Pending)` is a programming
/// error and must be rejected rather than silently corrupting the state
/// machine -- a run reset to pending here would be claimed and executed twice.
#[tokio::test]
async fn complete_rejects_a_non_terminal_outcome() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let err = repo
        .complete(RunId(Uuid::new_v4()), RunState::Pending)
        .await
        .unwrap_err();
    assert!(matches!(err, DomainError::Invalid(_)), "got {err:?}");
}

/// Completion clears the lease, as `release` does. A finished run that still
/// names an owner would mislead the reaper.
#[tokio::test]
async fn complete_clears_the_lease() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();
    let runs = seed_runs(1, now);
    repo.insert_runs(&runs).await.unwrap();
    repo.claim_due(now, 10, "engine-1", 0).await.unwrap();
    repo.complete(runs[0].id, RunState::Succeeded)
        .await
        .unwrap();

    let row = sqlx::query("SELECT lease_owner, lease_expires_at FROM job_runs WHERE id = $1")
        .bind(runs[0].id.0)
        .fetch_one(&*pool)
        .await
        .unwrap();
    assert!(row.get::<Option<String>, _>("lease_owner").is_none());
    assert!(
        row.get::<Option<OffsetDateTime>, _>("lease_expires_at")
            .is_none()
    );
}

// ---------------------------------------------------------------------------
// reclaim_expired -- the reaper's primitive.
//
// Every test below forces `lease_expires_at` with a raw UPDATE rather than
// waiting for a 30-second lease to elapse. That is not merely a speed
// optimization: it is what makes the expiry *observable*. A test that seeded a
// lease and then asserted on a reclaim without ever moving the lease or the
// clock across the boundary would pass whether or not the
// `lease_expires_at < $1` predicate exists at all.
// ---------------------------------------------------------------------------

/// Pushes `id`'s lease `secs` seconds into the past, standing in for an engine
/// that claimed the run and then died. Deliberately a raw UPDATE: no repository
/// method can produce this state, which is precisely why the state is
/// unreachable by any compensating code and needs a reaper.
async fn expire_lease(pool: &sqlx::PgPool, id: RunId, at: OffsetDateTime) {
    let n = sqlx::query("UPDATE job_runs SET lease_expires_at = $2 WHERE id = $1")
        .bind(id.0)
        .bind(at)
        .execute(pool)
        .await
        .unwrap()
        .rows_affected();
    assert_eq!(n, 1, "expire_lease must have moved exactly one row's lease");
}

/// The defect this whole phase exists for.
///
/// An engine that dies between committing the claim and publishing the batch
/// runs no compensating code at all -- `release` is never reached. Its rows sit
/// `Claimed`, and `claim_due` selects only `Pending`, so nothing would ever
/// pick them up again. Before `reclaim_expired` those runs were stranded
/// forever; README and ARCHITECTURE both said so.
#[tokio::test]
async fn reclaim_expired_returns_a_dead_owners_run_to_pending() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let runs = seed_runs(1, now);
    let id = runs[0].id;
    repo.insert_runs(&runs).await.unwrap();
    repo.claim_due(now, 10, "engine-that-died", 0)
        .await
        .unwrap();

    // The owner died; its lease ran out a minute ago.
    expire_lease(&pool, id, now - time::Duration::seconds(60)).await;

    let reclaimed = repo.reclaim_expired(now, 10).await.unwrap();

    assert_eq!(
        reclaimed,
        vec![id],
        "the reclaimed ids must be reported, not just silently repaired"
    );
    assert_eq!(
        repo.get(id).await.unwrap().state,
        RunState::Pending,
        "an expired claim must go back to pending"
    );

    let row = sqlx::query("SELECT lease_owner, lease_expires_at FROM job_runs WHERE id = $1")
        .bind(id.0)
        .fetch_one(&*pool)
        .await
        .unwrap();
    assert!(
        row.get::<Option<String>, _>("lease_owner").is_none(),
        "a reclaimed run must not still name the dead owner"
    );
    assert!(
        row.get::<Option<OffsetDateTime>, _>("lease_expires_at")
            .is_none(),
        "a reclaimed run must not still carry the expired lease"
    );
}

/// The dangerous direction. A live lease must NOT be reclaimed: the owner is
/// still working, and reclaiming would schedule a second execution of work
/// already in flight -- the reaper causing the very fault it exists to repair.
///
/// This is the test that pins `AND lease_expires_at < $1`. Without that
/// predicate the reaper steals every claim the instant it is made.
#[tokio::test]
async fn reclaim_expired_does_not_touch_a_live_lease() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let runs = seed_runs(1, now);
    let id = runs[0].id;
    repo.insert_runs(&runs).await.unwrap();
    repo.claim_due(now, 10, "engine-alive", 0).await.unwrap();

    // No expiry forced: the lease runs to `now + LEASE_SECS`, comfortably
    // ahead of the instant the reaper is given.
    let lease: OffsetDateTime =
        sqlx::query_scalar("SELECT lease_expires_at FROM job_runs WHERE id = $1")
            .bind(id.0)
            .fetch_one(&*pool)
            .await
            .unwrap();
    assert!(
        lease > now,
        "fixture is wrong: the lease must still be live at {now} (expires {lease})"
    );

    let reclaimed = repo.reclaim_expired(now, 10).await.unwrap();

    assert!(
        reclaimed.is_empty(),
        "a live lease was reclaimed -- this schedules a duplicate execution of \
         work already in flight: {reclaimed:?}"
    );
    assert_eq!(
        repo.get(id).await.unwrap().state,
        RunState::Claimed,
        "a run whose owner is still working must stay claimed"
    );
    // And the owner still holds it.
    let owner: Option<String> =
        sqlx::query_scalar("SELECT lease_owner FROM job_runs WHERE id = $1")
            .bind(id.0)
            .fetch_one(&*pool)
            .await
            .unwrap();
    assert_eq!(owner.as_deref(), Some("engine-alive"));
}

/// The boundary of the expiry comparison, stated exactly.
///
/// `lease_expires_at < now` -- a lease expiring at precisely `now` has not
/// expired yet, one expiring a microsecond earlier has. Asserted because a
/// test that only ever pushes leases a minute into the past cannot tell `<`
/// from `<=`, and cannot tell either from a predicate that was deleted.
#[tokio::test]
async fn reclaim_expired_is_exclusive_at_the_boundary() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let runs = seed_runs(2, now);
    let (exactly_now, just_before) = (runs[0].id, runs[1].id);
    repo.insert_runs(&runs).await.unwrap();
    repo.claim_due(now, 10, "engine-1", 0).await.unwrap();

    expire_lease(&pool, exactly_now, now).await;
    expire_lease(&pool, just_before, now - time::Duration::microseconds(1)).await;

    let reclaimed = repo.reclaim_expired(now, 10).await.unwrap();

    assert_eq!(
        reclaimed,
        vec![just_before],
        "only a lease strictly in the past is expired; one expiring exactly at \
         `now` still belongs to its owner"
    );
    assert_eq!(
        repo.get(exactly_now).await.unwrap().state,
        RunState::Claimed
    );
    assert_eq!(
        repo.get(just_before).await.unwrap().state,
        RunState::Pending
    );
}

/// Reaping is not a claim. A run that was never claimed must be left alone, or
/// the reaper would race the engine for pending work and hand back rows that
/// were never lost.
#[tokio::test]
async fn reclaim_expired_ignores_pending_runs() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let runs = seed_runs(1, now);
    let id = runs[0].id;
    repo.insert_runs(&runs).await.unwrap();

    // Never claimed, but carrying a long-expired lease -- the shape a
    // half-cleaned row would have. Only the state predicate can exclude it.
    expire_lease(&pool, id, now - time::Duration::seconds(60)).await;

    let reclaimed = repo.reclaim_expired(now, 10).await.unwrap();

    assert!(
        reclaimed.is_empty(),
        "a pending run is not a lost claim: {reclaimed:?}"
    );
    assert_eq!(repo.get(id).await.unwrap().state, RunState::Pending);
    assert_eq!(
        repo.get(id).await.unwrap().attempt,
        0,
        "an untouched pending run must not have been re-stamped"
    );
}

/// A terminal run whose lease somehow lingers must not be resurrected -- that
/// would re-execute completed work and, worse, make a recorded `Succeeded`
/// claimable again.
///
/// All three terminal states are checked. `Dead` is included deliberately: it
/// is the state Task 2 writes, and a reaper that resurrected it would make the
/// max-attempts cap unenforceable.
#[tokio::test]
async fn reclaim_expired_does_not_resurrect_a_terminal_run() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let runs = seed_runs(3, now);
    repo.insert_runs(&runs).await.unwrap();
    repo.claim_due(now, 10, "engine-1", 0).await.unwrap();

    let terminal = [
        (runs[0].id, RunState::Succeeded),
        (runs[1].id, RunState::Failed),
        (runs[2].id, RunState::Dead),
    ];
    for (id, outcome) in terminal {
        assert!(repo.complete(id, outcome).await.unwrap());
        // `complete` clears the lease, so put an expired one back: the point
        // is that the STATE predicate excludes these rows, not that they
        // happen to have no lease.
        expire_lease(&pool, id, now - time::Duration::seconds(60)).await;
    }

    let reclaimed = repo.reclaim_expired(now, 10).await.unwrap();

    assert!(
        reclaimed.is_empty(),
        "terminal runs were resurrected -- completed work would be executed \
         again: {reclaimed:?}"
    );
    for (id, outcome) in terminal {
        assert_eq!(
            repo.get(id).await.unwrap().state,
            outcome,
            "{outcome:?} run must stay {outcome:?}"
        );
    }
}

/// The bound is real. An unbounded reclaim after a long outage is a single
/// enormous statement holding locks over the whole backlog.
#[tokio::test]
async fn reclaim_expired_honors_the_limit() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let runs = seed_runs(10, now);
    repo.insert_runs(&runs).await.unwrap();
    repo.claim_due(now, 10, "engine-that-died", 0)
        .await
        .unwrap();
    for r in &runs {
        expire_lease(&pool, r.id, now - time::Duration::seconds(60)).await;
    }

    let first = repo.reclaim_expired(now, 3).await.unwrap();
    assert_eq!(first.len(), 3, "reclaim must honour the requested limit");

    // The rest are still there to be reaped: the limit bounds one pass, it
    // does not discard work.
    let second = repo.reclaim_expired(now, 100).await.unwrap();
    assert_eq!(second.len(), 7);

    // ...and a third pass finds nothing. Reaping is idempotent because the
    // reclaim clears the lease it selected on.
    assert!(
        repo.reclaim_expired(now, 100).await.unwrap().is_empty(),
        "a second reap of the same rows must reclaim nothing"
    );
}

/// Reclaimed runs must be claimable again -- the whole point. Asserting only
/// that the state column reads `pending` would not prove the run re-entered
/// the pipeline.
#[tokio::test]
async fn a_reclaimed_run_can_be_claimed_again() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let runs = seed_runs(1, now);
    let id = runs[0].id;
    repo.insert_runs(&runs).await.unwrap();
    repo.claim_due(now, 10, "engine-that-died", 0)
        .await
        .unwrap();
    expire_lease(&pool, id, now - time::Duration::seconds(60)).await;

    assert_eq!(repo.reclaim_expired(now, 10).await.unwrap(), vec![id]);

    let again = repo.claim_due(now, 10, "engine-2", 0).await.unwrap();
    assert_eq!(again.len(), 1, "a reclaimed run must be claimable again");
    assert_eq!(again[0].id, id);

    // The new owner holds a fresh live lease, so the reaper leaves it alone.
    let owner: Option<String> =
        sqlx::query_scalar("SELECT lease_owner FROM job_runs WHERE id = $1")
            .bind(id.0)
            .fetch_one(&*pool)
            .await
            .unwrap();
    assert_eq!(owner.as_deref(), Some("engine-2"));
    assert!(repo.reclaim_expired(now, 10).await.unwrap().is_empty());
}

/// The attempt count survives the reclaim, so a max-attempts policy can
/// eventually give up rather than reaping the same poisoned run forever.
///
/// The attempt was really consumed: the dead engine may well have published
/// the run and the work may well have executed before the engine lost track of
/// it. Resetting the counter here would make the cap unreachable and turn the
/// reaper into an infinite retry loop.
#[tokio::test]
async fn reclaim_expired_preserves_the_attempt_count() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let runs = seed_runs(1, now);
    let id = runs[0].id;
    repo.insert_runs(&runs).await.unwrap();

    // Two claim/reclaim cycles, so a reset to 0 is distinguishable from
    // "attempt happens to be 1".
    for expected in 1..=2 {
        let claimed = repo
            .claim_due(now, 10, "engine-that-died", 0)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1, "cycle {expected}: run must be claimable");
        assert_eq!(claimed[0].attempt, expected);

        expire_lease(&pool, id, now - time::Duration::seconds(60)).await;
        assert_eq!(repo.reclaim_expired(now, 10).await.unwrap(), vec![id]);

        let after = repo.get(id).await.unwrap();
        assert_eq!(
            after.attempt, expected,
            "reclaim must not touch attempt -- a reset makes the max-attempts \
             cap unreachable and the retry loop infinite"
        );
    }
}

/// Concurrent reapers must not block each other. `FOR UPDATE SKIP LOCKED`
/// means a row another transaction holds is skipped, not waited on --
/// serializing recovery across the fleet at exactly the moment the fleet is
/// least healthy.
///
/// Without SKIP LOCKED this call blocks on the held locks and the test fails
/// on the timeout with a clear message rather than silently passing.
#[tokio::test]
async fn reclaim_skips_rows_locked_by_another_transaction() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let runs = seed_runs_staggered(10, now);
    repo.insert_runs(&runs).await.unwrap();
    repo.claim_due(now, 10, "engine-that-died", 0)
        .await
        .unwrap();
    // Staggered expiries, so `ORDER BY lease_expires_at` is a total order and
    // the held rows are disjoint from the reapable ones by construction.
    for (i, r) in runs.iter().enumerate() {
        expire_lease(&pool, r.id, now - time::Duration::seconds(100 - i as i64)).await;
    }

    let mut holder = pool.begin().await.unwrap();
    let held: HashSet<Uuid> = sqlx::query(
        "SELECT id FROM job_runs WHERE state = 'claimed'
         ORDER BY lease_expires_at LIMIT 4 FOR UPDATE",
    )
    .fetch_all(&mut *holder)
    .await
    .unwrap()
    .iter()
    .map(|r| r.get::<Uuid, _>("id"))
    .collect();
    assert_eq!(held.len(), 4);

    let reclaimed = tokio::time::timeout(Duration::from_secs(10), repo.reclaim_expired(now, 10))
        .await
        .expect("reclaim_expired blocked -- SKIP LOCKED is not taking effect")
        .expect("reclaim_expired returned an error");

    let got: HashSet<Uuid> = reclaimed.iter().map(|r| r.0).collect();
    assert_eq!(got.len(), 6, "the six unlocked rows must be reclaimed");
    assert!(
        got.is_disjoint(&held),
        "reclaim must not touch rows locked by the still-open transaction"
    );

    holder.rollback().await.unwrap();
}

// ---------------------------------------------------------------------------
// The attempt cap -- terminal `Dead` when a run has had every attempt it gets.
//
// Enforced inside `claim_due`, because every retry path ends at `Pending` and
// re-enters through it: a publish failure releases the run, a dead engine has
// its lease reclaimed by the reaper. Nothing decrements `attempt`, so without
// a ceiling a run that can never be published cycles forever.
// ---------------------------------------------------------------------------

/// Seeds one due, pending run already carrying `attempt` attempts, standing in
/// for a run that has been round the retry loop that many times.
fn seed_run_with_attempt(now: OffsetDateTime, attempt: i32) -> JobRun {
    JobRun {
        id: RunId(Uuid::new_v4()),
        job_id: JobId(Uuid::new_v4()),
        tenant: TenantId("t1".into()),
        scheduled_at: now - time::Duration::seconds(10),
        state: RunState::Pending,
        attempt,
    }
}

/// **The cap, exactly.** A run that keeps coming back is attempted precisely
/// `MAX_ATTEMPTS` times and then dies.
///
/// Counting the claims is what makes this catch the off-by-one in either
/// direction: `attempt <= MAX_ATTEMPTS` (i.e. burying on `>` rather than `>=`)
/// yields six attempts, and burying one step early yields four. An assertion
/// that merely checked "it eventually goes dead" would pass against both.
#[tokio::test]
async fn a_run_is_attempted_exactly_max_attempts_times_then_dies() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let run = seed_run_with_attempt(now, 0);
    let id = run.id;
    repo.insert_runs(&[run]).await.unwrap();

    let mut attempts = 0i32;
    // Bounded well above MAX_ATTEMPTS so a cap that never fires fails the
    // assertion below rather than looping forever.
    for _ in 0..(MAX_ATTEMPTS + 5) {
        let claimed = repo.claim_due(now, 10, "engine-1", 0).await.unwrap();
        if claimed.is_empty() {
            break;
        }
        attempts += 1;
        assert_eq!(claimed.len(), 1);
        assert_eq!(
            claimed[0].attempt, attempts,
            "the claim must count the attempt it is making"
        );
        // The publish failed, as it will every time for a genuinely poisoned
        // run: hand it straight back.
        repo.release(&[id]).await.unwrap();
    }

    assert_eq!(
        attempts, MAX_ATTEMPTS,
        "a run must be attempted exactly MAX_ATTEMPTS times, no more and no fewer"
    );

    let dead = repo.get(id).await.unwrap();
    assert_eq!(
        dead.state,
        RunState::Dead,
        "a run out of attempts must be terminal, not retried forever"
    );
    assert_eq!(
        dead.attempt, MAX_ATTEMPTS,
        "the attempt count must show why it died"
    );
}

/// The boundary, stated directly rather than inferred from a loop.
///
/// A run sitting at `MAX_ATTEMPTS - 1` has one attempt left and must be
/// claimed; the same run at `MAX_ATTEMPTS` has none and must be buried. Both
/// halves are needed: the first alone passes an implementation that never
/// buries, the second alone passes one that buries everything.
#[tokio::test]
async fn the_attempt_cap_is_exclusive_at_the_boundary() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let last_chance = seed_run_with_attempt(now, MAX_ATTEMPTS - 1);
    let exhausted = seed_run_with_attempt(now, MAX_ATTEMPTS);
    repo.insert_runs(&[last_chance.clone(), exhausted.clone()])
        .await
        .unwrap();

    let claimed = repo.claim_due(now, 10, "engine-1", 0).await.unwrap();

    assert_eq!(
        claimed.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![last_chance.id],
        "a run with one attempt left must be claimed; one with none must not"
    );
    assert_eq!(
        claimed[0].attempt, MAX_ATTEMPTS,
        "the last attempt still counts"
    );
    assert_eq!(
        repo.get(exhausted.id).await.unwrap().state,
        RunState::Dead,
        "a run at the cap must be buried rather than claimed"
    );
    assert_eq!(
        repo.get(exhausted.id).await.unwrap().attempt,
        MAX_ATTEMPTS,
        "burying must not alter the count that explains the death"
    );
}

/// `Dead` is terminal in the same sense `Succeeded` is: once buried, a run is
/// never handed to a worker again, no matter how many ticks pass.
#[tokio::test]
async fn a_dead_run_is_never_claimed_again() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let run = seed_run_with_attempt(now, MAX_ATTEMPTS);
    repo.insert_runs(&[run.clone()]).await.unwrap();

    // First claim buries it and returns nothing.
    assert!(
        repo.claim_due(now, 10, "engine-1", 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(repo.get(run.id).await.unwrap().state, RunState::Dead);

    for _ in 0..3 {
        assert!(
            repo.claim_due(now, 10, "engine-1", 0)
                .await
                .unwrap()
                .is_empty(),
            "a dead run must never be claimed again"
        );
    }
    // And a release cannot resurrect it either -- `release` is scoped to
    // `claimed`, but the cap would be meaningless if it were not.
    repo.release(&[run.id]).await.unwrap();
    assert_eq!(repo.get(run.id).await.unwrap().state, RunState::Dead);
}

/// Burying one run must not cost another its claim. The cap applies per run,
/// not per batch.
#[tokio::test]
async fn burying_an_exhausted_run_does_not_block_the_rest_of_the_batch() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let exhausted = seed_run_with_attempt(now, MAX_ATTEMPTS);
    let fresh: Vec<JobRun> = (0..3).map(|_| seed_run_with_attempt(now, 0)).collect();
    let mut all = vec![exhausted.clone()];
    all.extend(fresh.clone());
    repo.insert_runs(&all).await.unwrap();

    let claimed = repo.claim_due(now, 10, "engine-1", 0).await.unwrap();

    let got: HashSet<RunId> = claimed.iter().map(|r| r.id).collect();
    assert_eq!(
        got,
        fresh.iter().map(|r| r.id).collect::<HashSet<_>>(),
        "the three healthy runs must still be claimed"
    );
    assert_eq!(repo.get(exhausted.id).await.unwrap().state, RunState::Dead);
}

/// The reaper and the cap together: an engine that dies repeatedly must not
/// produce an infinite reclaim/re-claim loop.
///
/// This is why Task 2 follows Task 1. Reclaiming preserves `attempt`, so each
/// abandoned claim walks the run one step closer to the cap, and the cap is
/// where the cycle stops.
#[tokio::test]
async fn repeatedly_abandoned_runs_eventually_die_rather_than_looping() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let run = seed_run_with_attempt(now, 0);
    let id = run.id;
    repo.insert_runs(&[run]).await.unwrap();

    let mut cycles = 0i32;
    for _ in 0..(MAX_ATTEMPTS + 5) {
        let claimed = repo
            .claim_due(now, 10, "engine-that-dies", 0)
            .await
            .unwrap();
        if claimed.is_empty() {
            break;
        }
        cycles += 1;
        // The engine dies before publishing; only the lease expiry recovers it.
        expire_lease(&pool, id, now - time::Duration::seconds(60)).await;
        assert_eq!(repo.reclaim_expired(now, 10).await.unwrap(), vec![id]);
    }

    assert_eq!(
        cycles, MAX_ATTEMPTS,
        "the reclaim/re-claim cycle must terminate at the cap"
    );
    assert_eq!(repo.get(id).await.unwrap().state, RunState::Dead);
}

/// `Dead` is terminal for `complete` too.
///
/// `complete` excludes `('succeeded','failed','dead')`, and the existing
/// terminal tests only ever exercise `Succeeded`. Deleting `'dead'` from that
/// list would let a straggling worker report success on a run the scheduler
/// has already given up on -- resurrecting it into a terminal state it did not
/// earn, and doing so silently.
#[tokio::test]
async fn complete_does_not_overwrite_a_dead_run() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let run = seed_run_with_attempt(now, MAX_ATTEMPTS);
    repo.insert_runs(&[run.clone()]).await.unwrap();
    assert!(
        repo.claim_due(now, 10, "engine-1", 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(repo.get(run.id).await.unwrap().state, RunState::Dead);

    for outcome in [RunState::Succeeded, RunState::Failed, RunState::Dead] {
        assert!(
            !repo.complete(run.id, outcome).await.unwrap(),
            "completing a dead run with {outcome:?} must report no transition"
        );
        assert_eq!(
            repo.get(run.id).await.unwrap().state,
            RunState::Dead,
            "a dead run must stay dead"
        );
    }
}

/// `Dead` must survive the round trip through storage as itself. If
/// `state_from_str` or `state_str` mishandled it, the cap would be written and
/// then read back as something else -- and a `Dead` row misread as `Pending`
/// would be claimed again, which is the entire failure this prevents.
#[tokio::test]
async fn dead_round_trips_through_storage() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let run = seed_run_with_attempt(now, MAX_ATTEMPTS);
    repo.insert_runs(&[run.clone()]).await.unwrap();
    repo.claim_due(now, 10, "engine-1", 0).await.unwrap();

    let raw: String = sqlx::query_scalar("SELECT state FROM job_runs WHERE id = $1")
        .bind(run.id.0)
        .fetch_one(&*pool)
        .await
        .unwrap();
    assert_eq!(raw, "dead", "the persisted discriminant must be 'dead'");
    assert_eq!(repo.get(run.id).await.unwrap().state, RunState::Dead);
}

fn seed_runs_for_job(job_id: JobId, n: usize, now: OffsetDateTime) -> Vec<JobRun> {
    (0..n)
        .map(|i| JobRun {
            id: RunId(Uuid::new_v4()),
            job_id,
            tenant: TenantId("t1".into()),
            scheduled_at: now - time::Duration::seconds(i as i64),
            state: RunState::Pending,
            attempt: 0,
        })
        .collect()
}

#[tokio::test]
async fn runs_for_jobs_returns_runs_for_every_requested_job() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let ids: Vec<JobId> = (0..3).map(|_| JobId(Uuid::new_v4())).collect();
    for id in &ids {
        repo.insert_runs(&seed_runs_for_job(*id, 2, now))
            .await
            .unwrap();
    }

    let got = repo.runs_for_jobs(&ids, now, 10).await.unwrap();

    assert_eq!(got.len(), 6);
    let seen: HashSet<JobId> = got.iter().map(|r| r.job_id).collect();
    assert_eq!(seen, ids.iter().copied().collect::<HashSet<_>>());
}

/// The per-job limit is per *job*, not a cap on the result set.
///
/// Asserting only the total would pass an implementation using a plain
/// `LIMIT 4`, which could return four runs all belonging to one job and none
/// for the other -- so this asserts the per-job counts.
#[tokio::test]
async fn runs_for_jobs_honors_the_per_job_limit() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let a = JobId(Uuid::new_v4());
    let b = JobId(Uuid::new_v4());
    repo.insert_runs(&seed_runs_for_job(a, 5, now))
        .await
        .unwrap();
    repo.insert_runs(&seed_runs_for_job(b, 5, now))
        .await
        .unwrap();

    let got = repo.runs_for_jobs(&[a, b], now, 2).await.unwrap();

    assert_eq!(got.len(), 4, "2 per job across 2 jobs");
    assert_eq!(got.iter().filter(|r| r.job_id == a).count(), 2, "job a");
    assert_eq!(got.iter().filter(|r| r.job_id == b).count(), 2, "job b");
}

/// An empty slice must not build `IN ()`, which is a syntax error in Postgres.
#[tokio::test]
async fn runs_for_jobs_with_no_ids_returns_empty() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    assert!(
        repo.runs_for_jobs(&[], OffsetDateTime::now_utc(), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn runs_for_jobs_ignores_unknown_ids() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let known = JobId(Uuid::new_v4());
    let unknown = JobId(Uuid::new_v4());
    repo.insert_runs(&seed_runs_for_job(known, 2, now))
        .await
        .unwrap();

    let got = repo
        .runs_for_jobs(&[known, unknown], now, 10)
        .await
        .unwrap();

    assert_eq!(got.len(), 2);
    assert!(got.iter().all(|r| r.job_id == known));
}

/// The defect the `before` bound exists for, at the adapter.
///
/// The materializer writes runs `HORIZON_SECS` into the future, so for any
/// actively-scheduled job the newest rows are always ones that have not
/// happened yet. Ranking newest-first without an upper bound spends the whole
/// per-job window on them and a completed run can never surface -- observed on
/// a live stack as `Job.runs` returning 50 `pending` rows for a job with 82
/// runs already `succeeded`.
///
/// The limit (2) is deliberately smaller than the number of future runs (3):
/// that is what makes the unbounded query return *only* future rows rather
/// than a mix, and it is why filtering after ranking is not a fix either.
#[tokio::test]
async fn runs_for_jobs_excludes_runs_scheduled_after_the_bound() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let job = JobId(Uuid::new_v4());

    let past: Vec<JobRun> = (1..=3)
        .map(|i| JobRun {
            id: RunId(Uuid::new_v4()),
            job_id: job,
            tenant: TenantId("t1".into()),
            scheduled_at: now - time::Duration::seconds(i * 60),
            state: RunState::Pending,
            attempt: 0,
        })
        .collect();
    let future: Vec<JobRun> = (1..=3)
        .map(|i| JobRun {
            id: RunId(Uuid::new_v4()),
            job_id: job,
            tenant: TenantId("t1".into()),
            scheduled_at: now + time::Duration::seconds(i * 60),
            state: RunState::Pending,
            attempt: 0,
        })
        .collect();
    repo.insert_runs(&past).await.unwrap();
    repo.insert_runs(&future).await.unwrap();

    let got = repo.runs_for_jobs(&[job], now, 2).await.unwrap();

    assert_eq!(got.len(), 2, "the per-job limit still applies");
    let future_ids: HashSet<RunId> = future.iter().map(|r| r.id).collect();
    for r in &got {
        assert!(
            r.scheduled_at <= now,
            "run {:?} is scheduled at {}, after the bound {now}",
            r.id,
            r.scheduled_at
        );
        assert!(
            !future_ids.contains(&r.id),
            "a run the materializer wrote ahead of now must not appear"
        );
    }
    // Newest-first still holds, within the bound.
    assert_eq!(got[0].id, past[0].id, "newest past run first");
    assert_eq!(got[1].id, past[1].id);
}

/// The bound is a bound, not a hardcoded "now": a caller that wants the
/// upcoming schedule can still ask for it. This is what makes the port's
/// contract a choice the caller makes rather than a policy baked into the
/// adapter.
#[tokio::test]
async fn runs_for_jobs_with_a_future_bound_returns_upcoming_runs() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    let job = JobId(Uuid::new_v4());
    let upcoming = JobRun {
        id: RunId(Uuid::new_v4()),
        job_id: job,
        tenant: TenantId("t1".into()),
        scheduled_at: now + time::Duration::seconds(60),
        state: RunState::Pending,
        attempt: 0,
    };
    repo.insert_runs(&[upcoming.clone()]).await.unwrap();

    assert!(
        repo.runs_for_jobs(&[job], now, 10)
            .await
            .unwrap()
            .is_empty(),
        "bounded at now, an upcoming run is out of range"
    );

    let got = repo
        .runs_for_jobs(&[job], now + time::Duration::seconds(120), 10)
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].id, upcoming.id);
}

/// A tenant with a large backlog must not starve others: a positive
/// `per_tenant_cap` bounds any one tenant's share of a batch. Without the cap
/// (or with the rank filter defeated) the older "noisy" tenant's runs, being
/// oldest, would fill the whole `limit` and "quiet" would never be claimed.
#[tokio::test]
async fn per_tenant_cap_prevents_one_tenant_from_starving_others() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();

    // "noisy": 20 due runs, all OLDER than "quiet"'s 2 -- so oldest-first with no
    // cap would claim only noisy's.
    let mut runs = Vec::new();
    for i in 0..20 {
        runs.push(JobRun {
            id: RunId(Uuid::new_v4()),
            job_id: JobId(Uuid::new_v4()),
            tenant: TenantId("noisy".into()),
            scheduled_at: now - time::Duration::seconds(1000 - i),
            state: RunState::Pending,
            attempt: 0,
        });
    }
    for i in 0..2 {
        runs.push(JobRun {
            id: RunId(Uuid::new_v4()),
            job_id: JobId(Uuid::new_v4()),
            tenant: TenantId("quiet".into()),
            scheduled_at: now - time::Duration::seconds(10 - i),
            state: RunState::Pending,
            attempt: 0,
        });
    }
    repo.insert_runs(&runs).await.unwrap();

    // cap = 3, limit = 10: at most 3 of noisy's, and quiet must not be starved.
    let claimed = repo.claim_due(now, 10, "engine-1", 3).await.unwrap();

    let noisy = claimed
        .claimed
        .iter()
        .filter(|r| r.tenant.0 == "noisy")
        .count();
    let quiet = claimed
        .claimed
        .iter()
        .filter(|r| r.tenant.0 == "quiet")
        .count();
    assert!(noisy <= 3, "the cap must bound noisy's share, got {noisy}");
    assert_eq!(quiet, 2, "quiet's runs must not be starved, got {quiet}");
}

/// `claim_ids` claims exactly the given ids that are still pending and due, and
/// nothing else — ids not asked for stay pending, and an id that matches no row
/// (a stale hint) is silently dropped.
#[tokio::test]
async fn claim_ids_claims_only_the_given_pending_due_ids() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();
    let runs = seed_runs(5, now);
    repo.insert_runs(&runs).await.unwrap();

    // Ask for runs 0,1,2 plus one id that exists nowhere.
    let asked = vec![runs[0].id, runs[1].id, runs[2].id, RunId(Uuid::new_v4())];
    let outcome = repo.claim_ids(&asked, now, "engine-1", 0).await.unwrap();

    let got: HashSet<RunId> = outcome.claimed.iter().map(|r| r.id).collect();
    let want: HashSet<RunId> = [runs[0].id, runs[1].id, runs[2].id].into_iter().collect();
    assert_eq!(got, want, "only the asked-for pending ids are claimed");

    // The ids not asked for stay pending; the bogus id changed nothing.
    for r in &runs[3..] {
        assert_eq!(
            repo.get(r.id).await.unwrap().state,
            RunState::Pending,
            "an id not asked for must be untouched"
        );
    }
}

/// The hint-not-truth case at the adapter level: an id that is no longer pending
/// (already claimed) is silently skipped by `claim_ids`, never re-claimed.
#[tokio::test]
async fn claim_ids_skips_an_already_claimed_id() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };
    let now = OffsetDateTime::now_utc();
    let runs = seed_runs(2, now);
    repo.insert_runs(&runs).await.unwrap();

    // Claim the first via the scan path, so it is now `claimed`.
    let first = repo.claim_due(now, 1, "engine-1", 0).await.unwrap();
    assert_eq!(first.claimed.len(), 1);
    let already = first.claimed[0].id;

    // Ask claim_ids for both: the already-claimed one is dropped, the other claimed.
    let outcome = repo
        .claim_ids(&[runs[0].id, runs[1].id], now, "engine-2", 0)
        .await
        .unwrap();
    let got: HashSet<RunId> = outcome.claimed.iter().map(|r| r.id).collect();
    assert!(
        !got.contains(&already),
        "an already-claimed id must not be re-claimed"
    );
    assert_eq!(got.len(), 1, "exactly the one still-pending id is claimed");
}
