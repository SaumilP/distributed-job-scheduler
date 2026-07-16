//! Partition-specific behaviour of the `job_runs` table: the DEFAULT backstop,
//! the maintenance helpers, cross-partition queries, and that the due scan
//! actually prunes.
//!
//! The transparent-swap property — that every existing query behaves the same
//! against the partitioned table — is covered by `claim.rs` passing unmodified.
//! This file covers only what is new with partitioning.

use adapter_postgres::{PgRunRepository, drop_partitions_before, ensure_partitions};
use scheduler_domain::*;
use sqlx::Row;
use time::{Date, Month, OffsetDateTime};
use uuid::Uuid;

mod support;

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

fn day(year: i32, month: Month, day: u8) -> OffsetDateTime {
    Date::from_calendar_date(year, month, day)
        .unwrap()
        .midnight()
        .assume_utc()
}

async fn partition_exists(pool: &sqlx::PgPool, name: &str) -> bool {
    sqlx::query_scalar::<_, i32>("SELECT 1 FROM pg_class WHERE relname = $1 AND relkind = 'r'")
        .bind(name)
        .fetch_optional(pool)
        .await
        .unwrap()
        .is_some()
}

/// A run whose `scheduled_at` falls outside every range partition must still
/// insert and be readable — that is exactly what the DEFAULT partition is for.
/// Without it the insert would fail with "no partition of relation found".
#[tokio::test]
async fn out_of_range_insert_lands_in_default() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };

    // Year 2000: long before any seeded or maintained range partition.
    let run = run_at(day(2000, Month::January, 1));
    repo.insert_runs(std::slice::from_ref(&run)).await.unwrap();

    let got = repo.get(run.id).await.unwrap();
    assert_eq!(
        got.id, run.id,
        "the DEFAULT partition must accept out-of-range rows"
    );
}

/// `ensure_partitions` creates one partition per day in the window, and a second
/// call over the same window creates nothing — the maintenance job is safe to
/// run every tick.
#[tokio::test]
async fn ensure_partitions_is_idempotent() {
    let pool = support::pg_pool().await;

    // A far-future window that cannot collide with the migration's seeded week.
    let from = day(2035, Month::March, 1);
    let to = day(2035, Month::March, 8);

    let first = ensure_partitions(&pool, from, to).await.unwrap();
    assert_eq!(first, 7, "one partition per day in a 7-day window");

    let second = ensure_partitions(&pool, from, to).await.unwrap();
    assert_eq!(second, 0, "a second run must create nothing");

    assert!(partition_exists(&pool, "job_runs_20350301").await);
    assert!(partition_exists(&pool, "job_runs_20350307").await);
}

/// `drop_partitions_before` removes partitions fully in the past, taking their
/// rows with them, but must spare both `job_runs_default` and any partition not
/// yet fully past.
#[tokio::test]
async fn drop_partitions_before_spares_default_and_future() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };

    // Days 2020-01-01..2020-01-04 (four partitions), plus a far-future one.
    // Deliberately in 2020 — *before* the migration's seeded window around the
    // real "now" — so the cutoff below catches only these test partitions and
    // leaves the seeded ones (and thus the other tests) untouched.
    ensure_partitions(
        &pool,
        day(2020, Month::January, 1),
        day(2020, Month::January, 5),
    )
    .await
    .unwrap();
    ensure_partitions(
        &pool,
        day(2040, Month::January, 1),
        day(2040, Month::January, 2),
    )
    .await
    .unwrap();

    // A run in the 2020-01-02 partition, which the drop below will remove.
    let doomed = run_at(day(2020, Month::January, 2) + time::Duration::hours(3));
    repo.insert_runs(std::slice::from_ref(&doomed))
        .await
        .unwrap();

    // Cutoff at 2020-01-04 midnight: partitions 01-01, 01-02, 01-03 are fully
    // before it (their upper bounds are 01-02, 01-03, 01-04, all <= cutoff);
    // 01-04 (upper 01-05) is not, and the seeded 2026-ish partitions are all
    // after it.
    let dropped = drop_partitions_before(&pool, day(2020, Month::January, 4))
        .await
        .unwrap();
    assert_eq!(dropped, 3, "exactly the three fully-past test partitions");

    assert!(
        !partition_exists(&pool, "job_runs_20200102").await,
        "a fully-past partition must be dropped"
    );
    assert!(
        partition_exists(&pool, "job_runs_20200104").await,
        "a partition not yet fully past must survive"
    );
    assert!(
        partition_exists(&pool, "job_runs_20400101").await,
        "a future partition must survive"
    );
    assert!(
        partition_exists(&pool, "job_runs_default").await,
        "the DEFAULT partition must never be dropped"
    );

    // The run in the dropped partition is gone with it.
    assert!(
        matches!(repo.get(doomed.id).await, Err(DomainError::NotFound)),
        "rows in a dropped partition go with it"
    );
}

/// A claim whose due window spans two partitions returns runs from both — the
/// query is not confined to a single partition.
#[tokio::test]
async fn claim_spans_multiple_partitions() {
    let pool = support::pg_pool().await;
    let repo = PgRunRepository { pool: pool.clone() };

    let now = OffsetDateTime::now_utc();
    // One run today, one tomorrow — different daily partitions (both inside the
    // migration's seeded window around now).
    let today = run_at(now - time::Duration::minutes(5));
    let tomorrow = run_at(now + time::Duration::days(1));
    repo.insert_runs(&[today.clone(), tomorrow.clone()])
        .await
        .unwrap();

    // Claim as of two days out, so both are due.
    let outcome = repo
        .claim_due(now + time::Duration::days(2), 100, "engine-1", 0)
        .await
        .unwrap();
    let ids: std::collections::HashSet<RunId> = outcome.claimed.iter().map(|r| r.id).collect();
    assert!(ids.contains(&today.id) && ids.contains(&tomorrow.id));
    assert_eq!(
        outcome.claimed.len(),
        2,
        "the claim must span both partitions"
    );
}

/// The due scan prunes: a query bounded before every range partition reads only
/// the DEFAULT partition, so today's seeded partition — which an unbounded query
/// does read — is absent from the plan. Asserting both directions is what keeps
/// this from passing vacuously against a table where pruning does nothing.
#[tokio::test]
async fn due_scan_prunes_partitions() {
    let pool = support::pg_pool().await;

    let today = OffsetDateTime::now_utc().date();
    let today_part = format!(
        "job_runs_{:04}{:02}{:02}",
        today.year(),
        u8::from(today.month()),
        today.day()
    );

    let explain = |sql: &str| {
        let pool = &*pool;
        let sql = sql.to_string();
        async move {
            let rows = sqlx::query(&format!("EXPLAIN {sql}"))
                .fetch_all(pool)
                .await
                .unwrap();
            rows.iter()
                .map(|r| r.get::<String, _>(0))
                .collect::<Vec<_>>()
                .join("\n")
        }
    };

    // Unbounded on scheduled_at: every partition is a candidate, including
    // today's.
    let unpruned = explain("SELECT id FROM job_runs WHERE state = 'pending'").await;
    assert!(
        unpruned.contains(&today_part),
        "an unbounded scan should read today's partition; plan:\n{unpruned}"
    );

    // Bounded before every range partition: only DEFAULT remains, so today's is
    // pruned away.
    let pruned = explain(
        "SELECT id FROM job_runs WHERE state = 'pending' AND scheduled_at <= '2000-01-01'::timestamptz",
    )
    .await;
    assert!(
        !pruned.contains(&today_part),
        "a scan bounded before all range partitions must prune today's; plan:\n{pruned}"
    );
}
