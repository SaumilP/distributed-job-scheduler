//! Postgres fixture for the adapter tests.
//!
//! The container plumbing lives in the `test-support` crate -- including the
//! reasoning about why only the container is shared and never the pool, and
//! the known container-leak limitation. This module adds the two things that
//! are specific to these tests: the schema, and isolation between tests.

use adapter_postgres::run_migrations;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::ops::Deref;
use std::sync::OnceLock;
use tokio::sync::{Mutex, MutexGuard};

static MIGRATED: OnceLock<()> = OnceLock::new();

/// Serializes access to the single shared database.
///
/// Tests in a binary run concurrently by default, and every caller truncates
/// to get a clean slate -- so without this gate one test's TRUNCATE would wipe
/// another's seeded rows mid-run. Serializing trades intra-binary parallelism
/// for determinism; container startup, the actual cost this fixture removes,
/// is paid once either way.
static GATE: Mutex<()> = Mutex::const_new(());

/// A migrated [`PgPool`] with `job_runs` and `jobs` guaranteed empty for the
/// caller.
///
/// Derefs to `&PgPool`, so it works anywhere a `&PgPool` is expected and can
/// be cloned into `PgRunRepository { pool: pool.clone() }`. Keep the returned
/// value bound for the whole test (`let pool = support::pg_pool().await;`) --
/// dropping it early releases the gate before the test body finishes,
/// reopening the race this fixture exists to prevent.
pub struct PgFixture {
    _gate: MutexGuard<'static, ()>,
    pool: PgPool,
}

impl Deref for PgFixture {
    type Target = PgPool;

    fn deref(&self) -> &PgPool {
        &self.pool
    }
}

/// Returns a pool over an empty schema, exclusive to the caller until the
/// returned [`PgFixture`] is dropped.
///
/// `max_connections` is 10 because `claim_skips_rows_locked_by_another_transaction`
/// holds an uncommitted transaction on one connection while a concurrent
/// `claim_due` runs on another. A starved pool would make that test time out
/// for the wrong reason -- looking flaky instead of failing honestly.
pub async fn pg_pool() -> PgFixture {
    let gate = GATE.lock().await;
    let port = test_support::postgres_port();

    // Built on the caller's runtime -- see the `test_support` module docs.
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&test_support::postgres_url(port))
        .await
        .expect("failed to connect to shared postgres");

    if MIGRATED.get().is_none() {
        run_migrations(&pool).await.expect("migrations failed");
        let _ = MIGRATED.set(());
    }

    // Both tables, one statement. `jobs` must be included or the job tests see
    // each other's rows -- a failure that presents as flakiness rather than as
    // an obvious ordering bug.
    sqlx::query("TRUNCATE TABLE job_runs, jobs")
        .execute(&pool)
        .await
        .expect("failed to truncate job_runs, jobs");

    PgFixture { _gate: gate, pool }
}
