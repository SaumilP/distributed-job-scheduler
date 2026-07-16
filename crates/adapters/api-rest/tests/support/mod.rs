//! Test state for the API routes.
//!
//! Shares one migrated Postgres container across this binary (see
//! `test_support`), serializing access and truncating between tests so the
//! handlers see a clean database.

use adapter_postgres::run_migrations;
use api_rest::routes::AppState;
use sqlx::postgres::PgPoolOptions;
use std::sync::OnceLock;
use tokio::sync::{Mutex, MutexGuard};

static MIGRATED: OnceLock<()> = OnceLock::new();
static GATE: Mutex<()> = Mutex::const_new(());

/// A migrated, empty database wired into an `AppState`, held exclusively until
/// the returned fixture is dropped.
///
/// An earlier version released the gate as soon as setup finished, on the
/// reasoning that each test only reads rows it created by id. That was wrong:
/// another test's `TRUNCATE` lands between this test's POST and its GET and
/// deletes the row, so `post_jobs_creates_a_job_and_returns_its_id` failed in
/// the full suite while passing in isolation. Serializing setup is not enough
/// when the shared state is the whole table -- the gate has to cover the test
/// body.
///
/// Keep it bound: `let state = support::state().await;`.
pub struct ApiFixture {
    _gate: MutexGuard<'static, ()>,
    state: AppState,
}

impl std::ops::Deref for ApiFixture {
    type Target = AppState;

    fn deref(&self) -> &AppState {
        &self.state
    }
}

pub async fn state() -> ApiFixture {
    let gate = GATE.lock().await;
    let port = test_support::postgres_port();

    // Built on the caller's runtime -- tokio TcpStreams bind to the I/O driver
    // of the runtime that created them.
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&test_support::postgres_url(port))
        .await
        .expect("failed to connect to shared postgres");

    if MIGRATED.get().is_none() {
        run_migrations(&pool).await.expect("migrations failed");
        let _ = MIGRATED.set(());
    }

    sqlx::query("TRUNCATE TABLE job_runs, jobs")
        .execute(&pool)
        .await
        .expect("failed to truncate");

    ApiFixture {
        _gate: gate,
        state: AppState::new(pool),
    }
}

/// State whose pool points at a port with nothing listening.
///
/// Used to assert that liveness does not depend on the database. Built lazily
/// (`connect_lazy`) so construction itself does not fail -- the failure has to
/// happen at query time, which is exactly what `/ready` exercises and
/// `/health` must not.
pub fn state_with_unreachable_database() -> AppState {
    let pool = PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(250))
        .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/postgres")
        .expect("lazy pool construction must not fail");
    AppState::new(pool)
}
