mod job_repo;
mod partition;
mod run_repo;
pub use job_repo::PgJobRepository;
pub use partition::{drop_partitions_before, ensure_partitions};
pub use run_repo::PgRunRepository;

use sqlx::PgPool;

/// Apply embedded migrations. Uses sqlx's migrator over the `migrations/` dir.
pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("./migrations").run(pool).await
}
