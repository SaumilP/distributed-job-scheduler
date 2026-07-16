//! The batch loader that keeps nested `runs` from becoming N+1.
//!
//! `{ jobs { runs { id } } }` resolves the `runs` field once per job. Without
//! batching that is one database round trip per job in the result set, and a
//! client can ask for a lot of jobs in one request — the fan-out is GraphQL's
//! characteristic performance failure and the one real cost it adds over REST
//! and gRPC, both of which make the caller ask for each thing explicitly.
//!
//! `DataLoader` collects the ids requested within a resolution pass and calls
//! [`Loader::load`] once. This module is thin on purpose: the batching is the
//! interesting part, and it lives in `RunRepository::runs_for_jobs`.

use async_graphql::dataloader::Loader;
use scheduler_domain::{Clock, JobId, JobRun, RunRepository};
use std::collections::HashMap;
use std::sync::Arc;

/// Batches run lookups for a `RunRepository`.
///
/// Generic over the repository rather than bound to Postgres: the port's
/// methods are RPITIT and therefore not object-safe, so this cannot be
/// `Box<dyn>` — and keeping it generic is also what lets the batching test
/// substitute a counting fake and assert the call count directly.
///
/// Generic over the `Clock` for the same reason, plus one of its own: the
/// upper time bound this loader supplies has to be read *per load*, not
/// captured when the schema is built. A schema is built once at process start
/// and then serves requests for as long as the process lives; a timestamp
/// captured there would be stale within seconds and frozen forever after.
pub struct RunsLoader<R: RunRepository, C: Clock> {
    runs: R,
    clock: C,
    /// Per-job cap handed to the repository. Bounds the fan-out even when a
    /// client asks for many jobs at once.
    limit_per_job: i64,
}

impl<R: RunRepository, C: Clock> RunsLoader<R, C> {
    pub fn new(runs: R, clock: C, limit_per_job: i64) -> Self {
        Self {
            runs,
            clock,
            limit_per_job,
        }
    }
}

impl<R: RunRepository, C: Clock> Loader<JobId> for RunsLoader<R, C> {
    type Value = Vec<JobRun>;
    type Error = Arc<async_graphql::Error>;

    async fn load(&self, keys: &[JobId]) -> Result<HashMap<JobId, Self::Value>, Self::Error> {
        // `now`, so nested `Job.runs` means execution history.
        //
        // The materializer writes `Pending` runs a horizon into the future, so
        // without this bound the newest-first window fills entirely with runs
        // that have not happened yet: a job with 82 completed runs showed 50
        // rows, all `pending`, the oldest of them ahead of the wall clock.
        // Read here rather than at construction because the schema outlives
        // any one instant.
        let before = self.clock.now();
        let rows = self
            .runs
            .runs_for_jobs(keys, before, self.limit_per_job)
            .await
            .map_err(|e| {
                // Logged, not returned: a storage error string carries
                // connection details and table names, and GraphQL errors go
                // straight to the client.
                tracing::error!(error = %e, "batch run lookup failed");
                Arc::new(async_graphql::Error::new("internal error"))
            })?;

        // Group the flat result. The port returns a `Vec` rather than a map so
        // it does not carry one adapter's preferred shape; this is that
        // adapter doing its own grouping.
        let mut out: HashMap<JobId, Vec<JobRun>> = HashMap::with_capacity(keys.len());
        for run in rows {
            out.entry(run.job_id).or_default().push(run);
        }
        Ok(out)
    }
}
