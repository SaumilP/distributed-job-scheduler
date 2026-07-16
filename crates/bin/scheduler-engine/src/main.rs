//! The engine composition root: three loops on one runtime.
//!
//! Materialization, dispatch and reaping are separate loops rather than one
//! combined pass because they fail independently. A NATS outage stops dispatch
//! but must not stop the schedule from being materialized — otherwise the
//! backlog that should be waiting in Postgres when the broker returns was never
//! created. The reaper is separate for a different reason: it runs on its own,
//! much slower interval, because it is recovery rather than the hot path.

use adapter_nats::{NatsEventPublisher, ensure_stream};
use adapter_postgres::{PgJobRepository, PgRunRepository};
use anyhow::Context;
use scheduler_application::{ClaimAndDispatch, InFlight};
use scheduler_common::{Config, SystemClock, init_tracing, install_shutdown, serve_metrics};
use scheduler_engine::drain::{DRAIN_BUDGET, drain_leases};
use scheduler_engine::loops::{
    REAPER_INTERVAL, REAPER_PAGE, materialize_tick, reaper_tick, run_until_shutdown,
};
use sqlx::postgres::PgPoolOptions;
use tracing::Instrument;

/// How many active jobs one materialization tick will look at.
///
/// Not the run batch size (`Config::batch`) -- this bounds the *job* read.
/// Kept separate because the two scale with different things: jobs with
/// tenants, runs with schedule frequency.
const JOB_PAGE: i64 = 500;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing("scheduler-engine");
    let cfg = Config::from_env()?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&cfg.database_url)
        .await
        .context("failed to connect to Postgres")?;

    let client = async_nats::connect(&cfg.nats_url)
        .await
        .context("failed to connect to NATS")?;
    let js = async_nats::jetstream::new(client);
    ensure_stream(&js)
        .await
        .map_err(|e| anyhow::anyhow!("failed to ensure RUNS stream: {e}"))?;

    // Note: migrations are owned by scheduler-api. The engine's queries fail
    // until the schema exists, and its loops tolerate that by design.
    let jobs = PgJobRepository { pool: pool.clone() };
    let runs = PgRunRepository { pool: pool.clone() };
    let publisher = NatsEventPublisher::new(js);

    // One registry per process, shared across all three loops (materialize,
    // dispatch, reaper). The adapter records with no lock and no allocation, so
    // sharing one `Arc` costs nothing on the hot path. This process's metrics
    // are exposed on its own admin `/metrics` listener (see `serve_metrics`);
    // nothing aggregates them with the worker's or the API's.
    let metrics = adapter_metrics::PrometheusMetrics::shared();

    tracing::info!(owner = %cfg.owner, "scheduler-engine starting");

    // Installed before spawning: a failure to arrange shutdown must stop the
    // process starting, not surface later inside a task.
    let materialize_stop = install_shutdown()?;
    let dispatch_stop = install_shutdown()?;
    let reaper_stop = install_shutdown()?;

    let materialize = {
        let stop = materialize_stop;
        let jobs = jobs.clone();
        let runs = runs.clone();
        let horizon = cfg.horizon_secs;
        let period = cfg.poll_interval;
        let metrics = metrics.clone();
        tokio::spawn(async move {
            run_until_shutdown(period, stop, "materialize", move || {
                let jobs = jobs.clone();
                let runs = runs.clone();
                let metrics = metrics.clone();
                async move {
                    materialize_tick(&jobs, runs, SystemClock, horizon, JOB_PAGE, metrics).await
                }
            })
            .await;
        })
    };

    // Shared with the drain below: the dispatch tick records here what it has
    // claimed and not yet published, so a SIGTERM that cancels the tick
    // mid-batch leaves something for shutdown to release.
    let in_flight = InFlight::new();

    let dispatch = {
        let stop = dispatch_stop;
        let runs = runs.clone();
        let publisher = publisher.clone();
        let owner = cfg.owner.clone();
        let batch = cfg.batch;
        let period = cfg.poll_interval;
        let in_flight = in_flight.clone();
        let metrics = metrics.clone();
        let per_tenant_cap = cfg.per_tenant_cap;
        tokio::spawn(async move {
            run_until_shutdown(period, stop, "dispatch", move || {
                let uc = ClaimAndDispatch {
                    runs: runs.clone(),
                    publisher: publisher.clone(),
                    clock: SystemClock,
                    batch,
                    owner: owner.clone(),
                    in_flight: in_flight.clone(),
                    metrics: metrics.clone(),
                    per_tenant_cap,
                };
                // Run the dispatch tick under a span so the publisher captures a
                // real trace context to inject into each run's NATS headers —
                // that context is what links dispatch to the worker's execution.
                async move { uc.run().instrument(tracing::info_span!("dispatch")).await }
            })
            .await;
        })
    };

    // The reaper: returns runs whose lease expired to `Pending`, so an engine
    // that died between claiming and publishing does not strand its batch
    // forever.
    //
    // `REAPER_INTERVAL` and not `cfg.poll_interval` -- this is recovery, and
    // sweeping at the dispatch rate would pay a lock-taking scan per second per
    // replica to be told nothing expired. See the constant for the full
    // argument.
    let reaper = {
        let stop = reaper_stop;
        let runs = runs.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            run_until_shutdown(REAPER_INTERVAL, stop, "reaper", move || {
                let runs = runs.clone();
                let metrics = metrics.clone();
                async move { reaper_tick(&runs, &SystemClock, REAPER_PAGE, metrics.as_ref()).await }
            })
            .await;
        })
    };

    // The admin metrics listener, deliberately outside the `try_join!` below.
    // It is best-effort: a failure to bind `metrics_addr`, or the listener
    // dying, must not stop scheduling — losing observability is worse than
    // losing the schedule only if you value the dashboard over the work. It
    // gets its own shutdown signal, so SIGTERM stops it alongside the loops.
    {
        let addr = cfg.metrics_addr.clone();
        let stop = install_shutdown()?;
        let render: std::sync::Arc<dyn Fn() -> String + Send + Sync> = {
            let metrics = metrics.clone();
            std::sync::Arc::new(move || metrics.render())
        };
        tokio::spawn(async move {
            if let Err(e) = serve_metrics(&addr, render, stop).await {
                tracing::error!(error = %e, "metrics listener failed; continuing without it");
            }
        });
    }

    // `try_join!`, not `join!`. With `join!`, a task that dies leaves main
    // waiting on its still-running siblings forever: the process stays up,
    // never exits, and `restart: on-failure` never fires because there is no
    // failure -- scheduling stops silently while the container reports
    // healthy. `try_join!` returns as soon as any handle resolves to an
    // error, so the process exits non-zero and gets restarted.
    //
    // The third loop joins exactly the same way, deliberately. Adding it to a
    // `join!` would reintroduce that defect with a wider surface: three ways to
    // hang instead of two.
    let joined = tokio::try_join!(materialize, dispatch, reaper);

    // The drain runs only on the clean path, and the asymmetry is deliberate.
    //
    // `try_join!` returns the *instant* any handle fails, while its siblings are
    // still running -- so on the error path the dispatch loop may well be
    // mid-batch. Releasing then would hand a run back to `Pending` while it is
    // being published, which is a duplicate execution rather than a repair:
    // exactly the failure the reaper is written to avoid causing. On a clean
    // shutdown every loop has already stopped, so whatever is still registered
    // is genuinely abandoned and safe to release.
    //
    // Giving up on the error path costs at most `LEASE_SECS` of recovery
    // latency, which is what the reaper is for.
    match joined {
        Ok(_) => {
            drain_leases(&runs, &in_flight, DRAIN_BUDGET).await;
        }
        Err(_) => {
            tracing::warn!(
                held = in_flight.len(),
                "skipping the lease drain: a loop terminated unexpectedly and its \
                 siblings may still be publishing, so releasing now could duplicate \
                 work in flight. The reaper reclaims these after the lease expires."
            );
        }
    }

    joined.context("an engine loop terminated unexpectedly")?;

    tracing::info!("scheduler-engine stopped");
    Ok(())
}
