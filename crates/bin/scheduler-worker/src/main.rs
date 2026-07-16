//! The worker composition root: pull one run, handle it, repeat.

use adapter_nats::NatsRunConsumer;
use adapter_postgres::PgRunRepository;
use anyhow::Context;
use scheduler_common::{Config, init_tracing, install_shutdown, serve_metrics};
use scheduler_worker::handler::{LoggingExecutor, handle};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// How long a poll waits for work before coming back empty.
///
/// Long enough that an idle worker is not hammering the broker, short enough
/// that shutdown is not delayed by more than this. It bounds rollout time, so
/// it is deliberately well under a typical Kubernetes grace period.
const POLL_WAIT: Duration = Duration::from_secs(5);

/// Durable consumer name, shared by every replica.
///
/// Shared on purpose: replicas with the same durable name form one queue
/// group, so a run goes to exactly one of them and replicas can come and go
/// without losing position. A per-replica name would deliver every run to
/// every worker.
const CONSUMER_NAME: &str = "scheduler-worker";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing("scheduler-worker");
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

    let consumer = NatsRunConsumer::connect(js, CONSUMER_NAME).await.context(
        "failed to bind the RUNS consumer (is the engine running? it creates the stream)",
    )?;

    let runs = PgRunRepository { pool };
    let executor = LoggingExecutor;

    // This process's own registry: the worker records `execution_seconds`, the
    // histogram that tells the operator whether real jobs run well inside
    // `LEASE_SECS`. Exposed on the worker's own admin `/metrics` listener.
    let metrics = adapter_metrics::PrometheusMetrics::shared();

    tracing::info!(owner = %cfg.owner, "scheduler-worker starting");

    // Best-effort admin metrics listener, on its own shutdown signal. See the
    // engine's for the reasoning; the worker's registry carries
    // `execution_seconds`, which is the one only it can report.
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

    let mut shutdown = std::pin::pin!(install_shutdown()?);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("shutdown signalled, stopping worker");
                break;
            }
            next = consumer.next_run(POLL_WAIT) => {
                match next {
                    Ok(None) => {}
                    Ok(Some(msg)) => {
                        // Continue the trace the dispatcher started: parent the
                        // execution span to the context carried in the run's NATS
                        // headers, so a run's dispatch and execution appear under
                        // one distributed trace rather than two unrelated ones.
                        let span = tracing::info_span!("execute_run", run_id = ?msg.run().id);
                        span.set_parent(msg.trace_context());
                        // Errors are logged and swallowed for the same reason
                        // the engine's loops swallow theirs: a dependency blip
                        // must not kill the process. An unacked run redelivers.
                        if let Err(e) = handle(msg, &runs, &executor, metrics.as_ref())
                            .instrument(span)
                            .await
                        {
                            tracing::warn!(error = %e, "failed to handle run, leaving it unacked");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "poll failed, retrying");
                        // Back off so a persistent failure (a malformed payload
                        // redelivering, say) does not become a hot loop.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }

    tracing::info!("scheduler-worker stopped");
    Ok(())
}
