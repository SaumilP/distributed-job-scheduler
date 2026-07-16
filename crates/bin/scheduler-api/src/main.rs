//! The API composition root: three driving adapters on two listeners.
//!
//! `main` owns configuration, connection setup, migrations and the listeners,
//! and nothing else. REST and GraphQL share the axum server on `http_addr`;
//! gRPC gets its own listener on `grpc_addr` because it needs HTTP/2 with its
//! own service stack rather than an axum route.
//!
//! No adapter is constructed here beyond wiring: the routers, the schema and
//! the gRPC service are all built by their own crates, which is what lets each
//! be tested without binding a port.

use anyhow::Context;
use api_graphql::GraphQL;
use api_grpc::SchedulerService;
use api_grpc::pb::scheduler_server::SchedulerServer;
use api_rest::routes::{self, AppState};
use axum::response::Html;
use axum::routing::get;
use scheduler_common::{Config, SystemClock, init_tracing, install_shutdown, serve_metrics};
use sqlx::postgres::PgPoolOptions;
use std::net::SocketAddr;

/// How many runs one batched `Job.runs` lookup returns per job.
///
/// The GraphQL field has its own caller-supplied limit; this is the ceiling
/// the loader reads with. Unbounded here would defeat the point of taking a
/// `limit_per_job` in the port at all -- a client asking for 200 jobs would
/// pull every run of every one of them.
const RUNS_PER_JOB: i64 = 50;

/// The playground, served on `GET /graphql`.
async fn graphiql() -> Html<String> {
    Html(api_graphql::playground_html("/graphql"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing("scheduler-api");
    let cfg = Config::from_env()?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&cfg.database_url)
        .await
        .context("failed to connect to Postgres")?;

    // The API is the single migrator. Three roles racing the same migration
    // against a fresh database is a real, reproducible failure, and one
    // migrator is simpler than a separate one-shot service for a demo of this
    // size. The engine and worker wait for the schema by failing their first
    // queries and retrying, which their loops already tolerate.
    adapter_postgres::run_migrations(&pool)
        .await
        .context("migrations failed")?;

    // Parsed here rather than inside the server future: an unparseable address
    // is a configuration mistake, and it should stop the process before it has
    // connected to anything, not after it is half-serving.
    let grpc_addr: SocketAddr = cfg.grpc_addr.parse().with_context(|| {
        format!(
            "GRPC_ADDR must be an ip:port address, got {:?}",
            cfg.grpc_addr
        )
    })?;

    let state = AppState::new(pool.clone());
    let jobs = state.jobs.clone();
    let runs = state.runs.clone();

    // GraphQL is merged into the REST router rather than served separately:
    // one HTTP port is one thing to expose, map and probe. The playground is
    // on GET and the queries on POST at the same path, which is what every
    // GraphQL client already expects.
    let schema = api_graphql::build(jobs.clone(), runs.clone(), SystemClock, RUNS_PER_JOB);
    let http =
        routes::app(state).route("/graphql", get(graphiql).post_service(GraphQL::new(schema)));

    let listener = tokio::net::TcpListener::bind(&cfg.http_addr)
        .await
        .with_context(|| format!("failed to bind {}", cfg.http_addr))?;

    // Both installed at startup, before either server is running.
    // `install_shutdown` is fallible on purpose: a service that cannot arrange
    // its own shutdown must refuse to start rather than discover that fact
    // during a rollout, when the alternative to a graceful stop is SIGKILL.
    // Installing per server rather than sharing one future is what makes
    // *both* of them stop -- a single future can only be awaited once.
    let http_stop = install_shutdown()?;
    let grpc_stop = install_shutdown()?;

    // The API exposes `/metrics` on the same admin port as the other roles, for
    // a uniform scrape config. Its own registry is currently empty — the API
    // does no claiming, materializing or executing — so the scrape shows every
    // metric at zero. That is honest ("this process did none of these"), and it
    // means the endpoint is already here for when the API grows request metrics.
    {
        let addr = cfg.metrics_addr.clone();
        let stop = install_shutdown()?;
        let metrics = adapter_metrics::PrometheusMetrics::shared();
        let render: std::sync::Arc<dyn Fn() -> String + Send + Sync> =
            std::sync::Arc::new(move || metrics.render());
        tokio::spawn(async move {
            if let Err(e) = serve_metrics(&addr, render, stop).await {
                tracing::error!(error = %e, "metrics listener failed; continuing without it");
            }
        });
    }

    tracing::info!(http = %cfg.http_addr, grpc = %cfg.grpc_addr, "scheduler-api listening");

    let http_server = async move {
        axum::serve(listener, http)
            .with_graceful_shutdown(http_stop)
            .await
            .context("http server error")
    };

    let grpc_server = async move {
        tonic::transport::Server::builder()
            .add_service(SchedulerServer::new(SchedulerService::new(jobs, runs)))
            .serve_with_shutdown(grpc_addr, grpc_stop)
            .await
            .context("grpc server error")
    };

    // `serve_both` is `try_join!`, not `join!` -- see its doc comment for why,
    // and its tests for the property that pins it. It lives in the library
    // rather than here because `main` binds sockets and cannot be driven from
    // a test, while the failure semantics can be.
    scheduler_api::serve_both(http_server, grpc_server).await?;

    tracing::info!("scheduler-api stopped");
    Ok(())
}
