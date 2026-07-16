//! What every role binary needs: configuration, tracing, a clock, and a
//! shutdown signal.
//!
//! This crate exists so those four things are defined once. Three binaries
//! that each parse their own environment is how the three drift apart, and
//! the drift shows up as a deployment where the engine and the worker
//! disagree about which database they are talking to.

use anyhow::{Context, Result, bail};
use scheduler_domain::Clock;
use std::time::Duration;
use time::OffsetDateTime;

/// Real wall clock. Tests inject `FixedClock` instead; nothing in the
/// application layer reads the system clock directly.
#[derive(Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub database_url: String,
    pub nats_url: String,
    pub http_addr: String,
    pub grpc_addr: String,
    /// Where this process serves its Prometheus `/metrics`. A dedicated admin
    /// listener, **not** the public `http_addr`: every role has metrics but only
    /// the API has a public HTTP surface, and `/metrics` discloses tenant counts
    /// and traffic shape, which has no place on an unauthenticated public port.
    pub metrics_addr: String,
    pub batch: i64,
    pub poll_interval: Duration,
    pub horizon_secs: i64,
    pub owner: String,
    /// Per-tenant fairness cap for the claim (`PER_TENANT_CAP`). `0` — the
    /// default — is no cap, the historical behaviour. A positive value bounds
    /// how many runs one tenant contributes to a claim batch. See
    /// [`scheduler_domain::RunRepository::claim_due`].
    pub per_tenant_cap: i64,
}

/// Looks a setting up by name. Injected so the parser is a pure function:
/// process environment is global mutable state, and tests that set it cannot
/// run concurrently without interfering.
pub type Lookup<'a> = dyn Fn(&str) -> Option<String> + 'a;

impl Config {
    pub fn from_env() -> Result<Config> {
        // An empty value reads as absent. `FOO=""` is what a compose file or a
        // shell writes when a variable is unset upstream, and `var().ok()`
        // returns `Some("")` for it -- which sailed past every check here:
        // `DATABASE_URL=""` passed the required-check and failed later at
        // connect with a confusing message, and `SCHEDULER_OWNER=""` defeated
        // the hostname fallback so every replica shared the owner `""`.
        Config::from_lookup(&|k| std::env::var(k).ok().filter(|v| !v.trim().is_empty()))
    }

    pub fn from_lookup(get: &Lookup<'_>) -> Result<Config> {
        // Required, with no default. A default database URL is how a process
        // silently connects to the wrong database and appears to work.
        let database_url = get("DATABASE_URL").context(
            "DATABASE_URL is required (no default: a wrong default is worse than a crash)",
        )?;
        let nats_url = get("NATS_URL").context("NATS_URL is required")?;

        let http_addr = get("HTTP_ADDR").unwrap_or_else(|| "0.0.0.0:8080".to_string());

        // gRPC gets its own listener rather than sharing `http_addr`: it needs
        // HTTP/2 with its own service stack, which axum's router does not
        // provide. Read through the same `get`, so `GRPC_ADDR=""` -- what a
        // compose file writes for a variable that is unset upstream -- reads as
        // absent and takes the default, rather than being bound as an empty
        // address and failing later with a confusing message.
        let grpc_addr = get("GRPC_ADDR").unwrap_or_else(|| "0.0.0.0:50051".to_string());

        // Its own port, defaulted like the others and read through the same
        // `get` so `METRICS_ADDR=""` reads as absent. Same in every role: each
        // process serves only its own registry, so the three containers each
        // bind 9090 internally and the compose/K8s layer maps them apart.
        let metrics_addr = get("METRICS_ADDR").unwrap_or_else(|| "0.0.0.0:9090".to_string());

        // Parsed, not defaulted-on-error. Falling back on a malformed value
        // means a production tuning change silently does nothing.
        let batch = parse_or(get, "BATCH_SIZE", 100i64)?;
        let horizon_secs = parse_or(get, "HORIZON_SECS", 300i64)?;
        // Default 0 = no cap, so the reference behaviour is unchanged unless a
        // deployment opts in. Negative is treated as 0 (no cap) downstream.
        let per_tenant_cap = parse_or(get, "PER_TENANT_CAP", 0i64)?;
        let poll_ms = parse_or(get, "POLL_INTERVAL_MS", 1000u64)?;
        if poll_ms == 0 {
            bail!("POLL_INTERVAL_MS must be positive; 0 would busy-loop the engine");
        }
        if batch <= 0 {
            bail!("BATCH_SIZE must be positive, got {batch}");
        }
        if horizon_secs <= 0 {
            bail!("HORIZON_SECS must be positive, got {horizon_secs}");
        }

        // Two engine replicas sharing an owner string make lease attribution
        // meaningless -- the reaper could not tell whose lease expired.
        // Hostname is stable across restarts of the same pod; the UUID fallback
        // is unique but changes on restart, which is the safer failure.
        let owner = get("SCHEDULER_OWNER")
            .or_else(|| get("HOSTNAME"))
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        Ok(Config {
            database_url,
            nats_url,
            http_addr,
            grpc_addr,
            metrics_addr,
            batch,
            poll_interval: Duration::from_millis(poll_ms),
            horizon_secs,
            owner,
            per_tenant_cap,
        })
    }
}

fn parse_or<T>(get: &Lookup<'_>, key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    match get(key) {
        None => Ok(default),
        Some(raw) => raw
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("{key} is not a valid value: {raw:?} ({e})")),
    }
}

/// Initializes logging and tracing.
///
/// Always installs the W3C trace-context propagator, so a run's trace id
/// crosses the NATS boundary from dispatch to execution whether or not an
/// exporter is configured — the propagation is free and the adapters rely on it
/// being set. When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, spans are also exported
/// to that OTLP/HTTP collector; otherwise tracing goes only to the log.
///
/// `service` names this role in the exported traces (`scheduler-api`, etc.).
pub fn init_tracing(service: &str) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{EnvFilter, fmt};

    // The propagator is global and cheap; set it unconditionally so the
    // adapter-nats inject/extract are real rather than no-ops.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = fmt::layer();

    // `try_init`, ignoring the error: a second init in the same process (tests)
    // is harmless and must not abort the binary.
    match std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        Ok(endpoint) if !endpoint.trim().is_empty() => {
            match build_otlp_tracer(service, &endpoint) {
                Ok(tracer) => {
                    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
                    let _ = tracing_subscriber::registry()
                        .with(filter)
                        .with(fmt_layer)
                        .with(otel_layer)
                        .try_init();
                }
                Err(e) => {
                    let _ = tracing_subscriber::registry()
                        .with(filter)
                        .with(fmt_layer)
                        .try_init();
                    tracing::warn!(error = %e, "OTLP exporter setup failed; tracing to logs only");
                }
            }
        }
        _ => {
            let _ = tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer)
                .try_init();
        }
    }
}

/// Builds an OTLP/HTTP tracer for `endpoint`. Kept in the global tracer provider
/// so its batch exporter's background task outlives this function.
fn build_otlp_tracer(service: &str, endpoint: &str) -> Result<opentelemetry_sdk::trace::Tracer> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
        .context("building the OTLP span exporter")?;

    let resource = opentelemetry_sdk::Resource::new(vec![opentelemetry::KeyValue::new(
        "service.name",
        service.to_string(),
    )]);

    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer(service.to_string());
    opentelemetry::global::set_tracer_provider(provider);
    Ok(tracer)
}

/// Installs the signal handlers and returns a future that completes on SIGINT
/// or SIGTERM.
///
/// SIGTERM is the one that matters: it is what Kubernetes sends, and a binary
/// that only handles SIGINT gets SIGKILLed once the grace period expires. For
/// the worker that means being killed mid-execution with a message unacked --
/// recoverable, but it turns every routine rollout into a redelivery.
///
/// **Installation failure is fatal, deliberately.** This used to log and
/// return, i.e. report "shut down now": the process would immediately stop
/// serving, `main` would return `Ok(())`, the exit code would be 0, and
/// `restart: on-failure` would not restart it. A service that cannot arrange
/// its own shutdown must refuse to start, not shut down.
pub fn install_shutdown() -> Result<impl std::future::Future<Output = ()>> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term =
            signal(SignalKind::terminate()).context("failed to install the SIGTERM handler")?;
        Ok(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        })
    }
    #[cfg(not(unix))]
    {
        Ok(async {
            let _ = tokio::signal::ctrl_c().await;
        })
    }
}

/// Serves the Prometheus exposition format on `GET /metrics` at `addr` until
/// `shutdown` completes.
///
/// `render` is called once per scrape and returns the whole registry as text;
/// it is a closure rather than a concrete registry type so this crate stays
/// unaware of the metrics adapter (which is deliberately dependency-free and
/// must not grow an HTTP stack). Every role wires its own
/// `PrometheusMetrics::render` in here.
///
/// A scrape-time `render` call, not a cached string: Prometheus wants the
/// values as of the scrape, and the registry's render is lock-free, so there is
/// nothing to gain from caching and staleness to lose.
///
/// **Only `/metrics`.** No index, no other routes: this is an admin listener,
/// and anything else it served would be surface it does not need. Unknown paths
/// get axum's default 404.
pub async fn serve_metrics(
    addr: &str,
    render: std::sync::Arc<dyn Fn() -> String + Send + Sync>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind the metrics listener on {addr}"))?;
    tracing::info!(%addr, "serving Prometheus metrics on /metrics");
    axum::serve(listener, metrics_router(render))
        .with_graceful_shutdown(shutdown)
        .await
        .context("the metrics server exited with an error")?;
    Ok(())
}

/// The `/metrics`-only router. Split out from [`serve_metrics`] so the routing
/// and rendering can be tested with a request rather than a bound socket.
fn metrics_router(render: std::sync::Arc<dyn Fn() -> String + Send + Sync>) -> axum::Router {
    use axum::extract::State;
    use axum::http::header;
    use axum::response::IntoResponse;
    use axum::routing::get;

    async fn handler(
        State(render): State<std::sync::Arc<dyn Fn() -> String + Send + Sync>>,
    ) -> impl IntoResponse {
        // The exposition format's content type. Prometheus is lenient about it,
        // but emitting the right one keeps a generic scraper or `curl` honest.
        (
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            render(),
        )
    }

    axum::Router::new()
        .route("/metrics", get(handler))
        .with_state(render)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let owned: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k: &str| {
            owned
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.clone())
        }
    }

    const MINIMAL: &[(&str, &str)] = &[
        ("DATABASE_URL", "postgres://x"),
        ("NATS_URL", "nats://x:4222"),
    ];

    #[test]
    fn missing_database_url_is_an_error() {
        let cfg = Config::from_lookup(&lookup(&[("NATS_URL", "nats://x:4222")]));
        assert!(cfg.is_err(), "DATABASE_URL must be required, not defaulted");
    }

    #[test]
    fn missing_nats_url_is_an_error() {
        let cfg = Config::from_lookup(&lookup(&[("DATABASE_URL", "postgres://x")]));
        assert!(cfg.is_err());
    }

    #[test]
    fn defaults_apply_to_optional_settings() {
        let cfg = Config::from_lookup(&lookup(MINIMAL)).unwrap();
        assert_eq!(cfg.batch, 100);
        assert!(cfg.poll_interval >= Duration::from_millis(100));
        assert_eq!(cfg.horizon_secs, 300);
    }

    /// A typo'd tuning value must fail at startup, not silently fall back to
    /// the default and leave someone believing they changed something.
    #[test]
    fn invalid_numeric_setting_is_an_error_not_a_silent_default() {
        let mut pairs = MINIMAL.to_vec();
        pairs.push(("BATCH_SIZE", "not-a-number"));
        let cfg = Config::from_lookup(&lookup(&pairs));
        assert!(
            cfg.is_err(),
            "a malformed setting must fail loudly at startup"
        );
    }

    #[test]
    fn non_positive_tunables_are_rejected() {
        for (key, value) in [
            ("BATCH_SIZE", "0"),
            ("HORIZON_SECS", "0"),
            ("POLL_INTERVAL_MS", "0"),
        ] {
            let mut pairs = MINIMAL.to_vec();
            pairs.push((key, value));
            assert!(
                Config::from_lookup(&lookup(&pairs)).is_err(),
                "{key}={value} must be rejected"
            );
        }
    }

    /// Distinct replicas must not share an owner string.
    #[test]
    fn owner_falls_back_to_a_unique_value() {
        let a = Config::from_lookup(&lookup(MINIMAL)).unwrap().owner;
        let b = Config::from_lookup(&lookup(MINIMAL)).unwrap().owner;
        // With no SCHEDULER_OWNER and no HOSTNAME in this lookup, both fall
        // back to a fresh UUID -- unique per process, never shared.
        assert_ne!(a, b, "the fallback owner must not collide across replicas");
    }

    /// Pins the exact defaults. Asserting a range (`>= 100ms`) let a 60x
    /// regression in scheduling latency pass unnoticed, and the bind address
    /// had no assertion at all -- a default of `127.0.0.1` would bind loopback
    /// inside the container and make the compose port mapping serve nothing.
    #[test]
    fn defaults_are_exact() {
        let cfg = Config::from_lookup(&lookup(MINIMAL)).unwrap();
        assert_eq!(cfg.poll_interval, Duration::from_millis(1000));
        assert_eq!(cfg.http_addr, "0.0.0.0:8080");
        // Same reasoning as http_addr, and the same trap: gRPC is the surface
        // the e2e test and any external client reach through the compose port
        // mapping, and a 127.0.0.1 default would bind loopback inside the
        // container and serve nothing through it.
        assert_eq!(cfg.grpc_addr, "0.0.0.0:50051");
        assert_eq!(cfg.batch, 100);
        assert_eq!(cfg.horizon_secs, 300);
    }

    /// The defaults are overridable. Asserting only the default would pass an
    /// implementation that ignored the variable entirely.
    #[test]
    fn bind_addresses_are_overridable() {
        let mut pairs = MINIMAL.to_vec();
        pairs.push(("HTTP_ADDR", "127.0.0.1:9090"));
        pairs.push(("GRPC_ADDR", "127.0.0.1:59051"));
        let cfg = Config::from_lookup(&lookup(&pairs)).unwrap();
        assert_eq!(cfg.http_addr, "127.0.0.1:9090");
        assert_eq!(cfg.grpc_addr, "127.0.0.1:59051");
    }

    /// The path compose actually relies on for per-replica owner uniqueness.
    /// The previous test set HOSTNAME *and* SCHEDULER_OWNER, so deleting the
    /// hostname fallback entirely still passed.
    #[test]
    fn owner_falls_back_to_hostname_when_no_explicit_owner() {
        let mut pairs = MINIMAL.to_vec();
        pairs.push(("HOSTNAME", "pod-7"));
        assert_eq!(Config::from_lookup(&lookup(&pairs)).unwrap().owner, "pod-7");
    }

    #[test]
    fn explicit_owner_wins_over_hostname() {
        let mut pairs = MINIMAL.to_vec();
        pairs.push(("HOSTNAME", "pod-7"));
        pairs.push(("SCHEDULER_OWNER", "engine-a"));
        assert_eq!(
            Config::from_lookup(&lookup(&pairs)).unwrap().owner,
            "engine-a"
        );
    }

    /// `metrics_addr` has the same default-and-override contract as the other
    /// bind addresses, and the same 0.0.0.0 default for the same reason: a
    /// loopback default would bind inside the container and serve the scraper
    /// nothing.
    #[test]
    fn metrics_addr_defaults_and_overrides() {
        assert_eq!(
            Config::from_lookup(&lookup(MINIMAL)).unwrap().metrics_addr,
            "0.0.0.0:9090"
        );
        let mut pairs = MINIMAL.to_vec();
        pairs.push(("METRICS_ADDR", "127.0.0.1:9191"));
        assert_eq!(
            Config::from_lookup(&lookup(&pairs)).unwrap().metrics_addr,
            "127.0.0.1:9191"
        );
    }

    /// `per_tenant_cap` defaults off (`0`, the historical behaviour) and is
    /// overridable — asserting only the default would pass an implementation
    /// that ignored the variable.
    #[test]
    fn per_tenant_cap_defaults_off_and_overrides() {
        assert_eq!(
            Config::from_lookup(&lookup(MINIMAL))
                .unwrap()
                .per_tenant_cap,
            0
        );
        let mut pairs = MINIMAL.to_vec();
        pairs.push(("PER_TENANT_CAP", "50"));
        assert_eq!(
            Config::from_lookup(&lookup(&pairs)).unwrap().per_tenant_cap,
            50
        );
    }

    /// The endpoint renders the exposition format and includes a metric that
    /// was actually incremented — not just the empty schema. Driven with a real
    /// request through the router (no bound socket), so it exercises the route,
    /// the handler, and the render closure together.
    #[tokio::test]
    async fn metrics_endpoint_renders_an_incremented_metric() {
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        let registry = adapter_metrics::PrometheusMetrics::shared();
        scheduler_domain::Metrics::incr(&registry, scheduler_domain::Metric::RunsClaimed, 7);

        let render = {
            let registry = registry.clone();
            std::sync::Arc::new(move || registry.render())
                as std::sync::Arc<dyn Fn() -> String + Send + Sync>
        };

        let response = metrics_router(render)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/metrics")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            text.contains("\nruns_claimed 7\n"),
            "the scrape must reflect the real incremented value, got:\n{text}"
        );
        assert!(
            text.contains("# TYPE runs_claimed counter"),
            "the scrape must carry the exposition-format type line"
        );
    }
}
