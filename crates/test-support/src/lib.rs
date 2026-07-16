//! Container fixtures shared by every test binary that needs real
//! infrastructure.
//!
//! This crate exists because the fixture was about to be copied a third time.
//! It deliberately does **not** depend on `adapter-postgres` (which would
//! create a dependency cycle with that crate's own tests): it starts a
//! container and hands back a port, and the caller decides what schema to put
//! in it.
//!
//! ## Why only the container is shared, not the pool
//!
//! `#[tokio::test]` gives every test function its own short-lived runtime.
//! That makes sharing a `sqlx::PgPool` (or a NATS client) across tests
//! actively wrong in two compounding ways:
//!
//! 1. A pool built lazily inside whichever test ran first ties the pool's
//!    background upkeep to that test's runtime. When that runtime is dropped,
//!    upkeep dies and later acquisitions hang until `acquire_timeout` -- which
//!    presents as an alternating pass/fail pattern, not an obvious error.
//! 2. Moving construction to a dedicated long-lived runtime does *not* fix it,
//!    because Tokio's `TcpStream` registers with the I/O driver of the runtime
//!    that created it.
//!
//! So the split is: share the *expensive* thing (the container) and build the
//! *cheap* thing (the connection) per test. The only value crossing a runtime
//! boundary is a `u16`, which owns no I/O resources.
//!
//! ## Known limitation: containers outlive the test process
//!
//! `testcontainers` 0.23 has no ryuk-style reaper; cleanup relies on
//! `ContainerAsync`'s `Drop`, which cannot await and so does not reliably stop
//! the container. Containers therefore survive the run and accumulate. Reap
//! them with:
//!
//! ```sh
//! docker ps -aq --filter ancestor=postgres:17-alpine --filter ancestor=nats:2-alpine \
//!   | xargs -r docker rm -f
//! ```

use std::sync::OnceLock;
use std::sync::mpsc;
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::{GenericImage, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

static PG_PORT: OnceLock<u16> = OnceLock::new();
static NATS_PORT: OnceLock<u16> = OnceLock::new();
static REDIS_PORT: OnceLock<u16> = OnceLock::new();

const NATS_TCP: ContainerPort = ContainerPort::Tcp(4222);
const REDIS_TCP: ContainerPort = ContainerPort::Tcp(6379);

/// Runs `f` on a dedicated thread owning its own runtime, which then parks
/// forever holding whatever `f` returned.
///
/// The park is the point: dropping a `ContainerAsync` stops its container, so
/// the handle has to outlive every test in the binary. Nothing but the port
/// escapes the thread.
fn start_container<F, Fut, G>(build: F) -> u16
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = (G, u16)>,
    G: Send + 'static,
{
    let (tx, rx) = mpsc::channel::<u16>();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build fixture runtime");

        rt.block_on(async move {
            let (_guard, port) = build().await;
            tx.send(port).expect("failed to report container port");
            std::future::pending::<()>().await;
        });
    });

    rx.recv()
        .expect("fixture thread failed to report the container port")
}

/// Mapped port of this test binary's one Postgres container.
///
/// The database is empty: the caller runs whatever migrations it needs.
pub fn postgres_port() -> u16 {
    *PG_PORT.get_or_init(|| {
        start_container(|| async {
            let container = Postgres::default()
                .with_tag("17-alpine")
                .start()
                .await
                .expect("failed to start postgres container");
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("failed to get mapped postgres port");
            (container, port)
        })
    })
}

pub fn postgres_url(port: u16) -> String {
    format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres")
}

/// Mapped port of this test binary's one NATS container, JetStream enabled.
///
/// JetStream is **not** on by default; without `-js` the server rejects stream
/// creation with "jetstream not enabled for account".
pub fn nats_port() -> u16 {
    *NATS_PORT.get_or_init(|| {
        start_container(|| async {
            let container = GenericImage::new("nats", "2-alpine")
                .with_exposed_port(NATS_TCP)
                .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
                .with_cmd(["-js"])
                .start()
                .await
                .expect("failed to start nats container");
            let port = container
                .get_host_port_ipv4(4222.tcp())
                .await
                .expect("failed to get mapped nats port");
            (container, port)
        })
    })
}

pub fn nats_url(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

/// Mapped port of this test binary's one Redis container.
pub fn redis_port() -> u16 {
    *REDIS_PORT.get_or_init(|| {
        start_container(|| async {
            let container = GenericImage::new("redis", "7-alpine")
                .with_exposed_port(REDIS_TCP)
                .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
                .start()
                .await
                .expect("failed to start redis container");
            let port = container
                .get_host_port_ipv4(6379.tcp())
                .await
                .expect("failed to get mapped redis port");
            (container, port)
        })
    })
}

pub fn redis_url(port: u16) -> String {
    format!("redis://127.0.0.1:{port}")
}
