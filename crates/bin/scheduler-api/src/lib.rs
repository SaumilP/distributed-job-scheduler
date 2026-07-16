//! Composition root for the HTTP-facing surfaces.
//!
//! The transports themselves live in `crates/adapters/api-*`; this crate wires
//! them to concrete adapters and serves them.

use anyhow::Context;
use std::future::Future;

/// Runs both API servers to completion, failing as soon as *either* fails.
///
/// This exists as a named function rather than a `try_join!` inline in `main`
/// so the failure semantics can be tested. `main` itself binds sockets and
/// connects to Postgres, so it cannot be driven from a test; the property that
/// matters here can be, against two ordinary futures.
///
/// **`try_join!`, not `join!`.** With `join!` a server that dies leaves the
/// caller awaiting its still-running sibling forever: the process stays up,
/// never exits, and `restart: on-failure` never fires because there is no
/// failure. Half the API would be silently unreachable while the container
/// reported healthy -- the same defect that made a dead engine loop hang the
/// process in Phase 2b. `try_join!` returns as soon as either future resolves
/// to an error, dropping the other, so the process exits non-zero.
pub async fn serve_both<H, G>(http: H, grpc: G) -> anyhow::Result<()>
where
    H: Future<Output = anyhow::Result<()>>,
    G: Future<Output = anyhow::Result<()>>,
{
    tokio::try_join!(http, grpc).context("an API server terminated unexpectedly")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// A future that never completes -- a healthy server, still serving.
    async fn still_serving() -> anyhow::Result<()> {
        std::future::pending::<()>().await;
        Ok(())
    }

    async fn stopped_cleanly() -> anyhow::Result<()> {
        Ok(())
    }

    async fn died() -> anyhow::Result<()> {
        anyhow::bail!("listener closed")
    }

    /// The `try_join!`-vs-`join!` property, from the HTTP side.
    ///
    /// The bound is what makes this a real check: with `join!` the call never
    /// returns at all, and an unbounded test would hang rather than fail --
    /// which is not a mutation check, it is a stuck suite. The timeout turns
    /// the hang into an assertion failure.
    #[tokio::test]
    async fn a_dead_http_server_fails_without_waiting_for_the_live_grpc_server() {
        let outcome =
            tokio::time::timeout(Duration::from_secs(5), serve_both(died(), still_serving()))
                .await
                .expect("must not wait on the surviving server -- the join!/try_join! defect");

        assert!(outcome.is_err(), "a dead server must be a process failure");
    }

    /// The same property from the other side. Asserting only the HTTP case
    /// would pass an implementation that awaited the gRPC future first.
    #[tokio::test]
    async fn a_dead_grpc_server_fails_without_waiting_for_the_live_http_server() {
        let outcome =
            tokio::time::timeout(Duration::from_secs(5), serve_both(still_serving(), died()))
                .await
                .expect("must not wait on the surviving server -- the join!/try_join! defect");

        assert!(outcome.is_err(), "a dead server must be a process failure");
    }

    /// The shutdown path: both servers stop on the signal, and that is a clean
    /// exit, not a failure. Without this, "fail if either stops" would be
    /// satisfied by a function that always returned an error.
    #[tokio::test]
    async fn both_servers_stopping_cleanly_is_a_clean_exit() {
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            serve_both(stopped_cleanly(), stopped_cleanly()),
        )
        .await
        .expect("a clean shutdown must not hang");

        assert!(outcome.is_ok(), "a graceful shutdown must exit zero");
    }
}
