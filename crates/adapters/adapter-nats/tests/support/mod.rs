//! NATS/JetStream fixture for the adapter tests.
//!
//! Container plumbing (and the reasoning behind sharing only the container,
//! plus the known leak) lives in the `test-support` crate. This module adds
//! what is specific to these tests: stream state reset between them.

use async_nats::jetstream::{self, Context};
use std::ops::Deref;
use tokio::sync::{Mutex, MutexGuard};

/// Serializes access to the single shared stream. All tests in this binary
/// share one `RUNS` stream, so without this one test's published runs are
/// visible to another's consumer.
static GATE: Mutex<()> = Mutex::const_new(());

/// A JetStream context over this binary's shared NATS container.
///
/// Keep it bound for the whole test (`let js = support::jetstream().await;`)
/// -- dropping it early releases the gate and lets another test's reset wipe
/// messages mid-assertion.
pub struct NatsFixture {
    _gate: MutexGuard<'static, ()>,
    js: Context,
}

impl Deref for NatsFixture {
    type Target = Context;

    fn deref(&self) -> &Context {
        &self.js
    }
}

async fn connect() -> (MutexGuard<'static, ()>, Context) {
    let gate = GATE.lock().await;
    let port = test_support::nats_port();

    // Built on the caller's runtime -- see the `test_support` module docs.
    //
    // `retry_on_initial_connect` because `async_nats::connect` does *not* retry
    // the first connection: a single transient refusal is a hard panic. The
    // container readiness gate is not the problem -- the log line it waits for
    // ("Server is ready") genuinely postdates JetStream startup -- but on a
    // loaded Docker daemon the connect itself can still be refused once. Seen
    // as 1 failure in 4 runs immediately after mass container deletion, and 0
    // in 6 on an idle machine.
    let client = async_nats::ConnectOptions::new()
        .retry_on_initial_connect()
        .connect(test_support::nats_url(port))
        .await
        .expect("failed to connect to shared nats");
    (gate, jetstream::new(client))
}

/// A context whose `RUNS` stream, if it exists, holds no messages.
pub async fn jetstream() -> NatsFixture {
    let (gate, js) = connect().await;

    // Purge rather than delete: deleting the stream would also drop the
    // durable consumers tests create, and recreating it races the server's own
    // cleanup. An absent stream on the first test is expected, not an error.
    if let Ok(stream) = js.get_stream("RUNS").await {
        stream.purge().await.expect("failed to purge RUNS stream");
    }

    NatsFixture { _gate: gate, js }
}

/// A context with **no** `RUNS` stream, so nothing captures `runs.*`.
///
/// Used to observe what a publish does when there is nothing to persist to.
/// Safe to delete because the gate serializes tests and every test that needs
/// the stream calls `ensure_stream` itself.
pub async fn jetstream_without_stream() -> NatsFixture {
    let (gate, js) = connect().await;

    if js.get_stream("RUNS").await.is_ok() {
        js.delete_stream("RUNS")
            .await
            .expect("failed to delete RUNS stream");
    }

    NatsFixture { _gate: gate, js }
}
