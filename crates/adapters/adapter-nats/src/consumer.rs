use crate::publisher::{STREAM_NAME, STREAM_SUBJECT};
use crate::wire::RunEvent;
use async_nats::jetstream::Context;
use async_nats::jetstream::consumer::pull::Config;
use async_nats::jetstream::consumer::{AckPolicy, Consumer};
use async_nats::jetstream::message::Message;
use futures::StreamExt;
use scheduler_domain::{DomainError, DomainResult, JobRun};
use std::time::Duration;

/// How long the server waits for an ack before redelivering.
///
/// Five seconds is a test-friendly value, not a production one: it must be
/// short enough that the redelivery test observes a redelivery quickly, and
/// long enough that a normal execution is not redelivered while still
/// running. Phase 2b makes this configurable per deployment, where it needs
/// to exceed the worst-case job duration -- an `ack_wait` shorter than the
/// work itself turns every slow job into a duplicate.
const ACK_WAIT: Duration = Duration::from_secs(5);

/// Redelivery cap.
///
/// JetStream's default is unlimited, which means a message that can never be
/// processed -- a malformed payload, say -- is redelivered forever, once per
/// `ACK_WAIT`, for the life of the stream. This is the bound that keeps it from
/// being an unbounded loop. After this many attempts JetStream stops delivering
/// and the message can be inspected on the stream.
///
/// There is no dead-letter *queue* behind this and this repository does not
/// build one: `RunState::Dead` is a terminal state, and nothing republishes a
/// run that reaches it.
const MAX_DELIVER: i64 = 5;

/// A delivered run that has **not** been acknowledged yet.
///
/// The separation is the whole point. If `next_run` returned a bare `JobRun`,
/// the message would have to be acked at delivery time, and a worker that
/// crashed mid-execution would lose the job with the broker believing it
/// done. Holding the message until the caller explicitly acks is what makes
/// redelivery -- and therefore at-least-once -- real.
#[derive(Debug)]
pub struct ClaimedMessage {
    run: JobRun,
    msg: Message,
}

impl ClaimedMessage {
    pub fn run(&self) -> &JobRun {
        &self.run
    }

    /// The trace context the dispatcher injected when it published this run, so
    /// the worker can parent its execution span to the dispatch that caused it.
    /// The empty context if the message carried no propagation headers (an
    /// un-traced publish, or tracing not configured).
    pub fn trace_context(&self) -> opentelemetry::Context {
        match self.msg.headers.as_ref() {
            Some(headers) => crate::trace::extract_context(headers),
            None => opentelemetry::Context::new(),
        }
    }

    /// Acknowledges the message. Consumes `self`, so a message cannot be
    /// acked twice.
    pub async fn ack(self) -> DomainResult<()> {
        self.msg
            .ack()
            .await
            .map_err(|e| DomainError::Publish(e.to_string()))
    }
}

/// A durable pull consumer over the `RUNS` stream.
///
/// Pull rather than push: the worker asks for work when it has capacity,
/// which is its own backpressure. A push consumer would keep delivering into
/// a saturated worker and rely on ack_wait redelivery to sort it out.
pub struct NatsRunConsumer {
    consumer: Consumer<Config>,
}

impl NatsRunConsumer {
    /// Binds (or creates) the durable consumer named `durable_name`.
    ///
    /// Durable and shared by name: every worker replica connecting with the
    /// same name joins one queue group, so a run is delivered to exactly one
    /// of them and the set of replicas can change without losing position.
    pub async fn connect(js: Context, durable_name: &str) -> DomainResult<Self> {
        let stream = js
            .get_stream(STREAM_NAME)
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))?;

        let consumer = stream
            .get_or_create_consumer(
                durable_name,
                Config {
                    durable_name: Some(durable_name.to_string()),
                    filter_subject: STREAM_SUBJECT.to_string(),
                    // Explicit: the server waits for the worker to say the
                    // job actually ran. AckPolicy::None or ::All would
                    // acknowledge on delivery and defeat redelivery.
                    ack_policy: AckPolicy::Explicit,
                    ack_wait: ACK_WAIT,
                    max_deliver: MAX_DELIVER,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))?;

        Ok(Self { consumer })
    }

    /// Fetches at most one run, waiting up to `max_wait`.
    ///
    /// `Ok(None)` on an empty fetch: no work available is the steady state of
    /// an idle worker, not an error condition.
    pub async fn next_run(&self, max_wait: Duration) -> DomainResult<Option<ClaimedMessage>> {
        let mut batch = self
            .consumer
            .fetch()
            .max_messages(1)
            .expires(max_wait)
            .messages()
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))?;

        let Some(msg) = batch.next().await else {
            return Ok(None);
        };
        let msg = msg.map_err(|e| DomainError::Storage(e.to_string()))?;

        // A payload we cannot parse is a handled error, never a panic -- one
        // poisoned message must not take the worker process down. It stays
        // unacked and will redeliver until `MAX_DELIVER`, after which the
        // server stops and it can be inspected on the stream. Nothing routes it
        // onward -- there is no dead-letter queue here.
        let event: RunEvent = serde_json::from_slice(&msg.payload)
            .map_err(|e| DomainError::Storage(format!("malformed run event: {e}")))?;

        Ok(Some(ClaimedMessage {
            run: event.to_domain(),
            msg,
        }))
    }
}

// Compile-time invariants on the two constants above.
//
// These were runtime tests until clippy pointed out -- correctly -- that
// asserting on a constant at runtime is vacuous: the value is fixed at
// compile time, so the "test" only ever restates the literal. Expressed as
// `const _`, the same invariant is actually enforced by the compiler and
// cannot be skipped, filtered out, or pass because the test binary was not
// run.

// `ACK_WAIT`'s *lower* bound is the direction that hurts. The redelivery
// integration test waits up to 30s, so it catches the value growing past
// that; nothing catches it shrinking -- and an ack_wait shorter than a job's
// runtime redelivers work still in progress, turning every slow job into a
// duplicate execution.
const _: () = assert!(
    ACK_WAIT.as_secs() >= 5,
    "ACK_WAIT shorter than the worst-case job duration causes duplicate executions"
);

// Redelivery must be bounded. JetStream's default is unlimited (-1), which
// lets a poison message loop forever.
const _: () = assert!(MAX_DELIVER > 0, "max_deliver must be a finite cap");

// **The lease/ack relationship, enforced rather than asserted in prose.**
//
// The lease starts when the engine *claims* a run and must still be held when
// that run reaches a terminal state, or `reclaim_expired` returns a run that is
// still being worked to `Pending`, a later `claim_due` publishes it a second
// time, and the reaper has *caused* the duplicate execution it exists to
// repair. `complete()` is idempotent, so the duplicate is safe to **record**;
// nothing makes it safe to **execute** against a target that is not itself
// idempotent.
//
// The full requirement is
//
//     LEASE_SECS > publish + queue_wait + redelivery_window + execution
//                  + complete
//
// where the redelivery window is `ACK_WAIT * (MAX_DELIVER - 1)` -- the first
// delivery is free, and each of the rest costs a full `ACK_WAIT` before the
// server gives up on the previous one. That window is the *only* leg of the
// path this repository bounds; queue wait in particular is backlog over worker
// count, which no constant controls. So this cannot assert the real inequality.
//
// What it *can* assert is that the term we do control does not on its own
// consume the lease, with margin. A lease that merely exceeded the redelivery
// window would be satisfied at 21 seconds and leave nothing for the execution
// itself, which is how the previous value of 30 came to be wrong.
//
// This lives here, in the crate that can see both constants, rather than beside
// `LEASE_SECS` -- `scheduler-domain` has zero infrastructure dependencies and
// must not learn about JetStream to keep it. The arithmetic is inline rather
// than behind a named constant because a `const` read only by a `const _`
// assertion is reported as dead code.
const _: () = assert!(
    scheduler_domain::LEASE_SECS as u64 > ACK_WAIT.as_secs() * (MAX_DELIVER as u64 - 1) * 3,
    "LEASE_SECS must dominate the broker's redelivery window with margin; \
     a lease that can expire while JetStream is still legitimately redelivering \
     makes the reaper a source of duplicate execution"
);
