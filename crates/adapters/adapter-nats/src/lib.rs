//! NATS JetStream adapter: the driven side of the `EventPublisher` port, and
//! the consumer Phase 2b's worker binary drives.
//!
//! The wire format lives in [`wire`] and is deliberately free of NATS types,
//! so the publisher/consumer contract can be tested without a broker.

pub mod consumer;
pub mod publisher;
pub mod trace;
pub mod wire;

pub use consumer::{ClaimedMessage, NatsRunConsumer};
pub use publisher::{NatsEventPublisher, STREAM_NAME, STREAM_SUBJECT, ensure_stream};
pub use trace::{extract_context, inject_context};
