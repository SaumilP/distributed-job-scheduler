use crate::wire::{RunEvent, subject_for};
use async_nats::jetstream::Context;
use async_nats::jetstream::stream::{Config, StorageType};
use scheduler_domain::{DomainError, DomainResult, EventPublisher, JobRun};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// The stream every run is published to.
pub const STREAM_NAME: &str = "RUNS";

/// Every subject under the run hierarchy. `subject_for` emits exactly one
/// token after `runs.`, but `>` (rather than `*`) keeps room for a deeper
/// hierarchy later without a stream migration.
pub const STREAM_SUBJECT: &str = "runs.>";

/// Idempotently create the `RUNS` stream.
///
/// Called on every process start, so "already exists" must be a success, not
/// an error -- `get_or_create_stream` gives that directly. It does not,
/// however, reconcile an existing stream whose config has drifted; that is a
/// deliberate omission, since silently rewriting a production stream's
/// retention or storage from an app boot path is a worse failure than
/// noticing the drift.
pub async fn ensure_stream(js: &Context) -> Result<(), async_nats::Error> {
    js.get_or_create_stream(Config {
        name: STREAM_NAME.to_string(),
        subjects: vec![STREAM_SUBJECT.to_string()],
        // File, not memory: the whole point of publishing through JetStream
        // is that a claimed run survives a broker restart. Memory storage
        // would make the durability guarantee a lie.
        storage: StorageType::File,
        ..Default::default()
    })
    .await?;
    Ok(())
}

/// Publishes claimed runs to JetStream.
///
/// Cloneable because the engine hands one to each dispatch task; the
/// underlying `Context` is a cheap handle over a shared connection.
#[derive(Clone)]
pub struct NatsEventPublisher {
    js: Context,
}

impl NatsEventPublisher {
    pub fn new(js: Context) -> Self {
        Self { js }
    }
}

impl EventPublisher for NatsEventPublisher {
    fn publish_run(&self, run: &JobRun) -> impl Future<Output = DomainResult<()>> + Send {
        let js = self.js.clone();
        let subject = subject_for(&run.tenant.0);
        let event = RunEvent::from_domain(run);
        // The trace context of whoever is dispatching, captured before the
        // async move so the published headers carry the dispatch span's
        // traceparent to the worker that executes the run.
        let cx = tracing::Span::current().context();
        async move {
            let payload =
                serde_json::to_vec(&event).map_err(|e| DomainError::Publish(e.to_string()))?;

            let mut headers = async_nats::HeaderMap::new();
            crate::trace::inject_context(&cx, &mut headers);

            let ack = js
                .publish_with_headers(subject, headers, payload.into())
                .await
                .map_err(|e| DomainError::Publish(e.to_string()))?;

            // Awaiting the ack is what makes this a *durable* publish. The
            // first await only hands the message to the client; the server
            // has not yet persisted it, and returning here would mean a run
            // could be marked claimed in Postgres while its dispatch event
            // was still in flight and lost to a broker crash.
            ack.await.map_err(|e| DomainError::Publish(e.to_string()))?;
            Ok(())
        }
    }
}
