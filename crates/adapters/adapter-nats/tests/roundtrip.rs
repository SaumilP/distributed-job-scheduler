use adapter_nats::{NatsEventPublisher, NatsRunConsumer, ensure_stream, wire::RunEvent};
use futures::StreamExt;
use scheduler_domain::*;
use time::OffsetDateTime;
use uuid::Uuid;

mod support;

fn claimed_run(tenant: &str, attempt: i32) -> JobRun {
    JobRun {
        id: RunId(Uuid::new_v4()),
        job_id: JobId(Uuid::new_v4()),
        tenant: TenantId(tenant.into()),
        scheduled_at: OffsetDateTime::now_utc(),
        state: RunState::Claimed,
        attempt,
    }
}

/// The load-bearing assertion of this adapter: a run published *before* any
/// consumer exists is still delivered to a consumer created afterwards.
///
/// This is what separates JetStream from core NATS. A core `client.publish`
/// is fire-and-forget -- with no subscriber attached at publish time the
/// message is simply dropped, and this test would time out. So this does not
/// merely test "a message arrived"; it tests that the message was *persisted*.
#[tokio::test]
async fn published_run_is_durably_retrievable_from_the_stream() {
    let js = support::jetstream().await;
    ensure_stream(&js).await.unwrap();

    let run = claimed_run("acme", 1);

    let publisher = NatsEventPublisher::new(js.clone());
    publisher.publish_run(&run).await.unwrap();

    // Consumer created AFTER the publish -- see the doc comment.
    let consumer = js
        .get_stream("RUNS")
        .await
        .unwrap()
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some("test-worker".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut batch = consumer.fetch().max_messages(1).messages().await.unwrap();
    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), batch.next())
        .await
        .expect("no message within 10s -- publish did not durably land")
        .expect("stream closed")
        .unwrap();

    let event: RunEvent = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(event.run_id, run.id.0);
    assert_eq!(event.attempt, 1);
    msg.ack().await.unwrap();
}

/// `ensure_stream` runs on every process start, so it must be idempotent --
/// a second call must not fail with "stream name already in use".
#[tokio::test]
async fn ensure_stream_is_idempotent() {
    let js = support::jetstream().await;
    ensure_stream(&js).await.unwrap();
    ensure_stream(&js).await.unwrap();
}

/// The subject is derived from the tenant, so runs for different tenants must
/// land on different subjects -- otherwise a per-tenant consumer filter
/// (which Phase 2b relies on) cannot work.
#[tokio::test]
async fn runs_are_published_on_per_tenant_subjects() {
    let js = support::jetstream().await;
    ensure_stream(&js).await.unwrap();

    let publisher = NatsEventPublisher::new(js.clone());
    let acme = claimed_run("acme-sub", 0);
    let globex = claimed_run("globex-sub", 0);
    publisher.publish_run(&acme).await.unwrap();
    publisher.publish_run(&globex).await.unwrap();

    // A consumer filtered to one tenant must see that tenant's run and only it.
    let consumer = js
        .get_stream("RUNS")
        .await
        .unwrap()
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some("tenant-filtered".into()),
            filter_subject: adapter_nats::wire::subject_for("acme-sub"),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut batch = consumer.fetch().max_messages(10).messages().await.unwrap();
    let mut seen = Vec::new();
    while let Ok(Some(Ok(msg))) =
        tokio::time::timeout(std::time::Duration::from_secs(5), batch.next()).await
    {
        let event: RunEvent = serde_json::from_slice(&msg.payload).unwrap();
        seen.push(event.run_id);
        msg.ack().await.unwrap();
    }

    assert_eq!(
        seen,
        vec![acme.id.0],
        "a tenant-filtered consumer must see exactly that tenant's runs"
    );
}

/// Proves the publish acknowledgement is actually awaited.
///
/// This exists because the obvious durability test does *not* prove it. A
/// JetStream stream bound to `runs.>` captures every message on those
/// subjects, including plain core-NATS publishes -- so swapping
/// `js.publish(..).await.await` for `client.publish(..)` still leaves the
/// message retrievable by a late consumer, and the durability test passes
/// either way. What the core publish actually loses is the *confirmation*
/// that the server persisted the message.
///
/// With no stream covering the subject, that difference becomes observable
/// and deterministic: awaiting the ack surfaces an error, while a core
/// publish reports success into the void. Dropping the `ack.await` from
/// `publish_run` makes this test fail.
#[tokio::test]
async fn publish_reports_failure_when_no_stream_captures_the_subject() {
    let js = support::jetstream_without_stream().await;

    let publisher = NatsEventPublisher::new(js.clone());
    let err = publisher
        .publish_run(&claimed_run("orphan", 0))
        .await
        .expect_err("publishing with no stream to persist to must not report success");

    assert!(
        matches!(err, DomainError::Publish(_)),
        "expected DomainError::Publish, got {err:?}"
    );
}

/// The happy path, plus the assertion that an ack actually settles the
/// message: a second poll must come back empty rather than redelivering.
#[tokio::test]
async fn consumer_receives_published_run_and_acks_it() {
    let js = support::jetstream().await;
    ensure_stream(&js).await.unwrap();

    let run = claimed_run("acme", 3);
    NatsEventPublisher::new(js.clone())
        .publish_run(&run)
        .await
        .unwrap();

    let consumer = NatsRunConsumer::connect(js.clone(), "worker-ack")
        .await
        .unwrap();
    let msg = consumer
        .next_run(std::time::Duration::from_secs(10))
        .await
        .unwrap()
        .expect("expected a run within 10s");
    assert_eq!(msg.run().id, run.id);
    assert_eq!(msg.run().attempt, 3);
    assert_eq!(
        msg.run().state,
        RunState::Claimed,
        "a published run is claimed by construction"
    );
    msg.ack().await.unwrap();

    // Longer than ack_wait, so a redelivery would have happened by now if the
    // ack had not settled the message.
    let none = consumer
        .next_run(std::time::Duration::from_secs(8))
        .await
        .unwrap();
    assert!(none.is_none(), "acked message must not be redelivered");
}

/// The at-least-once proof.
///
/// Dropping the message without acking is what a worker crash looks like from
/// the broker's side. The run must come back. If it did not, a crash between
/// delivery and execution would silently lose the job -- which is the exact
/// failure this architecture claims not to have.
#[tokio::test]
async fn unacked_run_is_redelivered() {
    let js = support::jetstream().await;
    ensure_stream(&js).await.unwrap();

    let run = claimed_run("acme", 0);
    NatsEventPublisher::new(js.clone())
        .publish_run(&run)
        .await
        .unwrap();

    let consumer = NatsRunConsumer::connect(js.clone(), "worker-redeliver")
        .await
        .unwrap();
    let first = consumer
        .next_run(std::time::Duration::from_secs(10))
        .await
        .unwrap()
        .expect("expected a run");
    assert_eq!(first.run().id, run.id);
    drop(first); // simulate a worker crash: no ack

    let again = consumer
        .next_run(std::time::Duration::from_secs(30))
        .await
        .unwrap()
        .expect("unacked run must be redelivered -- this is the at-least-once guarantee");
    assert_eq!(again.run().id, run.id);
}

/// A malformed payload must be a handled error, not a panic that takes the
/// worker down. It redelivers until `MAX_DELIVER` and then stops on the
/// stream; nothing routes it onward, and no dead-letter queue is planned here.
#[tokio::test]
async fn malformed_payload_is_an_error_not_a_panic() {
    let js = support::jetstream().await;
    ensure_stream(&js).await.unwrap();

    js.publish(adapter_nats::wire::subject_for("acme"), "not json".into())
        .await
        .unwrap()
        .await
        .unwrap();

    let consumer = NatsRunConsumer::connect(js.clone(), "worker-malformed")
        .await
        .unwrap();
    let err = consumer
        .next_run(std::time::Duration::from_secs(10))
        .await
        .expect_err("a malformed payload must surface as an error");
    assert!(matches!(err, DomainError::Storage(_)), "got {err:?}");
}
