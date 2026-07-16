use api_rest::routes::{AppState, app};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

mod support;

async fn post_json(state: &AppState, path: &str, body: Value) -> (StatusCode, Value) {
    let res = app(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

async fn get(state: &AppState, path: &str) -> (StatusCode, Value) {
    let res = app(state.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

#[tokio::test]
async fn post_jobs_creates_a_job_and_returns_its_id() {
    let state = support::state().await;
    let (status, body) = post_json(
        &state,
        "/jobs",
        json!({
            "tenant": "acme",
            "target": "http://svc/run",
            "schedule": {"type": "interval", "every_secs": 60}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let id = body["id"].as_str().expect("response must carry an id");
    Uuid::parse_str(id).expect("id must be a UUID");

    let (status, job) = get(&state, &format!("/jobs/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(job["tenant"], "acme");
    assert_eq!(job["schedule"]["every_secs"], 60);
}

#[tokio::test]
async fn post_jobs_accepts_a_oneshot_schedule() {
    let state = support::state().await;
    let (status, body) = post_json(
        &state,
        "/jobs",
        json!({
            "tenant": "acme",
            "target": "http://svc/run",
            "schedule": {"type": "one_shot", "at": "2026-08-01T09:00:00Z"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body was {body}");
}

/// A non-positive interval must be a client error, and it must be the DOMAIN
/// that rejects it -- `Schedule::interval` already encodes this rule because a
/// non-advancing interval makes the materializer loop forever. The assertion
/// here is that the API surfaces it as 400 rather than leaking it as a 500 or,
/// worse, storing an unschedulable job.
#[tokio::test]
async fn post_jobs_rejects_a_non_positive_interval() {
    let state = support::state().await;
    for every_secs in [0, -1] {
        let (status, _) = post_json(
            &state,
            "/jobs",
            json!({
                "tenant": "acme",
                "target": "http://svc/run",
                "schedule": {"type": "interval", "every_secs": every_secs}
            }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "every_secs={every_secs} must be a client error"
        );
    }
}

#[tokio::test]
async fn post_jobs_rejects_an_unknown_schedule_type() {
    let state = support::state().await;
    let (status, _) = post_json(
        &state,
        "/jobs",
        json!({
            "tenant": "acme",
            "target": "http://svc/run",
            "schedule": {"type": "weekly", "every_secs": 60}
        }),
    )
    .await;
    assert!(
        status.is_client_error(),
        "an unknown schedule type must be a client error, got {status}"
    );
}

#[tokio::test]
async fn get_unknown_job_is_404() {
    let state = support::state().await;
    let (status, _) = get(&state, &format!("/jobs/{}", Uuid::new_v4())).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "must be 404, not 500");
}

#[tokio::test]
async fn get_unknown_run_is_404() {
    let state = support::state().await;
    let (status, _) = get(&state, &format!("/runs/{}", Uuid::new_v4())).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ready_is_ok_when_the_database_is_reachable() {
    let state = support::state().await;
    let (status, _) = get(&state, "/ready").await;
    assert_eq!(status, StatusCode::OK);
}

/// The property that keeps a database blip from becoming a restart storm:
/// liveness must not depend on the database. Built against a pool pointed at a
/// port with nothing behind it.
#[tokio::test]
async fn health_does_not_depend_on_the_database() {
    let state = support::state_with_unreachable_database();

    let (status, _) = get(&state, "/health").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "liveness must not fail when the database is down"
    );

    // And the counterpart: readiness *must* notice.
    let (status, _) = get(&state, "/ready").await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "readiness must fail when the database is down"
    );
}

/// The remote-stall regression, at the boundary it arrives through.
///
/// `every_secs = 1e12` is a milliseconds-for-seconds slip. It used to be
/// accepted, and the resulting job panicked the engine's materialize loop on
/// every tick — stalling scheduling for every tenant while the container still
/// reported healthy.
#[tokio::test]
async fn post_jobs_rejects_an_interval_that_would_overflow_time() {
    let state = support::state().await;
    for every_secs in [1_000_000_000_000i64, i64::MAX] {
        let (status, _) = post_json(
            &state,
            "/jobs",
            json!({
                "tenant": "acme",
                "target": "http://svc/run",
                "schedule": {"type": "interval", "every_secs": every_secs}
            }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "every_secs={every_secs} must be rejected at the boundary"
        );
    }
}

/// A job with no target is materialized forever and can never be delivered
/// anywhere; an empty tenant flows into the per-tenant NATS subject.
#[tokio::test]
async fn post_jobs_rejects_empty_tenant_or_target() {
    let state = support::state().await;
    for (tenant, target) in [
        ("", "http://svc/run"),
        ("acme", ""),
        ("   ", "http://svc/run"),
    ] {
        let (status, _) = post_json(
            &state,
            "/jobs",
            json!({
                "tenant": tenant,
                "target": target,
                "schedule": {"type": "interval", "every_secs": 60}
            }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "tenant={tenant:?} target={target:?} must be rejected"
        );
    }
}

/// Internal failures must not echo their detail to clients: a storage error
/// string carries connection details and table names. Nothing asserted this,
/// so a regression that echoed `self.0.to_string()` would have shipped green.
#[tokio::test]
async fn storage_errors_are_scrubbed_before_reaching_the_client() {
    let state = support::state_with_unreachable_database();

    let (status, body) = post_json(
        &state,
        "/jobs",
        json!({
            "tenant": "acme",
            "target": "http://svc/run",
            "schedule": {"type": "interval", "every_secs": 60}
        }),
    )
    .await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        body["error"], "internal error",
        "the raw storage error must not reach the client, got {body}"
    );
}
