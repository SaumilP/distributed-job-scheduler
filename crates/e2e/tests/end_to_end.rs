//! The end-to-end test.
//!
//! Every other test in this repository mocks at least one boundary. This one
//! asserts the actual claim of the project: a job created over one transport
//! gets materialized, claimed, dispatched over NATS, executed, and recorded --
//! and is then visible over the *other two* transports.
//!
//! Since Phase 2c there are three driving adapters over the same ports, so
//! there is a second property worth pinning: **they agree.** A job created
//! over gRPC is observed over GraphQL and read back over REST and gRPC, and
//! every observation must report the same run in the same state. Three
//! transports that each pass their own unit tests can still disagree about
//! what a run is -- a state name spelled differently, an id encoded
//! differently, a read that goes to the wrong row. Nothing below the transport
//! layer can catch that, because below the transport layer there is only one
//! of everything.
//!
//! The three roles run **in-process** as tokio tasks against real Postgres and
//! NATS containers, rather than as containers themselves. Orchestrating
//! containers from inside a test is slow and flaky, and the composition roots
//! are thin enough that assembling them here exercises the same wiring. What
//! this deliberately does *not* cover is the Dockerfile and compose file
//! themselves -- those are verified by hand (see `deploy/README.md`).
//!
//! The gRPC surface is the one exception to "no sockets": it binds a real
//! listener and connects a real client. The `api-grpc` unit tests drive the
//! service trait directly, so they never exercise the generated codec, the
//! HTTP/2 framing, or the client stubs. This is the only place any of that
//! runs, and a proto that compiles but does not round-trip would otherwise
//! reach the demo untested.

use adapter_nats::{NatsEventPublisher, NatsRunConsumer, ensure_stream};
use adapter_postgres::{PgJobRepository, PgRunRepository, run_migrations};
use api_grpc::SchedulerService;
use api_grpc::pb::scheduler_client::SchedulerClient;
use api_grpc::pb::scheduler_server::SchedulerServer;
use api_grpc::pb::{CreateJobRequest, GetRunRequest};
use api_rest::routes::{AppState, app};
use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use scheduler_application::ClaimAndDispatch;
use scheduler_common::SystemClock;
use scheduler_domain::*;
use scheduler_engine::loops::materialize_tick;
use scheduler_worker::handler::{LoggingExecutor, handle};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tower::ServiceExt;

/// Bounded, and reports the last state it saw.
///
/// A bare timeout tells you nothing about where the pipeline stalled --
/// "pending" means materialization ran but dispatch did not, "claimed" means
/// dispatch ran but the worker never finished. That distinction is the whole
/// diagnostic value of an end-to-end test when it fails in CI.
const DEADLINE: Duration = Duration::from_secs(30);

/// Bound on the gRPC listener accepting a connection. Separate from `DEADLINE`
/// because it measures a different thing: `DEADLINE` is the pipeline, this is
/// a socket coming up.
const GRPC_READY_DEADLINE: Duration = Duration::from_secs(10);

/// How many runs the GraphQL `Job.runs` loader reads per job. Matches
/// `RUNS_PER_JOB` in the `scheduler-api` composition root; if they drift, this
/// test observes a different page than the deployed API serves.
const RUNS_PER_JOB: i64 = 50;

#[tokio::test]
async fn a_job_created_over_grpc_is_executed_and_agrees_across_all_three_surfaces() {
    // --- real infrastructure -------------------------------------------------
    let pg_port = test_support::postgres_port();
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&test_support::postgres_url(pg_port))
        .await
        .expect("connect postgres");
    run_migrations(&pool).await.expect("migrations");
    sqlx::query("TRUNCATE TABLE job_runs, jobs")
        .execute(&pool)
        .await
        .expect("truncate");

    let nats_port = test_support::nats_port();
    let client = async_nats::connect(test_support::nats_url(nats_port))
        .await
        .expect("connect nats");
    let js = async_nats::jetstream::new(client);
    ensure_stream(&js).await.expect("ensure stream");
    // Unique consumer per test run so a leftover durable consumer from an
    // earlier run cannot swallow this run's message.
    let consumer_name = format!("e2e-{}", uuid::Uuid::new_v4().simple());

    let jobs = PgJobRepository { pool: pool.clone() };
    let runs = PgRunRepository { pool: pool.clone() };
    // This flow asserts behaviour, not metrics, so a no-op sink is enough; the
    // recording paths are covered by the unit tests that own them.
    let metrics: std::sync::Arc<dyn scheduler_domain::Metrics> =
        std::sync::Arc::new(scheduler_application::testing::NoopMetrics);

    // --- 1. create a job over gRPC, on a real socket ------------------------
    let grpc = GrpcHarness::start(jobs.clone(), runs.clone()).await;
    let mut grpc_client = grpc.client().await;

    let created = grpc_client
        .create_job(CreateJobRequest {
            tenant: "acme".into(),
            target: "http://example.invalid/run".into(),
            every_secs: 1,
        })
        .await
        .expect("CreateJob over gRPC")
        .into_inner();
    let job_id = created.id;
    uuid::Uuid::parse_str(&job_id).expect("gRPC must return a UUID job id");

    // --- 2. engine: materialize, then claim and dispatch --------------------
    let made = materialize_tick(&jobs, runs.clone(), SystemClock, 5, 100, metrics.clone())
        .await
        .expect("materialize");
    assert!(made > 0, "the engine must materialize runs for the new job");

    // The first run is due 1s after now (interval schedule), so wait for it
    // rather than claiming an empty window.
    tokio::time::sleep(Duration::from_millis(1200)).await;

    let dispatched = ClaimAndDispatch {
        runs: runs.clone(),
        publisher: NatsEventPublisher::new(js.clone()),
        clock: SystemClock,
        batch: 100,
        owner: "e2e-engine".into(),
        in_flight: Default::default(),
        metrics: metrics.clone(),
        per_tenant_cap: 0,
    }
    .run()
    .await
    .expect("claim and dispatch");
    assert!(dispatched > 0, "at least one due run must be dispatched");

    // --- 3. worker: consume, execute, complete, ack -------------------------
    let consumer = NatsRunConsumer::connect(js.clone(), &consumer_name)
        .await
        .expect("bind consumer");
    let msg = consumer
        .next_run(Duration::from_secs(10))
        .await
        .expect("poll")
        .expect("the dispatched run must arrive at the worker");
    let run_id = msg.run().id;
    handle(msg, &runs, &LoggingExecutor, metrics.as_ref())
        .await
        .expect("worker handling");

    // --- 4. observe the run reaching a terminal state, over GraphQL ---------
    //
    // The wait polls GraphQL rather than the repository. Polling the
    // repository asserts only that Postgres eventually holds the right row,
    // which the adapter tests already cover. Polling GraphQL additionally
    // asserts that the transport *reports* the transition, and that it reaches
    // the run through the nested `Job.runs` loader rather than a direct read --
    // a loader that batched the wrong ids would pass every unit test that asks
    // for one job at a time.
    let schema = api_graphql::build(jobs.clone(), runs.clone(), SystemClock, RUNS_PER_JOB);
    let graphql_state = await_terminal_over_graphql(&schema, &job_id, run_id).await;
    assert_eq!(
        graphql_state, "succeeded",
        "the run must end succeeded, as reported by GraphQL"
    );

    // --- 5. the same run, over REST and gRPC --------------------------------
    //
    // Same id, same state, same job, from all three. This is the agreement
    // property, and it fails if any surface encodes a state name or an id
    // differently from the others -- which no single-surface test can detect,
    // since each is internally consistent with itself.
    let rest_view = rest_run(&pool, run_id).await;
    assert_eq!(
        rest_view["state"], "succeeded",
        "REST must report the state GraphQL reported"
    );
    assert_eq!(
        rest_view["id"].as_str().expect("REST must return an id"),
        run_id.0.to_string(),
        "REST must return the same run id"
    );

    let grpc_view = grpc_client
        .get_run(GetRunRequest {
            id: run_id.0.to_string(),
        })
        .await
        .expect("GetRun over gRPC")
        .into_inner()
        .run
        .expect("gRPC must return the run");
    assert_eq!(
        grpc_view.state, "succeeded",
        "gRPC must report the state REST and GraphQL reported"
    );
    assert_eq!(
        grpc_view.id,
        run_id.0.to_string(),
        "gRPC must return the same run id"
    );
    assert_eq!(
        grpc_view.job_id, job_id,
        "gRPC must attribute the run to the job gRPC created"
    );

    // --- 6. invariants that are not about transports ------------------------
    let run = runs.get(run_id).await.unwrap();
    assert_eq!(
        run.job_id.0.to_string(),
        job_id,
        "run must belong to the job"
    );
    assert_eq!(run.attempt, 1, "exactly one claim attempt");

    grpc.shutdown().await;
}

/// Polls GraphQL until `run_id` is terminal or `DEADLINE` expires, returning
/// the state name it settled on.
///
/// Panics with the last state actually observed. A bare "timed out" says
/// nothing: "last state observed: claimed" says the worker never completed the
/// run, while "last state observed: <not yet materialized>" says the job
/// resolved but carried no such run at all -- a different bug in a different
/// place.
async fn await_terminal_over_graphql<J, R, C>(
    schema: &api_graphql::SchedulerSchema<J, R, C>,
    job_id: &str,
    run_id: RunId,
) -> String
where
    J: JobRepository + 'static,
    R: RunRepository + Clone + 'static,
    C: scheduler_domain::Clock,
{
    // Nested through `runs` on purpose: that is the field the DataLoader backs,
    // and the top-level `run(id:)` query would bypass it.
    const QUERY: &str = r#"
        query ($id: ID!) {
          job(id: $id) {
            id
            runs { id state }
          }
        }
    "#;

    let started = std::time::Instant::now();
    let wanted = run_id.0.to_string();
    let mut last = "<not yet materialized>".to_string();

    while started.elapsed() < DEADLINE {
        // Variables, not interpolation: an id spliced into a query document is
        // an injection point, and a test is the wrong place to model that.
        let request = api_graphql::Request::new(QUERY).variables(
            api_graphql::Variables::from_json(serde_json::json!({ "id": job_id })),
        );
        let response = schema.execute(request).await;
        assert!(
            response.errors.is_empty(),
            "GraphQL query must not error: {:?}",
            response.errors
        );

        let data = serde_json::to_value(&response.data).expect("GraphQL data must serialize");
        let job = &data["job"];
        assert!(
            !job.is_null(),
            "GraphQL must resolve the job gRPC created (id {job_id})"
        );
        assert_eq!(
            job["id"].as_str(),
            Some(job_id),
            "GraphQL must return the job that was asked for"
        );

        let runs = job["runs"].as_array().expect("runs must be a list");
        if let Some(run) = runs
            .iter()
            .find(|r| r["id"].as_str() == Some(wanted.as_str()))
        {
            let state = run["state"].as_str().unwrap_or("<unreadable>").to_string();
            last = state.clone();
            if matches!(state.as_str(), "succeeded" | "failed" | "dead") {
                return state;
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    panic!(
        "run {run_id:?} of job {job_id} did not reach a terminal state within \
         {DEADLINE:?}; last state observed: {last}"
    );
}

/// `GET /runs/{id}` through the REST router, as a JSON value.
async fn rest_run(pool: &sqlx::PgPool, run_id: RunId) -> serde_json::Value {
    let res = app(AppState::new(pool.clone()))
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{}", run_id.0))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "REST must find the run");
    let body = res.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

/// A tonic server on an ephemeral port, and a client for it.
///
/// Port 0, not a fixed one: this test runs alongside whatever else is on the
/// machine and inside CI, and a hardcoded port is a collision waiting for the
/// wrong moment. The address is read back from the bound listener, so the
/// client always dials the port the kernel actually assigned.
struct GrpcHarness {
    addr: std::net::SocketAddr,
    stop: tokio::sync::oneshot::Sender<()>,
    joined: tokio::task::JoinHandle<()>,
}

impl GrpcHarness {
    async fn start(jobs: PgJobRepository, runs: PgRunRepository) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral port for the gRPC server");
        let addr = listener.local_addr().expect("read back the bound address");
        let (stop, stopped) = tokio::sync::oneshot::channel();

        // The listener is handed to tonic already bound, rather than passing
        // it an address to bind itself. Binding, reading the port, closing,
        // and re-binding leaves a window in which something else can take the
        // port -- rare, and therefore the worst kind of flake.
        let joined = tokio::spawn(async move {
            let served = tonic::transport::Server::builder()
                .add_service(SchedulerServer::new(SchedulerService::new(jobs, runs)))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await;
            // A server that dies mid-test would otherwise surface as a
            // connection error inside whichever RPC ran next, pointing at the
            // client rather than at what actually failed.
            served.expect("the gRPC server must not fail while the test runs");
        });

        Self { addr, stop, joined }
    }

    /// Retries the connect until the listener answers or `GRPC_READY_DEADLINE`
    /// expires.
    ///
    /// `tokio::spawn` returning does not mean the task has been polled, so the
    /// first dial can legitimately lose the race. The bound is what makes this
    /// a check rather than a hang: an unbounded retry against a server that
    /// never starts is a stuck suite, not a failure.
    async fn client(&self) -> SchedulerClient<tonic::transport::Channel> {
        let endpoint = format!("http://{}", self.addr);
        let started = std::time::Instant::now();
        let mut last_error = None;

        while started.elapsed() < GRPC_READY_DEADLINE {
            match SchedulerClient::connect(endpoint.clone()).await {
                Ok(c) => return c,
                Err(e) => {
                    last_error = Some(e);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }

        panic!(
            "the gRPC server at {endpoint} did not accept a connection within \
             {GRPC_READY_DEADLINE:?}; last error: {last_error:?}"
        );
    }

    /// Stops the server and waits for its task to finish.
    ///
    /// Explicit rather than left to drop: a dropped `JoinHandle` detaches, so
    /// a panic inside the server task -- including the `expect` in `start` --
    /// would be swallowed and this test would pass with a dead server.
    async fn shutdown(self) {
        let _ = self.stop.send(());
        self.joined
            .await
            .expect("the gRPC server task must not panic");
    }
}
