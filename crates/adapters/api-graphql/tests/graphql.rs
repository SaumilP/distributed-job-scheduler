use api_graphql::{MAX_PAGE, build};
use scheduler_application::testing::{FixedClock, InMemoryJobs, InMemoryRuns};
use scheduler_domain::*;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

/// Wraps a `RunRepository` and counts batch lookups.
///
/// The N+1 assertion is a *call count*, not a timing measurement. A timing
/// assertion is flaky and proves nothing on a fast machine; the number of
/// lookups is the thing that actually changes when batching breaks.
#[derive(Clone)]
struct CountingRuns {
    inner: InMemoryRuns,
    batch_calls: Arc<AtomicUsize>,
}

impl CountingRuns {
    fn new(inner: InMemoryRuns) -> Self {
        Self {
            inner,
            batch_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn batch_calls(&self) -> usize {
        self.batch_calls.load(Ordering::SeqCst)
    }
}

impl RunRepository for CountingRuns {
    fn insert_runs(&self, runs: &[JobRun]) -> impl Future<Output = DomainResult<()>> + Send {
        self.inner.insert_runs(runs)
    }
    fn claim_due(
        &self,
        now: time::OffsetDateTime,
        limit: i64,
        owner: &str,
        per_tenant_cap: i64,
    ) -> impl Future<Output = DomainResult<scheduler_domain::ClaimOutcome>> + Send {
        self.inner.claim_due(now, limit, owner, per_tenant_cap)
    }
    fn claim_ids(
        &self,
        ids: &[RunId],
        now: time::OffsetDateTime,
        owner: &str,
        per_tenant_cap: i64,
    ) -> impl Future<Output = DomainResult<scheduler_domain::ClaimOutcome>> + Send {
        self.inner.claim_ids(ids, now, owner, per_tenant_cap)
    }
    fn release(&self, ids: &[RunId]) -> impl Future<Output = DomainResult<()>> + Send {
        self.inner.release(ids)
    }
    fn reclaim_expired(
        &self,
        now: time::OffsetDateTime,
        limit: i64,
    ) -> impl Future<Output = DomainResult<Vec<RunId>>> + Send {
        self.inner.reclaim_expired(now, limit)
    }
    fn complete(
        &self,
        id: RunId,
        outcome: RunState,
    ) -> impl Future<Output = DomainResult<bool>> + Send {
        self.inner.complete(id, outcome)
    }
    fn runs_for_jobs(
        &self,
        job_ids: &[JobId],
        before: time::OffsetDateTime,
        limit_per_job: i64,
    ) -> impl Future<Output = DomainResult<Vec<JobRun>>> + Send {
        self.batch_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.runs_for_jobs(job_ids, before, limit_per_job)
    }
    fn get(&self, id: RunId) -> impl Future<Output = DomainResult<JobRun>> + Send {
        self.inner.get(id)
    }
}

/// A `RunRepository` whose reads always fail, so the error paths can be
/// observed. The message is deliberately shaped like a real sqlx error: those
/// carry the host, port, database user and table name.
#[derive(Clone)]
struct FailingRuns;

const LEAKY_MESSAGE: &str = "error returned from database: relation \"job_runs\" \
     does not exist (host=db.internal port=5432 user=scheduler)";

impl RunRepository for FailingRuns {
    async fn insert_runs(&self, _runs: &[JobRun]) -> DomainResult<()> {
        Ok(())
    }
    async fn claim_due(
        &self,
        _now: time::OffsetDateTime,
        _limit: i64,
        _owner: &str,
        _per_tenant_cap: i64,
    ) -> DomainResult<scheduler_domain::ClaimOutcome> {
        Ok(scheduler_domain::ClaimOutcome::default())
    }
    async fn claim_ids(
        &self,
        _ids: &[RunId],
        _now: time::OffsetDateTime,
        _owner: &str,
        _per_tenant_cap: i64,
    ) -> DomainResult<scheduler_domain::ClaimOutcome> {
        Ok(scheduler_domain::ClaimOutcome::default())
    }
    async fn release(&self, _ids: &[RunId]) -> DomainResult<()> {
        Ok(())
    }
    async fn reclaim_expired(
        &self,
        _now: time::OffsetDateTime,
        _limit: i64,
    ) -> DomainResult<Vec<RunId>> {
        Ok(Vec::new())
    }
    async fn complete(&self, _id: RunId, _outcome: RunState) -> DomainResult<bool> {
        Ok(false)
    }
    async fn runs_for_jobs(
        &self,
        _job_ids: &[JobId],
        _before: time::OffsetDateTime,
        _limit_per_job: i64,
    ) -> DomainResult<Vec<JobRun>> {
        Err(DomainError::Storage(LEAKY_MESSAGE.into()))
    }
    async fn get(&self, _id: RunId) -> DomainResult<JobRun> {
        Err(DomainError::Storage(LEAKY_MESSAGE.into()))
    }
}

fn job(tenant: &str) -> Job {
    Job::new(
        JobId(Uuid::new_v4()),
        tenant,
        Schedule::Interval { every_secs: 60 },
        "http://svc/run",
        time::macros::datetime!(2026-07-19 10:00:00 UTC),
    )
    .unwrap()
}

fn run_for(job_id: JobId, offset: i64) -> JobRun {
    run_at(
        job_id,
        time::OffsetDateTime::now_utc() - time::Duration::seconds(offset),
        RunState::Pending,
    )
}

fn run_at(job_id: JobId, scheduled_at: time::OffsetDateTime, state: RunState) -> JobRun {
    JobRun {
        id: RunId(Uuid::new_v4()),
        job_id,
        tenant: TenantId("acme".into()),
        scheduled_at,
        state,
        attempt: 0,
    }
}

/// A clock reading the present moment.
///
/// Called *after* a fixture is seeded, so every `run_for` row is strictly in
/// its past. Every `build` in this file goes through a fixed clock rather than
/// the system one -- the schema now bounds nested `runs` at the clock's
/// instant, and a test that could not pin that instant could not distinguish
/// "bounded correctly" from "not bounded at all".
fn now_clock() -> FixedClock {
    FixedClock(time::OffsetDateTime::now_utc())
}

/// A `Clock` whose instant can be moved forward mid-test.
///
/// `FixedClock` cannot distinguish "read the clock per load" from "read it once
/// at construction" -- both answer the same value forever. Only a clock that
/// moves can, which is why this exists alongside it. `std::sync::Mutex` rather
/// than tokio's: `Clock::now` is synchronous, and the guard is never held
/// across an await.
#[derive(Clone)]
struct MovingClock(Arc<std::sync::Mutex<time::OffsetDateTime>>);

impl MovingClock {
    fn at(t: time::OffsetDateTime) -> Self {
        Self(Arc::new(std::sync::Mutex::new(t)))
    }
    fn advance(&self, by: time::Duration) {
        *self.0.lock().unwrap() += by;
    }
}

impl Clock for MovingClock {
    fn now(&self) -> time::OffsetDateTime {
        *self.0.lock().unwrap()
    }
}

async fn fixture(jobs_with_runs: usize) -> (InMemoryJobs, CountingRuns, Vec<JobId>) {
    let jobs = InMemoryJobs::new();
    let runs = CountingRuns::new(InMemoryRuns::new());
    let mut ids = Vec::new();
    for _ in 0..jobs_with_runs {
        let j = job("acme");
        jobs.insert(&j).await.unwrap();
        runs.insert_runs(&[run_for(j.id, 1), run_for(j.id, 2)])
            .await
            .unwrap();
        ids.push(j.id);
    }
    (jobs, runs, ids)
}

/// The N+1 assertion — the reason `DataLoader` is here at all.
///
/// Three jobs, each with runs, resolved in one request. The batch loader must
/// be called ONCE. Without batching this is three calls, and with thirty jobs
/// it would be thirty — the fan-out that makes a nested GraphQL query quietly
/// expensive.
#[tokio::test]
async fn nested_runs_are_batched_into_one_lookup() {
    let (jobs, runs, ids) = fixture(3).await;
    let schema = build(jobs, runs.clone(), now_clock(), 10);

    let res = schema.execute("{ jobs { id runs { id } } }").await;
    assert!(res.errors.is_empty(), "query failed: {:?}", res.errors);

    assert_eq!(
        runs.batch_calls(),
        1,
        "three jobs must cost one batched lookup, not one per job"
    );

    // Batching must not lose data: every job's runs still came back.
    let data = res.data.into_json().unwrap();
    let returned = data["jobs"].as_array().unwrap();
    assert_eq!(returned.len(), ids.len());
    for j in returned {
        assert_eq!(
            j["runs"].as_array().unwrap().len(),
            2,
            "each job must still carry its own runs"
        );
    }
}

#[tokio::test]
async fn create_job_mutation_persists_a_job() {
    let jobs = InMemoryJobs::new();
    let runs = CountingRuns::new(InMemoryRuns::new());
    let schema = build(jobs.clone(), runs, now_clock(), 10);

    let res = schema
        .execute(
            r#"mutation { createJob(tenant: "acme", target: "http://svc/run", everySecs: 60) { id tenant } }"#,
        )
        .await;
    assert!(res.errors.is_empty(), "{:?}", res.errors);

    let data = res.data.into_json().unwrap();
    let id = data["createJob"]["id"].as_str().unwrap();
    let uuid = Uuid::parse_str(id).expect("id must be a UUID");
    assert_eq!(data["createJob"]["tenant"], "acme");

    jobs.get(JobId(uuid)).await.expect("job must be persisted");
}

/// A second transport is a second chance to make the same validation mistake.
/// The Phase 2b review found the REST surface accepting all three of these.
#[tokio::test]
async fn create_job_rejects_invalid_input() {
    let cases = [
        // (every_secs, tenant, target, what)
        (0i64, "acme", "http://x", "a zero interval"),
        (-1, "acme", "http://x", "a negative interval"),
        (
            1_000_000_000_000,
            "acme",
            "http://x",
            "an interval that would overflow time",
        ),
        (60, "", "http://x", "an empty tenant"),
        (60, "   ", "http://x", "a whitespace tenant"),
        (60, "acme", "", "an empty target"),
    ];

    for (every_secs, tenant, target, what) in cases {
        let jobs = InMemoryJobs::new();
        let runs = CountingRuns::new(InMemoryRuns::new());
        let schema = build(jobs.clone(), runs, now_clock(), 10);

        let res = schema
            .execute(format!(
                r#"mutation {{ createJob(tenant: "{tenant}", target: "{target}", everySecs: {every_secs}) {{ id }} }}"#
            ))
            .await;

        assert!(!res.errors.is_empty(), "{what} must be rejected");
        assert!(
            jobs.list_active(10).await.unwrap().is_empty(),
            "{what} must not persist a job"
        );
    }
}

/// An absent job is `null`, not an error: "not there" is a normal result, and
/// making it an error complicates every client's partial-response handling.
#[tokio::test]
async fn query_for_unknown_job_returns_null_not_an_error() {
    let jobs = InMemoryJobs::new();
    let runs = CountingRuns::new(InMemoryRuns::new());
    let schema = build(jobs, runs, now_clock(), 10);

    let res = schema
        .execute(format!(r#"{{ job(id: "{}") {{ id }} }}"#, Uuid::new_v4()))
        .await;

    assert!(res.errors.is_empty(), "{:?}", res.errors);
    assert!(res.data.into_json().unwrap()["job"].is_null());
}

/// A malformed id must not be a 500-equivalent either.
#[tokio::test]
async fn query_with_a_malformed_id_returns_null() {
    let jobs = InMemoryJobs::new();
    let runs = CountingRuns::new(InMemoryRuns::new());
    let schema = build(jobs, runs, now_clock(), 10);

    let res = schema.execute(r#"{ job(id: "not-a-uuid") { id } }"#).await;
    assert!(res.errors.is_empty(), "{:?}", res.errors);
    assert!(res.data.into_json().unwrap()["job"].is_null());
}

/// The depth limit, on the queries that can actually reach it: introspection.
///
/// No *data* query can. `RunNode` is a `SimpleObject` of scalars, so nothing
/// below `Job.runs` returns an object and the deepest data query in this schema
/// is three levels. The recursive part of the surface is the introspection
/// schema — `type { fields { type { fields { ... } } } }` nests without limit —
/// and that is what this test exercises and what `MAX_DEPTH` bounds. The bound
/// on data queries is complexity; see
/// `excessive_query_complexity_is_rejected`.
///
/// Exceeding the limit must be a clean error, not an attempt to execute.
#[tokio::test]
async fn excessive_query_depth_is_rejected() {
    let jobs = InMemoryJobs::new();
    let runs = CountingRuns::new(InMemoryRuns::new());
    let schema = build(jobs, runs.clone(), now_clock(), 10);

    // Introspection nests `type { fields { type { fields { ... } } } }`
    // indefinitely; this is well past MAX_DEPTH (8).
    let deep = "{ __schema { types { fields { type { fields {                 type { fields { type { fields { name } } } } } } } } } }";

    let res = schema.execute(deep).await;

    assert!(
        !res.errors.is_empty(),
        "a query deeper than MAX_DEPTH must be rejected"
    );
    assert_eq!(
        runs.batch_calls(),
        0,
        "a rejected query must not have executed any lookups"
    );
}

/// The limit must not reject ordinary queries -- a depth cap set too low is a
/// broken API rather than a protected one.
#[tokio::test]
async fn a_normal_nested_query_is_within_the_depth_limit() {
    let (jobs, runs, _) = fixture(1).await;
    let schema = build(jobs, runs, now_clock(), 10);

    let res = schema
        .execute("{ jobs { id tenant runs { id state } } }")
        .await;

    assert!(
        res.errors.is_empty(),
        "the ordinary nested query must be allowed: {:?}",
        res.errors
    );
}

/// The bug this port change exists for, stated end to end.
///
/// The materializer writes `Pending` runs a horizon into the *future*. Nested
/// `Job.runs` orders newest-first, so on a live stack the entire window filled
/// with runs that had not happened yet: 82 runs had reached `succeeded`, the
/// field returned 50 rows all `pending`, and the oldest row in the window was
/// ahead of the wall clock. A user asking a job for its runs got a schedule,
/// never a history.
///
/// The limit here (2) is smaller than the number of future runs (3) on purpose
/// -- that is the shape that makes the failure total rather than partial.
/// Without the clock bound the window is spent entirely on future rows and no
/// past run can appear at all.
#[tokio::test]
async fn nested_runs_show_execution_history_not_the_future_schedule() {
    let jobs = InMemoryJobs::new();
    let runs = CountingRuns::new(InMemoryRuns::new());
    let j = job("acme");
    jobs.insert(&j).await.unwrap();

    let now = time::OffsetDateTime::now_utc();
    let past: Vec<JobRun> = (1..=3)
        .map(|i| {
            run_at(
                j.id,
                now - time::Duration::seconds(i * 60),
                RunState::Succeeded,
            )
        })
        .collect();
    // As the materializer writes them: pending, ahead of now.
    let future: Vec<JobRun> = (1..=3)
        .map(|i| {
            run_at(
                j.id,
                now + time::Duration::seconds(i * 60),
                RunState::Pending,
            )
        })
        .collect();
    runs.insert_runs(&past).await.unwrap();
    runs.insert_runs(&future).await.unwrap();

    let schema = build(jobs, runs, FixedClock(now), 2);
    let res = schema
        .execute("{ jobs { runs { id state scheduledAt } } }")
        .await;
    assert!(res.errors.is_empty(), "query failed: {:?}", res.errors);

    let data = res.data.into_json().unwrap();
    let returned = data["jobs"][0]["runs"].as_array().unwrap();

    assert_eq!(returned.len(), 2, "the per-job limit still applies");
    for r in returned {
        assert_eq!(
            r["state"], "succeeded",
            "nested runs must be execution history, not the future schedule"
        );
        let at = time::OffsetDateTime::parse(
            r["scheduledAt"].as_str().unwrap(),
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        assert!(
            at <= now,
            "a run scheduled at {at} is ahead of the clock ({now}) and must not appear"
        );
    }
}

/// GraphQL errors go straight to the client, so a storage failure must not
/// carry its detail into one. A sqlx error names the host, port, database user
/// and table.
///
/// This test was specified in the phase plan and then never written. The
/// scrubbing was correct but unguarded: replacing it with
/// `Error::new(e.to_string())` passed the entire suite. Found by an
/// independent reviewer's mutation, not by the suite itself.
#[tokio::test]
async fn storage_errors_are_scrubbed_from_the_batch_loader() {
    let jobs = InMemoryJobs::new();
    jobs.insert(&job("acme")).await.unwrap();
    let schema = build(jobs, FailingRuns, now_clock(), 10);

    let res = schema.execute("{ jobs { id runs { id } } }").await;

    assert!(
        !res.errors.is_empty(),
        "the failure must surface as an error"
    );
    assert_leaks_nothing(&res.errors);
}

/// The same rule on the single-run path -- a different resolver and a
/// different error branch (`to_gql_error`, not the loader's `map_err`).
#[tokio::test]
async fn storage_errors_are_scrubbed_on_the_run_query() {
    let schema = build(InMemoryJobs::new(), FailingRuns, now_clock(), 10);

    let res = schema
        .execute(format!(r#"{{ run(id: "{}") {{ id }} }}"#, Uuid::new_v4()))
        .await;

    assert!(
        !res.errors.is_empty(),
        "the failure must surface as an error"
    );
    assert_leaks_nothing(&res.errors);
}

/// The limit that actually bounds data queries.
///
/// Depth cannot stop a *wide* query: several dozen aliases of `jobs { ... }`
/// stay two levels deep and each is a full lookup. Complexity counts fields, so
/// it rejects that shape. `MAX_COMPLEXITY` was unguarded — deleting
/// `.limit_complexity(...)` from `build` passed the entire suite, on the one
/// limit doing the real work. Found by an independent reviewer's mutation.
#[tokio::test]
async fn excessive_query_complexity_is_rejected() {
    let (jobs, runs, _) = fixture(1).await;
    let schema = build(jobs, runs.clone(), now_clock(), 10);

    // 60 aliases x (1 + 4 fields) = 300, past MAX_COMPLEXITY (256) while
    // staying two levels deep -- well inside MAX_DEPTH, so only the complexity
    // limit can reject it.
    let wide: String = (0..60)
        .map(|i| format!("a{i}: jobs(limit: 1) {{ id tenant target schedule }} "))
        .collect();
    let res = schema.execute(format!("{{ {wide} }}")).await;

    assert!(
        !res.errors.is_empty(),
        "a query past MAX_COMPLEXITY must be rejected"
    );
    assert!(
        res.errors
            .iter()
            .any(|e| e.message.to_lowercase().contains("complex")),
        "it must be the complexity limit rejecting it, not something else: {:?}",
        res.errors
    );
    assert_eq!(
        runs.batch_calls(),
        0,
        "a rejected query must not have executed any lookups"
    );
}

/// A complexity cap set too low is a broken API rather than a protected one:
/// ordinary queries must still be served.
#[tokio::test]
async fn ordinary_queries_are_within_the_complexity_limit() {
    let (jobs, runs, _) = fixture(1).await;
    let schema = build(jobs, runs, now_clock(), 10);

    for query in [
        "{ jobs { id tenant target schedule runs { id job_id: jobId state attempt scheduledAt } } }",
        "{ a: jobs { id } b: jobs { id } c: jobs { id runs { id state } } }",
    ] {
        let res = schema.execute(query).await;
        assert!(
            res.errors.is_empty(),
            "an ordinary query must be allowed: {query} -> {:?}",
            res.errors
        );
    }
}

/// `jobs(limit:)` is clamped, so a client cannot ask for the whole table.
///
/// The clamp was unguarded: raising `MAX_PAGE` from 200 to 1,000,000 passed the
/// entire suite. Found by an independent reviewer's mutation.
///
/// Note the literals. Seeding `MAX_PAGE + n` and asserting `MAX_PAGE` back
/// would be self-referential — it holds for *any* value of the constant, which
/// is precisely the mutation that has to fail here. So the fixture size and the
/// expected count are both fixed numbers, and the constant is checked against
/// the value the README publishes.
#[tokio::test]
async fn jobs_limit_is_clamped_to_max_page() {
    assert_eq!(
        MAX_PAGE, 200,
        "README.md documents a clamp of 200; changing the constant changes a \
         published contract and has to be a deliberate edit here too"
    );

    let jobs = InMemoryJobs::new();
    for _ in 0..250 {
        jobs.insert(&job("acme")).await.unwrap();
    }
    let runs = CountingRuns::new(InMemoryRuns::new());
    let schema = build(jobs, runs, now_clock(), 10);

    let res = schema.execute("{ jobs(limit: 100000) { id } }").await;
    assert!(res.errors.is_empty(), "{:?}", res.errors);

    let data = res.data.into_json().unwrap();
    let returned = data["jobs"].as_array().unwrap().len();
    assert_eq!(
        returned, 200,
        "an over-large limit must be clamped to 200, got {returned} rows"
    );
}

/// The loader must read the clock on **every load**, not once when the schema
/// is built.
///
/// A schema is constructed once at process start and then serves requests for
/// as long as the process lives, so an instant captured in `RunsLoader::new`
/// would be stale within seconds and frozen forever after — nested `runs` would
/// stop advancing and permanently hide everything that ran since boot.
///
/// Every other test in this file builds with a `FixedClock`, under which
/// per-load and per-build reads are indistinguishable. That is exactly the
/// vacuous-`FixedClock` pattern that let the materializer's anchoring bug ship
/// green, now sitting on the code path that fixed it: moving `clock.now()` into
/// `RunsLoader::new` passed the entire suite. Hence a moving clock here.
#[tokio::test]
async fn the_loader_reads_the_clock_on_every_load_not_at_build() {
    let start = time::OffsetDateTime::now_utc();
    let clock = MovingClock::at(start);

    let jobs = InMemoryJobs::new();
    let j = job("acme");
    jobs.insert(&j).await.unwrap();

    let runs = CountingRuns::new(InMemoryRuns::new());
    // One run already in the past, one still ahead of `start`.
    runs.insert_runs(&[
        run_at(
            j.id,
            start - time::Duration::seconds(30),
            RunState::Succeeded,
        ),
        run_at(j.id, start + time::Duration::seconds(30), RunState::Pending),
    ])
    .await
    .unwrap();

    // Built ONCE, then queried twice across a clock advance -- the shape the
    // live composition root has.
    let schema = build(jobs, runs, clock.clone(), 10);

    let before = run_count(&schema).await;
    assert_eq!(
        before, 1,
        "only the run at or before the clock's instant may appear"
    );

    clock.advance(time::Duration::seconds(60));

    let after = run_count(&schema).await;
    assert_eq!(
        after, 2,
        "after the clock advances past it, the second run must appear -- a \
         loader that captured the instant at build time would still return 1"
    );
}

async fn run_count<J, R, C>(schema: &api_graphql::SchedulerSchema<J, R, C>) -> usize
where
    J: JobRepository,
    R: RunRepository,
    C: Clock,
{
    let res = schema.execute("{ jobs { runs { id } } }").await;
    assert!(res.errors.is_empty(), "query failed: {:?}", res.errors);
    res.data.into_json().unwrap()["jobs"][0]["runs"]
        .as_array()
        .unwrap()
        .len()
}

fn assert_leaks_nothing(errors: &[async_graphql::ServerError]) {
    for err in errors {
        for secret in ["db.internal", "job_runs", "5432", "user=scheduler"] {
            assert!(
                !err.message.contains(secret),
                "storage detail {secret:?} leaked to the client: {}",
                err.message
            );
        }
    }
}
