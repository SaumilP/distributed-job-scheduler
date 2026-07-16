use crate::model::{Job, JobId, JobRun, RunId, RunState};
#[allow(unused_imports)] // Referenced from the `claim_due` doc comment.
use crate::model::{LEASE_SECS, MAX_ATTEMPTS};
use std::future::Future;
use time::OffsetDateTime;

#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("not found")]
    NotFound,
    #[error("storage error: {0}")]
    Storage(String),
    #[error("publish error: {0}")]
    Publish(String),
    #[error("invalid: {0}")]
    Invalid(String),
}

pub type DomainResult<T> = Result<T, DomainError>;

/// Wall clock — injected so scheduling is testable without real time.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> OffsetDateTime;
}

pub trait JobRepository: Send + Sync + 'static {
    fn insert(&self, job: &Job) -> impl Future<Output = DomainResult<()>> + Send;
    fn get(&self, id: crate::model::JobId) -> impl Future<Output = DomainResult<Job>> + Send;

    /// Active jobs the materializer should produce runs for, capped at `limit`.
    ///
    /// The cap is not optional politeness. The engine calls this every tick,
    /// and an unbounded read of every job in a multi-tenant table is precisely
    /// the query that works in a demo and falls over in production. Callers
    /// that need everything must page.
    ///
    /// "Active" is the repository's concern, not the caller's: a paused job
    /// must stop producing runs without the engine knowing why.
    fn list_active(&self, limit: i64) -> impl Future<Output = DomainResult<Vec<Job>>> + Send;
}

/// What a single `claim_due` pass did: the runs it handed out, and how many it
/// buried on the way.
///
/// **Burial is only observable here.** A run that has used its last attempt is
/// moved to `Dead` *inside* `claim_due` and deliberately excluded from the
/// claimed batch — no later stage sees it. Returning the count is the only way
/// the `runs_buried` metric can be recorded without a second query racing the
/// first. It is a count, not the ids, because nothing acts on a buried run; it
/// is terminal, and only its rate is worth knowing.
///
/// Derefs to the claimed slice so the overwhelmingly common caller — one that
/// only cares about the runs it got — reads `.len()`, `.iter()`, and indexing
/// unchanged, while the rarer caller that reports metrics reaches for `.buried`.
#[derive(Debug, Default)]
pub struct ClaimOutcome {
    /// The runs flipped to `Claimed` and handed to this owner.
    pub claimed: Vec<JobRun>,
    /// How many due candidates were buried as `Dead` this pass because they had
    /// exhausted [`MAX_ATTEMPTS`]. Counts against the same `limit` as claims.
    pub buried: u64,
}

impl std::ops::Deref for ClaimOutcome {
    type Target = [JobRun];
    fn deref(&self) -> &[JobRun] {
        &self.claimed
    }
}

pub trait RunRepository: Send + Sync + 'static {
    /// Persist newly materialized runs (state = Pending).
    fn insert_runs(&self, runs: &[JobRun]) -> impl Future<Output = DomainResult<()>> + Send;

    /// Atomically claim up to `limit` due (Pending, scheduled_at <= now) runs for
    /// `owner`, flipping them to Claimed. Non-blocking across concurrent
    /// claimers: contended rows are skipped rather than waited on, so no
    /// central coordinator is required.
    ///
    /// Records a lease (`lease_owner` / `lease_expires_at`) on the claimed
    /// rows, expiring [`LEASE_SECS`] after the supplied `now`. The lease is
    /// derived from the injected clock, never from the storage engine's own
    /// wall clock, so a skewed or simulated `now` produces a lease that
    /// tracks it. [`RunRepository::reclaim_expired`] is what reads the lease
    /// back and returns an abandoned claim to `Pending`.
    ///
    /// **Enforces the attempt cap, and is the only place that does.** A due
    /// run whose `attempt` has already reached [`MAX_ATTEMPTS`] is moved to
    /// `Dead` instead of being claimed, so no worker ever receives a run past
    /// its cap and the run is never claimed again. Such a run is *not*
    /// included in the returned batch.
    ///
    /// This is the chokepoint because every retry path leads back here:
    /// `release` returns a run to `Pending` and so does `reclaim_expired`,
    /// and neither decrements `attempt`. Enforcing the cap in the reaper
    /// instead would leave the release-driven loop unbounded — the reaper
    /// never sees a run that was released rather than abandoned — and
    /// enforcing it in both would put one rule in two places that can
    /// disagree.
    ///
    /// The comparison is `attempt < MAX_ATTEMPTS` claims / `attempt >=
    /// MAX_ATTEMPTS` buries. `attempt` counts claims already made, so a run
    /// sitting at `MAX_ATTEMPTS` has had all of them; `<=` would grant one
    /// more attempt than the constant names.
    ///
    /// Returns a [`ClaimOutcome`]: the claimed batch plus the count of runs
    /// buried this pass. It derefs to the claimed slice, so a caller that only
    /// wants the runs treats the result as `&[JobRun]` unchanged.
    ///
    /// `per_tenant_cap` bounds how many runs any single tenant may contribute to
    /// this batch, so one tenant's backlog cannot starve the others. A value
    /// `<= 0` means **no cap** — the historical behaviour — and takes a query
    /// path identical to the uncapped one. A positive cap admits only each
    /// tenant's oldest `cap` due runs before the overall `limit` and oldest-first
    /// ordering apply. It is a per-*batch* bound, an approximation of a rate, not
    /// a token bucket: a tenant can still be claimed on every tick, just not
    /// without limit within one.
    fn claim_due(
        &self,
        now: OffsetDateTime,
        limit: i64,
        owner: &str,
        per_tenant_cap: i64,
    ) -> impl Future<Output = DomainResult<ClaimOutcome>> + Send;

    /// Claim exactly the given ids that are still `pending` and due, under the
    /// same `SKIP LOCKED` and per-tenant-cap rules as [`claim_due`]. The id set
    /// is the batch, so there is no separate limit.
    ///
    /// This is the authoritative half of a hot-index claim: a fast external
    /// index (Redis) proposes candidate ids and this confirms them against the
    /// database. An id that is no longer claimable — already claimed, completed,
    /// or not yet due — simply matches nothing and is dropped, which is what
    /// lets the index be a hint that can lag or be wiped without ever causing a
    /// lost or double claim. `claim_due` remains the complete, index-free path.
    fn claim_ids(
        &self,
        ids: &[RunId],
        now: OffsetDateTime,
        owner: &str,
        per_tenant_cap: i64,
    ) -> impl Future<Output = DomainResult<ClaimOutcome>> + Send;

    /// Return claimed runs to `Pending` so they can be claimed again.
    ///
    /// This is the compensating half of claim-then-publish. `claim_due`
    /// commits the state flip before any publish happens, so a publish
    /// failure leaves a run marked `Claimed` that no worker will ever hear
    /// about -- and since `claim_due` only selects `Pending` rows, nothing
    /// would ever pick it up again. Releasing it is what keeps "at-least
    /// -once" honest.
    ///
    /// Deliberately does **not** decrement `attempt`. The attempt was really
    /// consumed, and counting it is what lets the max-attempts policy in
    /// `claim_due` terminate a run that can never be published (an unroutable
    /// subject, say) instead of spinning on it forever.
    fn release(&self, ids: &[RunId]) -> impl Future<Output = DomainResult<()>> + Send;

    /// Return up to `limit` runs whose lease has expired to `Pending`,
    /// yielding the ids actually reclaimed.
    ///
    /// This is the backstop for the failure `release` cannot compensate: an
    /// engine that dies *between* committing the claim and publishing the
    /// batch runs no compensating code at all. Its rows stay `Claimed`, and
    /// since `claim_due` selects only `Pending` rows nothing would ever pick
    /// them up again. Without this method the lease columns are written and
    /// read by nothing, and such a run is stranded forever.
    ///
    /// **Selects `state = 'claimed'` AND `lease_expires_at < now`. Both
    /// predicates are load-bearing, in opposite directions:**
    ///
    /// - Dropping the state predicate makes this a resurrection machine. A
    ///   `succeeded` run whose lease column somehow lingered would be dragged
    ///   back to `Pending` and executed a second time, and a `pending` run
    ///   would be racing the engine for work it is already eligible for.
    /// - Dropping the expiry predicate makes this *cause* the duplicate
    ///   execution it exists to repair: a live lease means the owner is still
    ///   working, and reclaiming it schedules a second execution of work
    ///   already in flight.
    ///
    /// `now` is the caller's clock, matching `claim_due`, so the comparison is
    /// against the same time base the lease was written from.
    ///
    /// **Does not touch `attempt`.** The attempt was consumed by the claim
    /// that died — the work may well have been executed before the engine
    /// lost it. Counting it is what lets a max-attempts policy eventually give
    /// up on a run that cannot be completed, instead of reaping and
    /// re-claiming it forever.
    ///
    /// `limit` is not optional, for the same reason no other read here is
    /// unbounded: reclaiming after a long outage would otherwise be a single
    /// enormous statement holding locks over the whole backlog.
    ///
    /// Idempotent: the reclaim clears the lease, so a second pass over the
    /// same rows finds nothing and returns an empty `Vec`. Concurrent reapers
    /// must not block each other — implementations skip contended rows rather
    /// than waiting on them, as the claim does.
    fn reclaim_expired(
        &self,
        now: OffsetDateTime,
        limit: i64,
    ) -> impl Future<Output = DomainResult<Vec<RunId>>> + Send;

    /// Record a terminal outcome for a run.
    ///
    /// Returns `true` if this call performed the transition, `false` if the
    /// run was already terminal. That boolean is the duplicate-suppression
    /// primitive: a worker handling a redelivered message uses it to tell
    /// "I did this work" from "this work was already done", and skips
    /// re-executing. Returning `()` would make that undecidable without a
    /// second query and a race between the two.
    ///
    /// `outcome` must be terminal (`Succeeded`, `Failed`, `Dead`). Anything
    /// else is a programming error and yields `DomainError::Invalid` rather
    /// than corrupting the state machine.
    ///
    /// Already-terminal runs are never overwritten -- a late `Failed` must not
    /// bury a recorded `Succeeded`.
    fn complete(
        &self,
        id: RunId,
        outcome: RunState,
    ) -> impl Future<Output = DomainResult<bool>> + Send;

    /// Runs belonging to any of `job_ids` scheduled at or before `before`,
    /// newest first, at most `limit_per_job` each.
    ///
    /// Exists so a GraphQL resolver can batch. `jobs { runs { ... } }` resolves
    /// runs once per job otherwise -- the N+1 fan-out that is GraphQL's
    /// characteristic performance failure, and the one real cost it adds over
    /// REST and gRPC.
    ///
    /// Takes a slice and returns a flat `Vec`; the caller groups. Returning a
    /// map would push a collection type into the port for one adapter's
    /// convenience.
    ///
    /// `limit_per_job` is not optional, and it is *per job*, not a cap on the
    /// result set. A GraphQL client can ask for a thousand jobs in one request,
    /// so an unbounded read here is exactly the query that works in a demo and
    /// falls over in production.
    ///
    /// `before` is not optional either, and it is the more interesting of the
    /// two bounds. This scheduler *writes ahead of now*: the materializer
    /// creates `Pending` runs a horizon into the future, so "the newest runs
    /// for this job" is, for any actively-scheduled job, entirely future rows.
    /// An unbounded version of this method spends the whole per-job window on
    /// runs that have not happened yet and can never show a completed one --
    /// which is what a caller asking "show me this job's runs" actually means.
    /// A caller passing `now` gets execution history; a caller passing a future
    /// instant gets the upcoming schedule. The point is that the caller
    /// chooses, rather than being handed whichever rows the materializer
    /// happened to have written.
    ///
    /// The bound is on `scheduled_at`, and implementations must apply it
    /// *before* ranking, not after: filtering a newest-first window after the
    /// fact still spends the limit on future rows and returns fewer than
    /// `limit_per_job` past ones (usually zero).
    ///
    /// An empty `job_ids` returns `Ok(vec![])` without querying.
    fn runs_for_jobs(
        &self,
        job_ids: &[JobId],
        before: OffsetDateTime,
        limit_per_job: i64,
    ) -> impl Future<Output = DomainResult<Vec<JobRun>>> + Send;

    fn get(&self, id: RunId) -> impl Future<Output = DomainResult<JobRun>> + Send;
}

pub trait EventPublisher: Send + Sync + 'static {
    /// Publish a run for execution (dispatcher -> workers).
    fn publish_run(&self, run: &JobRun) -> impl Future<Output = DomainResult<()>> + Send;
}

/// The closed set of metrics this system records.
///
/// **A domain enum, not a string.** A string metric name means a typo compiles
/// and silently records nothing; an enum means the set is closed, greppable,
/// and the adapter's name/label mapping lives in exactly one place. It also
/// carries no data and needs nothing beyond `core`, which is what lets it live
/// in the hexagon without adding a dependency — the invariant this crate has
/// held since Phase 1.
///
/// Counter vs. histogram is the *adapter's* concern (it owns the mapping);
/// callers record a counter with [`Metrics::incr`] and a histogram with
/// [`Metrics::observe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Metric {
    /// Counter. Runs *proposed* by the materializer — not created. `insert_runs`
    /// does not report affected rows, so this counts proposals, most of which
    /// `ON CONFLICT DO NOTHING` absorbs; it is not a creation rate.
    RunsMaterialized,
    /// Counter. Runs claimed for dispatch — claim throughput.
    RunsClaimed,
    /// Histogram. Size of each claim batch: whether batches come back full
    /// (saturated) or short (idle).
    ClaimBatchSize,
    /// Counter. Runs successfully published to the broker.
    RunsPublished,
    /// Counter. Publish attempts that failed — the dual-write's failure rate.
    PublishFailures,
    /// Counter. Claimed runs released back to `Pending` after a publish failure
    /// — the compensation rate; nonzero means publishes are failing.
    RunsReleased,
    /// Counter. Expired leases reclaimed by the reaper — engine crash rate;
    /// nonzero in steady state means engines are dying.
    RunsReclaimed,
    /// Counter. Runs buried as `Dead` after exhausting attempts; nonzero means
    /// work is being abandoned.
    RunsBuried,
    /// Histogram. `now - scheduled_at` at claim time, in seconds — how late runs
    /// are. The SLO that actually matters.
    DueLagSeconds,
    /// Histogram. Worker execution wall time, in seconds — the input to the
    /// `LEASE_SECS` argument.
    ExecutionSeconds,
}

/// Recording sink for [`Metric`]s.
///
/// **Synchronous on purpose.** Recording a metric must not be `await`-able: an
/// instrumented hot loop that could suspend at every measurement point changes
/// the thing it measures. For the same reason implementations must not block,
/// must not allocate per call, and must not take a contended lock on the
/// recording path — the claim loop calls this per run.
///
/// Object-safe by construction (no generics, no RPITIT), unlike the async ports
/// in this crate — which is what lets a single handle be shared behind an
/// `Arc` (see the blanket impl below) rather than threaded as a type parameter
/// everywhere.
pub trait Metrics: Send + Sync + 'static {
    /// Add `by` to a counter metric.
    fn incr(&self, metric: Metric, by: u64);
    /// Record one observation of a histogram metric.
    fn observe(&self, metric: Metric, value: f64);
}

/// Lets an `Arc<M>` be used anywhere an `M: Metrics` is expected, so one
/// registry can be shared across the loops that write it without every use case
/// carrying an extra owned copy.
impl<M: Metrics + ?Sized> Metrics for std::sync::Arc<M> {
    fn incr(&self, metric: Metric, by: u64) {
        (**self).incr(metric, by)
    }
    fn observe(&self, metric: Metric, value: f64) {
        (**self).observe(metric, value)
    }
}
