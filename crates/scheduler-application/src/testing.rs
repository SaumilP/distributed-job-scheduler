use scheduler_domain::*;
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::Mutex;

#[derive(Clone, Copy)]
pub struct FixedClock(pub OffsetDateTime);
impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        self.0
    }
}

/// The lease a claim records, mirroring the `lease_owner` /
/// `lease_expires_at` columns.
///
/// Held beside the runs rather than on `JobRun` because the domain model does
/// not carry the lease either -- it is storage bookkeeping, not part of a run
/// as the rest of the system sees one.
#[derive(Clone, Debug)]
struct Lease {
    #[allow(dead_code)] // Recorded so the fake stores what the table stores.
    owner: String,
    expires_at: OffsetDateTime,
}

#[derive(Default)]
struct Store {
    runs: Vec<JobRun>,
    leases: HashMap<RunId, Lease>,
}

#[derive(Clone)]
pub struct InMemoryRuns {
    inner: Arc<Mutex<Store>>,
}
impl Default for InMemoryRuns {
    fn default() -> Self {
        Self::new()
    }
}
impl InMemoryRuns {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Store::default())),
        }
    }
    pub async fn seed(&self, runs: Vec<JobRun>) {
        self.inner.lock().await.runs.extend(runs);
    }
    /// Every run held, in insertion order.
    ///
    /// Note this fake does **not** enforce `UNIQUE (job_id, scheduled_at)` the
    /// way the `job_runs` table does, so a caller counting rows here is
    /// counting *proposals*, not stored runs. That is deliberate: it is what
    /// lets a test assert how many distinct instants the materializer proposed
    /// without the constraint hiding the answer.
    pub async fn snapshot(&self) -> Vec<JobRun> {
        self.inner.lock().await.runs.clone()
    }

    /// When `id`'s lease expires, or `None` if it holds no lease.
    ///
    /// Exposed so a test can assert the fake clears the lease exactly where
    /// the adapter does -- otherwise "the lease was cleared" is a claim only
    /// the Postgres tests can check, and the fake is free to lie about it.
    pub async fn lease_expiry(&self, id: RunId) -> Option<OffsetDateTime> {
        self.inner
            .lock()
            .await
            .leases
            .get(&id)
            .map(|l| l.expires_at)
    }

    /// Forces `id`'s lease to `expires_at`, standing in for the raw `UPDATE`
    /// the Postgres tests use to simulate an engine that died.
    ///
    /// Necessary because the fake's clock is injected by the caller: there is
    /// no way to *wait* for a lease to expire, so a test must either move the
    /// `now` it passes to `reclaim_expired` past the lease or push the lease
    /// into the past. Both make the expiry observable; a test that does
    /// neither would pass whether or not the expiry predicate exists.
    pub async fn force_lease_expiry(&self, id: RunId, expires_at: OffsetDateTime) {
        if let Some(lease) = self.inner.lock().await.leases.get_mut(&id) {
            lease.expires_at = expires_at;
        }
    }
}
impl RunRepository for InMemoryRuns {
    fn insert_runs(
        &self,
        runs: &[JobRun],
    ) -> impl std::future::Future<Output = DomainResult<()>> + Send {
        let inner = self.inner.clone();
        let runs = runs.to_vec();
        async move {
            inner.lock().await.runs.extend(runs);
            Ok(())
        }
    }
    /// Mirrors the adapter on all three counts that matter here.
    ///
    /// 1. Records a lease expiring `LEASE_SECS` after `now`, so
    ///    `reclaim_expired` below has something real to read. A fake that
    ///    skipped the lease would make every reclaim test vacuous.
    /// 2. Increments `attempt`, as `attempt = attempt + 1` does. Without this
    ///    the cap below is unreachable through the fake and the fake would
    ///    happily retry forever.
    /// 3. Buries a candidate that has reached `MAX_ATTEMPTS` as `Dead`
    ///    instead of claiming it, and excludes it from the returned batch.
    ///
    /// The burial counts against `limit`, as it does in the adapter: the
    /// candidate set is bounded before it is split into claimed and buried.
    fn claim_due(
        &self,
        now: OffsetDateTime,
        limit: i64,
        owner: &str,
        per_tenant_cap: i64,
    ) -> impl std::future::Future<Output = DomainResult<ClaimOutcome>> + Send {
        let inner = self.inner.clone();
        let owner = owner.to_string();
        async move {
            let mut guard = inner.lock().await;
            let store = &mut *guard;
            let mut claimed = Vec::new();
            // Counts burials the same way the adapter does, so the
            // `runs_buried` metric is exercised against this fake too. A fake
            // that dropped the count would make every buried-metric test
            // vacuous.
            let mut buried = 0u64;

            // Which store indices are candidates, in claim order.
            //
            // `cap <= 0`: unchanged behaviour -- scan in insertion order and take
            // due rows up to `limit`, exactly as before, so every existing test
            // sees the same fake. `cap > 0`: rank due rows oldest-first, admit
            // only each tenant's first `cap`, mirroring the adapter's `ranked`
            // CTE; a fake that ignored the cap would let the fairness tests pass
            // against a use case that never capped.
            let order: Vec<usize> = if per_tenant_cap > 0 {
                let mut due: Vec<usize> = (0..store.runs.len())
                    .filter(|&i| {
                        store.runs[i].state == RunState::Pending
                            && store.runs[i].scheduled_at <= now
                    })
                    .collect();
                due.sort_by(|&a, &b| {
                    store.runs[a]
                        .scheduled_at
                        .cmp(&store.runs[b].scheduled_at)
                        .then(store.runs[a].id.0.cmp(&store.runs[b].id.0))
                });
                let mut per_tenant: HashMap<String, i64> = HashMap::new();
                due.into_iter()
                    .filter(|&i| {
                        let count = per_tenant
                            .entry(store.runs[i].tenant.0.clone())
                            .or_insert(0);
                        if *count >= per_tenant_cap {
                            false
                        } else {
                            *count += 1;
                            true
                        }
                    })
                    .collect()
            } else {
                (0..store.runs.len())
                    .filter(|&i| {
                        store.runs[i].state == RunState::Pending
                            && store.runs[i].scheduled_at <= now
                    })
                    .collect()
            };

            // The first `limit` candidates, claimed or buried. Burial counts
            // against `limit` exactly as in the adapter (the candidate set is
            // bounded before the claimed/buried split).
            for i in order.into_iter().take(limit.max(0) as usize) {
                let id = store.runs[i].id;
                if store.runs[i].attempt >= MAX_ATTEMPTS {
                    // Out of attempts: terminal, and not handed to a worker.
                    store.runs[i].state = RunState::Dead;
                    store.leases.remove(&id);
                    buried += 1;
                    continue;
                }
                store.runs[i].state = RunState::Claimed;
                store.runs[i].attempt += 1;
                store.leases.insert(
                    id,
                    Lease {
                        owner: owner.clone(),
                        expires_at: now + time::Duration::seconds(LEASE_SECS),
                    },
                );
                claimed.push(store.runs[i].clone());
            }
            Ok(ClaimOutcome { claimed, buried })
        }
    }
    /// Mirrors the adapter's `claim_ids`: claim exactly the given ids that are
    /// still pending and due, under the same per-tenant cap, and silently drop
    /// any id that is no longer claimable — the hint-not-truth behaviour a fake
    /// must reproduce or the Redis-path tests pass against a broken adapter.
    fn claim_ids(
        &self,
        ids: &[RunId],
        now: OffsetDateTime,
        owner: &str,
        per_tenant_cap: i64,
    ) -> impl std::future::Future<Output = DomainResult<ClaimOutcome>> + Send {
        let inner = self.inner.clone();
        let owner = owner.to_string();
        let id_set: std::collections::HashSet<RunId> = ids.iter().copied().collect();
        async move {
            let mut guard = inner.lock().await;
            let store = &mut *guard;
            let mut claimed = Vec::new();
            let mut buried = 0u64;

            let is_candidate = |run: &JobRun| {
                run.state == RunState::Pending
                    && run.scheduled_at <= now
                    && id_set.contains(&run.id)
            };
            let order: Vec<usize> = if per_tenant_cap > 0 {
                let mut due: Vec<usize> = (0..store.runs.len())
                    .filter(|&i| is_candidate(&store.runs[i]))
                    .collect();
                due.sort_by(|&a, &b| {
                    store.runs[a]
                        .scheduled_at
                        .cmp(&store.runs[b].scheduled_at)
                        .then(store.runs[a].id.0.cmp(&store.runs[b].id.0))
                });
                let mut per_tenant: HashMap<String, i64> = HashMap::new();
                due.into_iter()
                    .filter(|&i| {
                        let count = per_tenant
                            .entry(store.runs[i].tenant.0.clone())
                            .or_insert(0);
                        if *count >= per_tenant_cap {
                            false
                        } else {
                            *count += 1;
                            true
                        }
                    })
                    .collect()
            } else {
                (0..store.runs.len())
                    .filter(|&i| is_candidate(&store.runs[i]))
                    .collect()
            };

            // No `take(limit)`: the id set is already the batch.
            for i in order {
                let id = store.runs[i].id;
                if store.runs[i].attempt >= MAX_ATTEMPTS {
                    store.runs[i].state = RunState::Dead;
                    store.leases.remove(&id);
                    buried += 1;
                    continue;
                }
                store.runs[i].state = RunState::Claimed;
                store.runs[i].attempt += 1;
                store.leases.insert(
                    id,
                    Lease {
                        owner: owner.clone(),
                        expires_at: now + time::Duration::seconds(LEASE_SECS),
                    },
                );
                claimed.push(store.runs[i].clone());
            }
            Ok(ClaimOutcome { claimed, buried })
        }
    }
    /// Clears the lease along with the state, as the adapter does: a released
    /// run is owned by nobody, and a stale lease left behind would make it a
    /// candidate for `reclaim_expired` on top of being `Pending` already.
    fn release(&self, ids: &[RunId]) -> impl std::future::Future<Output = DomainResult<()>> + Send {
        let inner = self.inner.clone();
        let ids = ids.to_vec();
        async move {
            let mut guard = inner.lock().await;
            let store = &mut *guard;
            for run in store.runs.iter_mut() {
                // Scoped to `claimed`, as the adapter's `WHERE ... AND state =
                // 'claimed'` is: a release races the worker it compensates
                // for, and must not drag a progressed run backwards.
                if ids.contains(&run.id) && run.state == RunState::Claimed {
                    run.state = RunState::Pending;
                    store.leases.remove(&run.id);
                }
            }
            Ok(())
        }
    }
    /// Mirrors the adapter's `reclaim_expired`, including both predicates.
    ///
    /// A permissive fake here is the specific trap this phase invites: the
    /// engine's reaper-loop tests run against this type, so a fake that
    /// reclaimed live leases, or resurrected terminal runs, would let those
    /// tests pass against a broken adapter.
    fn reclaim_expired(
        &self,
        now: OffsetDateTime,
        limit: i64,
    ) -> impl std::future::Future<Output = DomainResult<Vec<RunId>>> + Send {
        let inner = self.inner.clone();
        async move {
            let mut guard = inner.lock().await;
            let store = &mut *guard;
            let mut out = Vec::new();
            for run in store.runs.iter_mut() {
                if out.len() as i64 >= limit {
                    break;
                }
                if run.state != RunState::Claimed {
                    continue;
                }
                let expired = store
                    .leases
                    .get(&run.id)
                    .is_some_and(|l| l.expires_at < now);
                if !expired {
                    continue;
                }
                run.state = RunState::Pending;
                // `attempt` untouched: the attempt was really consumed.
                store.leases.remove(&run.id);
                out.push(run.id);
            }
            Ok(out)
        }
    }
    /// Mirrors the Postgres adapter's semantics exactly, including the
    /// boolean and the terminal guard. A fake that always returned `true`
    /// would let the worker's duplicate-suppression tests pass against a
    /// broken adapter -- the fake has to be as strict as the real thing or it
    /// is not a test double, it is a way to avoid testing.
    fn complete(
        &self,
        id: RunId,
        outcome: RunState,
    ) -> impl std::future::Future<Output = DomainResult<bool>> + Send {
        let inner = self.inner.clone();
        async move {
            if !matches!(
                outcome,
                RunState::Succeeded | RunState::Failed | RunState::Dead
            ) {
                return Err(DomainError::Invalid(format!(
                    "complete requires a terminal outcome, got {outcome:?}"
                )));
            }
            let mut guard = inner.lock().await;
            let store = &mut *guard;
            let Some(run) = store.runs.iter_mut().find(|r| r.id == id) else {
                return Ok(false);
            };
            if matches!(
                run.state,
                RunState::Succeeded | RunState::Failed | RunState::Dead
            ) {
                return Ok(false);
            }
            run.state = outcome;
            // Completion clears the lease, as in the adapter. A terminal run
            // that still held one would be a candidate for `reclaim_expired`
            // if its state predicate were ever weakened.
            store.leases.remove(&id);
            Ok(true)
        }
    }
    /// Honours the `before` bound and the per-job limit, like the Postgres
    /// adapter.
    ///
    /// A fake that ignored either would let the GraphQL tests pass against an
    /// adapter that silently returned everything -- the same trap the
    /// `complete` fake was documented against. The `before` bound especially:
    /// it is applied *before* the truncate, mirroring the adapter's filter
    /// sitting inside the ranked subquery. Filtering after the truncate would
    /// make the fake agree with a broken adapter on small fixtures and
    /// disagree on real data.
    fn runs_for_jobs(
        &self,
        job_ids: &[JobId],
        before: OffsetDateTime,
        limit_per_job: i64,
    ) -> impl std::future::Future<Output = DomainResult<Vec<JobRun>>> + Send {
        let inner = self.inner.clone();
        let ids = job_ids.to_vec();
        async move {
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            let guard = inner.lock().await;
            let mut out = Vec::new();
            for id in &ids {
                let mut for_job: Vec<JobRun> = guard
                    .runs
                    .iter()
                    .filter(|r| r.job_id == *id && r.scheduled_at <= before)
                    .cloned()
                    .collect();
                // Newest first, matching the adapter's ORDER BY.
                for_job.sort_by(|a, b| b.scheduled_at.cmp(&a.scheduled_at));
                for_job.truncate(limit_per_job.max(0) as usize);
                out.extend(for_job);
            }
            Ok(out)
        }
    }
    fn get(&self, id: RunId) -> impl std::future::Future<Output = DomainResult<JobRun>> + Send {
        let inner = self.inner.clone();
        async move {
            inner
                .lock()
                .await
                .runs
                .iter()
                .find(|r| r.id == id)
                .cloned()
                .ok_or(DomainError::NotFound)
        }
    }
}

#[derive(Clone)]
pub struct RecordingPublisher {
    published: Arc<Mutex<Vec<RunId>>>,
    fail_ids: Arc<Mutex<Vec<RunId>>>,
}
impl Default for RecordingPublisher {
    fn default() -> Self {
        Self::new()
    }
}
impl RecordingPublisher {
    pub fn new() -> Self {
        Self {
            published: Arc::new(Mutex::new(Vec::new())),
            fail_ids: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Makes `publish_run` fail for exactly these run ids.
    ///
    /// A publisher that can only succeed cannot exercise the compensating
    /// release in `ClaimAndDispatch` -- which is how a lost-run bug survived
    /// undetected through Phase 1 and 2a.
    pub async fn fail_for(&self, ids: &[RunId]) {
        *self.fail_ids.lock().await = ids.to_vec();
    }

    pub async fn count(&self) -> usize {
        self.published.lock().await.len()
    }

    pub async fn published_ids(&self) -> Vec<RunId> {
        self.published.lock().await.clone()
    }
}
impl EventPublisher for RecordingPublisher {
    fn publish_run(
        &self,
        run: &JobRun,
    ) -> impl std::future::Future<Output = DomainResult<()>> + Send {
        let published = self.published.clone();
        let fail_ids = self.fail_ids.clone();
        let id = run.id;
        async move {
            if fail_ids.lock().await.contains(&id) {
                return Err(DomainError::Publish(format!("injected failure for {id:?}")));
            }
            published.lock().await.push(id);
            Ok(())
        }
    }
}

/// In-memory `JobRepository` for engine-loop tests.
///
/// `list_active` honors the limit and returns insertion order, mirroring the
/// Postgres adapter's `ORDER BY created_at`. A fake that ignored the limit
/// would let a loop test pass against an engine that never paged.
#[derive(Clone)]
pub struct InMemoryJobs {
    inner: Arc<Mutex<Vec<Job>>>,
}
impl Default for InMemoryJobs {
    fn default() -> Self {
        Self::new()
    }
}
impl InMemoryJobs {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }
}
impl JobRepository for InMemoryJobs {
    fn insert(&self, job: &Job) -> impl std::future::Future<Output = DomainResult<()>> + Send {
        let inner = self.inner.clone();
        let job = job.clone();
        async move {
            inner.lock().await.push(job);
            Ok(())
        }
    }
    fn get(&self, id: JobId) -> impl std::future::Future<Output = DomainResult<Job>> + Send {
        let inner = self.inner.clone();
        async move {
            inner
                .lock()
                .await
                .iter()
                .find(|j| j.id == id)
                .cloned()
                .ok_or(DomainError::NotFound)
        }
    }
    fn list_active(
        &self,
        limit: i64,
    ) -> impl std::future::Future<Output = DomainResult<Vec<Job>>> + Send {
        let inner = self.inner.clone();
        async move {
            Ok(inner
                .lock()
                .await
                .iter()
                .take(limit.max(0) as usize)
                .cloned()
                .collect())
        }
    }
}

/// A [`Metrics`] sink that records what it was told, so a test can assert an
/// instrumented path recorded the metric it claims to — without standing up a
/// Prometheus registry.
///
/// Backed by a `std::sync::Mutex`, not the tokio one aliased above: the
/// `Metrics` port is synchronous by design (recording must not be an await
/// point on a hot loop), so its critical sections never cross a yield and an
/// async mutex would be the wrong tool.
#[derive(Clone, Default)]
pub struct RecordingMetrics {
    counters: Arc<std::sync::Mutex<HashMap<Metric, u64>>>,
    observations: Arc<std::sync::Mutex<Vec<(Metric, f64)>>>,
}

impl RecordingMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Total increments recorded against a counter metric.
    pub fn count(&self, metric: Metric) -> u64 {
        self.counters
            .lock()
            .expect("metrics lock poisoned")
            .get(&metric)
            .copied()
            .unwrap_or(0)
    }

    /// Every value observed for a histogram metric, in call order.
    pub fn observations(&self, metric: Metric) -> Vec<f64> {
        self.observations
            .lock()
            .expect("metrics lock poisoned")
            .iter()
            .filter(|(m, _)| *m == metric)
            .map(|(_, v)| *v)
            .collect()
    }
}

impl Metrics for RecordingMetrics {
    fn incr(&self, metric: Metric, by: u64) {
        *self
            .counters
            .lock()
            .expect("metrics lock poisoned")
            .entry(metric)
            .or_default() += by;
    }

    fn observe(&self, metric: Metric, value: f64) {
        self.observations
            .lock()
            .expect("metrics lock poisoned")
            .push((metric, value));
    }
}

/// A [`Metrics`] sink that discards everything. The default for any use case or
/// loop whose test does not assert on metrics, and a legitimate production wire
/// for a process that exports nothing.
#[derive(Clone, Copy, Default)]
pub struct NoopMetrics;

impl Metrics for NoopMetrics {
    fn incr(&self, _metric: Metric, _by: u64) {}
    fn observe(&self, _metric: Metric, _value: f64) {}
}
