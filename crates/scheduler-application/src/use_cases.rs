use scheduler_domain::{
    Clock, DomainResult, EventPublisher, Job, JobRun, Metric, Metrics, RunId, RunRepository,
    RunState,
};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// The runs this process has claimed and not yet published.
///
/// **Exists because a claim and its publish are not one atomic act, and the
/// window between them survives being dropped.** `claim_due` commits `Claimed`
/// for the whole batch before any publish is attempted; if the dispatch tick is
/// cancelled in that window -- which is exactly what a SIGTERM does, since
/// `run_until_shutdown` selects the shutdown branch against a pending tick --
/// the compensating `release` in [`ClaimAndDispatch::run`] never runs. Those
/// rows are then `Claimed` with nobody working them, and `claim_due` selects
/// only `Pending`, so nothing picks them up until the lease expires.
///
/// The reaper does eventually recover them. The point of this registry is that
/// a *planned* stop should not have to wait [`scheduler_domain::LEASE_SECS`] to
/// recover from something it knew about in advance.
///
/// A `std::sync::Mutex` and not tokio's: every critical section here is a set
/// insert or remove with no await inside it, so the lock is never held across a
/// yield point and an async mutex would only add cost. It is deliberately *not*
/// generic and not a port -- this is process-local bookkeeping about work in
/// flight, not a thing the domain models.
#[derive(Clone, Default)]
pub struct InFlight {
    ids: Arc<Mutex<HashSet<RunId>>>,
}

impl InFlight {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records ids as claimed-but-unpublished. Called after the claim commits
    /// and before the first publish is attempted -- any later and there is a
    /// window the drain cannot see.
    pub fn add(&self, ids: impl IntoIterator<Item = RunId>) {
        let mut guard = self.ids.lock().expect("in-flight lock poisoned");
        guard.extend(ids);
    }

    /// Forgets a run: it has been published, or its claim has been released.
    pub fn remove(&self, id: RunId) {
        self.ids
            .lock()
            .expect("in-flight lock poisoned")
            .remove(&id);
    }

    /// Everything still held, as a `Vec` for `release`.
    pub fn snapshot(&self) -> Vec<RunId> {
        self.ids
            .lock()
            .expect("in-flight lock poisoned")
            .iter()
            .copied()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.ids.lock().expect("in-flight lock poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// For a recurring job, materialize the runs due within `horizon_secs` and persist them.
pub struct MaterializeDueRuns<R: RunRepository, C: Clock> {
    pub runs: R,
    pub clock: C,
    pub horizon_secs: i64,
    /// Records `runs_materialized`. An `Arc<dyn Metrics>` rather than a generic
    /// parameter because the port is object-safe by design and threading a
    /// fourth type through every construction site would buy nothing. A no-op
    /// sink is a valid value, so a caller that does not export metrics passes
    /// `Arc::new(NoopMetrics)`.
    pub metrics: Arc<dyn Metrics>,
}

impl<R: RunRepository, C: Clock> MaterializeDueRuns<R, C> {
    /// Propose every run of `job` falling inside `[now, now + horizon_secs]`
    /// and persist them.
    ///
    /// **The instants proposed are a function of the job, not of when this
    /// ran.** The grid is anchored at `job.created_at`, so a run is always
    /// proposed at `created_at + k * every_secs`. Two ticks a second apart
    /// therefore propose overlapping sets — the later one has dropped whatever
    /// went past and may have gained one at the far edge of the horizon — and
    /// `UNIQUE (job_id, scheduled_at)` collides on the overlap, which is what
    /// makes `ON CONFLICT DO NOTHING` in `insert_runs` actually absorb
    /// anything.
    ///
    /// This method used to anchor the grid on `clock.now()`. `now` moves every
    /// tick, so every tick proposed a fresh set of instants that collided with
    /// nothing, the unique constraint absorbed nothing, and a job accumulated
    /// roughly one run per *poll interval* instead of one per schedule period —
    /// unbounded growth at any real scale. See
    /// `materialize_is_stable_under_a_moving_clock`.
    pub async fn run(&self, job: &Job) -> DomainResult<Vec<JobRun>> {
        let now = self.clock.now();
        let horizon = now + time::Duration::seconds(self.horizon_secs);
        let mut out = Vec::new();
        let mut cursor = now;
        // `job.created_at` and not `now`: the anchor has to be stable across
        // ticks or the grid moves with the clock. It is also per-job, so jobs
        // sharing a period do not all fire on the same second the way a shared
        // Unix-epoch anchor would make them.
        while let Some(next) = job.schedule.next_after(cursor, job.created_at) {
            if next > horizon {
                break;
            }
            // Defensive guard: a non-advancing (or backward-moving) schedule
            // must not spin forever regardless of how such a value made it
            // past the domain boundary (see `Schedule::interval`). This is a
            // belt-and-braces check, not the primary defense.
            if next <= cursor {
                break;
            }
            out.push(JobRun {
                id: RunId(Uuid::new_v4()),
                job_id: job.id,
                tenant: job.tenant.clone(),
                scheduled_at: next,
                state: RunState::Pending,
                attempt: 0,
            });
            cursor = next;
        }
        if !out.is_empty() {
            self.runs.insert_runs(&out).await?;
        }
        // Proposed, not created: this counts the instants this pass generated,
        // most of which `ON CONFLICT DO NOTHING` will absorb on insert. The
        // metric name and its help text both say "proposed" for exactly this
        // reason — `insert_runs` does not report affected rows, so a creation
        // count is not available here to record.
        self.metrics
            .incr(Metric::RunsMaterialized, out.len() as u64);
        Ok(out)
    }
}

/// Claim a batch of due runs and publish each for execution.
pub struct ClaimAndDispatch<R: RunRepository, P: EventPublisher, C: Clock> {
    pub runs: R,
    pub publisher: P,
    pub clock: C,
    pub batch: i64,
    pub owner: String,
    /// Where claimed-but-unpublished runs are recorded so a shutdown can
    /// release them. See [`InFlight`].
    ///
    /// `Default::default()` is a working no-op registry, so a caller that does
    /// not drain (every test that predates the drain, and every non-engine
    /// construction) can ignore this field.
    pub in_flight: InFlight,
    /// Records every claim-path metric — throughput, batch size, due-lag,
    /// publish success/failure, releases, and burials — from the single
    /// [`scheduler_domain::ClaimOutcome`] the claim returns. See
    /// [`MaterializeDueRuns::metrics`] for why this is `Arc<dyn Metrics>`.
    pub metrics: Arc<dyn Metrics>,
    /// Per-tenant fairness cap passed to `claim_due`; `0` (or negative) is no
    /// cap, the default. See [`scheduler_domain::RunRepository::claim_due`].
    pub per_tenant_cap: i64,
}

impl<R: RunRepository, P: EventPublisher, C: Clock> ClaimAndDispatch<R, P, C> {
    /// Claims a batch and publishes it, releasing anything that could not be
    /// published.
    ///
    /// Two properties are load-bearing here, and both exist because
    /// `claim_due` commits the `Claimed` flip for the whole batch *before*
    /// any publish is attempted -- an unavoidable dual write without an
    /// outbox:
    ///
    /// 1. **One bad run does not strand the rest.** The loop does not
    ///    short-circuit on the first error. Aborting would leave every
    ///    subsequent run of the batch `Claimed` but unpublished, and since
    ///    `claim_due` selects only `Pending` rows, they would never be
    ///    retried -- silently lost.
    /// 2. **Failures are handed back.** Anything that failed to publish is
    ///    returned to `Pending` so a later tick can claim it again.
    ///
    /// Returns the first publish error if any run failed, after the release.
    /// The runs that *did* publish stay claimed and are not republished --
    /// the error means "this tick was partially degraded", not "nothing
    /// happened".
    ///
    /// **Cancellation-aware.** The batch is registered in `in_flight` the
    /// instant the claim commits and each run is deregistered only once it is
    /// safely published (or its claim released). Dropping this future part-way
    /// -- what a SIGTERM does to the pending tick -- therefore leaves precisely
    /// the unpublished remainder recorded, which is what the shutdown drain
    /// releases. Registering later, or deregistering earlier, would each open a
    /// window where a claimed run is invisible to both the drain and the
    /// compensating release.
    pub async fn run(&self) -> DomainResult<usize> {
        let now = self.clock.now();
        let claimed = self
            .runs
            .claim_due(now, self.batch, &self.owner, self.per_tenant_cap)
            .await?;

        // Before the first publish: from here until each run is dealt with,
        // this process owns a claim that only it knows about.
        self.in_flight.add(claimed.iter().map(|r| r.id));

        // Claim-path measurement, recorded from the one `ClaimOutcome`.
        // `runs_buried` is only observable here: the claim buries a run past
        // its cap internally and excludes it from the batch, so the count it
        // returns is the sole record of it.
        self.metrics.incr(Metric::RunsClaimed, claimed.len() as u64);
        self.metrics
            .observe(Metric::ClaimBatchSize, claimed.len() as f64);
        self.metrics.incr(Metric::RunsBuried, claimed.buried);
        // Due-lag per run: how far past its scheduled instant it was when
        // claimed. Measured against the injected `now`, and against each run's
        // real `scheduled_at` — not a frozen zero — so the observation reflects
        // actual lateness rather than the clock the test happens to hold.
        for run in claimed.iter() {
            self.metrics.observe(
                Metric::DueLagSeconds,
                (now - run.scheduled_at).as_seconds_f64(),
            );
        }

        let mut published = 0usize;
        let mut failed = Vec::new();
        let mut first_err = None;

        for run in claimed.iter() {
            match self.publisher.publish_run(run).await {
                Ok(()) => {
                    published += 1;
                    // Published: the broker has it, and re-releasing it now
                    // would return a run to `Pending` that a worker is about
                    // to execute -- a duplicate, not a repair.
                    self.in_flight.remove(run.id);
                }
                Err(e) => {
                    // Deliberately still in flight. The release below is what
                    // clears it, and if we are cancelled before reaching that
                    // release the drain performs exactly the same compensation.
                    failed.push(run.id);
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }

        // Published vs. failed: together they are the dual-write's success and
        // failure rates. Recorded before the release so a release that itself
        // errors does not swallow the publish accounting.
        self.metrics.incr(Metric::RunsPublished, published as u64);
        self.metrics
            .incr(Metric::PublishFailures, failed.len() as u64);

        if let Some(err) = first_err {
            // If the release itself fails, that error wins: the runs are
            // still claimed, and reporting the release failure is what makes
            // the stuck state visible. They also stay registered in flight, so
            // the drain retries the release on the way down and the reaper
            // remains the backstop behind that.
            self.runs.release(&failed).await?;
            // The compensation rate: a nonzero `runs_released` means publishes
            // are failing and their claims are being handed back. Recorded only
            // once the release has committed, so it counts repairs that
            // actually happened.
            self.metrics.incr(Metric::RunsReleased, failed.len() as u64);
            for id in &failed {
                self.in_flight.remove(*id);
            }
            return Err(err);
        }

        Ok(published)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{
        FixedClock, InMemoryRuns, NoopMetrics, RecordingMetrics, RecordingPublisher,
    };
    use scheduler_domain::*;
    use time::macros::datetime;
    use uuid::Uuid;

    /// A no-op metrics sink as `Arc<dyn Metrics>`, for the tests that assert
    /// scheduling behaviour rather than instrumentation.
    fn noop() -> Arc<dyn Metrics> {
        Arc::new(NoopMetrics)
    }

    fn due_run(secs_ago: i64, now: time::OffsetDateTime) -> JobRun {
        JobRun {
            id: RunId(Uuid::new_v4()),
            job_id: JobId(Uuid::new_v4()),
            tenant: TenantId("t1".into()),
            scheduled_at: now - time::Duration::seconds(secs_ago),
            state: RunState::Pending,
            attempt: 0,
        }
    }

    #[tokio::test]
    async fn claims_due_and_publishes_each() {
        let now = datetime!(2026-07-16 10:00:00 UTC);
        let runs = InMemoryRuns::new();
        runs.seed(vec![due_run(5, now), due_run(1, now)]).await;
        let publisher = RecordingPublisher::new();
        let uc = ClaimAndDispatch {
            runs: runs.clone(),
            publisher: publisher.clone(),
            clock: FixedClock(now),
            batch: 10,
            owner: "engine-1".into(),
            in_flight: InFlight::new(),
            metrics: noop(),
            per_tenant_cap: 0,
        };

        let dispatched = uc.run().await.unwrap();

        assert_eq!(dispatched, 2);
        assert_eq!(publisher.count().await, 2);
    }

    /// The claim path records throughput and, crucially, **real** due-lag. Both
    /// runs are seeded late by a known amount, so the histogram observations are
    /// the actual 30s and 90s lateness — not the frozen zero a same-instant
    /// clock would produce, which is this project's recurring vacuous shape and
    /// the exact trap the plan calls out for this metric.
    #[tokio::test]
    async fn dispatch_records_claim_throughput_and_real_due_lag() {
        let now = datetime!(2026-07-18 12:00:00 UTC);
        let runs = InMemoryRuns::new();
        runs.seed(vec![due_run(30, now), due_run(90, now)]).await;
        let publisher = RecordingPublisher::new();
        let metrics = RecordingMetrics::new();
        let uc = ClaimAndDispatch {
            runs: runs.clone(),
            publisher: publisher.clone(),
            clock: FixedClock(now),
            batch: 10,
            owner: "engine-1".into(),
            in_flight: InFlight::new(),
            metrics: Arc::new(metrics.clone()),
            per_tenant_cap: 0,
        };

        assert_eq!(uc.run().await.unwrap(), 2);

        assert_eq!(metrics.count(Metric::RunsClaimed), 2);
        assert_eq!(metrics.count(Metric::RunsPublished), 2);
        assert_eq!(metrics.count(Metric::PublishFailures), 0);
        assert_eq!(metrics.count(Metric::RunsReleased), 0);

        let mut lags = metrics.observations(Metric::DueLagSeconds);
        lags.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            lags,
            vec![30.0, 90.0],
            "due-lag must be the runs' real lateness, not a frozen zero"
        );
    }

    /// A publish failure drives the compensation counters: the failed run is
    /// counted as a failure and, once released, as a release.
    #[tokio::test]
    async fn dispatch_records_publish_failures_and_releases() {
        let now = datetime!(2026-07-18 12:00:00 UTC);
        let runs = InMemoryRuns::new();
        let run = due_run(10, now);
        runs.seed(vec![run.clone()]).await;
        let publisher = RecordingPublisher::new();
        publisher.fail_for(&[run.id]).await;
        let metrics = RecordingMetrics::new();
        let uc = ClaimAndDispatch {
            runs: runs.clone(),
            publisher: publisher.clone(),
            clock: FixedClock(now),
            batch: 10,
            owner: "engine-1".into(),
            in_flight: InFlight::new(),
            metrics: Arc::new(metrics.clone()),
            per_tenant_cap: 0,
        };

        uc.run().await.unwrap_err();

        assert_eq!(metrics.count(Metric::RunsClaimed), 1);
        assert_eq!(metrics.count(Metric::RunsPublished), 0);
        assert_eq!(metrics.count(Metric::PublishFailures), 1);
        assert_eq!(metrics.count(Metric::RunsReleased), 1);
    }

    /// An exhausted run is buried, not dispatched, and `runs_buried` is the only
    /// place that fact surfaces — the claim excludes it from the batch.
    #[tokio::test]
    async fn dispatch_records_buried_runs() {
        let now = datetime!(2026-07-18 12:00:00 UTC);
        let runs = InMemoryRuns::new();
        let mut exhausted = due_run(5, now);
        exhausted.attempt = MAX_ATTEMPTS;
        runs.seed(vec![exhausted]).await;
        let metrics = RecordingMetrics::new();
        let uc = ClaimAndDispatch {
            runs: runs.clone(),
            publisher: RecordingPublisher::new(),
            clock: FixedClock(now),
            batch: 10,
            owner: "engine-1".into(),
            in_flight: InFlight::new(),
            metrics: Arc::new(metrics.clone()),
            per_tenant_cap: 0,
        };

        assert_eq!(
            uc.run().await.unwrap(),
            0,
            "an exhausted run is not dispatched"
        );
        assert_eq!(metrics.count(Metric::RunsBuried), 1);
        assert_eq!(metrics.count(Metric::RunsClaimed), 0);
    }

    /// Materialization records the count it **proposed** (see the metric's help:
    /// proposed, not created).
    #[tokio::test]
    async fn materialize_records_the_proposed_count() {
        let now = datetime!(2026-07-16 10:00:00 UTC);
        let job = interval_job(60, now);
        let runs = InMemoryRuns::new();
        let metrics = RecordingMetrics::new();
        let uc = MaterializeDueRuns {
            runs: runs.clone(),
            clock: FixedClock(now),
            horizon_secs: 180,
            metrics: Arc::new(metrics.clone()),
        };

        assert_eq!(uc.run(&job).await.unwrap().len(), 3);
        assert_eq!(metrics.count(Metric::RunsMaterialized), 3);
    }

    /// With `per_tenant_cap` set, the dispatch does not let a noisy tenant's
    /// backlog fill the batch and starve a quiet one. Exercises the cap through
    /// the use case *and* the fake's capped path — a fake that ignored the cap
    /// would let this pass against a use case that never capped.
    #[tokio::test]
    async fn dispatch_caps_a_noisy_tenant() {
        let now = datetime!(2026-07-18 12:00:00 UTC);
        let runs = InMemoryRuns::new();
        let mut seeded = Vec::new();
        // Noisy: 5 runs, older; quiet: 2, newer.
        for i in 0..5 {
            let mut r = due_run(100 - i, now);
            r.tenant = TenantId("noisy".into());
            seeded.push(r);
        }
        for i in 0..2 {
            let mut r = due_run(5 - i, now);
            r.tenant = TenantId("quiet".into());
            seeded.push(r);
        }
        runs.seed(seeded.clone()).await;

        let publisher = RecordingPublisher::new();
        let uc = ClaimAndDispatch {
            runs: runs.clone(),
            publisher: publisher.clone(),
            clock: FixedClock(now),
            batch: 10,
            owner: "engine-1".into(),
            in_flight: InFlight::new(),
            metrics: noop(),
            per_tenant_cap: 2,
        };
        uc.run().await.unwrap();

        let published: HashSet<RunId> = publisher.published_ids().await.into_iter().collect();
        let noisy = seeded
            .iter()
            .filter(|r| r.tenant.0 == "noisy" && published.contains(&r.id))
            .count();
        let quiet = seeded
            .iter()
            .filter(|r| r.tenant.0 == "quiet" && published.contains(&r.id))
            .count();
        assert!(
            noisy <= 2,
            "the cap must bound the noisy tenant, got {noisy}"
        );
        assert_eq!(
            quiet, 2,
            "the quiet tenant must not be starved, got {quiet}"
        );
    }

    fn interval_job(every_secs: i64, created_at: time::OffsetDateTime) -> Job {
        Job {
            id: JobId(Uuid::new_v4()),
            tenant: TenantId("t1".into()),
            schedule: Schedule::Interval { every_secs },
            target: "http://svc/run".into(),
            created_at,
        }
    }

    #[tokio::test]
    async fn materializes_interval_runs_within_horizon() {
        let now = datetime!(2026-07-16 10:00:00 UTC);
        // Created exactly on the cursor, so the grid is 10:01, 10:02, 10:03...
        let job = interval_job(60, now);
        let runs = InMemoryRuns::new();
        let uc = MaterializeDueRuns {
            runs: runs.clone(),
            clock: FixedClock(now),
            horizon_secs: 180,
            metrics: noop(),
        };
        let made = uc.run(&job).await.unwrap();
        assert_eq!(made.len(), 3); // +60, +120, +180
    }

    /// **The regression this whole change exists for, with a MOVING clock.**
    ///
    /// The materializer used to anchor its cursor on `clock.now()`. `now` moves
    /// every tick, so each tick walked a *different* grid: the proposed
    /// `scheduled_at` values never repeated, `UNIQUE (job_id, scheduled_at)`
    /// never collided, `ON CONFLICT DO NOTHING` absorbed nothing, and a job
    /// grew by a full horizon of runs every poll interval. A 5-second job
    /// reached 888 runs in about two minutes on the demo stack.
    ///
    /// The suite did not catch it because every materializer test used
    /// `FixedClock`, under which a cursor-anchored grid and a job-anchored grid
    /// are indistinguishable. The clock has to move for this to say anything.
    ///
    /// Shape: 5s period, 60s horizon, 10 ticks with the clock advancing 1s per
    /// tick. Correct behaviour proposes 12 instants per tick drawn from a grid
    /// that only slides forward as fast as the clock does — 14 distinct
    /// instants total (12, plus the 2 the 10 seconds of clock movement exposes
    /// at the far edge). The bug produced 120 proposals across 65 distinct
    /// instants.
    #[tokio::test]
    async fn materialize_is_stable_under_a_moving_clock() {
        let start = datetime!(2026-07-19 10:00:00 UTC);
        const PERIOD: i64 = 5;
        const HORIZON: i64 = 60;
        const TICKS: i64 = 10;

        // Deliberately not aligned to the clock: the grid must come from the
        // job, and an anchor that shares a boundary with `start` could let a
        // cursor-anchored implementation coincidentally agree.
        let job = interval_job(PERIOD, start - time::Duration::seconds(3));

        let mut proposals = 0usize;
        let mut distinct = std::collections::BTreeSet::new();

        for tick in 0..TICKS {
            let runs = InMemoryRuns::new();
            let uc = MaterializeDueRuns {
                runs,
                clock: FixedClock(start + time::Duration::seconds(tick)),
                horizon_secs: HORIZON,
                metrics: noop(),
            };
            let made = uc.run(&job).await.unwrap();
            proposals += made.len();
            for run in &made {
                distinct.insert(run.scheduled_at);
            }
        }

        // Every tick still proposes a full horizon: this is not "the later
        // ticks stopped producing", which would pass the assertion below for
        // entirely the wrong reason.
        assert_eq!(
            proposals,
            (TICKS as usize) * (HORIZON / PERIOD) as usize,
            "each tick should still propose a full horizon of runs"
        );

        // ...but they are the SAME instants. The distinct set grows only as
        // fast as the clock sweeps new grid points into the horizon, never as
        // fast as the poll rate.
        let expected = (HORIZON / PERIOD + TICKS / PERIOD) as usize;
        assert_eq!(
            distinct.len(),
            expected,
            "distinct instants must grow with the schedule period, not the poll \
             interval — got {} across {proposals} proposals; the cursor-anchored \
             bug produced 65",
            distinct.len()
        );

        // And every one of them is on the job's own grid, not on some grid the
        // clock happened to define.
        for at in &distinct {
            assert_eq!(
                (*at - job.created_at).whole_seconds() % PERIOD,
                0,
                "{at} is not on the grid anchored at {}",
                job.created_at
            );
        }
    }

    /// Two ticks inside the same period must propose byte-identical instants —
    /// the strongest form of the property, with no "it grew a bit at the edge"
    /// slack for a regression to hide in.
    #[tokio::test]
    async fn two_ticks_within_one_period_propose_identical_instants() {
        let start = datetime!(2026-07-19 10:00:00 UTC);
        let job = interval_job(60, start - time::Duration::seconds(17));

        let instants = |offset_secs: i64| {
            let job = job.clone();
            async move {
                let uc = MaterializeDueRuns {
                    runs: InMemoryRuns::new(),
                    clock: FixedClock(start + time::Duration::seconds(offset_secs)),
                    horizon_secs: 600,
                    metrics: noop(),
                };
                uc.run(&job)
                    .await
                    .unwrap()
                    .iter()
                    .map(|r| r.scheduled_at)
                    .collect::<Vec<_>>()
            }
        };

        // 0s and 5s apart, both well inside the 60s period.
        assert_eq!(
            instants(0).await,
            instants(5).await,
            "two ticks inside one period must propose exactly the same instants"
        );
    }

    /// A one-shot schedule is unaffected by anchoring: it has a single instant,
    /// and it must be proposed once and only once however the clock moves.
    #[tokio::test]
    async fn oneshot_materializes_the_same_instant_under_a_moving_clock() {
        let start = datetime!(2026-07-19 10:00:00 UTC);
        let at = start + time::Duration::seconds(30);
        let job = Job {
            id: JobId(Uuid::new_v4()),
            tenant: TenantId("t1".into()),
            schedule: Schedule::OneShot { at },
            target: "http://svc/run".into(),
            created_at: start,
        };

        for tick in 0..10 {
            let uc = MaterializeDueRuns {
                runs: InMemoryRuns::new(),
                clock: FixedClock(start + time::Duration::seconds(tick)),
                horizon_secs: 60,
                metrics: noop(),
            };
            let made = uc.run(&job).await.unwrap();
            assert_eq!(made.len(), 1);
            assert_eq!(made[0].scheduled_at, at);
        }
    }

    /// A non-advancing `Interval { every_secs: 0 }` built directly (bypassing
    /// the `Schedule::interval` constructor, e.g. as old/foreign data or a
    /// test double) must not hang the materializer. Kept fast: if the guard
    /// regressed, this test would hang rather than merely fail.
    ///
    /// Anchoring changed *where* this is caught, not whether it is. A
    /// zero period is now rejected inside `next_after` — it has no grid, and
    /// it would divide by zero — so the `while let` ends on the first `None`.
    /// The `next <= cursor` guard in the loop is now unreachable for an
    /// interval and stays as belt-and-braces.
    #[tokio::test]
    async fn materialize_terminates_on_non_advancing_schedule() {
        let now = datetime!(2026-07-16 10:00:00 UTC);
        let job = interval_job(0, now);
        let runs = InMemoryRuns::new();
        let uc = MaterializeDueRuns {
            runs: runs.clone(),
            clock: FixedClock(now),
            horizon_secs: 180,
            metrics: noop(),
        };

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), uc.run(&job))
            .await
            .expect("materialize hung on a non-advancing schedule")
            .unwrap();

        // `next_after` reports `None` for a period with no grid, so the loop
        // ends immediately without materializing any run.
        assert_eq!(result.len(), 0);
    }

    /// The lost-run regression.
    ///
    /// A publish failure used to abort the dispatch loop with `?`. Because
    /// `claim_due` had already committed `Claimed` for the whole batch, every
    /// run after the failing one was left claimed-but-unpublished, and
    /// `claim_due` only ever selects `Pending` rows -- so they were silently
    /// lost forever. This asserts the two properties that fix it: the rest of
    /// the batch is still published, and the failure is handed back.
    #[tokio::test]
    async fn publish_failure_does_not_strand_the_rest_of_the_batch() {
        let now = datetime!(2026-07-18 12:00:00 UTC);
        let runs = InMemoryRuns::new();
        let batch: Vec<JobRun> = (0..5).map(|_| due_run(10, now)).collect();
        runs.seed(batch.clone()).await;

        let publisher = RecordingPublisher::new();
        publisher.fail_for(&[batch[1].id]).await;

        let uc = ClaimAndDispatch {
            runs: runs.clone(),
            publisher: publisher.clone(),
            clock: FixedClock(now),
            batch: 10,
            owner: "engine-1".into(),
            in_flight: InFlight::new(),
            metrics: noop(),
            per_tenant_cap: 0,
        };

        let err = uc.run().await.unwrap_err();
        assert!(matches!(err, DomainError::Publish(_)), "got {err:?}");

        // The other four were published despite the failure in the middle.
        let published = publisher.published_ids().await;
        assert_eq!(published.len(), 4, "one failure must not abort the batch");
        assert!(!published.contains(&batch[1].id));

        // The failed one is claimable again; the published ones are not.
        for (i, run) in batch.iter().enumerate() {
            let state = runs.get(run.id).await.unwrap().state;
            let expected = if i == 1 {
                RunState::Pending
            } else {
                RunState::Claimed
            };
            assert_eq!(state, expected, "run {i} in the wrong state");
        }
    }

    /// The released run must actually come back on a later tick -- the point
    /// of releasing it at all.
    #[tokio::test]
    async fn released_run_is_claimable_again() {
        let now = datetime!(2026-07-18 12:00:00 UTC);
        let runs = InMemoryRuns::new();
        let run = due_run(10, now);
        runs.seed(vec![run.clone()]).await;

        let publisher = RecordingPublisher::new();
        publisher.fail_for(&[run.id]).await;
        let uc = ClaimAndDispatch {
            runs: runs.clone(),
            publisher: publisher.clone(),
            clock: FixedClock(now),
            batch: 10,
            owner: "engine-1".into(),
            in_flight: InFlight::new(),
            metrics: noop(),
            per_tenant_cap: 0,
        };
        uc.run().await.unwrap_err();

        // Broker recovers: the same run publishes on the next tick.
        publisher.fail_for(&[]).await;
        let n = uc.run().await.unwrap();
        assert_eq!(n, 1, "a released run must be picked up again");
        assert_eq!(publisher.published_ids().await, vec![run.id]);
    }

    /// The in-memory fake must behave like the Postgres adapter, because the
    /// worker's duplicate-suppression tests run against the fake. Its doc
    /// comment says exactly this; nothing enforced it, so deleting the fake's
    /// terminal guard passed the whole suite while making the fake claim every
    /// completion succeeded.
    ///
    /// Mirrors `completing_an_already_terminal_run_reports_no_transition` and
    /// `complete_does_not_overwrite_a_terminal_state` in adapter-postgres.
    #[tokio::test]
    async fn in_memory_complete_matches_the_adapter_semantics() {
        let now = datetime!(2026-07-19 12:00:00 UTC);
        let runs = InMemoryRuns::new();
        let run = due_run(10, now);
        runs.seed(vec![run.clone()]).await;
        runs.claim_due(now, 10, "engine-1", 0).await.unwrap();

        // First completion performs the transition.
        assert!(runs.complete(run.id, RunState::Succeeded).await.unwrap());
        // Second reports it changed nothing.
        assert!(!runs.complete(run.id, RunState::Succeeded).await.unwrap());
        // A late Failed must not bury the recorded Succeeded.
        assert!(!runs.complete(run.id, RunState::Failed).await.unwrap());
        assert_eq!(runs.get(run.id).await.unwrap().state, RunState::Succeeded);

        // Non-terminal outcomes are rejected, as in the adapter.
        let err = runs.complete(run.id, RunState::Pending).await.unwrap_err();
        assert!(matches!(err, DomainError::Invalid(_)), "got {err:?}");

        // A missing run reports no transition rather than erroring.
        assert!(
            !runs
                .complete(RunId(Uuid::new_v4()), RunState::Succeeded)
                .await
                .unwrap()
        );
    }

    /// The fake's `reclaim_expired` must be as strict as the adapter's, for the
    /// same reason its `complete` must be: the engine's reaper loop is tested
    /// against this type, so a permissive fake would let those tests pass
    /// against an adapter that steals live leases or resurrects finished work.
    ///
    /// Mirrors `reclaim_expired_does_not_touch_a_live_lease`,
    /// `..._ignores_pending_runs`, `..._does_not_resurrect_a_terminal_run`,
    /// `..._preserves_the_attempt_count` and
    /// `a_reclaimed_run_can_be_claimed_again` in adapter-postgres.
    #[tokio::test]
    async fn in_memory_reclaim_expired_matches_the_adapter_semantics() {
        let now = datetime!(2026-07-20 12:00:00 UTC);
        let runs = InMemoryRuns::new();
        let (lost, live, finished, never_claimed) = (
            due_run(10, now),
            due_run(10, now),
            due_run(10, now),
            due_run(10, now),
        );
        // `never_claimed` is seeded last so the three-row claim below leaves it
        // pending -- the claim walks the store in insertion order.
        runs.seed(vec![
            lost.clone(),
            live.clone(),
            finished.clone(),
            never_claimed.clone(),
        ])
        .await;

        // Claim the first three (insertion order); `never_claimed` is left
        // pending, which is the state the reaper must ignore.
        let claimed = runs.claim_due(now, 3, "engine-1", 0).await.unwrap();
        assert_eq!(
            claimed.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![lost.id, live.id, finished.id],
            "fixture: the claim takes the first three in insertion order"
        );
        assert_eq!(
            runs.get(never_claimed.id).await.unwrap().state,
            RunState::Pending
        );

        // The engine holding `lost` died: force its lease into the past. The
        // engine holding `live` is still working, so its lease stands.
        runs.force_lease_expiry(lost.id, now - time::Duration::seconds(1))
            .await;
        assert!(
            runs.lease_expiry(live.id).await.is_some_and(|e| e > now),
            "fixture: `live` must hold a lease that has not expired at `now`"
        );
        // Finish one, and confirm completion clears its lease -- as the
        // adapter's `complete` does.
        assert!(
            runs.complete(finished.id, RunState::Succeeded)
                .await
                .unwrap()
        );
        assert!(runs.lease_expiry(finished.id).await.is_none());

        let reclaimed = runs.reclaim_expired(now, 10).await.unwrap();

        assert_eq!(
            reclaimed,
            vec![lost.id],
            "exactly the expired claim must be reclaimed"
        );
        assert_eq!(runs.get(lost.id).await.unwrap().state, RunState::Pending);
        assert!(
            runs.lease_expiry(lost.id).await.is_none(),
            "a reclaimed run must not still carry the dead owner's lease"
        );
        assert_eq!(
            runs.get(live.id).await.unwrap().state,
            RunState::Claimed,
            "a live lease must not be stolen -- that duplicates work in flight"
        );
        assert_eq!(
            runs.get(finished.id).await.unwrap().state,
            RunState::Succeeded,
            "a terminal run must not be resurrected"
        );
        assert_eq!(
            runs.get(never_claimed.id).await.unwrap().state,
            RunState::Pending
        );

        // The attempt survives, and the run is claimable again.
        assert_eq!(
            runs.get(lost.id).await.unwrap().attempt,
            claimed[0].attempt,
            "reclaim must leave `attempt` exactly as the claim left it"
        );
        let again = runs.claim_due(now, 10, "engine-2", 0).await.unwrap();
        assert!(
            again.iter().any(|r| r.id == lost.id),
            "a reclaimed run must be claimable again"
        );

        // Idempotent: nothing left to reap.
        assert!(runs.reclaim_expired(now, 10).await.unwrap().is_empty());
    }

    /// The reclaim is bounded, as the adapter's `LIMIT` is.
    #[tokio::test]
    async fn in_memory_reclaim_expired_honors_the_limit() {
        let now = datetime!(2026-07-20 12:00:00 UTC);
        let runs = InMemoryRuns::new();
        let batch: Vec<JobRun> = (0..5).map(|_| due_run(10, now)).collect();
        runs.seed(batch.clone()).await;
        runs.claim_due(now, 10, "engine-that-died", 0)
            .await
            .unwrap();
        for r in &batch {
            runs.force_lease_expiry(r.id, now - time::Duration::seconds(1))
                .await;
        }

        assert_eq!(runs.reclaim_expired(now, 2).await.unwrap().len(), 2);
        assert_eq!(runs.reclaim_expired(now, 10).await.unwrap().len(), 3);
        assert!(runs.reclaim_expired(now, 10).await.unwrap().is_empty());
    }

    /// The fake must enforce the attempt cap exactly as the adapter's
    /// `claim_due` does, and must count attempts at all -- it did not before
    /// the cap existed, which would have made the cap unreachable through the
    /// fake and let a loop test retry a poisoned run forever.
    ///
    /// Mirrors `a_run_is_attempted_exactly_max_attempts_times_then_dies` and
    /// `the_attempt_cap_is_exclusive_at_the_boundary` in adapter-postgres.
    #[tokio::test]
    async fn in_memory_claim_enforces_the_attempt_cap_like_the_adapter() {
        let now = datetime!(2026-07-20 12:00:00 UTC);
        let runs = InMemoryRuns::new();
        let run = due_run(10, now);
        runs.seed(vec![run.clone()]).await;

        let mut attempts = 0i32;
        for _ in 0..(MAX_ATTEMPTS + 5) {
            let claimed = runs.claim_due(now, 10, "engine-1", 0).await.unwrap();
            if claimed.is_empty() {
                break;
            }
            attempts += 1;
            assert_eq!(
                claimed[0].attempt, attempts,
                "the fake must count the attempt, as `attempt = attempt + 1` does"
            );
            runs.release(&[run.id]).await.unwrap();
        }

        assert_eq!(
            attempts, MAX_ATTEMPTS,
            "exactly MAX_ATTEMPTS attempts, no more and no fewer"
        );
        let dead = runs.get(run.id).await.unwrap();
        assert_eq!(dead.state, RunState::Dead);
        assert_eq!(
            dead.attempt, MAX_ATTEMPTS,
            "the count must show why it died"
        );

        // Terminal: never claimed again, and `complete` will not overwrite it.
        assert!(
            runs.claim_due(now, 10, "engine-1", 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(!runs.complete(run.id, RunState::Succeeded).await.unwrap());
        assert_eq!(runs.get(run.id).await.unwrap().state, RunState::Dead);
    }

    /// The boundary, and that burying one run does not cost another its claim.
    #[tokio::test]
    async fn in_memory_claim_buries_only_the_run_at_the_cap() {
        let now = datetime!(2026-07-20 12:00:00 UTC);
        let runs = InMemoryRuns::new();

        let mut last_chance = due_run(10, now);
        last_chance.attempt = MAX_ATTEMPTS - 1;
        let mut exhausted = due_run(10, now);
        exhausted.attempt = MAX_ATTEMPTS;
        let healthy = due_run(10, now);
        runs.seed(vec![
            last_chance.clone(),
            exhausted.clone(),
            healthy.clone(),
        ])
        .await;

        let claimed = runs.claim_due(now, 10, "engine-1", 0).await.unwrap();

        assert_eq!(
            claimed.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![last_chance.id, healthy.id],
            "a run with one attempt left is claimed; one at the cap is not, and \
             burying it must not block the rest of the batch"
        );
        assert_eq!(runs.get(exhausted.id).await.unwrap().state, RunState::Dead);
        assert_eq!(
            runs.get(exhausted.id).await.unwrap().attempt,
            MAX_ATTEMPTS,
            "burying must not alter the count"
        );
    }
}
