//! The engine's three loops, and the generic driver they share.
//!
//! The loop body is a separate `async fn` from the loop itself so both can be
//! tested: the body against fakes, the driver against a counting closure.
//! Testing only the composed binary would leave the two properties that
//! actually matter -- pacing and error tolerance -- unasserted.

use futures::FutureExt;
use scheduler_application::MaterializeDueRuns;
use scheduler_domain::{Clock, DomainResult, JobRepository, Metric, Metrics, RunRepository};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// How often the reaper sweeps for expired leases.
///
/// **Deliberately far slower than dispatch**, which runs at `POLL_INTERVAL_MS`
/// (1s by default). Three reasons, in order of weight:
///
/// 1. **Reaping is recovery, not the hot path.** Its steady-state answer is
///    "nothing expired". Dispatch's tick has work to do on most iterations;
///    this one does not, and paying a lock-taking `UPDATE ... FOR UPDATE SKIP
///    LOCKED` scan every second to be told "no" is waste that scales with the
///    replica count -- N engines each sweep independently.
/// 2. **It cannot beat the lease anyway.** A run becomes reclaimable only once
///    [`scheduler_domain::LEASE_SECS`] has passed. Sweeping ten times inside
///    one lease period does not shorten recovery; it only finds the same
///    nothing ten times.
/// 3. **What it does add to recovery latency is bounded and small.** Worst-case
///    recovery for a crashed engine is `LEASE_SECS + REAPER_INTERVAL` = 150s,
///    of which this contributes 30 -- a quarter of the lease, not a multiple of
///    it. Anything much larger would start to dominate the term it is meant to
///    trail.
///
/// The number that makes the fast path fast is not this one. A *planned* stop
/// drains its leases on the way down (see `drain`) and waits out neither the
/// lease nor this interval.
pub const REAPER_INTERVAL: Duration = Duration::from_secs(30);

/// How many expired leases one reaper tick will reclaim.
///
/// Bounded for the reason every read here is bounded: the backlog after a long
/// outage is a function of the outage, not of anything the engine chose, and an
/// unbounded reclaim is one enormous statement holding locks over all of it.
///
/// Sized independently of `Config::batch` on purpose -- that tunes dispatch
/// throughput, and a deployment lowering it to shed load would otherwise also
/// slow its own recovery, which is exactly backwards. A tick that fills this
/// page simply reclaims the rest on the next one.
pub const REAPER_PAGE: i64 = 500;

/// Runs `tick` every `period` until `shutdown` completes.
///
/// Two properties are load-bearing, and both have tests:
///
/// 1. **It awaits the interval.** Without that the engine spins a core and
///    issues an empty claim query thousands of times a second — which looks
///    like "it works" in a demo and like an incident in production.
/// 2. **A failing tick does not end the loop.** Postgres restarts, NATS
///    restarts; the engine has to still be running when they come back.
///    Propagating the error out of the loop would turn a five-second
///    dependency blip into a dead engine and a silently stalled schedule.
pub async fn run_until_shutdown<F, Fut>(
    period: Duration,
    shutdown: impl Future<Output = ()>,
    label: &str,
    mut tick: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = DomainResult<usize>>,
{
    let mut interval = tokio::time::interval(period);
    // Skip missed ticks rather than firing them back-to-back: if a tick
    // overruns the period (a slow query, say) `Burst` would queue the misses
    // and then run them with no delay, hammering a database that is already
    // struggling.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!(loop_name = label, "shutdown signalled, stopping loop");
                break;
            }
            _ = interval.tick() => {
                match tick().await {
                    Ok(0) => {}
                    // `count` is what the tick *handled*, which is not the
                    // same as what it changed. The materialize tick proposes
                    // the whole horizon every time; because the grid is
                    // anchored per job, consecutive ticks propose overlapping
                    // instants and `ON CONFLICT DO NOTHING` drops the overlap
                    // at the database. So this number sits flat at roughly the
                    // horizon size while the number of rows actually created
                    // is far smaller. Reading it as a creation rate overstates
                    // the system's activity by roughly the
                    // horizon-to-interval ratio.
                    //
                    // Note this is a claim about the *count*, not about
                    // idempotence: `insert_runs` does not report affected
                    // rows, so the engine genuinely cannot tell a new run from
                    // an absorbed duplicate here.
                    Ok(n) => tracing::info!(loop_name = label, handled = n, "tick completed"),
                    // Logged and swallowed on purpose -- see property 2 above.
                    Err(e) => tracing::warn!(loop_name = label, error = %e, "tick failed, continuing"),
                }
            }
        }
    }
}

/// One materialization pass: for every active job, create the runs due inside
/// the horizon.
///
/// Repeatable because each job's instants are fixed by the job, not by the
/// clock: `MaterializeDueRuns` anchors the interval grid at `job.created_at`,
/// so a run is always proposed at `created_at + k * every_secs`. Successive
/// ticks over an overlapping horizon therefore re-propose the same instants,
/// and `UNIQUE (job_id, scheduled_at)` plus `ON CONFLICT DO NOTHING` drops the
/// repeats. That is what lets the loop run on a timer without tracking where
/// it left off.
///
/// Precisely: a later tick's proposals are the earlier tick's minus whatever
/// has gone past, plus whatever the advanced horizon has newly exposed. New
/// rows accrue at the schedule period, not at the poll interval. This
/// paragraph is a test obligation, discharged by
/// `materialize_tick_is_stable_under_a_moving_clock` here and by
/// `materialize_over_a_moving_clock_leaves_the_row_count_stable` in
/// adapter-postgres, which is where `ON CONFLICT` actually runs.
///
/// **The returned count is runs *proposed*, not runs created.** Every tick
/// proposes the entire horizon and the database silently drops the ones that
/// already exist, so this number sits flat at roughly
/// `horizon_secs / every_secs` per job regardless of how many runs are
/// actually new. Reporting the true creation rate needs `insert_runs` to
/// surface its affected-row count, which is a port change deferred to the next
/// phase; until then this must not be presented as a throughput metric.
pub async fn materialize_tick<J, R, C>(
    jobs: &J,
    runs: R,
    clock: C,
    horizon_secs: i64,
    job_limit: i64,
    metrics: Arc<dyn Metrics>,
) -> DomainResult<usize>
where
    J: JobRepository,
    R: RunRepository + Clone,
    C: Clock + Clone,
{
    let active = jobs.list_active(job_limit).await?;
    let mut made = 0usize;

    for job in &active {
        let uc = MaterializeDueRuns {
            runs: runs.clone(),
            clock: clock.clone(),
            horizon_secs,
            metrics: metrics.clone(),
        };
        // One bad job must not stop the others: a single unschedulable job
        // would otherwise stall materialization for every tenant.
        //
        // `catch_unwind` covers the case `match` cannot: a *panic* is not an
        // `Err`, and a panic here unwinds the whole materialize task while its
        // sibling dispatch task keeps running. That combination used to hang
        // the process with scheduling silently stopped. The domain no longer
        // has a known panic path (see `Schedule::next_after`), so this is a
        // backstop for the next one rather than a fix for a live bug.
        let result = std::panic::AssertUnwindSafe(uc.run(job))
            .catch_unwind()
            .await;

        match result {
            Ok(Ok(created)) => made += created.len(),
            Ok(Err(e)) => {
                tracing::warn!(job_id = ?job.id, error = %e, "materialization failed for job");
            }
            Err(_panic) => {
                tracing::error!(
                    job_id = ?job.id,
                    "materialization PANICKED for job; skipping it and continuing. \
                     This is a bug -- the job is likely unschedulable and will panic every tick."
                );
            }
        }
    }

    Ok(made)
}

/// One reaper pass: return up to `limit` runs whose lease has expired to
/// `Pending`, and report how many.
///
/// This closes the one correctness hole the repository used to admit to. An
/// engine that dies *between* committing a claim and publishing the batch runs
/// no compensating code at all: `ClaimAndDispatch`'s release never happens, the
/// rows stay `Claimed`, and `claim_due` selects only `Pending` rows -- so
/// nothing would ever pick them up again. This is what does.
///
/// **Reclaiming is logged at `warn`, not `info`.** A non-zero count means an
/// engine died holding work, or a lease expired while a worker was still
/// running -- and the second of those is the case where this loop *causes* a
/// duplicate execution rather than repairing a loss (see
/// [`scheduler_domain::LEASE_SECS`] for the timing that keeps it rare). Neither
/// is routine, and neither should be filtered out with the tick chatter.
///
/// The count is exact, unlike the materialize tick's: `reclaim_expired` returns
/// the ids it actually updated, so this is rows changed and not rows proposed.
pub async fn reaper_tick<R, C>(
    runs: &R,
    clock: &C,
    limit: i64,
    metrics: &dyn Metrics,
) -> DomainResult<usize>
where
    R: RunRepository,
    C: Clock,
{
    let reclaimed = runs.reclaim_expired(clock.now(), limit).await?;

    // The engine-crash rate. Nonzero in steady state means engines are dying
    // holding work (or a lease expired under a still-running worker) — the same
    // event the `warn!` below records, as a number a dashboard can trend.
    metrics.incr(Metric::RunsReclaimed, reclaimed.len() as u64);

    if !reclaimed.is_empty() {
        tracing::warn!(
            count = reclaimed.len(),
            "reclaimed expired leases; an engine died holding these runs, or a \
             lease expired while a worker was still executing"
        );
    }

    Ok(reclaimed.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use scheduler_application::testing::{
        FixedClock, InMemoryJobs, InMemoryRuns, NoopMetrics, RecordingMetrics,
    };
    use scheduler_domain::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    /// A no-op metrics sink as an `Arc<dyn Metrics>`, for the materialize-tick
    /// tests that assert scheduling behaviour rather than instrumentation.
    fn noop() -> Arc<dyn Metrics> {
        Arc::new(NoopMetrics)
    }

    /// A `RunRepository` whose `insert_runs` always fails, so
    /// `MaterializeDueRuns` returns `Err` rather than `Ok(vec![])`.
    ///
    /// Needed because the obvious way to make a job "fail" -- `every_secs: 0`
    /// -- makes the materializer's guard `break` and return `Ok(vec![])`. A
    /// test built that way never reaches the `Err` arm it claims to cover, and
    /// the arm survived a mutation to `return Err(e)` because of it.
    #[derive(Clone)]
    struct FailingRuns;

    impl RunRepository for FailingRuns {
        async fn insert_runs(&self, _runs: &[JobRun]) -> DomainResult<()> {
            Err(DomainError::Storage("injected insert failure".into()))
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
        /// Fails too, so `reaper_tick`'s error path is reachable through this
        /// fake rather than being a branch no test can enter.
        async fn reclaim_expired(
            &self,
            _now: time::OffsetDateTime,
            _limit: i64,
        ) -> DomainResult<Vec<RunId>> {
            Err(DomainError::Storage("injected reclaim failure".into()))
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
            Ok(Vec::new())
        }
        async fn get(&self, _id: RunId) -> DomainResult<JobRun> {
            Err(DomainError::NotFound)
        }
    }

    /// Jobs created at a fixed instant, so the grid every assertion below
    /// reasons about is `EPOCH + k * every_secs`.
    const EPOCH: time::OffsetDateTime = time::macros::datetime!(2026-07-19 10:00:00 UTC);

    fn job(every_secs: i64) -> Job {
        Job {
            id: JobId(Uuid::new_v4()),
            tenant: TenantId("t1".into()),
            schedule: Schedule::Interval { every_secs },
            target: "http://svc/run".into(),
            created_at: EPOCH,
        }
    }

    /// The pacing guarantee: the loop must await its interval rather than
    /// spinning.
    ///
    /// The self-imposed tick ceiling is not decoration. Under `start_paused`,
    /// virtual time only advances when every task is idle — so a loop that
    /// never awaits anything pending also never lets the clock move, and this
    /// test would hang instead of failing. A hanging test tells you nothing
    /// and blocks CI, so the tick closure trips the shutdown itself once the
    /// count is impossibly high, letting the assertion below report the real
    /// number.
    #[tokio::test(start_paused = true)]
    async fn loop_ticks_once_per_period_and_does_not_busy_spin() {
        /// Far above the ~4 ticks 3.5 periods can legitimately produce, and far
        /// below what a busy loop reaches in an instant.
        const CEILING: usize = 50;

        let count = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(tokio::sync::Notify::new());

        let c = count.clone();
        let stop = notify.clone();
        let stop_from_tick = notify.clone();
        let handle = tokio::spawn(async move {
            run_until_shutdown(
                Duration::from_secs(1),
                async move { stop.notified().await },
                "test",
                move || {
                    let c = c.clone();
                    let stop_from_tick = stop_from_tick.clone();
                    async move {
                        if c.fetch_add(1, Ordering::SeqCst) >= CEILING {
                            stop_from_tick.notify_one();
                        }
                        Ok(0)
                    }
                },
            )
            .await;
        });

        // Virtual time: 3.5 periods elapse.
        tokio::time::sleep(Duration::from_millis(3500)).await;
        let n = count.load(Ordering::SeqCst);

        notify.notify_one();
        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

        assert!(
            (1..=5).contains(&n),
            "expected roughly one tick per period over 3.5 periods, got {n} -- \
             the loop is not awaiting its interval"
        );
    }

    /// Dependency outages are routine. A tick that returns an error must be
    /// logged and swallowed, not end the loop.
    #[tokio::test(start_paused = true)]
    async fn loop_survives_a_failing_tick_and_continues() {
        let count = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let c = count.clone();
        let handle = tokio::spawn(async move {
            run_until_shutdown(
                Duration::from_millis(100),
                async {
                    let _ = rx.await;
                },
                "test",
                move || {
                    let c = c.clone();
                    async move {
                        let n = c.fetch_add(1, Ordering::SeqCst);
                        if n == 0 {
                            Err(DomainError::Storage("transient".into()))
                        } else {
                            Ok(0)
                        }
                    }
                },
            )
            .await;
        });

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            count.load(Ordering::SeqCst) > 1,
            "the loop stopped after the first failing tick"
        );

        let _ = tx.send(());
        handle.await.unwrap();
    }

    /// Shutdown must actually stop the loop, and promptly -- a loop that only
    /// notices on its next tick delays every rollout by a full period.
    #[tokio::test(start_paused = true)]
    async fn loop_stops_when_the_shutdown_signal_fires() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let handle = tokio::spawn(async move {
            run_until_shutdown(
                Duration::from_secs(3600),
                async {
                    let _ = rx.await;
                },
                "test",
                || async { Ok(0) },
            )
            .await;
        });

        let _ = tx.send(());

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("loop did not stop on shutdown")
            .unwrap();
    }

    #[tokio::test]
    async fn materialize_tick_creates_runs_for_every_active_job() {
        let now = time::macros::datetime!(2026-07-19 10:00:00 UTC);
        let jobs = InMemoryJobs::new();
        jobs.insert(&job(60)).await.unwrap();
        jobs.insert(&job(60)).await.unwrap();
        let runs = InMemoryRuns::new();

        let made = materialize_tick(&jobs, runs.clone(), FixedClock(now), 180, 100, noop())
            .await
            .unwrap();

        // 3 runs per job over a 180s horizon at 60s intervals.
        assert_eq!(made, 6);
    }

    /// The engine-side view of Phase 2a's unique constraint: ticking repeatedly
    /// over an overlapping horizon must keep proposing the *same instants*, so
    /// there is something for the constraint to collide on.
    ///
    /// **This test used to use `FixedClock` and was therefore vacuous.** With a
    /// clock that never moves, a grid anchored on the cursor and a grid
    /// anchored on the job are indistinguishable, so it held while the
    /// materializer was creating a full horizon of brand-new runs on every
    /// single tick in production. The clock has to move for this to assert
    /// anything at all — that is the entire lesson of the defect, so it is
    /// pinned here rather than left to the doc comment on `materialize_tick`.
    ///
    /// Asserts against the in-memory fake, which does not enforce the
    /// constraint, so what it pins is the *materializer* half: the instants
    /// repeat. The database half — that repeats are actually absorbed — is
    /// `materialize_over_a_moving_clock_leaves_the_row_count_stable` in
    /// adapter-postgres.
    #[tokio::test]
    async fn materialize_tick_is_stable_under_a_moving_clock() {
        const PERIOD: i64 = 60;
        const HORIZON: i64 = 600;
        const TICKS: i64 = 20;

        let jobs = InMemoryJobs::new();
        jobs.insert(&job(PERIOD)).await.unwrap();

        let mut proposed = 0usize;
        let mut distinct = std::collections::BTreeSet::new();

        // 20 ticks, clock advancing 1s each: 20s of movement against a 60s
        // period, so the grid must expose no new instants beyond the first.
        for tick in 0..TICKS {
            let runs = InMemoryRuns::new();
            let clock = FixedClock(EPOCH + time::Duration::seconds(tick));
            let made = materialize_tick(&jobs, runs.clone(), clock, HORIZON, 100, noop())
                .await
                .unwrap();
            proposed += made;

            for run in runs.snapshot().await {
                distinct.insert(run.scheduled_at);
            }
        }

        assert_eq!(
            proposed,
            (TICKS as usize) * (HORIZON / PERIOD) as usize,
            "every tick must still propose a full horizon"
        );
        assert_eq!(
            distinct.len(),
            (HORIZON / PERIOD) as usize,
            "20s of clock movement against a 60s period must expose no new \
             instants; the cursor-anchored bug produced a fresh set every tick"
        );
    }

    /// One unschedulable job must not stall materialization for everyone else.
    ///
    /// `every_secs: 0` is a *non-advancing* schedule: the materializer's guard
    /// breaks and returns `Ok(vec![])`, so this exercises the "produced
    /// nothing" path, not the error path. Kept because it is still a real
    /// case, but see the test below for the `Err` arm.
    #[tokio::test]
    async fn materialize_tick_continues_past_a_job_that_produces_nothing() {
        let now = time::macros::datetime!(2026-07-19 10:00:00 UTC);
        let jobs = InMemoryJobs::new();
        jobs.insert(&job(0)).await.unwrap();
        jobs.insert(&job(60)).await.unwrap();
        let runs = InMemoryRuns::new();

        let made = materialize_tick(&jobs, runs.clone(), FixedClock(now), 180, 100, noop())
            .await
            .unwrap();

        assert_eq!(made, 3, "the healthy job must still have been materialized");
    }

    /// The `Err` arm, actually exercised.
    ///
    /// A storage failure while materializing one job must be logged and
    /// swallowed so the tick continues. Mutating that arm to `return Err(e)`
    /// used to pass the whole suite, because no test ever produced an `Err`.
    #[tokio::test]
    async fn materialize_tick_swallows_a_storage_error_and_keeps_going() {
        let now = time::macros::datetime!(2026-07-19 10:00:00 UTC);
        let jobs = InMemoryJobs::new();
        jobs.insert(&job(60)).await.unwrap();
        jobs.insert(&job(60)).await.unwrap();

        let made = materialize_tick(&jobs, FailingRuns, FixedClock(now), 180, 100, noop())
            .await
            .expect("a per-job failure must not fail the whole tick");

        assert_eq!(made, 0, "nothing was persisted, but the tick completed");
    }

    // -----------------------------------------------------------------
    // reaper_tick
    //
    // Every test below moves `now` across the lease boundary rather than
    // holding it still. A `FixedClock` pinned to the claim instant makes lease
    // expiry *unobservable* -- the reclaim finds nothing whether or not the
    // expiry predicate exists -- and that exact shape has produced three
    // vacuous tests on this project already. The clock the reaper reads is
    // therefore always offset from the clock the claim was made with, and the
    // pair `..._leaves_a_live_lease_alone` / `..._reclaims_an_expired_lease`
    // straddles the boundary so neither can pass for the wrong reason.
    // -----------------------------------------------------------------

    fn due_run(now: time::OffsetDateTime) -> JobRun {
        JobRun {
            id: RunId(Uuid::new_v4()),
            job_id: JobId(Uuid::new_v4()),
            tenant: TenantId("t1".into()),
            scheduled_at: now - time::Duration::seconds(5),
            state: RunState::Pending,
            attempt: 0,
        }
    }

    /// Claims `count` runs at `CLAIM_AT` and hands back the store.
    async fn claimed_at(count: usize) -> (InMemoryRuns, Vec<JobRun>) {
        let runs = InMemoryRuns::new();
        let seeded: Vec<JobRun> = (0..count).map(|_| due_run(CLAIM_AT)).collect();
        runs.seed(seeded.clone()).await;
        let claimed = runs
            .claim_due(CLAIM_AT, 100, "engine-that-died", 0)
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            count,
            "fixture: everything seeded was claimed"
        );
        (runs, seeded)
    }

    const CLAIM_AT: time::OffsetDateTime = time::macros::datetime!(2026-07-20 10:00:00 UTC);

    /// The whole point of the loop: a lease that has run out comes back.
    ///
    /// The reaper's clock is `LEASE_SECS + 1` past the claim, so the expiry is
    /// real time having passed and not a value forced into the row.
    #[tokio::test]
    async fn reaper_tick_reclaims_an_expired_lease_and_reports_the_count() {
        let (runs, seeded) = claimed_at(3).await;
        let after_expiry = FixedClock(CLAIM_AT + time::Duration::seconds(LEASE_SECS + 1));

        let n = reaper_tick(&runs, &after_expiry, REAPER_PAGE, &NoopMetrics)
            .await
            .unwrap();

        assert_eq!(n, 3, "the tick must report what it actually reclaimed");
        for run in &seeded {
            assert_eq!(
                runs.get(run.id).await.unwrap().state,
                RunState::Pending,
                "a reclaimed run must be claimable again"
            );
        }
    }

    /// The reaper records `runs_reclaimed` — the engine-crash signal. Uses the
    /// recording sink so the count the metric sees is asserted, not just the
    /// count the function returns; the two are recorded at the same point and a
    /// mutation removing the `incr` must fail this without touching the return.
    #[tokio::test]
    async fn reaper_tick_records_the_reclaimed_count() {
        let (runs, _seeded) = claimed_at(3).await;
        let after_expiry = FixedClock(CLAIM_AT + time::Duration::seconds(LEASE_SECS + 1));
        let metrics = RecordingMetrics::new();

        reaper_tick(&runs, &after_expiry, REAPER_PAGE, &metrics)
            .await
            .unwrap();

        assert_eq!(
            metrics.count(Metric::RunsReclaimed),
            3,
            "the reaper must record every lease it reclaims"
        );
    }

    /// **The dangerous direction, and the one the fixture must be able to
    /// observe.** One second before the lease runs out the owner is still
    /// entitled to the run; reclaiming it would schedule a second execution of
    /// work already in flight.
    ///
    /// Paired with the test above: the two clocks sit either side of
    /// `CLAIM_AT + LEASE_SECS`, so a reaper that ignored expiry fails this one
    /// and a reaper that never reclaimed fails that one. Neither can be
    /// satisfied by a frozen clock.
    #[tokio::test]
    async fn reaper_tick_leaves_a_live_lease_alone() {
        let (runs, seeded) = claimed_at(3).await;
        let before_expiry = FixedClock(CLAIM_AT + time::Duration::seconds(LEASE_SECS - 1));

        let n = reaper_tick(&runs, &before_expiry, REAPER_PAGE, &NoopMetrics)
            .await
            .unwrap();

        assert_eq!(n, 0, "a live lease must not be reclaimed");
        for run in &seeded {
            assert_eq!(runs.get(run.id).await.unwrap().state, RunState::Claimed);
        }
    }

    /// The tick is bounded, and the remainder is not lost -- a later tick takes
    /// it. An unbounded reclaim after a long outage is one enormous statement
    /// holding locks over the whole backlog.
    #[tokio::test]
    async fn reaper_tick_honors_the_limit_and_the_remainder_survives() {
        let (runs, _) = claimed_at(5).await;
        let after_expiry = FixedClock(CLAIM_AT + time::Duration::seconds(LEASE_SECS + 1));

        assert_eq!(
            reaper_tick(&runs, &after_expiry, 2, &NoopMetrics)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            reaper_tick(&runs, &after_expiry, 2, &NoopMetrics)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            reaper_tick(&runs, &after_expiry, 2, &NoopMetrics)
                .await
                .unwrap(),
            1
        );
        // Idempotent: nothing left to reap.
        assert_eq!(
            reaper_tick(&runs, &after_expiry, 2, &NoopMetrics)
                .await
                .unwrap(),
            0
        );
    }

    /// A storage failure must surface as `Err` so the driver can log and
    /// swallow it. Returning `Ok(0)` here would make a permanently broken
    /// reaper indistinguishable from a healthy idle one.
    ///
    /// That the *loop* survives such an error is not re-asserted here: it is
    /// `loop_survives_a_failing_tick_and_continues` above, which tests the
    /// driver every loop shares. Duplicating it against the reaper would test
    /// `run_until_shutdown` a second time, not the reaper.
    #[tokio::test]
    async fn reaper_tick_surfaces_a_storage_failure_rather_than_reporting_zero() {
        let err = reaper_tick(
            &FailingRuns,
            &FixedClock(CLAIM_AT),
            REAPER_PAGE,
            &NoopMetrics,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, DomainError::Storage(_)), "got {err:?}");
    }

    /// The reaper must be slower than dispatch. `POLL_INTERVAL_MS` defaults to
    /// 1s, and reaping at that rate is a lock-taking scan per second per
    /// replica for an answer that is almost always "nothing".
    ///
    /// It must also stay well under the lease, or it would dominate the
    /// recovery latency it is only supposed to add a fraction to.
    #[test]
    fn reaper_interval_sits_between_the_poll_interval_and_the_lease() {
        assert!(
            REAPER_INTERVAL >= Duration::from_secs(10),
            "reaping is recovery, not the hot path"
        );
        assert!(
            REAPER_INTERVAL < Duration::from_secs(LEASE_SECS as u64),
            "a reaper slower than the lease would dominate recovery latency \
             instead of trailing it"
        );
    }

    /// The job limit must be honored — an unbounded read of every job in a
    /// multi-tenant table is the query that stops working at scale.
    #[tokio::test]
    async fn materialize_tick_honors_the_job_limit() {
        let now = time::macros::datetime!(2026-07-19 10:00:00 UTC);
        let jobs = InMemoryJobs::new();
        for _ in 0..5 {
            jobs.insert(&job(60)).await.unwrap();
        }
        let runs = InMemoryRuns::new();

        let made = materialize_tick(&jobs, runs.clone(), FixedClock(now), 180, 2, noop())
            .await
            .unwrap();

        assert_eq!(
            made, 6,
            "only 2 jobs x 3 runs should have been materialized"
        );
    }
}
