//! Releasing this engine's unpublished claims on the way down.
//!
//! Without this, a SIGTERM during a rollout leaves whatever the dispatch tick
//! had claimed but not yet published sitting `Claimed` with nobody working it,
//! until [`scheduler_domain::LEASE_SECS`] elapses and the reaper picks it up.
//! Releasing on the way down turns a two-minute recovery into an immediate one
//! for the failure mode we actually see most often -- the planned one.

use scheduler_application::InFlight;
use scheduler_domain::RunRepository;
use std::time::Duration;

/// How long shutdown will wait for the drain before giving up.
///
/// **A drain that hangs turns a rollout into an outage**, which is the failure
/// this bound exists to prevent -- the container is killed at the end of its
/// termination grace period whether or not we are finished, and burning that
/// grace on a `release` that will never return costs the shutdown its dignity
/// without saving a single run.
///
/// Five seconds because the work is one `UPDATE ... WHERE id = ANY($1)` over at
/// most one dispatch batch of ids, which is single-digit milliseconds against a
/// healthy database. Three orders of magnitude of headroom means overrunning
/// this is never "we were nearly done" -- it means Postgres is unreachable or
/// wedged, in which case the drain could not have succeeded with any budget.
/// It also sits comfortably inside both common grace periods (Kubernetes
/// defaults to 30s, compose to 10s) with room for the rest of shutdown.
pub const DRAIN_BUDGET: Duration = Duration::from_secs(5);

/// The outcome of a drain, for logging and for tests to assert on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drained {
    /// Nothing was held; shutdown had nothing to release.
    Nothing,
    /// Released this many runs.
    Released(usize),
    /// `release` returned an error. The runs stay claimed and the reaper
    /// reclaims them after the lease.
    Failed(usize),
    /// The budget was exceeded. Same consequence as `Failed`.
    TimedOut(usize),
}

/// Releases every run this engine claimed and has not published, bounded by
/// `budget`.
///
/// **Best-effort by design, and it returns rather than propagating.** Failure
/// to drain must never prevent the process exiting: the drain is an
/// optimisation over the reaper, not a replacement for it, and the reaper is
/// precisely what makes giving up safe. An engine that refused to exit because
/// it could not tidy up would have converted a recoverable liveness delay into
/// a stuck container -- strictly worse than the thing it was avoiding.
///
/// Every outcome other than success is logged loudly, because a drain that
/// silently does nothing is indistinguishable from one that was never wired in.
pub async fn drain_leases<R: RunRepository>(
    runs: &R,
    in_flight: &InFlight,
    budget: Duration,
) -> Drained {
    let ids = in_flight.snapshot();
    if ids.is_empty() {
        tracing::info!("no unpublished claims to drain");
        return Drained::Nothing;
    }

    let held = ids.len();
    tracing::info!(count = held, "releasing unpublished claims before exit");

    match tokio::time::timeout(budget, runs.release(&ids)).await {
        Ok(Ok(())) => {
            for id in &ids {
                in_flight.remove(*id);
            }
            tracing::info!(count = held, "drained unpublished claims");
            Drained::Released(held)
        }
        Ok(Err(e)) => {
            tracing::warn!(
                count = held,
                error = %e,
                "lease drain failed; these runs stay claimed until the reaper \
                 reclaims them after the lease expires"
            );
            Drained::Failed(held)
        }
        Err(_elapsed) => {
            tracing::warn!(
                count = held,
                budget_secs = budget.as_secs(),
                "lease drain exceeded its budget and was abandoned; these runs \
                 stay claimed until the reaper reclaims them after the lease \
                 expires. Exiting anyway -- a drain that outlives the grace \
                 period turns a rollout into an outage."
            );
            Drained::TimedOut(held)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scheduler_application::testing::{
        FixedClock, InMemoryRuns, NoopMetrics, RecordingPublisher,
    };
    use scheduler_application::{ClaimAndDispatch, InFlight};
    use scheduler_domain::*;
    use std::future::Future;
    use std::sync::Arc;
    use uuid::Uuid;

    // ------------------------------------------------------------------
    // NOTE ON WHAT IS AND IS NOT TESTED HERE.
    //
    // The drain cannot be tested end-to-end in this suite. Doing so would mean
    // signalling SIGTERM to a real `scheduler-engine` process at the exact
    // moment its dispatch tick sits between a committed claim and its first
    // publish, then reading the rows back -- a race against a real process, a
    // real broker and a real signal, which is the kind of test that is flaky
    // first and useless second.
    //
    // So these are seam tests, and the seam is named honestly: they assert that
    // `ClaimAndDispatch` records exactly the unpublished remainder when its
    // future is *dropped* (which is what SIGTERM does to the pending tick under
    // `run_until_shutdown`'s select), and that `drain_leases` releases what the
    // registry holds within its budget. The one thing no test here covers is
    // the wiring in `main.rs` between the two -- that is read, not asserted.
    // ------------------------------------------------------------------

    const NOW: time::OffsetDateTime = time::macros::datetime!(2026-07-20 10:00:00 UTC);

    fn due_run() -> JobRun {
        JobRun {
            id: RunId(Uuid::new_v4()),
            job_id: JobId(Uuid::new_v4()),
            tenant: TenantId("t1".into()),
            scheduled_at: NOW - time::Duration::seconds(5),
            state: RunState::Pending,
            attempt: 0,
        }
    }

    /// A repository whose `release` never returns, for the budget test.
    #[derive(Clone)]
    struct HangingRuns(InMemoryRuns);

    /// A repository whose `release` always fails.
    #[derive(Clone)]
    struct FailingRelease(InMemoryRuns);

    macro_rules! delegate_except_release {
        ($t:ty) => {
            impl RunRepository for $t {
                fn insert_runs(
                    &self,
                    runs: &[JobRun],
                ) -> impl Future<Output = DomainResult<()>> + Send {
                    self.0.insert_runs(runs)
                }
                fn claim_due(
                    &self,
                    now: time::OffsetDateTime,
                    limit: i64,
                    owner: &str,
                    per_tenant_cap: i64,
                ) -> impl Future<Output = DomainResult<scheduler_domain::ClaimOutcome>> + Send {
                    self.0.claim_due(now, limit, owner, per_tenant_cap)
                }
                fn claim_ids(
                    &self,
                    ids: &[RunId],
                    now: time::OffsetDateTime,
                    owner: &str,
                    per_tenant_cap: i64,
                ) -> impl Future<Output = DomainResult<scheduler_domain::ClaimOutcome>> + Send {
                    self.0.claim_ids(ids, now, owner, per_tenant_cap)
                }
                fn reclaim_expired(
                    &self,
                    now: time::OffsetDateTime,
                    limit: i64,
                ) -> impl Future<Output = DomainResult<Vec<RunId>>> + Send {
                    self.0.reclaim_expired(now, limit)
                }
                fn complete(
                    &self,
                    id: RunId,
                    outcome: RunState,
                ) -> impl Future<Output = DomainResult<bool>> + Send {
                    self.0.complete(id, outcome)
                }
                fn runs_for_jobs(
                    &self,
                    job_ids: &[JobId],
                    before: time::OffsetDateTime,
                    limit_per_job: i64,
                ) -> impl Future<Output = DomainResult<Vec<JobRun>>> + Send {
                    self.0.runs_for_jobs(job_ids, before, limit_per_job)
                }
                fn get(&self, id: RunId) -> impl Future<Output = DomainResult<JobRun>> + Send {
                    self.0.get(id)
                }
                fn release(&self, ids: &[RunId]) -> impl Future<Output = DomainResult<()>> + Send {
                    release_impl(ids)
                }
            }
        };
    }

    mod hanging {
        use super::*;
        fn release_impl(_ids: &[RunId]) -> impl Future<Output = DomainResult<()>> + Send {
            std::future::pending()
        }
        delegate_except_release!(HangingRuns);
    }

    mod failing {
        use super::*;
        fn release_impl(_ids: &[RunId]) -> impl Future<Output = DomainResult<()>> + Send {
            std::future::ready(Err(DomainError::Storage("injected release failure".into())))
        }
        delegate_except_release!(FailingRelease);
    }

    /// Nothing held is the common case -- a clean shutdown between ticks.
    #[tokio::test]
    async fn draining_an_empty_registry_is_a_no_op() {
        let runs = InMemoryRuns::new();
        let outcome = drain_leases(&runs, &InFlight::new(), DRAIN_BUDGET).await;
        assert_eq!(outcome, Drained::Nothing);
    }

    /// **The property the whole task exists for.** A dispatch tick cancelled
    /// between the claim and the publishes must leave its unpublished runs
    /// recorded, and the drain must return them to `Pending` so another replica
    /// can pick them up immediately rather than after `LEASE_SECS`.
    ///
    /// The cancellation is real: the tick future is polled once (far enough to
    /// commit the claim and block on the first publish) and then dropped, which
    /// is exactly what `run_until_shutdown`'s `select!` does to a pending tick
    /// when the shutdown branch wins.
    #[tokio::test]
    async fn a_cancelled_dispatch_leaves_its_claims_drainable() {
        let runs = InMemoryRuns::new();
        let batch: Vec<JobRun> = (0..4).map(|_| due_run()).collect();
        runs.seed(batch.clone()).await;

        let in_flight = InFlight::new();
        let publisher = BlockingPublisher::new();
        let uc = ClaimAndDispatch {
            runs: runs.clone(),
            publisher: publisher.clone(),
            clock: FixedClock(NOW),
            batch: 10,
            owner: "engine-1".into(),
            in_flight: in_flight.clone(),
            metrics: Arc::new(NoopMetrics),
            per_tenant_cap: 0,
        };

        {
            // Poll to the point where the claim has committed and the first
            // publish is pending, then drop -- i.e. SIGTERM mid-batch.
            let mut fut = std::pin::pin!(uc.run());
            let polled = futures::poll!(&mut fut);
            assert!(polled.is_pending(), "fixture: the publish must block");
        }

        assert_eq!(
            in_flight.len(),
            4,
            "a cancelled tick must leave the whole unpublished batch registered"
        );
        for run in &batch {
            assert_eq!(
                runs.get(run.id).await.unwrap().state,
                RunState::Claimed,
                "fixture: the claim really did commit before cancellation"
            );
        }

        let outcome = drain_leases(&runs, &in_flight, DRAIN_BUDGET).await;

        assert_eq!(outcome, Drained::Released(4));
        assert!(
            in_flight.is_empty(),
            "the drain must clear what it released"
        );
        for run in &batch {
            assert_eq!(
                runs.get(run.id).await.unwrap().state,
                RunState::Pending,
                "a drained run must be claimable again without waiting out the lease"
            );
        }

        // And it really is claimable again -- the point of releasing it.
        let reclaimed = runs.claim_due(NOW, 10, "engine-2", 0).await.unwrap();
        assert_eq!(reclaimed.len(), 4);
    }

    /// A run that published successfully must NOT be drained. Releasing it
    /// would return a run to `Pending` that a worker is already executing --
    /// the drain would then be a source of duplicate execution rather than a
    /// recovery from one.
    #[tokio::test]
    async fn a_published_run_is_not_drained() {
        let runs = InMemoryRuns::new();
        let batch: Vec<JobRun> = (0..3).map(|_| due_run()).collect();
        runs.seed(batch.clone()).await;

        let in_flight = InFlight::new();
        let uc = ClaimAndDispatch {
            runs: runs.clone(),
            publisher: RecordingPublisher::new(),
            clock: FixedClock(NOW),
            batch: 10,
            owner: "engine-1".into(),
            in_flight: in_flight.clone(),
            metrics: Arc::new(NoopMetrics),
            per_tenant_cap: 0,
        };
        uc.run().await.unwrap();

        assert!(
            in_flight.is_empty(),
            "a fully published batch must leave nothing in flight"
        );
        assert_eq!(
            drain_leases(&runs, &in_flight, DRAIN_BUDGET).await,
            Drained::Nothing
        );
        for run in &batch {
            assert_eq!(
                runs.get(run.id).await.unwrap().state,
                RunState::Claimed,
                "a published run must stay claimed -- a worker has it"
            );
        }
    }

    /// Best-effort: a `release` that errors is logged and swallowed. The
    /// process must still exit, because the reaper is the backstop -- which is
    /// exactly what makes giving up here safe.
    #[tokio::test]
    async fn a_failing_drain_reports_failure_rather_than_propagating() {
        let inner = InMemoryRuns::new();
        let run = due_run();
        inner.seed(vec![run.clone()]).await;
        inner.claim_due(NOW, 10, "engine-1", 0).await.unwrap();

        let in_flight = InFlight::new();
        in_flight.add([run.id]);

        let outcome = drain_leases(&FailingRelease(inner.clone()), &in_flight, DRAIN_BUDGET).await;

        assert_eq!(outcome, Drained::Failed(1));
        assert_eq!(
            inner.get(run.id).await.unwrap().state,
            RunState::Claimed,
            "the run stays claimed; the reaper is what recovers it"
        );
    }

    /// **The budget is real, and this test can observe it.**
    ///
    /// Under `start_paused` virtual time only advances when every task is idle,
    /// so a `release` that never returns lets the clock jump straight to the
    /// timeout -- which is what makes the elapsed-time assertion below exact
    /// rather than approximate.
    ///
    /// The outer `timeout` is not belt-and-braces: without it, removing the
    /// budget from `drain_leases` would make this test *hang* rather than fail,
    /// and a mutation that hangs has not been checked. With it, deleting the
    /// budget produces a failure at 60 virtual seconds instead.
    #[tokio::test(start_paused = true)]
    async fn a_drain_that_hangs_is_abandoned_at_the_budget() {
        let inner = InMemoryRuns::new();
        let run = due_run();
        inner.seed(vec![run.clone()]).await;
        inner.claim_due(NOW, 10, "engine-1", 0).await.unwrap();

        let in_flight = InFlight::new();
        in_flight.add([run.id]);

        let started = tokio::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(60),
            drain_leases(&HangingRuns(inner.clone()), &in_flight, DRAIN_BUDGET),
        )
        .await
        .expect(
            "the drain outlived a bound 12x its own budget -- it is unbounded, \
             and a shutdown that hangs turns a rollout into an outage",
        );
        let elapsed = started.elapsed();

        assert_eq!(outcome, Drained::TimedOut(1));
        assert_eq!(
            elapsed, DRAIN_BUDGET,
            "the drain must be abandoned at its budget, not before and not after"
        );
        assert_eq!(
            inner.get(run.id).await.unwrap().state,
            RunState::Claimed,
            "an abandoned drain leaves the run to the reaper"
        );
    }

    /// A publisher whose `publish_run` never completes, so a dispatch tick can
    /// be caught mid-batch.
    #[derive(Clone, Default)]
    struct BlockingPublisher;

    impl BlockingPublisher {
        fn new() -> Self {
            Self
        }
    }

    impl EventPublisher for BlockingPublisher {
        fn publish_run(&self, _run: &JobRun) -> impl Future<Output = DomainResult<()>> + Send {
            std::future::pending()
        }
    }
}
