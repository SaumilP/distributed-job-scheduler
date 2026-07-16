//! What a worker does with one delivered run.
//!
//! The whole file exists to make one ordering explicit and testable:
//! **execute → complete → ack**.
//!
//! Ack first, and a crash in the gap loses the run: the broker believes it
//! delivered successfully and the database never learns it ran. Complete
//! first, and a crash in the gap causes a redelivery of a run that is already
//! terminal — which `complete` reports as `false`, so the second attempt skips
//! the work and acks. The asymmetry is the entire argument: **the tolerable
//! failure is a duplicate delivery, never a lost run.**

use scheduler_domain::{DomainResult, JobRun, Metric, Metrics, RunRepository, RunState};
use std::future::Future;

/// A delivered message that has not been acknowledged.
///
/// Abstracted over the NATS type so the handler's ordering can be tested
/// without a broker — the ordering is the thing most worth testing, and it
/// would be the least accessible if it could only be exercised end-to-end.
pub trait Delivery: Send {
    fn run(&self) -> &JobRun;
    fn ack(self) -> impl Future<Output = DomainResult<()>> + Send;
}

impl Delivery for adapter_nats::ClaimedMessage {
    fn run(&self) -> &JobRun {
        adapter_nats::ClaimedMessage::run(self)
    }

    fn ack(self) -> impl Future<Output = DomainResult<()>> + Send {
        adapter_nats::ClaimedMessage::ack(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecOutcome {
    Succeeded,
    Failed(String),
}

pub trait Executor: Send + Sync {
    fn execute(&self, run: &JobRun) -> impl Future<Output = ExecOutcome> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handled {
    /// Executed and recorded by this call.
    Executed(RunState),
    /// Already terminal on arrival — a redelivery. Acked, not re-executed.
    AlreadyDone,
}

fn is_terminal(s: RunState) -> bool {
    matches!(s, RunState::Succeeded | RunState::Failed | RunState::Dead)
}

/// Handles one delivery.
///
/// Returns `Err` **without acking** if the outcome could not be persisted.
/// Acking there would be the one genuinely unrecoverable move: the broker
/// forgets the message and the database never learned the run happened, so
/// nothing is left to reconcile from. Leaving it unacked means it redelivers,
/// which is merely wasteful.
pub async fn handle<D, R, E>(
    msg: D,
    runs: &R,
    exec: &E,
    metrics: &dyn Metrics,
) -> DomainResult<Handled>
where
    D: Delivery,
    R: RunRepository,
    E: Executor,
{
    let run = msg.run().clone();

    // Read before executing. `complete` would also report a redelivery via its
    // boolean, but only *after* the side effect had already happened a second
    // time -- and re-running someone's job is not something you can take back.
    //
    // This read is not a lock: two workers can both observe non-terminal and
    // both execute. At-least-once tolerates that by design, and `complete`
    // keeps the *recording* single. Making execution itself exclusive needs a
    // fencing token, which is not built. The reaper widens this case slightly:
    // an expired lease can put a second copy of a still-running run back in the
    // queue, which is why the lease is sized to outlive a real execution.
    let current = runs.get(run.id).await?;
    if is_terminal(current.state) {
        tracing::info!(run_id = ?run.id, state = ?current.state, "redelivery of a finished run, acking without re-executing");
        msg.ack().await?;
        return Ok(Handled::AlreadyDone);
    }

    // Wall time of the actual work, observed only on the path that actually
    // executes — the redelivery-of-a-finished-run branch above returns before
    // here, so a duplicate does not contribute a spurious zero. This histogram
    // is the empirical input to the `LEASE_SECS` argument: the lease has to
    // outlive real executions, and this is what "real" measures to.
    let started = std::time::Instant::now();
    let outcome = exec.execute(&run).await;
    metrics.observe(Metric::ExecutionSeconds, started.elapsed().as_secs_f64());
    let state = match &outcome {
        ExecOutcome::Succeeded => RunState::Succeeded,
        ExecOutcome::Failed(err) => {
            tracing::warn!(run_id = ?run.id, error = %err, "run execution failed");
            RunState::Failed
        }
    };

    // If this fails we return early and never ack -- see the doc comment.
    //
    // The boolean matters: `false` means another worker got there first, so
    // this call recorded nothing. Reporting `Executed(Failed)` in that case
    // would claim an outcome that was never written. The pre-execution read
    // above catches the common redelivery, but it is explicitly not a lock, so
    // this is the case it cannot catch.
    let recorded = runs.complete(run.id, state).await?;

    // A failed run is acked too. Leaving it unacked would redeliver failing
    // work up to `max_deliver` times for no benefit: the failure is already
    // recorded, and retry policy belongs to the scheduler, not to the broker's
    // redelivery timer.
    msg.ack().await?;

    Ok(if recorded {
        Handled::Executed(state)
    } else {
        Handled::AlreadyDone
    })
}

/// The Phase 2b executor: records the intent and reports success.
///
/// It deliberately does **not** call `target`. Making real outbound calls
/// brings timeouts, retries, TLS and SSRF protection with it, none of which
/// this phase covers — a fake that pretends to be an HTTP client would be
/// worse than an obvious placeholder. The README says so plainly.
#[derive(Clone, Copy, Default)]
pub struct LoggingExecutor;

impl Executor for LoggingExecutor {
    fn execute(&self, run: &JobRun) -> impl Future<Output = ExecOutcome> + Send {
        let id = run.id;
        let attempt = run.attempt;
        async move {
            tracing::info!(run_id = ?id, attempt, "executing run (simulated)");
            ExecOutcome::Succeeded
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scheduler_application::testing::{InMemoryRuns, RecordingMetrics};
    use scheduler_domain::*;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    /// Records what happened, in order, so the ordering can be asserted rather
    /// than assumed.
    type Journal = Arc<Mutex<Vec<&'static str>>>;

    struct FakeDelivery {
        run: JobRun,
        journal: Journal,
        ack_result: DomainResult<()>,
    }

    impl Delivery for FakeDelivery {
        fn run(&self) -> &JobRun {
            &self.run
        }
        async fn ack(self) -> DomainResult<()> {
            self.journal.lock().unwrap().push("ack");
            self.ack_result
        }
    }

    struct FakeExecutor {
        journal: Journal,
        outcome: ExecOutcome,
    }

    impl Executor for FakeExecutor {
        fn execute(&self, _run: &JobRun) -> impl Future<Output = ExecOutcome> + Send {
            let journal = self.journal.clone();
            let outcome = self.outcome.clone();
            async move {
                journal.lock().unwrap().push("execute");
                outcome
            }
        }
    }

    /// A repository that records completion attempts and can be made to fail.
    #[derive(Clone)]
    struct JournalingRuns {
        inner: InMemoryRuns,
        journal: Journal,
        fail_complete: bool,
        /// Simulates losing the completion race: `get` still reports the run
        /// as claimed, but `complete` reports that it changed nothing.
        ///
        /// A flag is needed because the race cannot be reproduced by seeding a
        /// terminal run -- that takes the pre-execution short-circuit instead,
        /// which is a different code path and passes even with the boolean
        /// ignored.
        lose_completion_race: bool,
    }

    impl RunRepository for JournalingRuns {
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
            let journal = self.journal.clone();
            let fail = self.fail_complete;
            let inner = self.inner.clone();
            let lose = self.lose_completion_race;
            async move {
                journal.lock().unwrap().push("complete");
                if fail {
                    return Err(DomainError::Storage("injected".into()));
                }
                if lose {
                    return Ok(false);
                }
                inner.complete(id, outcome).await
            }
        }
        fn runs_for_jobs(
            &self,
            job_ids: &[JobId],
            before: time::OffsetDateTime,
            limit_per_job: i64,
        ) -> impl Future<Output = DomainResult<Vec<JobRun>>> + Send {
            self.inner.runs_for_jobs(job_ids, before, limit_per_job)
        }
        fn get(&self, id: RunId) -> impl Future<Output = DomainResult<JobRun>> + Send {
            self.inner.get(id)
        }
    }

    fn claimed_run() -> JobRun {
        JobRun {
            id: RunId(Uuid::new_v4()),
            job_id: JobId(Uuid::new_v4()),
            tenant: TenantId("acme".into()),
            scheduled_at: time::OffsetDateTime::now_utc(),
            state: RunState::Claimed,
            attempt: 1,
        }
    }

    async fn fixture(
        state: RunState,
        fail_complete: bool,
        outcome: ExecOutcome,
    ) -> (JobRun, JournalingRuns, FakeExecutor, Journal) {
        let journal: Journal = Arc::new(Mutex::new(Vec::new()));
        let mut run = claimed_run();
        run.state = state;
        let inner = InMemoryRuns::new();
        inner.seed(vec![run.clone()]).await;
        let runs = JournalingRuns {
            inner,
            journal: journal.clone(),
            fail_complete,
            lose_completion_race: false,
        };
        let exec = FakeExecutor {
            journal: journal.clone(),
            outcome,
        };
        (run, runs, exec, journal)
    }

    #[tokio::test]
    async fn handle_executes_completes_then_acks_in_that_order() {
        let (run, runs, exec, journal) =
            fixture(RunState::Claimed, false, ExecOutcome::Succeeded).await;
        let msg = FakeDelivery {
            run: run.clone(),
            journal: journal.clone(),
            ack_result: Ok(()),
        };

        let handled = handle(
            msg,
            &runs,
            &exec,
            &scheduler_application::testing::NoopMetrics,
        )
        .await
        .unwrap();

        assert_eq!(handled, Handled::Executed(RunState::Succeeded));
        assert_eq!(
            *journal.lock().unwrap(),
            vec!["execute", "complete", "ack"],
            "ordering is the guarantee: ack must come last"
        );
        assert_eq!(runs.get(run.id).await.unwrap().state, RunState::Succeeded);
    }

    /// A run that actually executes contributes one `execution_seconds`
    /// observation; a redelivery that short-circuits before executing
    /// contributes none. Both are asserted, because the value of this histogram
    /// is that it measures *work*, and a spurious zero from a skipped
    /// redelivery would drag the distribution the `LEASE_SECS` argument reads.
    #[tokio::test]
    async fn execution_time_is_observed_only_when_the_run_executes() {
        // Executed path: one observation.
        let (run, runs, exec, journal) =
            fixture(RunState::Claimed, false, ExecOutcome::Succeeded).await;
        let msg = FakeDelivery {
            run,
            journal: journal.clone(),
            ack_result: Ok(()),
        };
        let metrics = RecordingMetrics::new();
        handle(msg, &runs, &exec, &metrics).await.unwrap();
        assert_eq!(
            metrics.observations(Metric::ExecutionSeconds).len(),
            1,
            "an executed run must record exactly one execution-time observation"
        );

        // Redelivery of a finished run: acked without executing, so no
        // observation.
        let (run, runs, exec, journal) =
            fixture(RunState::Succeeded, false, ExecOutcome::Succeeded).await;
        let msg = FakeDelivery {
            run,
            journal: journal.clone(),
            ack_result: Ok(()),
        };
        let metrics = RecordingMetrics::new();
        handle(msg, &runs, &exec, &metrics).await.unwrap();
        assert!(
            metrics.observations(Metric::ExecutionSeconds).is_empty(),
            "a skipped redelivery must not record an execution time"
        );
    }

    /// The effectively-once mechanism, as a test. A redelivered run that is
    /// already terminal must be acked *without* running the work again.
    #[tokio::test]
    async fn redelivered_terminal_run_is_acked_without_re_executing() {
        let (run, runs, exec, journal) =
            fixture(RunState::Succeeded, false, ExecOutcome::Succeeded).await;
        let msg = FakeDelivery {
            run,
            journal: journal.clone(),
            ack_result: Ok(()),
        };

        let handled = handle(
            msg,
            &runs,
            &exec,
            &scheduler_application::testing::NoopMetrics,
        )
        .await
        .unwrap();

        assert_eq!(handled, Handled::AlreadyDone);
        let log = journal.lock().unwrap().clone();
        assert!(
            !log.contains(&"execute"),
            "a finished run must not be executed again, got {log:?}"
        );
        assert_eq!(log, vec!["ack"], "and it must still be acked");
    }

    /// A failure is recorded and acked. Leaving it unacked would redeliver
    /// failing work up to `max_deliver` times for no benefit.
    #[tokio::test]
    async fn failed_execution_records_failed_and_acks() {
        let (run, runs, exec, journal) =
            fixture(RunState::Claimed, false, ExecOutcome::Failed("boom".into())).await;
        let msg = FakeDelivery {
            run: run.clone(),
            journal: journal.clone(),
            ack_result: Ok(()),
        };

        let handled = handle(
            msg,
            &runs,
            &exec,
            &scheduler_application::testing::NoopMetrics,
        )
        .await
        .unwrap();

        assert_eq!(handled, Handled::Executed(RunState::Failed));
        assert_eq!(*journal.lock().unwrap(), vec!["execute", "complete", "ack"]);
        assert_eq!(runs.get(run.id).await.unwrap().state, RunState::Failed);
    }

    /// Two workers both pass the pre-execution read (it is not a lock) and both
    /// execute. The loser's `complete` writes nothing, and it must say so
    /// rather than claiming it recorded an outcome the database never took.
    ///
    /// Note the race is injected rather than reproduced by seeding a terminal
    /// run: a terminal run takes the pre-execution short-circuit, which is a
    /// different path and passes even when the boolean is ignored. An earlier
    /// version of this test did exactly that and was vacuous.
    #[tokio::test]
    async fn a_worker_that_loses_the_completion_race_reports_already_done() {
        let (run, mut runs, exec, journal) =
            fixture(RunState::Claimed, false, ExecOutcome::Succeeded).await;
        runs.lose_completion_race = true;

        let msg = FakeDelivery {
            run: run.clone(),
            journal: journal.clone(),
            ack_result: Ok(()),
        };

        let handled = handle(
            msg,
            &runs,
            &exec,
            &scheduler_application::testing::NoopMetrics,
        )
        .await
        .unwrap();

        assert_eq!(
            handled,
            Handled::AlreadyDone,
            "the losing worker must not claim it recorded an outcome"
        );
        // It still executed (the read said claimed) and still acked.
        assert_eq!(*journal.lock().unwrap(), vec!["execute", "complete", "ack"]);
    }

    /// The sharpest assertion here: if the outcome cannot be persisted, the
    /// message must NOT be acked. Acking would be unrecoverable -- the broker
    /// forgets the run and the database never learned it happened.
    #[tokio::test]
    async fn storage_failure_during_completion_does_not_ack() {
        let (run, runs, exec, journal) =
            fixture(RunState::Claimed, true, ExecOutcome::Succeeded).await;
        let msg = FakeDelivery {
            run,
            journal: journal.clone(),
            ack_result: Ok(()),
        };

        let err = handle(
            msg,
            &runs,
            &exec,
            &scheduler_application::testing::NoopMetrics,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, DomainError::Storage(_)), "got {err:?}");
        let log = journal.lock().unwrap().clone();
        assert!(
            !log.contains(&"ack"),
            "a run whose completion failed must stay unacked so it redelivers, got {log:?}"
        );
    }
}
