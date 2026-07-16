use scheduler_domain::{
    ClaimOutcome, DomainError, DomainResult, JobId, JobRun, LEASE_SECS, MAX_ATTEMPTS, RunId,
    RunRepository, RunState, TenantId,
};
use sqlx::{PgPool, QueryBuilder, Row};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone)]
pub struct PgRunRepository {
    pub pool: PgPool,
}

fn state_str(s: RunState) -> &'static str {
    match s {
        RunState::Pending => "pending",
        RunState::Claimed => "claimed",
        RunState::Running => "running",
        RunState::Succeeded => "succeeded",
        RunState::Failed => "failed",
        RunState::Dead => "dead",
    }
}

/// Maps the persisted `state` column back to a `RunState`.
///
/// Unrecognized values are a handled error, not a silent fallback to
/// `Pending`. The `state` column carries a CHECK constraint (see
/// `migrations/0001_init.sql`) restricting it to the six known values, so an
/// unrecognized value here should be unreachable in the common case -- but
/// the migration uses `CREATE TABLE` against a database that may already
/// have a pre-existing `job_runs` table from outside this migration's
/// control, in which case the CHECK constraint never lands. Panicking in an
/// async task on that data is worse than returning a `DomainError::Storage`,
/// so this is a recoverable error rather than a panic.
fn state_from_str(s: &str) -> DomainResult<RunState> {
    match s {
        "pending" => Ok(RunState::Pending),
        "claimed" => Ok(RunState::Claimed),
        "running" => Ok(RunState::Running),
        "succeeded" => Ok(RunState::Succeeded),
        "failed" => Ok(RunState::Failed),
        "dead" => Ok(RunState::Dead),
        other => Err(DomainError::Storage(format!(
            "corrupt job_runs.state value {other:?}"
        ))),
    }
}

/// Single mapping used by every query that selects/returns a full run row.
/// Requires the row to include an `id, job_id, tenant, scheduled_at, state, attempt`
/// projection (order does not matter, sqlx looks columns up by name).
fn row_to_run(row: &sqlx::postgres::PgRow) -> DomainResult<JobRun> {
    Ok(JobRun {
        id: RunId(row.get::<Uuid, _>("id")),
        job_id: JobId(row.get::<Uuid, _>("job_id")),
        tenant: TenantId(row.get::<String, _>("tenant")),
        scheduled_at: row.get::<OffsetDateTime, _>("scheduled_at"),
        state: state_from_str(row.get::<&str, _>("state"))?,
        attempt: row.get::<i32, _>("attempt"),
    })
}

/// The part of a claim shared by `claim_due` and `claim_ids`: over whatever set
/// of ids `candidates` locked, bury the ones past their cap and claim the rest,
/// returning a tagged `UNION ALL` so the buried *count* survives an all-buried
/// pass (see the long note in `claim_due`). It references `$1` (owner), `$4`
/// (lease expiry) and `$5` (`MAX_ATTEMPTS`); the two callers own `$2`, `$3` and
/// `$6`. Prefixed with `,` so it appends directly after a caller's `candidates`
/// CTE.
const CLAIM_TAIL: &str = ",
     buried AS (
         UPDATE job_runs
         SET state = 'dead', lease_owner = NULL, lease_expires_at = NULL
         WHERE id IN (SELECT id FROM candidates) AND attempt >= $5
         RETURNING id
     ),
     claimed AS (
         UPDATE job_runs SET state = 'claimed', lease_owner = $1,
                lease_expires_at = $4, attempt = attempt + 1
         WHERE id IN (SELECT id FROM candidates) AND attempt < $5
         RETURNING id, job_id, tenant, scheduled_at, state, attempt
     )
     SELECT id, job_id, tenant, scheduled_at, state, attempt, false AS is_buried
     FROM claimed
     UNION ALL
     SELECT id, NULL::uuid, NULL::text, NULL::timestamptz, NULL::text, NULL::int,
            true AS is_buried
     FROM buried";

/// Splits the tagged rows a claim returns into the claimed runs and the buried
/// count. `row_to_run` is only ever called on the non-buried rows, whose columns
/// are never null.
fn parse_claim_rows(rows: &[sqlx::postgres::PgRow]) -> DomainResult<ClaimOutcome> {
    let mut claimed = Vec::new();
    let mut buried = 0u64;
    for row in rows {
        if row.get::<bool, _>("is_buried") {
            buried += 1;
        } else {
            claimed.push(row_to_run(row)?);
        }
    }
    Ok(ClaimOutcome { claimed, buried })
}

impl RunRepository for PgRunRepository {
    fn insert_runs(
        &self,
        runs: &[JobRun],
    ) -> impl std::future::Future<Output = DomainResult<()>> + Send {
        let pool = self.pool.clone();
        let runs = runs.to_vec();
        async move {
            if runs.is_empty() {
                return Ok(());
            }
            // Single multi-row INSERT instead of one round trip per row: a
            // 1-second interval over a one-day horizon materializes 86,400
            // rows, and issuing that many individual round trips would
            // contradict the scaling argument for this design. A single
            // statement is atomic on its own, so the explicit transaction
            // wrapper this replaced is no longer necessary.
            let mut qb = QueryBuilder::new(
                "INSERT INTO job_runs (id, job_id, tenant, scheduled_at, state, attempt) ",
            );
            qb.push_values(&runs, |mut b, r| {
                b.push_bind(r.id.0)
                    .push_bind(r.job_id.0)
                    .push_bind(&r.tenant.0)
                    .push_bind(r.scheduled_at)
                    .push_bind(state_str(r.state))
                    .push_bind(r.attempt);
            });
            // Absorb duplicate materializations instead of aborting the batch.
            // `(job_id, scheduled_at)` is the natural key (migration 0002); a
            // second proposal for the same logical run carries a different
            // surrogate `id`, and DO NOTHING is what makes the original row --
            // and the `id` already referenced by emitted events -- authoritative.
            //
            // DO NOTHING rather than DO UPDATE deliberately: an upsert here
            // would let a late materializer pass reset a run that has already
            // been claimed or completed back to `pending`.
            //
            // Postgres applies this per-row, so a batch that conflicts on some
            // rows still inserts the rest -- which is the normal shape when
            // successive materializer horizons overlap.
            qb.push(" ON CONFLICT (job_id, scheduled_at) DO NOTHING");
            qb.build()
                .execute(&pool)
                .await
                .map_err(|e| DomainError::Storage(e.to_string()))?;
            Ok(())
        }
    }

    fn claim_due(
        &self,
        now: OffsetDateTime,
        limit: i64,
        owner: &str,
        per_tenant_cap: i64,
    ) -> impl std::future::Future<Output = DomainResult<ClaimOutcome>> + Send {
        let pool = self.pool.clone();
        let owner = owner.to_string();
        async move {
            // The lease instant is computed here, from the injected `now`,
            // rather than in SQL. It used to be `now() + interval '30 seconds'`
            // -- the *database's* wall clock -- which made the lease ignore the
            // injected clock entirely. Binding it keeps `LEASE_SECS` a single
            // domain constant the in-memory fake can share, instead of a
            // literal duplicated into a SQL string.
            let lease_expires_at = now + time::Duration::seconds(LEASE_SECS);
            // One statement, two outcomes, over one set of locked candidates.
            //
            // `candidates` picks and locks the due rows exactly as before.
            // `buried` then takes the ones that have already used every
            // attempt they are going to get and makes them `dead`; the primary
            // UPDATE claims the rest. The two are disjoint by the `attempt`
            // predicate, so no row is touched twice.
            //
            // A data-modifying CTE runs to completion whether or not the
            // primary query reads from it, which is why `buried` needs no
            // reference below. Both see the same snapshot, so `attempt` means
            // the same thing to each.
            //
            // **The cap is enforced here and nowhere else.** Every retry path
            // ends at `Pending` and re-enters through this method: a publish
            // failure releases the run, a dead engine has its lease reclaimed.
            // `claim_due` is therefore the only chokepoint every retry must
            // pass, and burying a run here means no worker ever receives one
            // that is past its cap. Putting the same check in the reaper as
            // well would enforce it in two places that can disagree, and
            // putting it *only* there would leave the release-driven loop
            // (an unroutable subject, say) unbounded -- the reaper never sees
            // those runs.
            //
            // `attempt < MAX_ATTEMPTS` claims, `attempt >= MAX_ATTEMPTS`
            // buries: `attempt` counts claims already made, so a run at
            // `MAX_ATTEMPTS` has had all of them. `<=` / `>` here would allow
            // one more attempt than the constant names.
            //
            // Both writes are now CTEs and the final statement is a tagged
            // `UNION ALL` over their `RETURNING`s: claimed rows carry the full
            // projection with `is_buried = false`, buried rows carry only their
            // id with `is_buried = true`. This is what lets the buried *count*
            // survive a pass that buried runs but claimed none — the previous
            // shape returned only claimed rows, so an all-buried tick came back
            // empty and the count was unobservable. The buried columns are
            // typed `NULL` casts so the two arms union; `row_to_run` is only
            // called on the non-buried rows, whose columns are never null.
            //
            // Fairness: with `per_tenant_cap > 0`, a `ranked` CTE numbers each
            // tenant's due runs oldest-first and `candidates` admits only each
            // tenant's first `cap` before the overall `LIMIT` and oldest-first
            // ordering apply -- so one tenant's backlog cannot fill the batch.
            // The window function lives in its own CTE because `FOR UPDATE` is
            // not allowed in a query that uses one; `candidates` does the
            // locking over the fair set. With `cap <= 0` the `ranked` CTE is
            // omitted entirely and the query is identical to the historical one,
            // so the uncapped path pays nothing for a feature it does not use.
            let candidates_cte = if per_tenant_cap > 0 {
                "ranked AS (
                     SELECT id, ROW_NUMBER() OVER (PARTITION BY tenant ORDER BY scheduled_at, id) AS rn
                     FROM job_runs
                     WHERE state = 'pending' AND scheduled_at <= $2
                 ),
                 candidates AS (
                     SELECT id FROM job_runs
                     WHERE state = 'pending' AND scheduled_at <= $2
                       AND id IN (SELECT id FROM ranked WHERE rn <= $6)
                     ORDER BY scheduled_at
                     FOR UPDATE SKIP LOCKED
                     LIMIT $3
                 )"
            } else {
                "candidates AS (
                     SELECT id FROM job_runs
                     WHERE state = 'pending' AND scheduled_at <= $2
                     ORDER BY scheduled_at
                     FOR UPDATE SKIP LOCKED
                     LIMIT $3
                 )"
            };
            let sql = format!("WITH {candidates_cte}{CLAIM_TAIL}");
            let mut query = sqlx::query(&sql)
                .bind(&owner)
                .bind(now)
                .bind(limit)
                .bind(lease_expires_at)
                .bind(MAX_ATTEMPTS);
            // $6 exists only in the capped query; bind it only then, or sqlx
            // rejects the statement for a parameter-count mismatch.
            if per_tenant_cap > 0 {
                query = query.bind(per_tenant_cap);
            }
            let rows = query
                .fetch_all(&pool)
                .await
                .map_err(|e| DomainError::Storage(e.to_string()))?;
            parse_claim_rows(&rows)
        }
    }

    fn claim_ids(
        &self,
        ids: &[RunId],
        now: OffsetDateTime,
        owner: &str,
        per_tenant_cap: i64,
    ) -> impl std::future::Future<Output = DomainResult<ClaimOutcome>> + Send {
        let pool = self.pool.clone();
        let owner = owner.to_string();
        let ids: Vec<Uuid> = ids.iter().map(|r| r.0).collect();
        async move {
            // The authoritative half of the Redis hot path: claim exactly the
            // ids Redis offered that are *still* `pending` and due, under
            // `SKIP LOCKED`. Postgres, not Redis, decides -- a stale id (already
            // claimed) simply matches nothing and is silently dropped, which is
            // what keeps the index a hint rather than a second source of truth.
            //
            // Structurally identical to `claim_due` (same buried/claimed/UNION
            // tail, same per-tenant fairness) except the candidate base is
            // `id = ANY($3)` instead of the ordered due scan, and there is no
            // `LIMIT` -- the id set is already the batch.
            if ids.is_empty() {
                return Ok(ClaimOutcome::default());
            }
            let lease_expires_at = now + time::Duration::seconds(LEASE_SECS);
            let candidates_cte = if per_tenant_cap > 0 {
                "ranked AS (
                     SELECT id, ROW_NUMBER() OVER (PARTITION BY tenant ORDER BY scheduled_at, id) AS rn
                     FROM job_runs
                     WHERE state = 'pending' AND scheduled_at <= $2 AND id = ANY($3)
                 ),
                 candidates AS (
                     SELECT id FROM job_runs
                     WHERE state = 'pending' AND scheduled_at <= $2 AND id = ANY($3)
                       AND id IN (SELECT id FROM ranked WHERE rn <= $6)
                     ORDER BY scheduled_at
                     FOR UPDATE SKIP LOCKED
                 )"
            } else {
                "candidates AS (
                     SELECT id FROM job_runs
                     WHERE state = 'pending' AND scheduled_at <= $2 AND id = ANY($3)
                     ORDER BY scheduled_at
                     FOR UPDATE SKIP LOCKED
                 )"
            };
            let sql = format!("WITH {candidates_cte}{CLAIM_TAIL}");
            let mut query = sqlx::query(&sql)
                .bind(&owner)
                .bind(now)
                .bind(&ids)
                .bind(lease_expires_at)
                .bind(MAX_ATTEMPTS);
            if per_tenant_cap > 0 {
                query = query.bind(per_tenant_cap);
            }
            let rows = query
                .fetch_all(&pool)
                .await
                .map_err(|e| DomainError::Storage(e.to_string()))?;
            parse_claim_rows(&rows)
        }
    }

    fn reclaim_expired(
        &self,
        now: OffsetDateTime,
        limit: i64,
    ) -> impl std::future::Future<Output = DomainResult<Vec<RunId>>> + Send {
        let pool = self.pool.clone();
        async move {
            // Shaped like `claim_due` on purpose, and for the same reasons.
            //
            // `FOR UPDATE SKIP LOCKED` so several engines can reap
            // concurrently: a reaper that blocked on a row another reaper (or
            // an in-flight claim) held would serialize recovery across the
            // fleet, which is exactly when the fleet is least healthy.
            //
            // `state = 'claimed' AND lease_expires_at < $1` is the whole
            // safety argument. Without the state predicate this resurrects
            // terminal runs and races the engine for pending ones; without the
            // expiry predicate it steals live leases and causes duplicate
            // execution. Neither is decoration.
            //
            // `ORDER BY lease_expires_at` drains longest-abandoned first, so a
            // limited reap under a backlog cannot starve the runs that have
            // been stranded longest.
            //
            // `attempt` is deliberately untouched -- see the port docs. The
            // attempt was really consumed, and preserving it is what lets the
            // max-attempts policy terminate a run that can never succeed.
            let rows = sqlx::query(
                "UPDATE job_runs
                 SET state = 'pending', lease_owner = NULL, lease_expires_at = NULL
                 WHERE id IN (
                     SELECT id FROM job_runs
                     WHERE state = 'claimed' AND lease_expires_at < $1
                     ORDER BY lease_expires_at
                     FOR UPDATE SKIP LOCKED
                     LIMIT $2
                 )
                 RETURNING id",
            )
            .bind(now)
            .bind(limit)
            .fetch_all(&pool)
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))?;

            Ok(rows.iter().map(|r| RunId(r.get::<Uuid, _>("id"))).collect())
        }
    }

    fn release(&self, ids: &[RunId]) -> impl std::future::Future<Output = DomainResult<()>> + Send {
        let pool = self.pool.clone();
        let ids: Vec<Uuid> = ids.iter().map(|r| r.0).collect();
        async move {
            if ids.is_empty() {
                return Ok(());
            }
            // Clears the lease along with the state: a released run is not
            // owned by anyone, and leaving a stale `lease_owner` behind would
            // mislead the reaper about who last held it.
            //
            // Scoped to `state = 'claimed'` so this cannot drag a run that has
            // since progressed (running/succeeded) backwards -- releases race
            // with the worker they were compensating for.
            sqlx::query(
                "UPDATE job_runs
                 SET state = 'pending', lease_owner = NULL, lease_expires_at = NULL
                 WHERE id = ANY($1) AND state = 'claimed'",
            )
            .bind(&ids)
            .execute(&pool)
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))?;
            Ok(())
        }
    }

    fn complete(
        &self,
        id: RunId,
        outcome: RunState,
    ) -> impl std::future::Future<Output = DomainResult<bool>> + Send {
        let pool = self.pool.clone();
        async move {
            // Validated before touching the database: a non-terminal outcome is
            // a caller bug, and the database would happily accept it (the CHECK
            // constraint allows all six states) while silently making the run
            // claimable again -- i.e. executed twice.
            if !matches!(
                outcome,
                RunState::Succeeded | RunState::Failed | RunState::Dead
            ) {
                return Err(DomainError::Invalid(format!(
                    "complete requires a terminal outcome, got {outcome:?}"
                )));
            }

            // The `state NOT IN (terminal)` predicate is what makes this
            // idempotent under redelivery: the first completion wins, later
            // ones affect zero rows and report `false`. It is also what stops a
            // late `Failed` burying an already-recorded `Succeeded`.
            let result = sqlx::query(
                "UPDATE job_runs
                 SET state = $2, lease_owner = NULL, lease_expires_at = NULL
                 WHERE id = $1 AND state NOT IN ('succeeded','failed','dead')",
            )
            .bind(id.0)
            .bind(state_str(outcome))
            .execute(&pool)
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))?;

            Ok(result.rows_affected() == 1)
        }
    }

    fn runs_for_jobs(
        &self,
        job_ids: &[JobId],
        before: OffsetDateTime,
        limit_per_job: i64,
    ) -> impl std::future::Future<Output = DomainResult<Vec<JobRun>>> + Send {
        let pool = self.pool.clone();
        let ids: Vec<Uuid> = job_ids.iter().map(|j| j.0).collect();
        async move {
            // Not just an optimization: `id = ANY($1)` with an empty array is
            // fine, but returning early keeps the contract ("no query") honest
            // and avoids a pointless round trip on every childless GraphQL
            // request.
            if ids.is_empty() {
                return Ok(Vec::new());
            }

            // A window function, because the limit is PER JOB. A plain
            // `LIMIT $3` caps the whole result set, which would happily return
            // every row for one job and nothing for the others -- the batch
            // would silently lose data for all but the first job, and a test
            // asserting only the total would not notice.
            //
            // `scheduled_at <= $2` sits in the INNER query, before
            // `ROW_NUMBER()` ranks. That placement is the whole point: the
            // materializer writes runs a horizon into the future, so ranking
            // first and filtering after would number the future rows 1..n,
            // spend the entire per-job window on them, and then discard them --
            // returning nothing for a job with a full schedule ahead of it.
            let rows = sqlx::query(
                "SELECT id, job_id, tenant, scheduled_at, state, attempt
                 FROM (
                     SELECT *, ROW_NUMBER() OVER (
                         PARTITION BY job_id ORDER BY scheduled_at DESC
                     ) AS rn
                     FROM job_runs
                     WHERE job_id = ANY($1) AND scheduled_at <= $2
                 ) ranked
                 WHERE rn <= $3
                 ORDER BY job_id, scheduled_at DESC",
            )
            .bind(&ids)
            .bind(before)
            .bind(limit_per_job)
            .fetch_all(&pool)
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))?;

            rows.iter().map(row_to_run).collect()
        }
    }

    fn get(&self, id: RunId) -> impl std::future::Future<Output = DomainResult<JobRun>> + Send {
        let pool = self.pool.clone();
        async move {
            let row = sqlx::query(
                "SELECT id, job_id, tenant, scheduled_at, state, attempt FROM job_runs WHERE id = $1",
            )
            .bind(id.0)
            .fetch_optional(&pool)
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))?
            .ok_or(DomainError::NotFound)?;
            row_to_run(&row)
        }
    }
}
