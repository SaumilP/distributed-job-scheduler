//! A Redis "hot index" of due runs — a **hint, never a source of truth**.
//!
//! The index is a single sorted set (`sched:due`) mapping each pending run's id
//! to its `scheduled_at` as a score. The engine can pop the ids that are due
//! *now* from Redis — no Postgres scan — and then claim exactly those ids in
//! Postgres under `SKIP LOCKED` ([`scheduler_domain::RunRepository::claim_ids`]).
//!
//! # Why this is only a hint
//!
//! Postgres stays authoritative. Everything this index returns is confirmed by a
//! Postgres claim, so the index is free to be stale, to lag, or to be wiped
//! entirely: a missing id just means the engine falls back to the Postgres due
//! scan, and a stale id (one already claimed) is silently skipped by the claim
//! because it is no longer `pending`. Nothing here may ever be treated as truth
//! on its own — that is the invariant that keeps a second store from becoming a
//! second, disagreeing answer to "what is claimable".
//!
//! Concurrency between poppers is deliberately not made atomic: two engines can
//! pop overlapping ids, and both will try to claim them, but Postgres'
//! `SKIP LOCKED` hands each row to exactly one. The index does not need a lock
//! because the thing it feeds already has one.

use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use scheduler_domain::{DomainError, DomainResult, JobRun, RunId};
use time::OffsetDateTime;
use uuid::Uuid;

/// The one sorted set this index maintains.
const DUE_KEY: &str = "sched:due";

/// A handle on the due index. Cheap to clone via the underlying
/// `ConnectionManager`, which multiplexes over one connection and reconnects
/// on its own.
#[derive(Clone)]
pub struct RedisDueIndex {
    conn: ConnectionManager,
}

impl RedisDueIndex {
    /// Connects and returns a multiplexed, self-healing handle.
    pub async fn connect(url: &str) -> DomainResult<Self> {
        let client = redis::Client::open(url).map_err(|e| DomainError::Storage(e.to_string()))?;
        let conn = ConnectionManager::new(client)
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))?;
        Ok(Self { conn })
    }

    /// Indexes each run's id scored by its `scheduled_at` (unix seconds). A
    /// re-push of the same id updates its score rather than duplicating it, so
    /// pushing the same materialized horizon twice is harmless — the same
    /// idempotence the Postgres side gets from `ON CONFLICT`.
    pub async fn push(&self, runs: &[JobRun]) -> DomainResult<()> {
        if runs.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.clone();
        let items: Vec<(f64, String)> = runs
            .iter()
            .map(|r| (r.scheduled_at.unix_timestamp() as f64, r.id.0.to_string()))
            .collect();
        conn.zadd_multiple::<_, f64, String, ()>(DUE_KEY, &items)
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))
    }

    /// Pops up to `limit` ids that are due at or before `now`, oldest score
    /// first, removing them from the index. The removal is what makes this a
    /// queue rather than a repeated read; a popped id that then fails to claim
    /// in Postgres (stale, or lost to a racing popper) is simply re-added by the
    /// next refill, and until then the Postgres due scan still covers it.
    pub async fn pop_due(&self, now: OffsetDateTime, limit: i64) -> DomainResult<Vec<RunId>> {
        let mut conn = self.conn.clone();
        let now_score = now.unix_timestamp() as f64;
        let members: Vec<String> = conn
            .zrangebyscore_limit(DUE_KEY, "-inf", now_score, 0, limit as isize)
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))?;
        if members.is_empty() {
            return Ok(Vec::new());
        }
        conn.zrem::<_, _, ()>(DUE_KEY, &members)
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))?;
        members
            .iter()
            .map(|m| {
                Uuid::parse_str(m)
                    .map(RunId)
                    .map_err(|e| DomainError::Storage(format!("corrupt run id in due index: {e}")))
            })
            .collect()
    }

    /// How many ids the index currently holds.
    pub async fn len(&self) -> DomainResult<u64> {
        let mut conn = self.conn.clone();
        conn.zcard::<_, u64>(DUE_KEY)
            .await
            .map_err(|e| DomainError::Storage(e.to_string()))
    }
}
