-- sqlx already tracks which migrations have been applied (via its
-- _sqlx_migrations bookkeeping table), so this migration does not need to be
-- defensive about re-running. `CREATE TABLE IF NOT EXISTS` was dropped
-- deliberately: against a pre-existing `job_runs` table from outside sqlx's
-- control it would silently no-op, so the CHECK constraint below would never
-- land, and unrecognized `state` values would surface as a handled storage
-- error rather than a loud migration failure.
CREATE TABLE job_runs (
    id            UUID PRIMARY KEY,
    job_id        UUID        NOT NULL,
    tenant        TEXT        NOT NULL,
    scheduled_at  TIMESTAMPTZ NOT NULL,
    state         TEXT        NOT NULL DEFAULT 'pending'
                              CHECK (state IN ('pending','claimed','running','succeeded','failed','dead')),
    attempt       INT         NOT NULL DEFAULT 0,
    lease_owner   TEXT,
    -- Recorded on claim (see PgRunRepository::claim_due) but not yet acted
    -- upon: nothing currently reads this column to reclaim a run whose owner
    -- died mid-lease. That reclaim-on-expiry behavior arrives with the
    -- Phase 2/3 reaper. Until then a claimed run whose owner disappears
    -- stays `claimed` forever -- an accepted Phase 1 scope boundary.
    lease_expires_at TIMESTAMPTZ
);

-- Supports the due-window scan used by the claim.
CREATE INDEX IF NOT EXISTS idx_job_runs_due
    ON job_runs (state, scheduled_at);
