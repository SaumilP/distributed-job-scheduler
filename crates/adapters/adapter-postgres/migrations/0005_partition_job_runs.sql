-- Convert job_runs to RANGE partitioning on scheduled_at.
--
-- No IF NOT EXISTS anywhere (project rule; sqlx tracks applied migrations, so
-- the guard would only ever mask schema drift).
--
-- The swap is data-preserving: production may run this against a populated
-- table, and a migration that only works on an empty one is a latent failure.
--
-- The primary key changes from (id) to (id, scheduled_at): Postgres requires
-- every unique/primary key on a partitioned table to contain the partition
-- column. `id` stays first so it remains the practical surrogate key that event
-- payloads and lease bookkeeping reference; scheduled_at is appended only to
-- satisfy the partitioning rule. The existing UNIQUE (job_id, scheduled_at)
-- already contains the key and is recreated unchanged, so `insert_runs`'
-- `ON CONFLICT (job_id, scheduled_at)` still resolves against the parent.

-- Rename the table AND every named object it owns out of the way first: the new
-- partitioned table recreates a primary key, a unique constraint and an index
-- under the original names, and Postgres index/constraint names are unique per
-- schema, so the old ones must move aside even though they are dropped with the
-- old table at the end. (Renaming a constraint renames its backing index too.)
ALTER TABLE job_runs RENAME TO job_runs_unpartitioned;
ALTER TABLE job_runs_unpartitioned RENAME CONSTRAINT job_runs_pkey
    TO job_runs_unpartitioned_pkey;
ALTER TABLE job_runs_unpartitioned RENAME CONSTRAINT job_runs_job_scheduled_unique
    TO job_runs_unpartitioned_job_scheduled_unique;
ALTER INDEX idx_job_runs_due RENAME TO idx_job_runs_due_unpartitioned;

CREATE TABLE job_runs (
    id            UUID        NOT NULL,
    job_id        UUID        NOT NULL,
    tenant        TEXT        NOT NULL,
    scheduled_at  TIMESTAMPTZ NOT NULL,
    state         TEXT        NOT NULL DEFAULT 'pending'
                              CHECK (state IN ('pending','claimed','running','succeeded','failed','dead')),
    attempt       INT         NOT NULL DEFAULT 0,
    lease_owner   TEXT,
    lease_expires_at TIMESTAMPTZ,
    PRIMARY KEY (id, scheduled_at),
    CONSTRAINT job_runs_job_scheduled_unique UNIQUE (job_id, scheduled_at)
) PARTITION BY RANGE (scheduled_at);

-- Supports the due-window scan used by the claim, exactly as before. On a
-- partitioned parent this creates the matching index on every partition.
CREATE INDEX idx_job_runs_due ON job_runs (state, scheduled_at);

-- The DEFAULT partition guarantees no insert ever fails for lack of a matching
-- range. It is the correctness backstop; the range partitions below are the
-- performance path. A row landing here is a signal that maintenance has not
-- created the range it needed yet -- monitorable, never fatal.
CREATE TABLE job_runs_default PARTITION OF job_runs DEFAULT;

-- Seed a small window of daily range partitions around "now" so a fresh
-- deployment has real partitions to prune against immediately, without waiting
-- for the maintenance helper's first run. Production widens this window and
-- automates it (see adapter-postgres `partition` module and the pg_partman note
-- in ARCHITECTURE.md). A DO block because plain SQL migrations cannot loop.
DO $$
DECLARE
    d date := current_date - 1;
    stop date := current_date + 7;
BEGIN
    WHILE d < stop LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF job_runs FOR VALUES FROM (%L) TO (%L)',
            'job_runs_' || to_char(d, 'YYYYMMDD'),
            d::timestamptz,
            (d + 1)::timestamptz
        );
        d := d + 1;
    END LOOP;
END $$;

-- Move existing rows. They route to the matching range partition, or to
-- job_runs_default if outside the seeded window -- either way nothing is lost.
INSERT INTO job_runs
SELECT id, job_id, tenant, scheduled_at, state, attempt, lease_owner, lease_expires_at
FROM job_runs_unpartitioned;

DROP TABLE job_runs_unpartitioned;
