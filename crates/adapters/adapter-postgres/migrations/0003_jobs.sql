-- Jobs: the schedules that runs are materialized from.
--
-- `Schedule` is an enum in the domain, and it is stored as a CHECK-constrained
-- discriminant plus one column per shape rather than as JSON. JSON would move
-- the failure from write time to read time: a malformed schedule would insert
-- happily and then fail to deserialize inside the engine's materialize loop,
-- far from whoever wrote it. A constraint fails the write that is actually
-- wrong.
CREATE TABLE jobs (
    id          UUID PRIMARY KEY,
    tenant      TEXT NOT NULL,
    target      TEXT NOT NULL,
    kind        TEXT NOT NULL CHECK (kind IN ('interval','oneshot')),
    every_secs  BIGINT,
    fire_at     TIMESTAMPTZ,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Exactly one shape must be populated, and it must match `kind`. Without
    -- this, an 'interval' row with a NULL every_secs reads back as a job that
    -- cannot be scheduled at all.
    CONSTRAINT jobs_schedule_shape CHECK (
        (kind = 'interval' AND every_secs IS NOT NULL AND fire_at IS NULL) OR
        (kind = 'oneshot'  AND fire_at    IS NOT NULL AND every_secs IS NULL)
    ),

    -- `Schedule::interval` rejects a non-positive period because a
    -- non-advancing interval makes the materializer loop forever. That rule
    -- has to hold for rows written by anything other than the repository too.
    CONSTRAINT jobs_interval_positive CHECK (every_secs IS NULL OR every_secs > 0)
);

-- Partial index: the materializer only ever asks for active jobs, so the
-- inactive ones do not belong in the index at all.
CREATE INDEX idx_jobs_active ON jobs (active) WHERE active;
