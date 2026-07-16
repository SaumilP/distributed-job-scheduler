-- Run identity: a job's run at a given instant is one run, forever.
--
-- The delivery story for this scheduler is "at-least-once + idempotency =
-- effectively-once". This constraint is the idempotency half, and it lives in
-- the schema rather than in application code on purpose: the materializer,
-- a retried API call, and a re-delivered NATS message are three independent
-- writers, and only the database sees all three.
--
-- `(job_id, scheduled_at)` is the natural key. `id` stays the surrogate
-- primary key because it is what event payloads and lease bookkeeping
-- reference, and it must stay stable even when a duplicate materialization
-- proposes a different `id` for the same logical run -- the proposal loses,
-- the original row and its `id` survive.
--
-- No `IF NOT EXISTS`: sqlx tracks applied migrations, so the guard would only
-- ever mask schema drift.
ALTER TABLE job_runs
    ADD CONSTRAINT job_runs_job_scheduled_unique UNIQUE (job_id, scheduled_at);
