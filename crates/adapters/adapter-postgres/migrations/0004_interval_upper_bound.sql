-- Bound the interval period in the database too.
--
-- `Schedule::interval` rejects anything above MAX_INTERVAL_SECS because
-- `now + period` must stay inside the representable time range; past that it
-- overflowed and panicked the engine's materialize loop, stalling scheduling
-- for every tenant. The existing CHECK only enforced `> 0`, so a row written
-- by anything other than the repository -- a migration, a fixup script, an
-- older binary -- could still poison the engine, and it would survive every
-- restart.
--
-- 10 * 366 days, matching scheduler_domain::MAX_INTERVAL_SECS. The two are
-- kept in sync by hand; the domain constant is the authority.
ALTER TABLE jobs DROP CONSTRAINT jobs_interval_positive;

ALTER TABLE jobs ADD CONSTRAINT jobs_interval_bounded
    CHECK (every_secs IS NULL OR (every_secs > 0 AND every_secs <= 316224000));
