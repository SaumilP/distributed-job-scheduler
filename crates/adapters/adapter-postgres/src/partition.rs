//! Range-partition maintenance for `job_runs`.
//!
//! The `0005` migration seeds a starter week of daily partitions and a `DEFAULT`
//! partition. That is enough for a demo but not for a running system: partitions
//! have to be created *ahead* of the horizon so new runs land in a real range
//! rather than accumulating in `DEFAULT`, and old ones dropped once every run in
//! them is long finished. These two functions are that maintenance, meant to be
//! called on a schedule (a cron, a K8s `CronJob`, or `pg_partman` in a larger
//! deployment — see `ARCHITECTURE.md`).
//!
//! Both build DDL by string formatting because a partition name and its bounds
//! cannot be bind parameters — you cannot bind an identifier, and a partition
//! bound is parsed at DDL time. The values interpolated here are derived
//! entirely from dates this module computes, never from caller-supplied text, so
//! there is no injection surface; the naming convention (`job_runs_YYYYMMDD`,
//! one partition per UTC day) is owned here and nowhere else.

use sqlx::PgPool;
use time::{Date, Month, OffsetDateTime};

/// The partition covering the UTC day `d`, named `job_runs_YYYYMMDD`.
fn partition_name(d: Date) -> String {
    format!(
        "job_runs_{:04}{:02}{:02}",
        d.year(),
        u8::from(d.month()),
        d.day()
    )
}

/// A `YYYY-MM-DD` literal for a range bound. Postgres parses it to `timestamptz`
/// at midnight in the session time zone, matching the `d::timestamptz` bounds
/// the `0005` migration used, so helper-created partitions abut the seeded ones
/// without a gap or an overlap.
fn day_literal(d: Date) -> String {
    format!("{:04}-{:02}-{:02}", d.year(), u8::from(d.month()), d.day())
}

/// Parses `job_runs_YYYYMMDD` back to its day. `None` for any other name —
/// notably `job_runs_default`, which this module must never treat as a range.
fn parse_partition_day(name: &str) -> Option<Date> {
    let digits = name.strip_prefix("job_runs_")?;
    if digits.len() != 8 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let year: i32 = digits[0..4].parse().ok()?;
    let month = Month::try_from(digits[4..6].parse::<u8>().ok()?).ok()?;
    let day: u8 = digits[6..8].parse().ok()?;
    Date::from_calendar_date(year, month, day).ok()
}

/// Creates a daily range partition for every UTC day in `[from, to)` that does
/// not already exist. Returns how many it created, so a caller can tell a
/// first run (created several) from steady state (created none).
///
/// Idempotent by construction: it checks `pg_class` for each day's partition and
/// skips the ones already present, rather than relying on `IF NOT EXISTS` —
/// which this project keeps off `CREATE TABLE` so a name collision is a loud
/// error, not a silent no-op.
pub async fn ensure_partitions(
    pool: &PgPool,
    from: OffsetDateTime,
    to: OffsetDateTime,
) -> Result<u64, sqlx::Error> {
    let mut created = 0u64;
    let mut day = from.date();
    let end = to.date();
    while day < end {
        let name = partition_name(day);
        let exists: Option<i32> =
            sqlx::query_scalar("SELECT 1 FROM pg_class WHERE relname = $1 AND relkind = 'r'")
                .bind(&name)
                .fetch_optional(pool)
                .await?;
        if exists.is_none() {
            let next = day.next_day().expect("date overflow building partitions");
            let ddl = format!(
                "CREATE TABLE {name} PARTITION OF job_runs FOR VALUES FROM ('{}') TO ('{}')",
                day_literal(day),
                day_literal(next),
            );
            sqlx::query(&ddl).execute(pool).await?;
            created += 1;
        }
        day = day.next_day().expect("date overflow building partitions");
    }
    Ok(created)
}

/// Drops every daily range partition whose entire range is at or before
/// `cutoff` — i.e. whose upper bound (`day + 1` at midnight) is `<= cutoff`.
/// Returns how many it dropped.
///
/// **Never drops `job_runs_default`.** The default partition is the correctness
/// backstop for out-of-range inserts; dropping it would make an insert outside
/// every range fail. Anything whose name does not parse as a `YYYYMMDD` day is
/// left alone for the same reason — this function only ever removes partitions
/// it can prove are fully in the past.
pub async fn drop_partitions_before(
    pool: &PgPool,
    cutoff: OffsetDateTime,
) -> Result<u64, sqlx::Error> {
    let children: Vec<(String,)> = sqlx::query_as(
        "SELECT c.relname
         FROM pg_inherits i
         JOIN pg_class c ON c.oid = i.inhrelid
         JOIN pg_class p ON p.oid = i.inhparent
         WHERE p.relname = 'job_runs'",
    )
    .fetch_all(pool)
    .await?;

    let mut dropped = 0u64;
    for (name,) in children {
        let Some(day) = parse_partition_day(&name) else {
            continue; // job_runs_default, or anything not a dated range.
        };
        let upper = day
            .next_day()
            .expect("date overflow dropping partitions")
            .midnight()
            .assume_utc();
        if upper <= cutoff {
            // `name` is `job_runs_YYYYMMDD`, validated by `parse_partition_day`
            // above — no injection surface.
            sqlx::query(&format!("DROP TABLE {name}"))
                .execute(pool)
                .await?;
            dropped += 1;
        }
    }
    Ok(dropped)
}
