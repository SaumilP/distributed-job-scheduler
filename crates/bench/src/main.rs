//! A load harness for the claim and materialize paths, against **real**
//! Postgres and NATS.
//!
//! # What it measures, and how to read it
//!
//! Three numbers, each with a specific meaning:
//!
//! - **materialize cost per tick** — wall time to propose one full horizon of
//!   runs for every active job, once. This is the work the engine's materialize
//!   loop does every `POLL_INTERVAL_MS`.
//! - **claim throughput (runs/sec)** — how fast a single claimer drains a
//!   standing backlog of due runs through `claim_due` (`SELECT … FOR UPDATE SKIP
//!   LOCKED`) and publishes each to NATS. One claimer, not the N the engine
//!   could run.
//! - **due-lag p50/p95/p99 (seconds)** — for each claimed run, `now −
//!   scheduled_at`: how late it was when claimed. Measured **exactly** from
//!   per-run samples, not estimated from histogram buckets.
//!
//! # The environment is part of the result
//!
//! Postgres and NATS run in Docker on the **same host as this load generator**,
//! reached over a mapped port. That has two consequences the numbers cannot be
//! separated from and which this harness prints alongside them:
//!
//! 1. Every `claim_due` and every publish crosses Docker's port forwarding, so
//!    a large share of the per-operation cost is loopback + NAT round-trip, not
//!    database or broker work. The claim loop is deliberately single-threaded
//!    and issues one round-trip per batch, so throughput here is **bounded by
//!    round-trip latency times batches**, and a bigger `BATCH` raises it almost
//!    linearly — which is the tell that the harness, not the query, is the
//!    limit.
//! 2. The database and the load generator share CPU and memory, so under load
//!    they contend. A dedicated database on another host would not.
//!
//! **This is a laptop-with-Docker figure. It is not a production capacity
//! statement, and the 100M-schedule design target remains designed-for and
//! unverified.** See the closing notes the run prints.

use adapter_nats::{NatsEventPublisher, ensure_stream};
use adapter_postgres::{PgJobRepository, PgRunRepository};
use adapter_redis::RedisDueIndex;
use scheduler_application::MaterializeDueRuns;
use scheduler_application::testing::{FixedClock, NoopMetrics};
use scheduler_domain::{
    EventPublisher, Job, JobId, JobRepository, JobRun, RunRepository, Schedule, TenantId,
};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use uuid::Uuid;

/// One knob, read from the environment with a default. Parsed strictly: a
/// malformed value stops the run rather than silently taking the default and
/// producing a number for parameters nobody chose.
fn env_u64(key: &str, default: u64) -> u64 {
    match std::env::var(key) {
        Err(_) => default,
        Ok(raw) => raw
            .trim()
            .parse()
            .unwrap_or_else(|e| panic!("{key} must be a non-negative integer, got {raw:?}: {e}")),
    }
}

struct Params {
    jobs: u64,
    interval_secs: u64,
    horizon_secs: u64,
    batch: i64,
    reps: u64,
}

impl Params {
    fn from_env() -> Self {
        Self {
            jobs: env_u64("BENCH_JOBS", 200),
            interval_secs: env_u64("BENCH_INTERVAL_SECS", 60),
            horizon_secs: env_u64("BENCH_HORIZON_SECS", 3600),
            batch: env_u64("BENCH_BATCH", 500) as i64,
            reps: env_u64("BENCH_REPS", 3).max(1),
        }
    }
}

/// Exact percentile of a *sorted* sample slice, nearest-rank. Empty slice → 0.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[idx]
}

fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        0.0
    } else {
        xs.iter().sum::<f64>() / xs.len() as f64
    }
}

/// Truncates, seeds `p.jobs` jobs anchored `horizon` in the past, materializes a
/// full horizon for each, and returns every run proposed — a fresh standing
/// backlog whose `scheduled_at` values are all in the past (due). Used by the
/// Redis-vs-scan comparison, which needs the same backlog built twice.
async fn materialize_backlog(
    pool: &sqlx::PgPool,
    jobs_repo: &PgJobRepository,
    runs_repo: &PgRunRepository,
    metrics: &Arc<dyn scheduler_domain::Metrics>,
    p: &Params,
) -> anyhow::Result<Vec<JobRun>> {
    sqlx::query("TRUNCATE job_runs, jobs").execute(pool).await?;
    let base = OffsetDateTime::now_utc() - Duration::from_secs(p.horizon_secs);
    let mut runs = Vec::new();
    for i in 0..p.jobs {
        let job = Job {
            id: JobId(Uuid::new_v4()),
            tenant: TenantId(format!("tenant-{}", i % 16)),
            schedule: Schedule::Interval {
                every_secs: p.interval_secs as i64,
            },
            target: "bench://sink".to_string(),
            created_at: base,
        };
        jobs_repo.insert(&job).await?;
        let uc = MaterializeDueRuns {
            runs: runs_repo.clone(),
            clock: FixedClock(base),
            horizon_secs: p.horizon_secs as i64,
            metrics: metrics.clone(),
        };
        runs.extend(uc.run(&job).await?);
    }
    Ok(runs)
}

/// The outcome of one rep, so the loop can report variance rather than a single
/// figure — a benchmark that reports one number hides its own noise.
struct RepResult {
    materialize: Duration,
    proposed: u64,
    claim_throughput: f64,
    claim_due_time: Duration,
    publish_time: Duration,
    lags: Vec<f64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let p = Params::from_env();

    // --- environment, printed so every number below is qualified by it -------
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());
    println!("=== distributed-job-scheduler bench ===");
    println!("host: {host}   logical CPUs: {cpus}");
    println!(
        "postgres & nats: Docker (testcontainers), reached over a mapped port, \
         sharing this host with the load generator"
    );
    println!(
        "params: jobs={} interval={}s horizon={}s batch={} reps={}",
        p.jobs, p.interval_secs, p.horizon_secs, p.batch, p.reps
    );
    println!();

    // --- real infrastructure -------------------------------------------------
    let pg_port = test_support::postgres_port();
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&test_support::postgres_url(pg_port))
        .await?;
    adapter_postgres::run_migrations(&pool).await?;

    let nats_port = test_support::nats_port();
    let client = async_nats::connect(test_support::nats_url(nats_port)).await?;
    let js = async_nats::jetstream::new(client);
    ensure_stream(&js)
        .await
        .map_err(|e| anyhow::anyhow!("ensure_stream: {e}"))?;

    let jobs_repo = PgJobRepository { pool: pool.clone() };
    let runs_repo = PgRunRepository { pool: pool.clone() };
    let publisher = NatsEventPublisher::new(js);
    let noop: Arc<dyn scheduler_domain::Metrics> = Arc::new(NoopMetrics);

    let mut results: Vec<RepResult> = Vec::new();

    for rep in 1..=p.reps {
        // A clean slate each rep, so a run measures draining a fresh backlog and
        // not the leftovers of the previous one. TRUNCATE both tables together
        // in case a foreign key ties them.
        sqlx::query("TRUNCATE job_runs, jobs")
            .execute(&pool)
            .await?;

        // Anchor every job `horizon` in the past, so materializing a full
        // horizon from that anchor proposes runs whose scheduled_at spans
        // [base, base+horizon] = [now-horizon, now] — all already due, with a
        // realistic spread of lateness rather than a single instant.
        let base = OffsetDateTime::now_utc() - Duration::from_secs(p.horizon_secs);

        let mut job_ids = Vec::with_capacity(p.jobs as usize);
        for i in 0..p.jobs {
            let job = Job {
                id: JobId(Uuid::new_v4()),
                tenant: TenantId(format!("tenant-{}", i % 16)),
                schedule: Schedule::Interval {
                    every_secs: p.interval_secs as i64,
                },
                target: "bench://sink".to_string(),
                created_at: base,
            };
            jobs_repo.insert(&job).await?;
            job_ids.push(job);
        }

        // --- materialize: one full pass over every job, timed ----------------
        let mat_start = Instant::now();
        let mut proposed = 0u64;
        for job in &job_ids {
            let uc = MaterializeDueRuns {
                runs: runs_repo.clone(),
                clock: FixedClock(base),
                horizon_secs: p.horizon_secs as i64,
                metrics: noop.clone(),
            };
            proposed += uc.run(job).await?.len() as u64;
        }
        let materialize = mat_start.elapsed();

        // --- claim + publish: drain the standing backlog, timed --------------
        let mut lags: Vec<f64> = Vec::new();
        let mut claimed_total = 0u64;
        let mut claim_due_time = Duration::ZERO;
        let mut publish_time = Duration::ZERO;

        let drain_start = Instant::now();
        loop {
            let now = OffsetDateTime::now_utc();
            let t0 = Instant::now();
            // Uncapped: the bench measures raw drain throughput, not fairness.
            let outcome = runs_repo.claim_due(now, p.batch, "bench", 0).await?;
            claim_due_time += t0.elapsed();

            if outcome.claimed.is_empty() {
                break;
            }
            for run in &outcome.claimed {
                let t1 = Instant::now();
                publisher.publish_run(run).await?;
                publish_time += t1.elapsed();
                lags.push((now - run.scheduled_at).as_seconds_f64());
            }
            claimed_total += outcome.claimed.len() as u64;
        }
        let drain = drain_start.elapsed();
        let claim_throughput = claimed_total as f64 / drain.as_secs_f64().max(f64::MIN_POSITIVE);

        println!(
            "rep {rep}/{}: proposed={proposed} claimed={claimed_total} \
             materialize={:.3}s drain={:.3}s throughput={:.0} runs/s",
            p.reps,
            materialize.as_secs_f64(),
            drain.as_secs_f64(),
            claim_throughput
        );

        results.push(RepResult {
            materialize,
            proposed,
            claim_throughput,
            claim_due_time,
            publish_time,
            lags,
        });
    }

    // --- aggregate -----------------------------------------------------------
    println!();
    println!("=== results across {} reps ===", p.reps);

    let throughputs: Vec<f64> = results.iter().map(|r| r.claim_throughput).collect();
    let mat_secs: Vec<f64> = results
        .iter()
        .map(|r| r.materialize.as_secs_f64())
        .collect();
    let proposed_each = results.first().map(|r| r.proposed).unwrap_or(0);

    println!(
        "materialize cost per tick ({} jobs): mean {:.3}s  (min {:.3}s, max {:.3}s)  \
         → {:.2} ms/job, {} runs proposed/tick",
        p.jobs,
        mean(&mat_secs),
        mat_secs.iter().cloned().fold(f64::INFINITY, f64::min),
        mat_secs.iter().cloned().fold(0.0, f64::max),
        mean(&mat_secs) * 1000.0 / p.jobs.max(1) as f64,
        proposed_each,
    );
    println!(
        "claim throughput: mean {:.0} runs/s  (min {:.0}, max {:.0}) — ONE claimer",
        mean(&throughputs),
        throughputs.iter().cloned().fold(f64::INFINITY, f64::min),
        throughputs.iter().cloned().fold(0.0, f64::max),
    );

    // Due-lag over every sample from every rep, exact.
    let mut all_lags: Vec<f64> = results
        .iter()
        .flat_map(|r| r.lags.iter().copied())
        .collect();
    all_lags.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "due-lag seconds (exact, n={}): p50 {:.1}  p95 {:.1}  p99 {:.1}  max {:.1}",
        all_lags.len(),
        percentile(&all_lags, 50.0),
        percentile(&all_lags, 95.0),
        percentile(&all_lags, 99.0),
        all_lags.last().copied().unwrap_or(0.0),
    );

    // --- where the time went: the bottleneck question this phase exists for --
    let total_claim_due: f64 = results.iter().map(|r| r.claim_due_time.as_secs_f64()).sum();
    let total_publish: f64 = results.iter().map(|r| r.publish_time.as_secs_f64()).sum();
    let split = total_claim_due + total_publish;
    println!();
    println!("=== where the claim loop spent its time ===");
    if split > 0.0 {
        println!(
            "claim_due (SKIP LOCKED query): {:.1}%   publish (NATS): {:.1}%",
            100.0 * total_claim_due / split,
            100.0 * total_publish / split,
        );
    }
    println!(
        "Both legs cross Docker's port forward. The single-threaded loop issues \
         one claim_due round-trip per batch of {} and one publish round-trip per \
         run, so the dominant leg above is where a round-trip is paid most often \
         — which is a statement about the harness's shape as much as the system's.",
        p.batch
    );

    // --- Redis hot index vs Postgres scan, claim only ------------------------
    //
    // The comparison Phase 3c-3 exists to make. Publishing is excluded on both
    // sides so this isolates the *claim mechanism*: the scan path claims via one
    // `claim_due` (`SKIP LOCKED` over the due index) per batch; the Redis path
    // pops candidate ids from Redis and claims exactly those via `claim_ids`.
    // Same backlog size, built fresh for each so neither drains the other's.
    {
        let index =
            RedisDueIndex::connect(&test_support::redis_url(test_support::redis_port())).await?;

        // Scan path.
        let _backlog = materialize_backlog(&pool, &jobs_repo, &runs_repo, &noop, &p).await?;
        let scan_now = OffsetDateTime::now_utc();
        let scan_start = Instant::now();
        let mut scan_claimed = 0u64;
        loop {
            let o = runs_repo.claim_due(scan_now, p.batch, "bench", 0).await?;
            if o.claimed.is_empty() {
                break;
            }
            scan_claimed += o.claimed.len() as u64;
        }
        let scan_secs = scan_start.elapsed().as_secs_f64();

        // Redis path: rebuild the same backlog, index it, drain via pop + claim_ids.
        let backlog = materialize_backlog(&pool, &jobs_repo, &runs_repo, &noop, &p).await?;
        index.push(&backlog).await?;
        let redis_now = OffsetDateTime::now_utc();
        let redis_start = Instant::now();
        let mut redis_claimed = 0u64;
        loop {
            let ids = index.pop_due(redis_now, p.batch).await?;
            if ids.is_empty() {
                break;
            }
            let o = runs_repo.claim_ids(&ids, redis_now, "bench", 0).await?;
            redis_claimed += o.claimed.len() as u64;
        }
        let redis_secs = redis_start.elapsed().as_secs_f64();

        let scan_tput = scan_claimed as f64 / scan_secs.max(f64::MIN_POSITIVE);
        let redis_tput = redis_claimed as f64 / redis_secs.max(f64::MIN_POSITIVE);
        println!();
        println!("=== Redis hot index vs Postgres scan (claim only, no publish) ===");
        println!(
            "scan  (claim_due):            {scan_tput:.0} runs/s  ({scan_claimed} runs in {scan_secs:.3}s)"
        );
        println!(
            "redis (pop_due + claim_ids):  {redis_tput:.0} runs/s  ({redis_claimed} runs in {redis_secs:.3}s)"
        );
        let verdict = if redis_tput >= scan_tput * 1.05 {
            format!(
                "the index is {:.0}% FASTER",
                100.0 * (redis_tput / scan_tput - 1.0)
            )
        } else if redis_tput <= scan_tput * 0.95 {
            format!(
                "the index is {:.0}% SLOWER",
                100.0 * (1.0 - redis_tput / scan_tput)
            )
        } else {
            "no material difference".to_string()
        };
        println!(
            "verdict: {verdict}. The Redis path adds a pop_due round-trip per batch \
             and still does the same claim_ids SKIP LOCKED write, so it can only pay \
             off if the scan the pop replaces was the bottleneck — which Phase 3b \
             measured at 3% of the claim loop. This is the evidence for the Redis \
             decision, not an assertion of it."
        );
    }

    // --- what these numbers do NOT mean --------------------------------------
    println!();
    println!("=== what this does NOT tell you ===");
    println!(
        "- It is a single-claimer, single-host, containerised measurement. The \
         engine runs multiple claimers; SKIP LOCKED is designed for exactly that \
         concurrency, and none of it is exercised here."
    );
    println!(
        "- due-lag here is backlog-drain lag: a pre-built backlog drained as fast \
         as one claimer can. It is the saturation case, not steady-state lateness \
         under a matched arrival rate."
    );
    println!(
        "- Absolute throughput includes Docker port-forward latency on every \
         round-trip. Treat it as a floor for this setup, not a property of the \
         scheduler."
    );
    println!(
        "- Do NOT extrapolate to 100M. Linear scaling would ignore connection \
         limits, index growth on job_runs, partition maintenance, per-tenant skew, \
         and the fact that materialize re-proposes the whole horizon every tick. \
         The 100M target remains designed-for and UNVERIFIED."
    );

    Ok(())
}
