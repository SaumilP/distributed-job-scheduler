//! A Prometheus adapter for the domain's [`Metrics`] port.
//!
//! It is a driven adapter: the hexagon defines the [`Metric`] set and the
//! synchronous recording contract, and this crate maps that set to Prometheus
//! names, buckets and the text exposition format. The mapping lives *here* and
//! only here — the enum in the domain is closed, so a metric can never be
//! recorded under a name this crate has not been taught.
//!
//! # Why hand-rolled rather than `metrics-exporter-prometheus`
//!
//! The [`Metrics`] port is synchronous precisely so recording cannot suspend a
//! hot loop, and the same reasoning rules out recording that blocks, allocates
//! per call, or takes a contended lock. The `metrics` facade records through a
//! keyed registry lookup — a map guarded by a lock — on every `counter!` /
//! `histogram!`. Against a closed enum that is unnecessary: each metric maps to
//! a fixed array slot, so `incr` is one relaxed `fetch_add` and `observe` is a
//! short bounded loop of them, with no allocation and no lock. It also keeps a
//! second hyper/TLS/quanta tree out of the build and off the 1.88.0 MSRV
//! surface.
//!
//! # What it does *not* do
//!
//! It does not serve HTTP and it does not register a global recorder. The
//! caller holds the registry and renders it where it wants to (the API's
//! `/metrics` route). One process's registry sees only that process's metrics:
//! the engine, worker and API each hold their own, and nothing here aggregates
//! across them.

use scheduler_domain::{Metric, Metrics};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

/// Second-valued histogram buckets, for latencies that span sub-second to
/// several minutes (`due_lag_seconds`). The top bucket is well past
/// `LEASE_SECS`, so a run late enough to be reclaimed still lands in a bucket
/// rather than only in `+Inf`.
const SECONDS_BUCKETS: &[f64] = &[
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
];

/// Execution-time buckets, skewed shorter than `SECONDS_BUCKETS`: a run's own
/// work is expected in the milliseconds-to-seconds range, and resolution there
/// is what informs the `LEASE_SECS` argument.
const EXEC_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// Count-valued buckets for `claim_batch_size`. Chosen around the default
/// `BATCH_SIZE` of 100 so it is visible whether batches come back full
/// (saturated) or short (idle).
const BATCH_BUCKETS: &[f64] = &[1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0];

#[derive(Clone, Copy)]
enum Kind {
    Counter,
    Histogram(&'static [f64]),
}

struct Def {
    name: &'static str,
    help: &'static str,
    kind: Kind,
}

/// The name/help/kind mapping. **Order matters:** it is the slot order that
/// [`slot_index`] returns, and `defs_and_slots_agree` pins the two together so a
/// reordering that desynchronised them would fail rather than mis-record.
const DEFS: &[Def] = &[
    Def {
        name: "runs_materialized",
        help: "Runs proposed by the materializer (proposed, not created; insert_runs does not report affected rows).",
        kind: Kind::Counter,
    },
    Def {
        name: "runs_claimed",
        help: "Runs claimed for dispatch.",
        kind: Kind::Counter,
    },
    Def {
        name: "claim_batch_size",
        help: "Size of each claim batch.",
        kind: Kind::Histogram(BATCH_BUCKETS),
    },
    Def {
        name: "runs_published",
        help: "Runs successfully published to the broker.",
        kind: Kind::Counter,
    },
    Def {
        name: "publish_failures",
        help: "Publish attempts that failed.",
        kind: Kind::Counter,
    },
    Def {
        name: "runs_released",
        help: "Claimed runs released back to pending after a publish failure.",
        kind: Kind::Counter,
    },
    Def {
        name: "runs_reclaimed",
        help: "Expired leases reclaimed by the reaper.",
        kind: Kind::Counter,
    },
    Def {
        name: "runs_buried",
        help: "Runs buried as Dead after exhausting attempts.",
        kind: Kind::Counter,
    },
    Def {
        name: "due_lag_seconds",
        help: "now - scheduled_at at claim time, in seconds.",
        kind: Kind::Histogram(SECONDS_BUCKETS),
    },
    Def {
        name: "execution_seconds",
        help: "Worker execution wall time, in seconds.",
        kind: Kind::Histogram(EXEC_BUCKETS),
    },
];

/// Maps a [`Metric`] to its slot in `DEFS` and the registry. A `match` rather
/// than a derived discriminant so the domain enum stays free of any ordering
/// obligation to this adapter, and so adding a metric fails to compile here
/// until it is mapped.
fn slot_index(metric: Metric) -> usize {
    match metric {
        Metric::RunsMaterialized => 0,
        Metric::RunsClaimed => 1,
        Metric::ClaimBatchSize => 2,
        Metric::RunsPublished => 3,
        Metric::PublishFailures => 4,
        Metric::RunsReleased => 5,
        Metric::RunsReclaimed => 6,
        Metric::RunsBuried => 7,
        Metric::DueLagSeconds => 8,
        Metric::ExecutionSeconds => 9,
    }
}

/// A histogram with fixed, cumulative `le` buckets.
///
/// Each bucket holds the count of observations `<= bound`, so `observe`
/// increments every bucket whose bound is `>= value`. That is O(buckets) per
/// call over ~13 relaxed atomics — no allocation, no lock — and lets `render`
/// print each bucket directly as its cumulative Prometheus value.
struct Hist {
    bounds: &'static [f64],
    buckets: Box<[AtomicU64]>,
    count: AtomicU64,
    /// The running sum, as the bits of an `f64`. Updated with a compare-exchange
    /// loop: lock-free and allocation-free. The loop can spin under extreme
    /// contention but never blocks a thread the way a mutex would.
    sum_bits: AtomicU64,
}

impl Hist {
    fn observe(&self, value: f64) {
        for (i, &bound) in self.bounds.iter().enumerate() {
            if value <= bound {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);

        let mut current = self.sum_bits.load(Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(current) + value).to_bits();
            match self.sum_bits.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }
}

enum Slot {
    Counter(AtomicU64),
    Histogram(Hist),
}

/// A process-local Prometheus metrics registry.
///
/// Cheap to share: implements [`Metrics`], and `Arc<PrometheusMetrics>` does too
/// (via the blanket impl in the domain), so one registry can back every loop in
/// a process while a single reader renders it.
pub struct PrometheusMetrics {
    slots: Vec<Slot>,
}

impl Default for PrometheusMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PrometheusMetrics {
    pub fn new() -> Self {
        let slots = DEFS
            .iter()
            .map(|def| match def.kind {
                Kind::Counter => Slot::Counter(AtomicU64::new(0)),
                Kind::Histogram(bounds) => Slot::Histogram(Hist {
                    bounds,
                    buckets: bounds.iter().map(|_| AtomicU64::new(0)).collect(),
                    count: AtomicU64::new(0),
                    sum_bits: AtomicU64::new(0),
                }),
            })
            .collect();
        Self { slots }
    }

    /// A registry behind an `Arc`, the shape the binaries share across loops.
    pub fn shared() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new())
    }

    /// Renders the whole registry in the Prometheus text exposition format.
    ///
    /// Every metric is emitted, including those still at zero: a scraper should
    /// see the full schema from the first scrape rather than have series appear
    /// only once they are first touched.
    pub fn render(&self) -> String {
        let mut out = String::new();
        for (def, slot) in DEFS.iter().zip(&self.slots) {
            let _ = writeln!(out, "# HELP {} {}", def.name, def.help);
            match slot {
                Slot::Counter(value) => {
                    let _ = writeln!(out, "# TYPE {} counter", def.name);
                    let _ = writeln!(out, "{} {}", def.name, value.load(Ordering::Relaxed));
                }
                Slot::Histogram(hist) => {
                    let _ = writeln!(out, "# TYPE {} histogram", def.name);
                    for (i, &bound) in hist.bounds.iter().enumerate() {
                        let _ = writeln!(
                            out,
                            "{}_bucket{{le=\"{}\"}} {}",
                            def.name,
                            bound,
                            hist.buckets[i].load(Ordering::Relaxed)
                        );
                    }
                    let count = hist.count.load(Ordering::Relaxed);
                    let _ = writeln!(out, "{}_bucket{{le=\"+Inf\"}} {}", def.name, count);
                    let sum = f64::from_bits(hist.sum_bits.load(Ordering::Relaxed));
                    let _ = writeln!(out, "{}_sum {}", def.name, sum);
                    let _ = writeln!(out, "{}_count {}", def.name, count);
                }
            }
        }
        out
    }
}

impl Metrics for PrometheusMetrics {
    fn incr(&self, metric: Metric, by: u64) {
        match &self.slots[slot_index(metric)] {
            Slot::Counter(value) => {
                value.fetch_add(by, Ordering::Relaxed);
            }
            // A histogram recorded through `incr` is a wiring bug, not a runtime
            // condition. Assert loudly in debug, ignore in release rather than
            // corrupt an unrelated series.
            Slot::Histogram(_) => debug_assert!(false, "incr on histogram metric {metric:?}"),
        }
    }

    fn observe(&self, metric: Metric, value: f64) {
        match &self.slots[slot_index(metric)] {
            Slot::Histogram(hist) => hist.observe(value),
            Slot::Counter(_) => debug_assert!(false, "observe on counter metric {metric:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every metric the domain defines, so the tests below cover the whole
    /// closed set rather than a sample of it.
    const ALL: &[Metric] = &[
        Metric::RunsMaterialized,
        Metric::RunsClaimed,
        Metric::ClaimBatchSize,
        Metric::RunsPublished,
        Metric::PublishFailures,
        Metric::RunsReleased,
        Metric::RunsReclaimed,
        Metric::RunsBuried,
        Metric::DueLagSeconds,
        Metric::ExecutionSeconds,
    ];

    /// The one invariant the whole slot scheme rests on: `slot_index` and the
    /// order of `DEFS` must not drift apart, or a metric records into the wrong
    /// series. `ALL` must also list every metric, so a new variant that was not
    /// added here (or not mapped in `slot_index`) is caught.
    #[test]
    fn defs_and_slots_agree() {
        assert_eq!(ALL.len(), DEFS.len(), "ALL must list every metric");
        let mut seen = std::collections::BTreeSet::new();
        for &m in ALL {
            let idx = slot_index(m);
            assert!(idx < DEFS.len(), "{m:?} maps out of range");
            assert!(seen.insert(idx), "two metrics map to slot {idx}");
        }
    }

    #[test]
    fn render_emits_every_metric_even_at_zero() {
        let render = PrometheusMetrics::new().render();
        for def in DEFS {
            assert!(
                render.contains(&format!("# TYPE {} ", def.name)),
                "render must expose {} from the first scrape",
                def.name
            );
        }
    }

    #[test]
    fn a_counter_renders_its_value() {
        let m = PrometheusMetrics::new();
        m.incr(Metric::RunsClaimed, 3);
        m.incr(Metric::RunsClaimed, 2);
        let render = m.render();
        assert!(render.contains("# TYPE runs_claimed counter"));
        assert!(
            render.contains("\nruns_claimed 5\n"),
            "counter must accumulate, got:\n{render}"
        );
    }

    #[test]
    fn a_histogram_renders_cumulative_buckets_sum_and_count() {
        let m = PrometheusMetrics::new();
        m.observe(Metric::DueLagSeconds, 30.0);
        m.observe(Metric::DueLagSeconds, 90.0);
        let render = m.render();

        assert!(render.contains("# TYPE due_lag_seconds histogram"));
        // 30s and 90s: neither is <= 10, one (30) is <= 30, both are <= 120.
        assert!(
            render.contains("due_lag_seconds_bucket{le=\"10\"} 0"),
            "no observation is <= 10s, got:\n{render}"
        );
        assert!(
            render.contains("due_lag_seconds_bucket{le=\"30\"} 1"),
            "exactly the 30s observation is <= 30s, got:\n{render}"
        );
        assert!(
            render.contains("due_lag_seconds_bucket{le=\"120\"} 2"),
            "both observations are <= 120s, got:\n{render}"
        );
        assert!(render.contains("due_lag_seconds_bucket{le=\"+Inf\"} 2"));
        assert!(render.contains("due_lag_seconds_sum 120"));
        assert!(render.contains("due_lag_seconds_count 2"));
    }

    #[test]
    fn arc_forwards_to_the_inner_registry() {
        // Exercises the domain's blanket `impl Metrics for Arc<M>`: recording
        // through the Arc must land in the same registry the reader renders.
        let m = PrometheusMetrics::shared();
        Metrics::incr(&m, Metric::RunsReclaimed, 4);
        assert!(m.render().contains("\nruns_reclaimed 4\n"));
    }
}
