//! Prometheus text exposition (format 0.0.4), hand-rolled.
//!
//! ponytail: no metrics crate. Every value is read at scrape time from its
//! owner (admission, registry, pool), so there is no duplicated state to keep
//! in sync. Latency histograms are fixed-bucket atomics (ADR 0019). Upgrade
//! path: `prometheus-client` or OTLP export (see ROADMAP).

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use plimsoll_control::governance::QueryTotals;

/// Memory pool figures from the compute plane's CeilingPool.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PoolStats {
    pub used_bytes: u64,
    pub limit_bytes: u64,
    /// Part of the limit set aside for DataFusion's caches (ADR 0018).
    pub cache_reserve_bytes: u64,
    pub spill_bytes_total: u64,
    /// `(group, share, used)` for resource groups with a share (ADR 0036).
    pub groups: Vec<(String, u64, u64)>,
}

/// INTEGRATION POINT: a closure reading the live CeilingPool counters.
pub type PoolStatsProvider = Arc<dyn Fn() -> PoolStats + Send + Sync>;

pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Upper bounds (seconds) of the latency buckets: 1 ms (cached point
/// lookups) to 5 min (the default query timeout's order of magnitude).
const BUCKETS: [f64; 14] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 60.0, 300.0,
];

/// Cumulative-on-render Prometheus histogram: per-bucket counts (plus `+Inf`)
/// and the sum in microseconds. Lock-free; one `fetch_add` per bucket hit.
#[derive(Debug, Default)]
pub struct Histogram {
    counts: [AtomicU64; BUCKETS.len() + 1],
    sum_us: AtomicU64,
}

impl Histogram {
    pub fn observe(&self, d: Duration) {
        let secs = d.as_secs_f64();
        let i = BUCKETS
            .iter()
            .position(|b| secs <= *b)
            .unwrap_or(BUCKETS.len());
        self.counts[i].fetch_add(1, Ordering::Relaxed);
        let us = u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, name: &str, labels: &str) {
        let mut cumulative = 0;
        for (i, count) in self.counts.iter().enumerate() {
            cumulative += count.load(Ordering::Relaxed);
            let le = BUCKETS
                .get(i)
                .map_or("+Inf".to_owned(), ToString::to_string);
            let _ = writeln!(out, "{name}_bucket{{{labels},le=\"{le}\"}} {cumulative}");
        }
        let sum = self.sum_us.load(Ordering::Relaxed) as f64 / 1e6;
        let _ = writeln!(out, "{name}_sum{{{labels}}} {sum}");
        let _ = writeln!(out, "{name}_count{{{labels}}} {cumulative}");
    }
}

/// Query latency by phase (ADR 0019). `queue` and `total` cover every query
/// admitted to the engine; the others only successful ones (a failed query
/// has no complete plan/execute/serialize split).
#[derive(Debug, Default)]
pub struct Latency {
    pub queue: Histogram,
    pub planning: Histogram,
    pub execution: Histogram,
    pub serialization: Histogram,
    pub total: Histogram,
}

pub fn render(
    pool: PoolStats,
    running: usize,
    queued: usize,
    totals: QueryTotals,
    errors: &BTreeMap<&'static str, u64>,
    latency: &Latency,
) -> String {
    let mut out = String::new();
    let mut metric = |name: &str, kind: &str, help: &str, samples: &[(&str, u64)]| {
        // Writing to a String cannot fail.
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
        for (labels, value) in samples {
            let _ = writeln!(out, "{name}{labels} {value}");
        }
    };
    metric(
        "plimsoll_pool_used_bytes",
        "gauge",
        "Bytes reserved in the memory pool.",
        &[("", pool.used_bytes)],
    );
    metric(
        "plimsoll_pool_limit_bytes",
        "gauge",
        "Memory pool ceiling (the load line).",
        &[("", pool.limit_bytes)],
    );
    metric(
        "plimsoll_pool_cache_reserve_bytes",
        "gauge",
        "Part of the ceiling reserved for file metadata/statistics/delete caches.",
        &[("", pool.cache_reserve_bytes)],
    );
    let share: Vec<(String, u64)> = pool
        .groups
        .iter()
        .map(|(g, share, _)| (format!("{{group=\"{g}\"}}"), *share))
        .collect();
    let used: Vec<(String, u64)> = pool
        .groups
        .iter()
        .map(|(g, _, used)| (format!("{{group=\"{g}\"}}"), *used))
        .collect();
    fn refs(v: &[(String, u64)]) -> Vec<(&str, u64)> {
        v.iter().map(|(l, n)| (l.as_str(), *n)).collect()
    }
    metric(
        "plimsoll_pool_group_share_bytes",
        "gauge",
        "Memory a resource group's queries may hold together (ADR 0036).",
        &refs(&share),
    );
    metric(
        "plimsoll_pool_group_used_bytes",
        "gauge",
        "Bytes held by a resource group's queries.",
        &refs(&used),
    );
    metric(
        "plimsoll_spill_bytes_total",
        "counter",
        "Bytes spilled to disk.",
        &[("", pool.spill_bytes_total)],
    );
    metric(
        "plimsoll_queries_running",
        "gauge",
        "Queries holding an admission slot.",
        &[("", running as u64)],
    );
    metric(
        "plimsoll_queries_queued",
        "gauge",
        "Queries waiting for admission.",
        &[("", queued as u64)],
    );
    metric(
        "plimsoll_queries_total",
        "counter",
        "Queries by terminal state.",
        &[
            ("{state=\"finished\"}", totals.finished),
            ("{state=\"failed\"}", totals.failed),
            ("{state=\"cancelled\"}", totals.cancelled),
        ],
    );
    let by_code: Vec<(String, u64)> = errors
        .iter()
        .map(|(code, n)| (format!("{{code=\"{code}\"}}"), *n))
        .collect();
    metric(
        "plimsoll_query_errors_total",
        "counter",
        "SQL endpoint errors by code (ADR 0013).",
        &by_code
            .iter()
            .map(|(l, n)| (l.as_str(), *n))
            .collect::<Vec<_>>(),
    );
    let name = "plimsoll_query_duration_seconds";
    let _ = writeln!(
        out,
        "# HELP {name} Query latency by phase (ADR 0019).\n# TYPE {name} histogram"
    );
    for (phase, h) in [
        ("queue", &latency.queue),
        ("planning", &latency.planning),
        ("execution", &latency.execution),
        ("serialization", &latency.serialization),
        ("total", &latency.total),
    ] {
        h.render(&mut out, name, &format!("phase=\"{phase}\""));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_exposition_lines() {
        let pool = PoolStats {
            used_bytes: 10,
            limit_bytes: 100,
            cache_reserve_bytes: 6,
            spill_bytes_total: 3,
            groups: vec![("batch".into(), 40, 7)],
        };
        let totals = QueryTotals {
            finished: 4,
            failed: 1,
            cancelled: 0,
        };
        let errors = BTreeMap::from([("dependency_failed", 2), ("unsupported_feature", 1)]);
        let latency = Latency::default();
        latency.planning.observe(Duration::from_millis(3));
        latency.planning.observe(Duration::from_millis(30));
        latency.planning.observe(Duration::from_secs(900));
        let text = render(pool, 2, 1, totals, &errors, &latency);
        for line in [
            "plimsoll_pool_used_bytes 10",
            "plimsoll_pool_limit_bytes 100",
            "plimsoll_pool_cache_reserve_bytes 6",
            "plimsoll_spill_bytes_total 3",
            "plimsoll_pool_group_share_bytes{group=\"batch\"} 40",
            "plimsoll_pool_group_used_bytes{group=\"batch\"} 7",
            "plimsoll_queries_running 2",
            "plimsoll_queries_queued 1",
            "plimsoll_queries_total{state=\"finished\"} 4",
            "# TYPE plimsoll_queries_total counter",
            "plimsoll_query_errors_total{code=\"dependency_failed\"} 2",
            "# TYPE plimsoll_query_duration_seconds histogram",
            // Cumulative: 3 ms is in le=0.005, 30 ms joins at le=0.05, 15 min only in +Inf.
            "plimsoll_query_duration_seconds_bucket{phase=\"planning\",le=\"0.001\"} 0",
            "plimsoll_query_duration_seconds_bucket{phase=\"planning\",le=\"0.005\"} 1",
            "plimsoll_query_duration_seconds_bucket{phase=\"planning\",le=\"0.05\"} 2",
            "plimsoll_query_duration_seconds_bucket{phase=\"planning\",le=\"300\"} 2",
            "plimsoll_query_duration_seconds_bucket{phase=\"planning\",le=\"+Inf\"} 3",
            "plimsoll_query_duration_seconds_sum{phase=\"planning\"} 900.033",
            "plimsoll_query_duration_seconds_count{phase=\"planning\"} 3",
            "plimsoll_query_duration_seconds_count{phase=\"queue\"} 0",
            "plimsoll_query_errors_total{code=\"unsupported_feature\"} 1",
        ] {
            assert!(text.lines().any(|l| l == line), "missing {line}\n{text}");
        }
    }
}
