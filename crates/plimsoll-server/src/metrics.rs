//! Prometheus text exposition (format 0.0.4), hand-rolled.
//!
//! ponytail: no metrics crate. Every value is read at scrape time from its
//! owner (admission, registry, pool), so there is no duplicated state to keep
//! in sync. Upgrade path: `prometheus-client` once histograms (query latency)
//! are needed, or OTLP export (see ROADMAP).

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Arc;

use plimsoll_control::governance::QueryTotals;

/// Memory pool figures from the compute plane's CeilingPool.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    pub used_bytes: u64,
    pub limit_bytes: u64,
    /// Part of the limit set aside for DataFusion's caches (ADR 0018).
    pub cache_reserve_bytes: u64,
    pub spill_bytes_total: u64,
}

/// INTEGRATION POINT: a closure reading the live CeilingPool counters.
pub type PoolStatsProvider = Arc<dyn Fn() -> PoolStats + Send + Sync>;

pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

pub fn render(
    pool: PoolStats,
    running: usize,
    queued: usize,
    totals: QueryTotals,
    errors: &BTreeMap<&'static str, u64>,
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
        };
        let totals = QueryTotals {
            finished: 4,
            failed: 1,
            cancelled: 0,
        };
        let errors = BTreeMap::from([("dependency_failed", 2), ("unsupported_feature", 1)]);
        let text = render(pool, 2, 1, totals, &errors);
        for line in [
            "plimsoll_pool_used_bytes 10",
            "plimsoll_pool_limit_bytes 100",
            "plimsoll_pool_cache_reserve_bytes 6",
            "plimsoll_spill_bytes_total 3",
            "plimsoll_queries_running 2",
            "plimsoll_queries_queued 1",
            "plimsoll_queries_total{state=\"finished\"} 4",
            "# TYPE plimsoll_queries_total counter",
            "plimsoll_query_errors_total{code=\"dependency_failed\"} 2",
            "plimsoll_query_errors_total{code=\"unsupported_feature\"} 1",
        ] {
            assert!(text.lines().any(|l| l == line), "missing {line}\n{text}");
        }
    }
}
