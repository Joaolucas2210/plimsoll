//! Prometheus text exposition (format 0.0.4), hand-rolled.
//!
//! ponytail: no metrics crate. Every value is read at scrape time from its
//! owner (admission, registry, pool), so there is no duplicated state to keep
//! in sync. Upgrade path: `prometheus-client` once histograms (query latency)
//! are needed, or OTLP export (see ROADMAP).

use std::fmt::Write;
use std::sync::Arc;

use plimsoll_control::governance::QueryTotals;

/// Memory pool figures from the compute plane's CeilingPool.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    pub used_bytes: u64,
    pub limit_bytes: u64,
    pub spill_bytes_total: u64,
}

/// INTEGRATION POINT: a closure reading the live CeilingPool counters.
pub type PoolStatsProvider = Arc<dyn Fn() -> PoolStats + Send + Sync>;

pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

pub fn render(pool: PoolStats, running: usize, queued: usize, totals: QueryTotals) -> String {
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
            spill_bytes_total: 3,
        };
        let totals = QueryTotals {
            finished: 4,
            failed: 1,
            cancelled: 0,
        };
        let text = render(pool, 2, 1, totals);
        for line in [
            "plimsoll_pool_used_bytes 10",
            "plimsoll_pool_limit_bytes 100",
            "plimsoll_spill_bytes_total 3",
            "plimsoll_queries_running 2",
            "plimsoll_queries_queued 1",
            "plimsoll_queries_total{state=\"finished\"} 4",
            "# TYPE plimsoll_queries_total counter",
        ] {
            assert!(text.lines().any(|l| l == line), "missing {line}\n{text}");
        }
    }
}
