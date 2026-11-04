//! Prometheus text exposition (format 0.0.4), hand-rolled.
//!
//! ponytail: no metrics crate. Every value is read at scrape time from its
//! owner (admission, registry, pool), so there is no duplicated state to keep
//! in sync. Latency histograms are fixed-bucket atomics (ADR 0019). Upgrade
//! path: `prometheus-client`.
//!
//! The same exposition is pushed over OTLP/HTTP JSON (ADR 0100): parsed back
//! into gauges, cumulative sums and histograms, so both outputs always carry
//! the same series.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use plimsoll_control::governance::QueryTotals;
use plimsoll_core::result_cache::CacheStats;
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::log::{self, Level};
use crate::trace::{self, OtlpConfig};

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
    /// When the result cache is on (ADR 0038).
    pub result_cache: Option<CacheStats>,
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

/// One kind of fragment a worker served (ADR 0119): requests that ended
/// well or not, and the rows and Arrow bytes they sent back.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FragmentStats {
    pub ok: u64,
    pub failed: u64,
    pub rows: u64,
    pub bytes: u64,
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
    fragments: &BTreeMap<&'static str, FragmentStats>,
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
    if let Some(c) = pool.result_cache {
        metric(
            "plimsoll_result_cache_requests_total",
            "counter",
            "Cacheable queries by outcome (ADR 0038).",
            &[
                ("{outcome=\"hit\"}", c.hits),
                ("{outcome=\"miss\"}", c.misses),
            ],
        );
        metric(
            "plimsoll_result_cache_bytes",
            "gauge",
            "Bytes held by cached results and results being collected.",
            &[("", c.bytes as u64)],
        );
        metric(
            "plimsoll_result_cache_entries",
            "gauge",
            "Cached results.",
            &[("", c.entries as u64)],
        );
    }
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
    // What this node served as a worker, by kind (ADR 0119).
    let served: Vec<(String, u64)> = fragments
        .iter()
        .flat_map(|(kind, s)| {
            [
                (format!("{{kind=\"{kind}\",outcome=\"ok\"}}"), s.ok),
                (format!("{{kind=\"{kind}\",outcome=\"error\"}}"), s.failed),
            ]
        })
        .collect();
    metric(
        "plimsoll_fragments_total",
        "counter",
        "Fragments served as a worker, by kind and outcome (ADR 0119).",
        &refs(&served),
    );
    let by_kind = |f: fn(&FragmentStats) -> u64| -> Vec<(String, u64)> {
        fragments
            .iter()
            .map(|(kind, s)| (format!("{{kind=\"{kind}\"}}"), f(s)))
            .collect()
    };
    metric(
        "plimsoll_fragment_rows_total",
        "counter",
        "Rows fragments sent back, by kind.",
        &refs(&by_kind(|s| s.rows)),
    );
    metric(
        "plimsoll_fragment_bytes_total",
        "counter",
        "Arrow IPC bytes fragments sent back, by kind.",
        &refs(&by_kind(|s| s.bytes)),
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

/// Pushes `scrape()` (the `/metrics` text) to `<endpoint>/v1/metrics` every
/// `every`, and once more when `stop` is notified, then returns.
pub fn spawn_export(
    cfg: OtlpConfig,
    every: Duration,
    scrape: impl Fn() -> String + Send + 'static,
    stop: Arc<Notify>,
) -> Result<JoinHandle<()>, reqwest::Error> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let url = format!("{}/v1/metrics", cfg.endpoint.trim_end_matches('/'));
    // Counters and histograms count from process start.
    let start = trace::nanos(SystemTime::now());
    Ok(tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The first tick is immediate: nothing has happened yet.
        tick.tick().await;
        loop {
            let last = tokio::select! {
                _ = tick.tick() => false,
                () = stop.notified() => true,
            };
            let metrics = to_otlp(&scrape(), &start, &trace::nanos(SystemTime::now()));
            let body = json!({"resourceMetrics": [{
                "resource": {"attributes": [trace::attr("service.name", json!(cfg.service_name))]},
                "scopeMetrics": [{"scope": {"name": "plimsoll-server"}, "metrics": metrics}],
            }]});
            if let Some(error) = trace::post(&client, &cfg, &url, &body).await {
                log::emit(
                    Level::Warn,
                    "metrics_export_failed",
                    json!({ "error": error }),
                );
            }
            if last {
                break;
            }
        }
    }))
}

/// A sample's or series' labels.
type Labels = Vec<(String, String)>;

/// A histogram's label set: `(le, cumulative count)`, sum and count.
#[derive(Default)]
struct Series {
    labels: Labels,
    buckets: Vec<(String, u64)>,
    sum: f64,
    count: u64,
}

/// One metric of the exposition text, as it is parsed.
struct Parsed<'a> {
    name: &'a str,
    kind: &'a str,
    help: &'a str,
    points: Vec<Value>,
    series: Vec<Series>,
}

/// The exposition text written by [`render`] as OTLP JSON metrics: gauges,
/// monotonic cumulative sums for counters, and explicit-bucket histograms.
fn to_otlp(text: &str, start: &str, now: &str) -> Vec<Value> {
    let mut help = "";
    let mut out: Vec<Parsed> = Vec::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# HELP ") {
            help = rest.split_once(' ').map_or("", |(_, h)| h);
        } else if let Some(rest) = line.strip_prefix("# TYPE ") {
            if let Some((name, kind)) = rest.split_once(' ') {
                out.push(Parsed {
                    name,
                    kind,
                    help,
                    points: Vec::new(),
                    series: Vec::new(),
                });
            }
        } else if let (Some(m), Some((name, labels, value))) = (out.last_mut(), sample(line)) {
            if m.kind != "histogram" {
                let mut p = json!({ "attributes": attributes(&labels), "timeUnixNano": now });
                if m.kind == "counter" {
                    p["startTimeUnixNano"] = json!(start);
                }
                match value.parse::<u64>() {
                    // OTLP JSON encodes 64-bit ints as strings.
                    Ok(n) => p["asInt"] = json!(n.to_string()),
                    Err(_) => p["asDouble"] = json!(value.parse::<f64>().unwrap_or(f64::NAN)),
                }
                m.points.push(p);
                continue;
            }
            let (mut labels, le): (Vec<_>, Vec<_>) =
                labels.into_iter().partition(|(k, _)| k != "le");
            labels.sort();
            let i = match m.series.iter().position(|s| s.labels == labels) {
                Some(i) => i,
                None => {
                    m.series.push(Series {
                        labels,
                        ..Default::default()
                    });
                    m.series.len() - 1
                }
            };
            let s = &mut m.series[i];
            match name.strip_prefix(m.name) {
                Some("_bucket") => {
                    if let Some((_, le)) = le.into_iter().next() {
                        s.buckets.push((le, value.parse().unwrap_or(0)));
                    }
                }
                Some("_sum") => s.sum = value.parse().unwrap_or(0.0),
                Some("_count") => s.count = value.parse().unwrap_or(0),
                _ => {}
            }
        }
    }
    out.into_iter()
        // No groups or no errors yet: nothing to send.
        .filter(|m| !(m.points.is_empty() && m.series.is_empty()))
        .map(|m| {
            let mut metric = json!({ "name": m.name, "description": m.help });
            match m.kind {
                "counter" => {
                    // 2: AGGREGATION_TEMPORALITY_CUMULATIVE.
                    metric["sum"] = json!({
                        "aggregationTemporality": 2,
                        "isMonotonic": true,
                        "dataPoints": m.points,
                    });
                }
                "histogram" => {
                    let points: Vec<Value> = m
                        .series
                        .iter()
                        .map(|s| {
                            let bounds: Vec<f64> = s
                                .buckets
                                .iter()
                                .filter_map(|(le, _)| le.parse().ok())
                                .filter(|b: &f64| b.is_finite())
                                .collect();
                            // Prometheus buckets are cumulative, OTLP's are not.
                            let mut previous = 0;
                            let counts: Vec<String> = s
                                .buckets
                                .iter()
                                .map(|(_, c)| {
                                    let n = c.saturating_sub(previous);
                                    previous = *c;
                                    n.to_string()
                                })
                                .collect();
                            json!({
                                "attributes": attributes(&s.labels),
                                "startTimeUnixNano": start,
                                "timeUnixNano": now,
                                "count": s.count.to_string(),
                                "sum": s.sum,
                                "bucketCounts": counts,
                                "explicitBounds": bounds,
                            })
                        })
                        .collect();
                    metric["histogram"] =
                        json!({ "aggregationTemporality": 2, "dataPoints": points });
                }
                _ => metric["gauge"] = json!({ "dataPoints": m.points }),
            }
            metric
        })
        .collect()
}

fn attributes(labels: &[(String, String)]) -> Vec<Value> {
    labels
        .iter()
        .map(|(k, v)| trace::attr(k, json!(v)))
        .collect()
}

/// `name{k="v",…} value` as its name, labels and value.
fn sample(line: &str) -> Option<(&str, Labels, &str)> {
    if line.starts_with('#') || line.trim().is_empty() {
        return None;
    }
    let (head, value) = line.rsplit_once(' ')?;
    let Some((name, rest)) = head.split_once('{') else {
        return Some((head, Vec::new(), value));
    };
    let mut labels = Vec::new();
    let mut chars = rest.strip_suffix('}')?.chars();
    loop {
        let key: String = chars.by_ref().take_while(|c| *c != '=').collect();
        if key.is_empty() {
            break;
        }
        if chars.next() != Some('"') {
            return None;
        }
        let mut value = String::new();
        while let Some(c) = chars.next() {
            match c {
                '\\' => value.extend(chars.next()),
                '"' => break,
                c => value.push(c),
            }
        }
        labels.push((key, value));
        // The `,` between labels.
        chars.next();
    }
    Some((name, labels, value))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
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
            result_cache: Some(CacheStats {
                hits: 3,
                misses: 2,
                entries: 1,
                bytes: 64,
                max_bytes: 128,
            }),
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
        let fragments = BTreeMap::from([(
            "scan",
            FragmentStats {
                ok: 5,
                failed: 1,
                rows: 40,
                bytes: 2048,
            },
        )]);
        let text = render(pool, 2, 1, totals, &errors, &latency, &fragments);
        for line in [
            "plimsoll_pool_used_bytes 10",
            "plimsoll_pool_limit_bytes 100",
            "plimsoll_pool_cache_reserve_bytes 6",
            "plimsoll_spill_bytes_total 3",
            "plimsoll_pool_group_share_bytes{group=\"batch\"} 40",
            "plimsoll_pool_group_used_bytes{group=\"batch\"} 7",
            "plimsoll_result_cache_requests_total{outcome=\"hit\"} 3",
            "plimsoll_result_cache_bytes 64",
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
            "plimsoll_fragments_total{kind=\"scan\",outcome=\"ok\"} 5",
            "plimsoll_fragments_total{kind=\"scan\",outcome=\"error\"} 1",
            "plimsoll_fragment_rows_total{kind=\"scan\"} 40",
            "plimsoll_fragment_bytes_total{kind=\"scan\"} 2048",
            "# TYPE plimsoll_fragments_total counter",
        ] {
            assert!(text.lines().any(|l| l == line), "missing {line}\n{text}");
        }
    }

    #[test]
    fn exposition_becomes_otlp_metrics() {
        let latency = Latency::default();
        latency.total.observe(Duration::from_millis(3));
        latency.total.observe(Duration::from_millis(30));
        let pool = PoolStats {
            used_bytes: 10,
            groups: vec![("batch".into(), 40, 7)],
            ..Default::default()
        };
        let totals = QueryTotals {
            finished: 4,
            failed: 1,
            cancelled: 0,
        };
        let text = render(
            pool,
            2,
            1,
            totals,
            &BTreeMap::new(),
            &latency,
            &BTreeMap::new(),
        );
        let metrics = to_otlp(&text, "1", "2");
        let get = |name: &str| {
            metrics
                .iter()
                .find(|m| m["name"] == name)
                .unwrap_or_else(|| panic!("no {name}"))
                .clone()
        };
        assert!(
            metrics
                .iter()
                .all(|m| m["name"] != "plimsoll_query_errors_total")
        );
        let used = get("plimsoll_pool_used_bytes");
        assert_eq!(used["gauge"]["dataPoints"][0]["asInt"], "10");
        assert_eq!(used["description"], "Bytes reserved in the memory pool.");
        let group = get("plimsoll_pool_group_used_bytes");
        let point = &group["gauge"]["dataPoints"][0];
        assert_eq!(point["attributes"][0]["key"], "group");
        assert_eq!(point["attributes"][0]["value"]["stringValue"], "batch");
        assert_eq!(point["asInt"], "7");
        let total = get("plimsoll_queries_total");
        assert_eq!(total["sum"]["isMonotonic"], true);
        assert_eq!(total["sum"]["dataPoints"][0]["startTimeUnixNano"], "1");
        assert_eq!(total["sum"]["dataPoints"][1]["asInt"], "1");
        let h = get("plimsoll_query_duration_seconds");
        let points = h["histogram"]["dataPoints"].as_array().unwrap();
        assert_eq!(points.len(), 5);
        let p = points
            .iter()
            .find(|p| p["attributes"][0]["value"]["stringValue"] == "total")
            .unwrap();
        assert_eq!(p["count"], "2");
        assert_eq!(p["explicitBounds"].as_array().unwrap().len(), BUCKETS.len());
        // 3 ms in le=0.005, 30 ms in le=0.05: per bucket, not cumulative.
        let counts: Vec<&str> = p["bucketCounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c.as_str().unwrap())
            .collect();
        assert_eq!(
            counts,
            [
                "0", "1", "0", "0", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"
            ]
        );
        assert!((p["sum"].as_f64().unwrap() - 0.033).abs() < 1e-9);
    }

    #[test]
    fn labels_with_escapes_are_parsed() {
        let (name, labels, value) = sample(r#"m{a="x\"y",b="z"} 3"#).unwrap();
        assert_eq!(name, "m");
        assert_eq!(
            labels,
            vec![
                ("a".to_owned(), "x\"y".to_owned()),
                ("b".to_owned(), "z".to_owned())
            ]
        );
        assert_eq!(value, "3");
        assert!(sample("# HELP m x").is_none());
    }
}
