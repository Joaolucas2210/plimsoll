//! Query traces over OTLP/HTTP JSON (ADR 0031).
//!
//! Every `/v1/sql` request gets a W3C trace context: the caller's
//! `traceparent` when it sent one, else a fresh trace. It is echoed in the
//! response and written in the `query` log line. When
//! `OTEL_EXPORTER_OTLP_ENDPOINT` is set, a `query` span (SERVER) with one child
//! per phase is exported to `<endpoint>/v1/traces`.
//!
//! ponytail: no OpenTelemetry SDK. The phases are already timed (ADR 0019),
//! so spans are built after the request from those durations and posted with
//! the reqwest client the catalog already uses. Upgrade path: `tracing` +
//! `opentelemetry-otlp` when spans are needed below the request level.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ring::rand::{SecureRandom, SystemRandom};
use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

use crate::log::{self, Level, QueryLog};

/// Spans buffered for export; beyond it spans are dropped, never awaited.
const QUEUE: usize = 4096;
/// Spans per OTLP request.
const BATCH: usize = 512;

/// A request's place in a distributed trace (W3C Trace Context).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
    /// The caller's span, when it sent a `traceparent`.
    pub parent_id: Option<String>,
    pub sampled: bool,
}

impl TraceContext {
    /// Continues the caller's trace from a valid `traceparent`, else starts
    /// a new sampled one. An invalid header is ignored, per the W3C spec.
    pub fn from_traceparent(header: Option<&str>) -> Self {
        let span_id = random_hex::<8>();
        match header.and_then(parse_traceparent) {
            Some((trace_id, parent, flags)) => Self {
                trace_id,
                span_id,
                parent_id: Some(parent),
                sampled: flags & 1 == 1,
            },
            None => Self {
                trace_id: random_hex::<16>(),
                span_id,
                parent_id: None,
                sampled: true,
            },
        }
    }

    /// `traceparent` naming this request's span, for the response.
    pub fn traceparent(&self) -> String {
        let flags = u8::from(self.sampled);
        format!("00-{}-{}-{flags:02x}", self.trace_id, self.span_id)
    }
}

/// `00-<32 hex>-<16 hex>-<2 hex>` → (trace id, parent id, flags). All-zero
/// ids and version `ff` are invalid. Later versions may append fields.
fn parse_traceparent(h: &str) -> Option<(String, String, u8)> {
    let mut parts = h.trim().split('-');
    let (version, trace, parent, flags) =
        (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
    let hex =
        |s: &str, n| s.len() == n && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
    let valid = hex(version, 2)
        && version != "ff"
        && (version != "00" || parts.next().is_none())
        && hex(trace, 32)
        && hex(parent, 16)
        && hex(flags, 2)
        && trace.bytes().any(|b| b != b'0')
        && parent.bytes().any(|b| b != b'0');
    if !valid {
        return None;
    }
    let flags = u8::from_str_radix(flags, 16).ok()?;
    Some((trace.to_owned(), parent.to_owned(), flags))
}

fn random_hex<const N: usize>() -> String {
    let mut bytes = [0u8; N];
    // ponytail: the OS RNG does not fail on supported platforms; if it ever
    // did, an id from the clock still correlates within the process.
    if SystemRandom::new().fill(&mut bytes).is_err() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = (nanos >> (8 * (i % 16))) as u8;
        }
    }
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// OTLP/HTTP exporter settings (`OTEL_EXPORTER_OTLP_*`).
#[derive(Clone, PartialEq, Eq)]
pub struct OtlpConfig {
    /// Base URL; spans go to `<endpoint>/v1/traces`.
    pub endpoint: String,
    /// `OTEL_EXPORTER_OTLP_HEADERS` (often an API key): never logged.
    pub headers: Vec<(String, String)>,
    pub service_name: String,
}

impl std::fmt::Debug for OtlpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<_> = self.headers.iter().map(|(k, _)| k.as_str()).collect();
        f.debug_struct("OtlpConfig")
            .field("endpoint", &self.endpoint)
            .field("headers", &names)
            .field("service_name", &self.service_name)
            .finish()
    }
}

/// Hands finished spans to the background exporter.
pub struct Tracer {
    tx: mpsc::Sender<Value>,
    dropped: Arc<AtomicU64>,
    stop: Arc<Notify>,
}

impl Tracer {
    /// Starts the exporter task. `shutdown` flushes what is queued; await the
    /// handle (with a deadline) before exiting.
    pub fn spawn(cfg: OtlpConfig) -> Result<(Self, JoinHandle<()>), reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        let (tx, rx) = mpsc::channel(QUEUE);
        let dropped = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(Notify::new());
        let handle = tokio::spawn(export(client, cfg, rx, dropped.clone(), stop.clone()));
        Ok((Self { tx, dropped, stop }, handle))
    }

    /// Queues the spans of one request. Never blocks: when the exporter is
    /// behind, the spans are dropped and counted.
    pub fn record(&self, spans: Vec<Value>) {
        for span in spans {
            if self.tx.try_send(span).is_err() {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn shutdown(&self) {
        self.stop.notify_one();
    }
}

async fn export(
    client: reqwest::Client,
    cfg: OtlpConfig,
    mut rx: mpsc::Receiver<Value>,
    dropped: Arc<AtomicU64>,
    stop: Arc<Notify>,
) {
    let url = format!("{}/v1/traces", cfg.endpoint.trim_end_matches('/'));
    loop {
        let first = tokio::select! {
            span = rx.recv() => span,
            () = stop.notified() => {
                rx.close();
                rx.recv().await
            }
        };
        let Some(first) = first else { break };
        // Whatever queued while the last POST ran goes in this one, so
        // batches grow with load without a timer.
        let mut spans = vec![first];
        while spans.len() < BATCH {
            match rx.try_recv() {
                Ok(s) => spans.push(s),
                Err(_) => break,
            }
        }
        let body = json!({"resourceSpans": [{
            "resource": {"attributes": [attr("service.name", json!(cfg.service_name))]},
            "scopeSpans": [{"scope": {"name": "plimsoll-server"}, "spans": spans}],
        }]});
        let mut req = client
            .post(&url)
            .header("content-type", "application/json")
            .body(body.to_string());
        for (k, v) in &cfg.headers {
            req = req.header(k, v);
        }
        let failure = match req.send().await {
            Ok(r) if r.status().is_success() => None,
            Ok(r) => Some(format!("HTTP {}", r.status())),
            Err(e) => Some(e.to_string()),
        };
        let lost = dropped.swap(0, Ordering::Relaxed);
        if failure.is_some() || lost > 0 {
            log::emit(
                Level::Warn,
                "trace_export_failed",
                json!({ "error": failure, "spans": body_len(&body), "dropped_queue_full": lost }),
            );
        }
    }
}

fn body_len(body: &Value) -> usize {
    body["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array()
        .map_or(0, Vec::len)
}

fn attr(key: &str, value: Value) -> Value {
    let value = match value {
        Value::String(s) => json!({ "stringValue": s }),
        // OTLP JSON encodes 64-bit ints as strings.
        Value::Number(n) => json!({ "intValue": n.to_string() }),
        Value::Bool(b) => json!({ "boolValue": b }),
        other => json!({ "stringValue": other.to_string() }),
    };
    json!({ "key": key, "value": value })
}

fn nanos(t: SystemTime) -> String {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos())
        .to_string()
}

/// One SERVER span for work done on behalf of another node (a worker's
/// fragment, ADR 0059): the caller's trace continues here.
pub fn server_span(
    ctx: &TraceContext,
    name: &str,
    started: SystemTime,
    ended: SystemTime,
    attributes: Vec<(&str, Value)>,
    error: Option<&str>,
) -> Value {
    let mut span = json!({
        "traceId": ctx.trace_id,
        "spanId": ctx.span_id,
        "name": name,
        "kind": 2,
        "startTimeUnixNano": nanos(started),
        "endTimeUnixNano": nanos(ended),
        "attributes": attributes.into_iter().map(|(k, v)| attr(k, v)).collect::<Vec<_>>(),
        "status": match error {
            Some(m) => json!({"code": 2, "message": m}),
            None => json!({"code": 0}),
        },
    });
    if let Some(parent) = &ctx.parent_id {
        span["parentSpanId"] = json!(parent);
    }
    span
}

/// The spans of one `/v1/sql` request: `query` (SERVER) and a child per
/// phase that ran. The phases are laid back to back, ending when the request
/// ended. ponytail: the only gap is auth and body parsing before the queue,
/// sub-millisecond; record real instants if that ever matters.
pub fn spans(
    ctx: &TraceContext,
    started: SystemTime,
    q: &QueryLog,
    status: u16,
    error: Option<(&str, &str)>,
) -> Vec<Value> {
    let end = started + q.total;
    let mut attributes = vec![
        attr("http.request.method", json!("POST")),
        attr("http.route", json!("/v1/sql")),
        attr("http.response.status_code", json!(status)),
        attr("plimsoll.code", json!(error.map_or("ok", |(c, _)| c))),
    ];
    for (key, v) in [
        ("plimsoll.query_id", q.query_id.map(|v| json!(v))),
        ("enduser.id", q.user.as_ref().map(|v| json!(v))),
        (
            "plimsoll.resource_group",
            q.resource_group.as_ref().map(|v| json!(v)),
        ),
        (
            "plimsoll.sql_sha256",
            q.sql_sha256.as_ref().map(|v| json!(v)),
        ),
    ] {
        if let Some(v) = v {
            attributes.push(attr(key, v));
        }
    }
    // Server spans: only 5xx is an error; a 4xx is the caller's (OTel HTTP
    // semantic conventions).
    let status_obj = match error {
        Some((_, message)) if status >= 500 => json!({"code": 2, "message": message}),
        _ => json!({"code": 0}),
    };
    let mut root = json!({
        "traceId": ctx.trace_id,
        "spanId": ctx.span_id,
        "name": "query",
        "kind": 2,
        "startTimeUnixNano": nanos(started),
        "endTimeUnixNano": nanos(end),
        "attributes": attributes,
        "status": status_obj,
    });
    if let Some(parent) = &ctx.parent_id {
        root["parentSpanId"] = json!(parent);
    }
    let mut out = vec![root];
    let phases = [
        ("queue", q.queue),
        ("planning", q.planning),
        ("execution", q.execution),
        ("serialization", q.serialization),
    ];
    let ran: Duration = phases.iter().filter_map(|(_, d)| *d).sum();
    let mut at = end.checked_sub(ran).unwrap_or(started).max(started);
    let mut planning = None;
    for (name, d) in phases {
        let Some(d) = d else { continue };
        let id = random_hex::<8>();
        if name == "planning" {
            planning = Some(id.clone());
        }
        out.push(json!({
            "traceId": ctx.trace_id,
            "spanId": id,
            "parentSpanId": ctx.span_id,
            "name": name,
            "kind": 1,
            "startTimeUnixNano": nanos(at),
            "endTimeUnixNano": nanos(at + d),
        }));
        at += d;
    }
    // What planning did (ADR 0056): catalog loads, connector planning, at
    // their real times, under the planning span (or the query, when
    // planning failed and has no span).
    let parent = planning.unwrap_or_else(|| ctx.span_id.clone());
    for e in &q.plan_events {
        let attributes: Vec<Value> = e
            .attrs
            .iter()
            .map(|(k, v)| attr(&format!("plimsoll.{k}"), json!(v)))
            .collect();
        out.push(json!({
            "traceId": ctx.trace_id,
            "spanId": random_hex::<8>(),
            "parentSpanId": parent,
            "name": e.name,
            "kind": 1,
            "startTimeUnixNano": nanos(e.start),
            "endTimeUnixNano": nanos(e.start + e.duration),
            "attributes": attributes,
        }));
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn traceparent_is_continued_or_started() {
        let h = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let ctx = TraceContext::from_traceparent(Some(h));
        assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(ctx.parent_id.as_deref(), Some("00f067aa0ba902b7"));
        assert!(ctx.sampled);
        assert_ne!(ctx.span_id, "00f067aa0ba902b7");
        let out = ctx.traceparent();
        assert!(out.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"));
        assert!(out.ends_with("-01") && out.len() == h.len(), "{out}");

        let unsampled = h.replace("-01", "-00");
        assert!(!TraceContext::from_traceparent(Some(&unsampled)).sampled);
        // A future version may append fields.
        assert!(parse_traceparent(&format!("01{}-extra", &h[2..])).is_some());

        for bad in [
            "",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-x",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
        ] {
            assert_eq!(parse_traceparent(bad), None, "{bad}");
            let fresh = TraceContext::from_traceparent(Some(bad));
            assert!(fresh.parent_id.is_none() && fresh.sampled);
            assert_eq!(fresh.trace_id.len(), 32);
        }
    }

    #[test]
    fn query_spans_nest_phases_back_to_back_inside_the_request() {
        let ctx = TraceContext::from_traceparent(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ));
        let q = QueryLog {
            query_id: Some(7),
            user: Some("ana".into()),
            queue: Some(Duration::from_millis(1)),
            planning: Some(Duration::from_millis(2)),
            execution: Some(Duration::from_millis(3)),
            total: Duration::from_millis(10),
            ..Default::default()
        };
        let t0 = UNIX_EPOCH + Duration::from_secs(1);
        let s = spans(&ctx, t0, &q, 502, Some(("dependency_failed", "s3 down")));
        let names: Vec<_> = s.iter().map(|s| s["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["query", "queue", "planning", "execution"]);
        assert_eq!(s[0]["parentSpanId"], "00f067aa0ba902b7");
        assert_eq!(s[0]["status"]["code"], 2);
        assert_eq!(s[0]["endTimeUnixNano"], "1010000000");
        // Phases end with the request: 1+2+3 ms before t0+10 ms.
        assert_eq!(s[1]["startTimeUnixNano"], "1004000000");
        assert_eq!(s[3]["endTimeUnixNano"], "1010000000");
        for child in &s[1..] {
            assert_eq!(child["parentSpanId"], ctx.span_id);
            assert_eq!(child["traceId"], ctx.trace_id);
        }
        let attrs = s[0]["attributes"].to_string();
        assert!(attrs.contains(r#""intValue":"7""#), "{attrs}");
        assert!(attrs.contains("enduser.id"), "{attrs}");
        // Planning events become children of the planning span (ADR 0056).
        let q = QueryLog {
            plan_events: vec![plimsoll_core::plan_trace::PlanEvent {
                name: "iceberg.plan",
                start: t0 + Duration::from_millis(2),
                duration: Duration::from_millis(1),
                attrs: vec![("files", "1/3".into())],
            }],
            ..q
        };
        let s = spans(&ctx, t0, &q, 200, None);
        let planning = s.iter().find(|s| s["name"] == "planning").unwrap();
        let plan = s.iter().find(|s| s["name"] == "iceberg.plan").unwrap();
        assert_eq!(plan["parentSpanId"], planning["spanId"]);
        assert_eq!(plan["startTimeUnixNano"], "1002000000");
        assert!(plan["attributes"].to_string().contains("plimsoll.files"));
        // A caller error is not a server error.
        let s = spans(&ctx, t0, &q, 403, Some(("forbidden", "no")));
        assert_eq!(s[0]["status"]["code"], 0);
    }

    #[tokio::test]
    async fn exporter_posts_otlp_json_and_flushes_on_shutdown() {
        use axum::routing::post;
        let (got_tx, mut got_rx) = mpsc::unbounded_channel::<(Value, Option<String>)>();
        let app = axum::Router::new().route(
            "/v1/traces",
            post(
                move |headers: axum::http::HeaderMap, body: axum::Json<Value>| async move {
                    let key = headers
                        .get("x-api-key")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned);
                    got_tx.send((body.0, key)).unwrap();
                    "{}"
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let (tracer, handle) = Tracer::spawn(OtlpConfig {
            endpoint: format!("http://{addr}/"),
            headers: vec![("x-api-key".into(), "secret".into())],
            service_name: "plimsoll-test".into(),
        })
        .unwrap();
        let ctx = TraceContext::from_traceparent(None);
        let q = QueryLog {
            total: Duration::from_millis(1),
            ..Default::default()
        };
        tracer.record(spans(&ctx, SystemTime::now(), &q, 200, None));
        tracer.shutdown();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .unwrap()
            .unwrap();

        let (body, key) = got_rx.recv().await.unwrap();
        assert_eq!(key.as_deref(), Some("secret"));
        let rs = &body["resourceSpans"][0];
        assert_eq!(
            rs["resource"]["attributes"][0]["value"]["stringValue"],
            "plimsoll-test"
        );
        assert_eq!(rs["scopeSpans"][0]["spans"][0]["traceId"], ctx.trace_id);
        assert!(
            !format!(
                "{:?}",
                OtlpConfig {
                    endpoint: String::new(),
                    headers: vec![("x-api-key".into(), "secret".into())],
                    service_name: String::new(),
                }
            )
            .contains("secret")
        );
    }
}
