//! Structured logs: one JSON object per line on stderr (ADR 0027).
//!
//! ponytail: no logging crate. A line is built with `serde_json` and written
//! with one `write_all` on the locked stderr, so lines from concurrent
//! requests never interleave. Upgrade path: `tracing` + OTLP export when
//! spans are needed (ROADMAP).

use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ring::digest::{SHA256, digest};
use serde_json::{Map, Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Info,
    Warn,
    Error,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// `{"ts_ms":..,"level":..,"event":..,<fields>}`; `fields` must be an object.
pub fn line(level: Level, event: &str, fields: Value) -> Value {
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let mut out = Map::new();
    out.insert("ts_ms".into(), json!(ts_ms));
    out.insert("level".into(), json!(level.as_str()));
    out.insert("event".into(), json!(event));
    if let Value::Object(fields) = fields {
        out.extend(fields);
    }
    Value::Object(out)
}

/// Writes one log line to stderr. Logging never fails a request.
pub fn emit(level: Level, event: &str, fields: Value) {
    let mut text = line(level, event, fields).to_string();
    text.push('\n');
    let _ = std::io::stderr().lock().write_all(text.as_bytes());
}

/// What one `/v1/sql` request did, filled in as the handler learns it.
#[derive(Debug, Default)]
pub struct QueryLog {
    pub query_id: Option<u64>,
    pub user: Option<String>,
    pub resource_group: Option<String>,
    /// Fingerprint, never the text: SQL carries literals (ids, emails) that
    /// do not belong in logs. Correlate with `system.runtime.queries`.
    pub sql_sha256: Option<String>,
    pub sql_bytes: usize,
    /// W3C trace id, to jump from a log line to its trace (ADR 0031).
    pub trace_id: Option<String>,
    /// Tables and columns the query read (ADR 0046); empty when it never
    /// planned (rejected, invalid, denied).
    pub reads: Vec<plimsoll_core::engine::TableRead>,
    /// This request's `traceparent`, passed to workers (ADR 0059); not
    /// logged (the trace id is).
    pub traceparent: Option<String>,
    /// Planning events for the trace (ADR 0056); not logged.
    pub plan_events: Vec<plimsoll_core::plan_trace::PlanEvent>,
    pub queue: Option<Duration>,
    pub planning: Option<Duration>,
    pub execution: Option<Duration>,
    pub serialization: Option<Duration>,
    pub total: Duration,
}

impl QueryLog {
    pub fn set_sql(&mut self, sql: &str) {
        let hash = digest(&SHA256, sql.as_bytes());
        // 16 hex chars: enough to correlate, too short to matter as a hash.
        let hex: String = hash.as_ref()[..8]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        self.sql_sha256 = Some(hex);
        self.sql_bytes = sql.len();
    }

    /// Fields for the `query` event; `error` is `(code, message)`.
    pub fn fields(&self, status: u16, error: Option<(&str, &str)>) -> Value {
        // Microsecond precision, no float noise (0.045, not 0.045000000000000005).
        let ms = |d: Option<Duration>| d.map(|d| (d.as_micros() as f64) / 1e3);
        let (code, message) = match error {
            Some((code, message)) => (code, Some(truncate(message, 512))),
            None => ("ok", None),
        };
        json!({
            "status": status,
            "code": code,
            "message": message,
            "query_id": self.query_id,
            "user": self.user,
            "resource_group": self.resource_group,
            "sql_sha256": self.sql_sha256,
            "sql_bytes": self.sql_bytes,
            "trace_id": self.trace_id,
            "reads": self.reads.iter().map(|r| json!({
                "table": r.table,
                "columns": r.columns,
                "row_filter": r.row_filter,
            })).collect::<Vec<_>>(),
            "queue_ms": ms(self.queue),
            "planning_ms": ms(self.planning),
            "execution_ms": ms(self.execution),
            "serialization_ms": ms(self.serialization),
            "total_ms": ms(Some(self.total)),
        })
    }
}

impl QueryLog {
    /// Fields for the `slow_query` event (ADR 0057): identity, timings,
    /// what was read, and each planning event (catalog loads, Iceberg
    /// file/manifest/delete counts). Never SQL text.
    pub fn slow_fields(&self, error: Option<(&str, &str)>) -> Value {
        let mut fields = self.fields(0, error);
        if let Value::Object(map) = &mut fields {
            map.remove("status");
            map.remove("message");
            map.insert(
                "planning_events".into(),
                Value::Array(
                    self.plan_events
                        .iter()
                        .map(|e| {
                            let mut o = Map::new();
                            o.insert("name".into(), json!(e.name));
                            o.insert("ms".into(), json!((e.duration.as_micros() as f64) / 1e3));
                            for (k, v) in &e.attrs {
                                o.insert((*k).into(), json!(v));
                            }
                            Value::Object(o)
                        })
                        .collect(),
                ),
            );
        }
        fields
    }
}

/// Level for an HTTP status: 5xx error, 4xx warn, else info.
pub fn level_for(status: u16) -> Level {
    match status {
        500.. => Level::Error,
        400.. => Level::Warn,
        _ => Level::Info,
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn query_lines_carry_identity_outcome_and_timings_but_no_sql() {
        let mut log = QueryLog {
            query_id: Some(7),
            user: Some("ana".into()),
            resource_group: Some("batch".into()),
            planning: Some(Duration::from_micros(2500)),
            total: Duration::from_millis(4),
            ..Default::default()
        };
        log.set_sql("SELECT * FROM t WHERE email = 'ana@example.com'");
        log.reads = vec![plimsoll_core::engine::TableRead {
            table: "datafusion.gold.t".into(),
            columns: vec!["email".into(), "id".into()],
            row_filter: true,
        }];
        let v = line(Level::Info, "query", log.fields(200, None));
        assert_eq!(v["event"], "query");
        assert_eq!(v["code"], "ok");
        assert_eq!(v["user"], "ana");
        assert_eq!(v["planning_ms"], 2.5);
        assert_eq!(v["execution_ms"], Value::Null);
        assert_eq!(v["sql_sha256"].as_str().unwrap().len(), 16);
        assert_eq!(
            v["reads"],
            json!([{"table": "datafusion.gold.t", "columns": ["email", "id"], "row_filter": true}])
        );
        assert!(
            !v.to_string().contains("example.com"),
            "no SQL text in logs"
        );

        // slow_query carries the planning story, still without SQL.
        log.plan_events = vec![plimsoll_core::plan_trace::PlanEvent {
            name: "iceberg.plan",
            start: SystemTime::now(),
            duration: Duration::from_micros(1500),
            attrs: vec![("files", "1/3".into())],
        }];
        let slow = log.slow_fields(None);
        assert_eq!(slow["planning_events"][0]["files"], "1/3");
        assert_eq!(slow["planning_events"][0]["ms"], 1.5);
        assert_eq!(slow["user"], "ana");
        assert!(!slow.to_string().contains("example.com"));

        let err = log.fields(403, Some(("forbidden", &"x".repeat(2000))));
        assert_eq!(err["code"], "forbidden");
        assert!(err["message"].as_str().unwrap().chars().count() <= 513);
        assert_eq!(level_for(403), Level::Warn);
        assert_eq!(level_for(502), Level::Error);
        assert_eq!(level_for(200), Level::Info);
    }
}
