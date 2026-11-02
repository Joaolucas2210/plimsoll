//! HTTP routes. Every handler is a thin translation layer: governance lives
//! in `plimsoll-control`, execution behind [`QueryEngine`].

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use plimsoll_control::governance::{
    Admission, AdmissionError, CancelOutcome, DEFAULT_GROUP, GovernedError, QueryRequest,
};
use plimsoll_core::datafusion::common::ScalarValue;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{mpsc, oneshot};

use crate::auth::Auth;
use crate::engine::{Chunk, EngineError, Format, QueryEngine, QueryOutput, Streamed};
use crate::log::{Level, QueryLog, level_for};
use crate::metrics::{self, Latency, PoolStatsProvider};
use crate::trace::{self, TraceContext, Tracer};
use futures::StreamExt;

pub struct AppState {
    pub engine: Arc<dyn QueryEngine>,
    pub admission: Admission,
    pub pool_stats: PoolStatsProvider,
    pub query_timeout: Duration,
    /// `/readyz`: false while starting and from SIGTERM on, so load
    /// balancers stop routing here.
    pub ready: AtomicBool,
    /// `/v1/sql` accepts queries: false only while starting. Stays true
    /// while draining, so requests still routed here are served (ADR 0026).
    pub serving: AtomicBool,
    pub started: Instant,
    /// `/v1/sql` errors by `code`, for `plimsoll_query_errors_total`.
    pub errors: ErrorCounts,
    pub latency: Latency,
    pub auth: Auth,
    /// OTLP export; traces are propagated even without it (ADR 0031).
    pub tracer: Option<Tracer>,
}

#[derive(Default)]
pub struct ErrorCounts(Mutex<BTreeMap<&'static str, u64>>);

impl ErrorCounts {
    fn inc(&self, code: &'static str) {
        *self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(code)
            .or_default() += 1;
    }

    pub fn snapshot(&self) -> BTreeMap<&'static str, u64> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_handler))
        .route("/v1/sql", post(sql))
        .route("/v1/queries/{id}", delete(cancel_query))
        .with_state(state)
}

/// Process is alive and the runtime is scheduling tasks. Never checks
/// dependencies: a failing liveness probe restarts the pod.
async fn livez() -> &'static str {
    "ok"
}

async fn readyz(State(st): State<Arc<AppState>>) -> Response {
    if st.ready.load(Ordering::Relaxed) {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
    }
}

async fn healthz(State(st): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let ready = st.ready.load(Ordering::Relaxed);
    let (running, queued) = st.admission.load();
    let pool = (st.pool_stats)();
    Json(json!({
        "status": if ready { "ok" } else { "not_ready" },
        "ready": ready,
        "uptime_secs": st.started.elapsed().as_secs(),
        "queries": { "running": running, "queued": queued },
        "pool": { "used_bytes": pool.used_bytes, "limit_bytes": pool.limit_bytes },
    }))
}

async fn metrics_handler(State(st): State<Arc<AppState>>) -> Response {
    let (running, queued) = st.admission.load();
    let body = metrics::render(
        (st.pool_stats)(),
        running,
        queued,
        st.admission.registry().totals(),
        &st.errors.snapshot(),
        &st.latency,
    );
    ([(header::CONTENT_TYPE, metrics::CONTENT_TYPE)], body).into_response()
}

/// SIGTERM handling (ADR 0026): stop advertising readiness, keep serving for
/// `delay` so load balancers that still route here (until their next probe)
/// get answers instead of refused connections, then return so the caller
/// closes the listener and drains in-flight requests.
pub async fn drain(state: &AppState, delay: Duration) {
    state.ready.store(false, Ordering::Relaxed);
    tokio::time::sleep(delay).await;
}

/// `{"query_id":..,"columns":..,"rows":<rows_json>}` sent as three chunks, so
/// the (possibly large) rows are never copied. The pool reservation covering
/// them is dropped only once the body stream is exhausted, i.e. after hyper
/// has taken the last chunk.
fn json_body(query_id: Option<u64>, out: QueryOutput) -> Response {
    let head = json!({ "query_id": query_id, "columns": out.columns }).to_string();
    // `{"query_id":..,"columns":[..]}` -> `{"query_id":..,"columns":[..],"rows":`
    let head = format!("{},\"rows\":", head.trim_end_matches('}'));
    let chunks = [head.into_bytes(), out.rows_json, b"}".to_vec()];
    let memory = out.memory;
    let stream = futures::stream::iter(chunks.map(Ok::<_, std::convert::Infallible>)).chain(
        futures::stream::once(async move {
            drop(memory);
            Ok(Vec::new())
        }),
    );
    (
        [(header::CONTENT_TYPE, "application/json")],
        axum::body::Body::from_stream(stream),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
pub struct SqlRequest {
    pub sql: String,
    /// With auth on, the token's principal; a different value is refused.
    /// With auth disabled, required and trusted as sent.
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub resource_group: Option<String>,
    /// `json` (default): one document. `ndjson`: streamed lines (ADR 0035).
    #[serde(default)]
    pub format: Option<String>,
    /// Values for `$1`, `$2`, ... (ADR 0043): JSON scalars, never spliced
    /// into the SQL text.
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
}

/// A JSON scalar as a SQL value; the engine casts it to the type its
/// placeholder needs. Arrays and objects are not parameters.
fn param(v: &serde_json::Value) -> Option<ScalarValue> {
    use serde_json::Value as J;
    Some(match v {
        J::Null => ScalarValue::Null,
        J::Bool(b) => ScalarValue::Boolean(Some(*b)),
        J::Number(n) => match (n.as_i64(), n.as_u64(), n.as_f64()) {
            (Some(i), _, _) => ScalarValue::Int64(Some(i)),
            (None, Some(u), _) => ScalarValue::UInt64(Some(u)),
            (None, None, Some(f)) => ScalarValue::Float64(Some(f)),
            _ => return None,
        },
        J::String(s) => ScalarValue::Utf8(Some(s.clone())),
        J::Array(_) | J::Object(_) => return None,
    })
}

/// Error contract (ADR 0013): `code` is stable and machine-readable;
/// `retryable` says whether the same request may succeed later unchanged.
struct ApiError {
    status: StatusCode,
    code: &'static str,
    retryable: bool,
    dependency: Option<&'static str>,
}

impl ApiError {
    const fn new(status: StatusCode, code: &'static str, retryable: bool) -> Self {
        Self {
            status,
            code,
            retryable,
            dependency: None,
        }
    }

    fn of(e: &GovernedError<EngineError>) -> Self {
        use StatusCode as S;
        match e {
            GovernedError::Rejected(AdmissionError::QueueFull { .. }) => {
                Self::new(S::TOO_MANY_REQUESTS, "queue_full", true)
            }
            GovernedError::Rejected(AdmissionError::Closed) => {
                Self::new(S::SERVICE_UNAVAILABLE, "not_ready", true)
            }
            GovernedError::TimedOut(_) => Self::new(S::GATEWAY_TIMEOUT, "query_timeout", true),
            // 409: the query conflicted with a cancel request (ADR 0034).
            GovernedError::Cancelled => Self::new(S::CONFLICT, "query_cancelled", false),
            GovernedError::Failed(e) => match e {
                EngineError::InvalidQuery(_) => Self::new(S::BAD_REQUEST, "invalid_query", false),
                // 4xx: the node is healthy; retrying unchanged will not help.
                EngineError::MemoryLimitExceeded(_) => {
                    Self::new(S::UNPROCESSABLE_ENTITY, "memory_limit_exceeded", false)
                }
                EngineError::Unsupported(_) => {
                    Self::new(S::UNPROCESSABLE_ENTITY, "unsupported_feature", false)
                }
                EngineError::InvalidTable(_) => {
                    Self::new(S::UNPROCESSABLE_ENTITY, "invalid_table", false)
                }
                EngineError::Forbidden(_) => Self::new(S::FORBIDDEN, "forbidden", false),
                // 502: an upstream failed, not this node.
                EngineError::Dependency { dependency, .. } => Self {
                    dependency: Some(dependency),
                    ..Self::new(S::BAD_GATEWAY, "dependency_failed", true)
                },
                EngineError::Internal(_) => Self::new(S::INTERNAL_SERVER_ERROR, "internal", false),
            },
        }
    }

    fn respond(self, st: &AppState, message: impl ToString, query_id: Option<u64>) -> Response {
        st.errors.inc(self.code);
        let message = message.to_string();
        let mut body = json!({
            "code": self.code,
            "message": message,
            "retryable": self.retryable,
            "query_id": query_id,
        });
        if let Some(d) = self.dependency {
            body["dependency"] = json!(d);
        }
        let mut response = (self.status, Json(json!({ "error": body }))).into_response();
        // For the query log line written by `sql` (ADR 0027).
        response.extensions_mut().insert(ErrorInfo {
            code: self.code,
            message,
        });
        response
    }
}

/// The error a response carries, read back by the query log.
#[derive(Clone)]
struct ErrorInfo {
    code: &'static str,
    message: String,
}

/// `DELETE /v1/queries/{id}` (ADR 0034): stops a queued or running query.
/// Owners cancel their own queries, operators any. A query the caller may
/// not see is "not found", so ids of other users' queries leak nothing.
async fn cancel_query(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    let st = st.as_ref();
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let principal = match st.auth.principal(authorization) {
        Ok(p) => p.map(str::to_owned),
        Err(e) => {
            let mut r =
                ApiError::new(StatusCode::UNAUTHORIZED, "unauthorized", false).respond(st, e, None);
            r.headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            return r;
        }
    };
    let not_found = || {
        ApiError::new(StatusCode::NOT_FOUND, "query_not_found", false).respond(
            st,
            format!("no query {id} visible to this principal"),
            None,
        )
    };
    let Ok(query_id) = id.parse::<u64>() else {
        return not_found();
    };
    let registry = st.admission.registry();
    // Auth disabled: development, anyone may cancel (ADR 0021).
    let viewer = principal.as_deref().map(|p| st.auth.viewer(p));
    let visible = registry.get(query_id).is_some_and(|q| {
        viewer
            .as_ref()
            .and_then(|v| v.only.as_deref())
            .is_none_or(|only| only == q.user)
    });
    if !visible {
        return not_found();
    }
    match registry.cancel(query_id) {
        CancelOutcome::Cancelling => (
            StatusCode::ACCEPTED,
            Json(json!({ "query_id": query_id, "state": "cancelling" })),
        )
            .into_response(),
        CancelOutcome::NotRunning(state) => {
            ApiError::new(StatusCode::CONFLICT, "query_not_running", false).respond(
                st,
                format!("query {query_id} is already {state}"),
                Some(query_id),
            )
        }
        CancelOutcome::NotFound => not_found(),
    }
}

/// `/v1/sql`: runs the request and writes one structured `query` log line
/// for every outcome, rejections included (ADR 0027).
async fn sql(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Result<Json<SqlRequest>, JsonRejection>,
) -> Response {
    let started = Instant::now();
    let wall = std::time::SystemTime::now();
    let ctx =
        TraceContext::from_traceparent(headers.get("traceparent").and_then(|v| v.to_str().ok()));
    let mut log = QueryLog {
        trace_id: Some(ctx.trace_id.clone()),
        ..Default::default()
    };
    let mut response = run_sql(&st, &headers, body, &mut log).await;
    if let Ok(v) = HeaderValue::from_str(&ctx.traceparent()) {
        response.headers_mut().insert("traceparent", v);
    }
    let status = response.status().as_u16();
    // A streamed result is logged when its last line is written (ADR 0035).
    let deferred = response
        .extensions_mut()
        .remove::<Deferred>()
        .and_then(|d| d.0.lock().unwrap_or_else(PoisonError::into_inner).take());
    if let Some(outcome) = deferred {
        tokio::spawn(async move {
            let outcome = outcome
                .await
                .unwrap_or_else(|_| Outcome::failed("internal", "stream task ended"));
            if let Some(s) = outcome.streamed {
                log.planning = Some(s.phases.planning);
                log.execution = Some(s.phases.execution);
                log.serialization = Some(s.phases.serialization);
            }
            log.total = started.elapsed();
            let error = outcome.error.as_ref().map(|(c, m)| (*c, m.as_str()));
            let level = if error.is_some() {
                Level::Warn
            } else {
                Level::Info
            };
            crate::log::emit(level, "query", log.fields(status, error));
            if let (Some(tracer), true) = (&st.tracer, ctx.sampled) {
                tracer.record(trace::spans(&ctx, wall, &log, status, error));
            }
        });
        return response;
    }
    log.total = started.elapsed();
    let error = response
        .extensions()
        .get::<ErrorInfo>()
        .map(|e| (e.code, e.message.as_str()));
    crate::log::emit(level_for(status), "query", log.fields(status, error));
    if let (Some(tracer), true) = (&st.tracer, ctx.sampled) {
        tracer.record(trace::spans(&ctx, wall, &log, status, error));
    }
    response
}

/// How a streamed query ended, for its deferred log line and trace.
struct Outcome {
    error: Option<(&'static str, String)>,
    streamed: Option<Streamed>,
}

impl Outcome {
    fn failed(code: &'static str, message: impl ToString) -> Self {
        Self {
            error: Some((code, message.to_string())),
            streamed: None,
        }
    }
}

/// Marks a streamed response; `sql` logs once the outcome arrives.
#[derive(Clone)]
struct Deferred(Arc<Mutex<Option<oneshot::Receiver<Outcome>>>>);

/// Lives in the response body: reports the outcome when the last line is
/// written, or, if the body is dropped first (the client went away),
/// cancels the query so its slot and memory are released at once.
struct StreamGuard {
    st: Arc<AppState>,
    query_id: u64,
    arrived: Instant,
    outcome: Option<oneshot::Sender<Outcome>>,
}

impl StreamGuard {
    fn finish(&mut self, outcome: Outcome) {
        self.st.latency.total.observe(self.arrived.elapsed());
        if let Some(code) = outcome.error.as_ref().map(|(c, _)| *c) {
            self.st.errors.inc(code);
        }
        if let Some(tx) = self.outcome.take() {
            let _ = tx.send(outcome);
        }
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        if self.outcome.is_some() {
            self.st.admission.registry().cancel(self.query_id);
            self.finish(Outcome::failed(
                "client_disconnected",
                "the client closed the connection before the last row",
            ));
        }
    }
}

/// The body's end of the channel. Dropped before the end (the client went
/// away), it cancels the query *before* the receiver closes, so the query
/// is recorded as cancelled rather than failing on its next send.
struct Receiving {
    rx: mpsc::Receiver<Chunk>,
    st: Arc<AppState>,
    query_id: u64,
    drained: bool,
}

impl Drop for Receiving {
    fn drop(&mut self) {
        if !self.drained {
            self.st.admission.registry().cancel(self.query_id);
        }
    }
}

fn ndjson_line(v: &serde_json::Value) -> Vec<u8> {
    let mut line = v.to_string().into_bytes();
    line.push(b'\n');
    line
}

/// `format: "ndjson"` (ADR 0035): the governed query runs in its own task
/// and sends NDJSON chunks through a bounded channel, so the response
/// starts before the result is complete and memory holds only the chunks
/// in flight. Admission, the deadline and cancellation cover the whole
/// stream. Errors before the first line keep their HTTP status; later ones
/// end the body with an `{"error": ..}` line.
async fn stream_sql(
    shared: Arc<AppState>,
    request: QueryRequest,
    params: Vec<ScalarValue>,
    viewer: crate::auth::Viewer,
    format: Format,
    arrived: Instant,
    log: &mut QueryLog,
) -> Response {
    let (tx, mut rx) = mpsc::channel::<Chunk>(2);
    let (done_tx, done_rx) = oneshot::channel();
    let assigned = Arc::new(AtomicU64::new(0));
    let queued_us = Arc::new(AtomicU64::new(0));
    {
        let (st, assigned, queued_us) = (shared.clone(), assigned.clone(), queued_us.clone());
        tokio::spawn(async move {
            let sql = request.sql.clone();
            let engine = st.clone();
            let result = st
                .admission
                .execute(request, st.query_timeout, move |id| {
                    assigned.store(id, Ordering::Relaxed);
                    let queued = arrived.elapsed();
                    engine.latency.queue.observe(queued);
                    queued_us.store(
                        u64::try_from(queued.as_micros()).unwrap_or(u64::MAX),
                        Ordering::Relaxed,
                    );
                    async move {
                        engine
                            .engine
                            .stream(&sql, &params, viewer, format, tx)
                            .await
                    }
                })
                .await;
            let _ = done_tx.send(result);
        });
    }
    let first = rx.recv().await;
    let query_id = Some(assigned.load(Ordering::Relaxed)).filter(|id| *id != 0);
    log.query_id = query_id;
    if query_id.is_some() {
        log.queue = Some(Duration::from_micros(queued_us.load(Ordering::Relaxed)));
    }
    let st = shared.as_ref();
    let (Some(Chunk::Header(columns)), Some(query_id)) = (first, query_id) else {
        // No header: the query failed before producing a result (rejected,
        // invalid, forbidden...). Answer with its status like `json` does.
        if query_id.is_some() {
            st.latency.total.observe(arrived.elapsed());
        }
        return match done_rx.await {
            Ok(Err(e)) => ApiError::of(&e).respond(st, &e, query_id),
            _ => ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", false).respond(
                st,
                "stream ended without a header",
                query_id,
            ),
        };
    };
    let (outcome_tx, outcome_rx) = oneshot::channel();
    let mut guard = StreamGuard {
        st: shared.clone(),
        query_id,
        arrived,
        outcome: Some(outcome_tx),
    };
    // Arrow carries its schema in the stream; NDJSON gets a header line.
    let head = match format {
        Format::Ndjson => ndjson_line(&json!({ "query_id": query_id, "columns": columns })),
        Format::Arrow => Vec::new(),
    };
    let receiving = Receiving {
        rx,
        st: shared.clone(),
        query_id,
        drained: false,
    };
    let rows = futures::stream::unfold(receiving, |mut r| async move {
        let chunk = r.rx.recv().await;
        r.drained = chunk.is_none();
        chunk.map(|chunk| (chunk, r))
    })
    .map(|chunk| match chunk {
        // The reservation ends as hyper takes the bytes.
        Chunk::Rows(bytes, _memory) => bytes,
        Chunk::Header(_) => Vec::new(),
    });
    let tail = futures::stream::once(async move {
        let (line, outcome) = match done_rx.await {
            Ok(Ok(s)) => {
                guard.st.latency.planning.observe(s.phases.planning);
                guard.st.latency.execution.observe(s.phases.execution);
                guard
                    .st
                    .latency
                    .serialization
                    .observe(s.phases.serialization);
                (
                    json!({ "done": true, "rows": s.rows }),
                    Outcome {
                        error: None,
                        streamed: Some(s),
                    },
                )
            }
            Ok(Err(e)) => {
                let api = ApiError::of(&e);
                let body = json!({ "error": {
                    "code": api.code, "message": e.to_string(),
                    "retryable": api.retryable, "query_id": query_id,
                }});
                (body, Outcome::failed(api.code, e))
            }
            Err(_) => (
                json!({ "error": { "code": "internal", "message": "stream task ended",
                                   "retryable": false, "query_id": query_id } }),
                Outcome::failed("internal", "stream task ended"),
            ),
        };
        let failed = outcome
            .error
            .as_ref()
            .map(|(code, m)| format!("{code}: {m}"));
        guard.finish(outcome);
        match (format, failed) {
            (Format::Ndjson, _) => Ok(ndjson_line(&line)),
            // The writer's end-of-stream marker was the last chunk.
            (Format::Arrow, None) => Ok(Vec::new()),
            // Arrow has no error message: abort the body, so the stream ends
            // without its end-of-stream marker and readers fail loudly
            // instead of seeing a short, valid-looking result.
            (Format::Arrow, Some(e)) => Err(std::io::Error::other(e)),
        }
    });
    let body = futures::stream::once(std::future::ready(Ok(head)))
        .chain(rows.map(Ok))
        .chain(tail);
    let content_type = match format {
        Format::Ndjson => "application/x-ndjson",
        Format::Arrow => "application/vnd.apache.arrow.stream",
    };
    let mut response = (
        [
            (header::CONTENT_TYPE, content_type.to_owned()),
            (
                HeaderName::from_static("x-plimsoll-query-id"),
                query_id.to_string(),
            ),
        ],
        axum::body::Body::from_stream(body),
    )
        .into_response();
    response
        .extensions_mut()
        .insert(Deferred(Arc::new(Mutex::new(Some(outcome_rx)))));
    response
}

async fn run_sql(
    st: &Arc<AppState>,
    headers: &HeaderMap,
    body: Result<Json<SqlRequest>, JsonRejection>,
    log: &mut QueryLog,
) -> Response {
    let shared = Arc::clone(st);
    let st = st.as_ref();
    // Authenticate first: an anonymous caller learns nothing else (ADR 0021).
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let principal = match st.auth.principal(authorization) {
        Ok(p) => p,
        Err(e) => {
            let mut r =
                ApiError::new(StatusCode::UNAUTHORIZED, "unauthorized", false).respond(st, e, None);
            r.headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            return r;
        }
    };
    if !st.serving.load(Ordering::Relaxed) {
        return ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "not_ready", true).respond(
            st,
            "server is not accepting queries",
            None,
        );
    }
    let req = match body {
        Ok(Json(r)) => {
            log.set_sql(&r.sql);
            r
        }
        Err(e) => {
            return ApiError::new(StatusCode::BAD_REQUEST, "invalid_request", false).respond(
                st,
                e.body_text(),
                None,
            );
        }
    };
    let user = match (principal, req.user) {
        (Some(p), Some(u)) if u != p => {
            return ApiError::new(StatusCode::FORBIDDEN, "forbidden", false).respond(
                st,
                format!("authenticated as {p:?}, cannot run as {u:?}"),
                None,
            );
        }
        (Some(p), _) => p.to_owned(),
        (None, Some(u)) => u,
        (None, None) => String::new(),
    };
    log.user = Some(user.clone()).filter(|u| !u.is_empty());
    if req.sql.trim().is_empty() || user.trim().is_empty() {
        return ApiError::new(StatusCode::BAD_REQUEST, "invalid_request", false).respond(
            st,
            "`sql` and `user` must be non-empty",
            None,
        );
    }
    // Resource group: requested, else the principal's default (ADR 0023).
    let allowed = st.auth.groups_for(&user);
    let group = req.resource_group.unwrap_or_else(|| {
        allowed
            .as_ref()
            .and_then(|a| a.first())
            .unwrap_or(&DEFAULT_GROUP)
            .to_string()
    });
    log.resource_group = Some(group.clone());
    if !st.admission.config().has_group(&group) {
        return ApiError::new(StatusCode::BAD_REQUEST, "invalid_request", false).respond(
            st,
            format!("unknown resource group {group:?}"),
            None,
        );
    }
    if allowed.is_some_and(|a| !a.contains(&group.as_str())) {
        return ApiError::new(StatusCode::FORBIDDEN, "forbidden", false).respond(
            st,
            format!("{user:?} may not use resource group {group:?}"),
            None,
        );
    }
    let Some(params) = req.params.iter().map(param).collect::<Option<Vec<_>>>() else {
        return ApiError::new(StatusCode::BAD_REQUEST, "invalid_request", false).respond(
            st,
            "`params` must be JSON scalars (number, string, boolean, null)",
            None,
        );
    };
    let request = QueryRequest {
        user,
        resource_group: group,
        sql: req.sql,
    };
    let mut viewer = st.auth.viewer(&request.user);
    viewer.group = Some(request.resource_group.clone());
    match req.format.as_deref() {
        None | Some("json") => {}
        Some("ndjson") => {
            return stream_sql(
                shared,
                request,
                params,
                viewer,
                Format::Ndjson,
                Instant::now(),
                log,
            )
            .await;
        }
        Some("arrow") => {
            return stream_sql(
                shared,
                request,
                params,
                viewer,
                Format::Arrow,
                Instant::now(),
                log,
            )
            .await;
        }
        Some(other) => {
            return ApiError::new(StatusCode::BAD_REQUEST, "invalid_request", false).respond(
                st,
                format!("unknown format {other:?} (json, ndjson, arrow)"),
                None,
            );
        }
    }
    let sql_text = request.sql.clone();
    let assigned = AtomicU64::new(0);
    let queued_us = AtomicU64::new(0);
    let arrived = Instant::now();
    let result = st
        .admission
        .execute(request, st.query_timeout, |id| {
            assigned.store(id, Ordering::Relaxed);
            // Called once a slot is granted: everything before was queueing.
            let queued = arrived.elapsed();
            st.latency.queue.observe(queued);
            queued_us.store(
                u64::try_from(queued.as_micros()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            st.engine.execute(&sql_text, &params, viewer)
        })
        .await;
    let query_id = Some(assigned.load(Ordering::Relaxed)).filter(|id| *id != 0);
    log.query_id = query_id;
    if query_id.is_some() {
        st.latency.total.observe(arrived.elapsed());
        log.queue = Some(Duration::from_micros(queued_us.load(Ordering::Relaxed)));
    }

    match result {
        Ok(out) => {
            log.planning = Some(out.phases.planning);
            log.execution = Some(out.phases.execution);
            log.serialization = Some(out.phases.serialization);
            st.latency.planning.observe(out.phases.planning);
            st.latency.execution.observe(out.phases.execution);
            st.latency.serialization.observe(out.phases.serialization);
            json_body(query_id, out)
        }
        Err(e) => ApiError::of(&e).respond(st, &e, query_id),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::auth::Viewer;
    use crate::metrics::PoolStats;
    use async_trait::async_trait;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use plimsoll_control::governance::{AdmissionConfig, Limits, QueryRegistry, QueryState};
    use tower::ServiceExt;

    struct FakeEngine;

    #[async_trait]
    impl QueryEngine for FakeEngine {
        async fn execute(
            &self,
            sql: &str,
            _params: &[ScalarValue],
            _viewer: Viewer,
        ) -> Result<QueryOutput, EngineError> {
            match sql {
                "select 1" => Ok(QueryOutput::from_rows(vec!["x".into()], &[vec![json!(1)]])),
                "oom" => Err(EngineError::MemoryLimitExceeded("needed 2GiB".into())),
                "deletes" => Err(EngineError::Unsupported("delete files".into())),
                "s3 down" => Err(EngineError::Dependency {
                    dependency: "object_store",
                    message: "connection refused".into(),
                }),
                "slow" => {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Err(EngineError::Internal("unreachable".into()))
                }
                _ => Err(EngineError::InvalidQuery("syntax error".into())),
            }
        }
    }

    fn state(limits: Limits, timeout: Duration) -> Arc<AppState> {
        let cfg = AdmissionConfig {
            global: limits,
            per_group: limits,
            per_user: limits,
            groups: std::collections::HashMap::from([(
                "batch".to_owned(),
                Limits {
                    max_running: 1,
                    max_queued: 0,
                },
            )]),
        };
        Arc::new(AppState {
            engine: Arc::new(FakeEngine),
            admission: Admission::new(cfg, Arc::new(QueryRegistry::new(100))),
            pool_stats: Arc::new(|| PoolStats {
                used_bytes: 5,
                limit_bytes: 1024,
                cache_reserve_bytes: 0,
                spill_bytes_total: 0,
                groups: vec![],
                result_cache: None,
            }),
            query_timeout: timeout,
            ready: AtomicBool::new(true),
            serving: AtomicBool::new(true),
            started: Instant::now(),
            errors: ErrorCounts::default(),
            latency: Latency::default(),
            auth: Auth::Disabled,
            tracer: None,
        })
    }

    fn default_state() -> Arc<AppState> {
        state(
            Limits {
                max_running: 4,
                max_queued: 4,
            },
            Duration::from_secs(5),
        )
    }

    async fn call(app: Router, req: Request<Body>) -> (StatusCode, String) {
        let res = app.oneshot(req).await.unwrap();
        let status = res.status();
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    fn get_req(path: &str) -> Request<Body> {
        Request::get(path).body(Body::empty()).unwrap()
    }

    fn sql_req(body: serde_json::Value) -> Request<Body> {
        Request::post("/v1/sql")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn error_code(body: &str) -> String {
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        v["error"]["code"].as_str().unwrap().to_owned()
    }

    #[tokio::test]
    async fn health_endpoints_follow_readiness() {
        let st = default_state();
        st.ready.store(false, Ordering::Relaxed);
        assert_eq!(
            call(router(st.clone()), get_req("/livez")).await.0,
            StatusCode::OK
        );
        assert_eq!(
            call(router(st.clone()), get_req("/readyz")).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );

        st.ready.store(true, Ordering::Relaxed);
        assert_eq!(
            call(router(st.clone()), get_req("/readyz")).await.0,
            StatusCode::OK
        );
        let (status, body) = call(router(st), get_req("/healthz")).await;
        assert_eq!(status, StatusCode::OK);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["pool"]["limit_bytes"], 1024);
    }

    #[tokio::test]
    async fn metrics_expose_pool_and_query_totals() {
        let st = default_state();
        call(
            router(st.clone()),
            sql_req(json!({"sql": "select 1", "user": "ana"})),
        )
        .await;
        let res = router(st.clone())
            .oneshot(get_req("/metrics"))
            .await
            .unwrap();
        assert_eq!(res.headers()[header::CONTENT_TYPE], metrics::CONTENT_TYPE);
        let (_, body) = call(router(st), get_req("/metrics")).await;
        assert!(body.contains("plimsoll_pool_used_bytes 5"));
        assert!(body.contains("plimsoll_queries_total{state=\"finished\"} 1"));
    }

    #[tokio::test]
    async fn sql_continues_the_callers_trace() {
        let mut req = sql_req(json!({"sql": "select 1", "user": "ana"}));
        req.headers_mut().insert(
            "traceparent",
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        let res = router(default_state()).oneshot(req).await.unwrap();
        let tp = res.headers()["traceparent"].to_str().unwrap();
        assert!(
            tp.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"),
            "{tp}"
        );
        assert!(
            !tp.contains("00f067aa0ba902b7"),
            "a new span for this request"
        );
        // Without one, every request (rejections included) starts a trace.
        let res = router(default_state())
            .oneshot(sql_req(json!({"sql": "select 1"})))
            .await
            .unwrap();
        assert_eq!(res.headers()["traceparent"].len(), 55);
    }

    #[tokio::test]
    async fn sql_returns_rows() {
        let (status, body) = call(
            router(default_state()),
            sql_req(json!({"sql": "select 1", "user": "ana"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["columns"], json!(["x"]));
        assert_eq!(v["rows"], json!([[1]]));
        assert_eq!(v["query_id"], 1);
    }

    /// End to end: HTTP -> admission -> CeilingPool engine -> system table.
    #[tokio::test]
    async fn real_engine_oom_is_query_only_and_visible_in_system_table() {
        use plimsoll_core::engine::Engine;
        use plimsoll_core::memory::CeilingPool;

        let pool = Arc::new(CeilingPool::new(1 << 20, 1 << 17).unwrap());
        let engine = Arc::new(Engine::new(pool, None).unwrap());
        let mut st = state(
            Limits {
                max_running: 2,
                max_queued: 2,
            },
            Duration::from_secs(10),
        );
        crate::system::register(engine.context(), st.admission.registry().clone()).unwrap();
        Arc::get_mut(&mut st).unwrap().engine = Arc::new(crate::engine::CoreEngine(engine));

        let big = "SELECT value, repeat('x', 100) FROM range(200000)";
        let (status, _) = call(
            router(st.clone()),
            sql_req(json!({"sql": big, "user": "ana"})),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

        let q = "SELECT state, error IS NOT NULL AS has_error FROM system.runtime.queries ORDER BY query_id";
        let (status, body) = call(router(st), sql_req(json!({"sql": q, "user": "ana"}))).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["rows"], json!([["failed", true], ["running", false]]));
    }

    #[tokio::test]
    async fn system_queries_are_scoped_to_the_principal() {
        use plimsoll_core::engine::Engine;
        use plimsoll_core::memory::CeilingPool;

        let engine =
            Arc::new(Engine::new(Arc::new(CeilingPool::new(64 << 20, 0).unwrap()), None).unwrap());
        let mut st = Arc::into_inner(default_state()).unwrap();
        crate::system::register(engine.context(), st.admission.registry().clone()).unwrap();
        st.engine = Arc::new(crate::engine::CoreEngine(engine.clone()));
        let hex = |t: &str| -> String {
            crate::auth::token_digest(t)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect()
        };
        let tokens = format!(
            "ana:sha256:{},bob:sha256:{},ops:sha256:{}",
            hex("ta"),
            hex("tb"),
            hex("to")
        );
        st.auth = Auth::from_lookup(
            &|k| match k {
                "PLIMSOLL_AUTH_TOKENS" => Some(tokens.clone()),
                "PLIMSOLL_AUTH_OPERATORS" => Some("ops".into()),
                _ => None,
            },
            &|g| g == DEFAULT_GROUP,
        )
        .unwrap();
        let st = Arc::new(st);
        let run = async |token: &str, sql: &str| {
            let req = Request::post("/v1/sql")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(json!({ "sql": sql }).to_string()))
                .unwrap();
            let (status, body) = call(router(st.clone()), req).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            serde_json::from_str::<serde_json::Value>(&body).unwrap()["rows"].clone()
        };
        run("ta", "SELECT 1").await;
        run("tb", "SELECT 2").await;
        let users = "SELECT DISTINCT \"user\" FROM system.runtime.queries ORDER BY 1";
        // Each sees only their own queries; the operator sees everyone's.
        assert_eq!(run("ta", users).await, json!([["ana"]]));
        assert_eq!(run("tb", users).await, json!([["bob"]]));
        assert_eq!(run("to", users).await, json!([["ana"], ["bob"], ["ops"]]));
        // A query path that forgets the viewer fails closed, not open.
        let err = engine.sql(users).await.unwrap_err().to_string();
        assert!(err.contains("needs the caller's identity"), "{err}");
    }

    #[tokio::test]
    async fn grants_limit_readable_schemas_over_http() {
        use plimsoll_core::engine::Engine;
        use plimsoll_core::memory::CeilingPool;

        let engine =
            Arc::new(Engine::new(Arc::new(CeilingPool::new(64 << 20, 0).unwrap()), None).unwrap());
        for t in ["gold.g", "bronze.b"] {
            engine
                .context()
                .sql(&format!("CREATE TABLE {t} AS SELECT 1 AS x"))
                .await
                .unwrap();
        }
        let mut st = Arc::into_inner(default_state()).unwrap();
        crate::system::register(engine.context(), st.admission.registry().clone()).unwrap();
        st.engine = Arc::new(crate::engine::CoreEngine(engine));
        let hex: String = crate::auth::token_digest("ta")
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        st.auth = Auth::from_lookup(
            &|k| match k {
                "PLIMSOLL_AUTH_TOKENS" => Some(format!("ana:sha256:{hex}")),
                "PLIMSOLL_AUTH_GRANTS" => Some("ana=gold".into()),
                _ => None,
            },
            &|g| g == DEFAULT_GROUP,
        )
        .unwrap();
        let st = Arc::new(st);
        for (sql, status) in [
            ("SELECT x FROM gold.g", StatusCode::OK),
            ("SELECT x FROM bronze.b", StatusCode::FORBIDDEN),
            (
                "SELECT x FROM gold.g WHERE x IN (SELECT x FROM bronze.b)",
                StatusCode::FORBIDDEN,
            ),
            (
                "SELECT count(*) FROM system.runtime.queries",
                StatusCode::OK,
            ),
        ] {
            let req = Request::post("/v1/sql")
                .header("content-type", "application/json")
                .header("authorization", "Bearer ta")
                .body(Body::from(json!({ "sql": sql }).to_string()))
                .unwrap();
            let (got, body) = call(router(st.clone()), req).await;
            assert_eq!(got, status, "{sql}: {body}");
            if status == StatusCode::FORBIDDEN {
                assert!(
                    body.contains("access denied to table datafusion.bronze.b"),
                    "{body}"
                );
            }
        }
    }

    #[tokio::test]
    async fn sql_maps_errors_to_typed_responses() {
        let cases = [
            (
                json!({"sql": "selec", "user": "ana"}),
                StatusCode::BAD_REQUEST,
                "invalid_query",
            ),
            (
                json!({"sql": "oom", "user": "ana"}),
                StatusCode::UNPROCESSABLE_ENTITY,
                "memory_limit_exceeded",
            ),
            (
                json!({"sql": "select 1"}),
                StatusCode::BAD_REQUEST,
                "invalid_request",
            ),
            (
                json!({"sql": " ", "user": "ana"}),
                StatusCode::BAD_REQUEST,
                "invalid_request",
            ),
        ];
        for (body, status, code) in cases {
            let (got, text) = call(router(default_state()), sql_req(body)).await;
            assert_eq!((got, error_code(&text).as_str()), (status, code), "{text}");
        }
    }

    #[tokio::test]
    async fn ndjson_streams_rows_and_keeps_early_error_statuses() {
        let res = router(default_state())
            .oneshot(sql_req(
                json!({"sql": "select 1", "user": "ana", "format": "ndjson"}),
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()[header::CONTENT_TYPE], "application/x-ndjson");
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let lines: Vec<serde_json::Value> = std::str::from_utf8(&body)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(
            lines,
            [
                json!({"query_id": 1, "columns": ["x"]}),
                json!([1]),
                json!({"done": true, "rows": 1}),
            ]
        );
        // Failures before the first line keep their status and error body.
        for (sql, status, code) in [
            (
                "oom",
                StatusCode::UNPROCESSABLE_ENTITY,
                "memory_limit_exceeded",
            ),
            ("bad", StatusCode::BAD_REQUEST, "invalid_query"),
        ] {
            let (got, body) = call(
                router(default_state()),
                sql_req(json!({"sql": sql, "user": "ana", "format": "ndjson"})),
            )
            .await;
            assert_eq!((got, error_code(&body).as_str()), (status, code), "{body}");
        }
        let (got, body) = call(
            router(default_state()),
            sql_req(json!({"sql": "select 1", "user": "ana", "format": "csv"})),
        )
        .await;
        assert_eq!(got, StatusCode::BAD_REQUEST, "{body}");
    }

    /// Column grants (ADR 0044) on the real engine: what a scan reads after
    /// projection pushdown decides, filters and subqueries included.
    #[tokio::test]
    async fn denied_columns_are_withheld_wherever_they_are_read() {
        use plimsoll_core::engine::Engine;
        use plimsoll_core::memory::CeilingPool;
        let engine =
            Engine::new(Arc::new(CeilingPool::new(64 << 20, 1 << 20).unwrap()), None).unwrap();
        engine
            .context()
            .sql("CREATE TABLE gold.users AS SELECT * FROM (VALUES (1, 'a@x', 'ana'), (2, 'b@x', 'bob')) AS t(id, email, name)")
            .await
            .unwrap();
        let mut st = Arc::into_inner(default_state()).unwrap();
        st.engine = Arc::new(crate::engine::CoreEngine(Arc::new(engine)));
        let tokens = format!(
            "ana:sha256:{},bob:sha256:{}",
            crate::auth::tests::hex("ta"),
            crate::auth::tests::hex("tb")
        );
        st.auth = Auth::from_lookup(
            &|k| match k {
                "PLIMSOLL_AUTH_TOKENS" => Some(tokens.clone()),
                "PLIMSOLL_AUTH_COLUMN_DENY" => Some("ana=gold.users.email".into()),
                _ => None,
            },
            &|g| g == "default",
        )
        .unwrap();
        let st = Arc::new(st);
        let run = async |token: &str, sql: &str| {
            let req = Request::post("/v1/sql")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(json!({ "sql": sql }).to_string()))
                .unwrap();
            call(router(st.clone()), req).await
        };
        for sql in [
            "SELECT id, name FROM gold.users",
            "SELECT count(*) FROM gold.users",
            "SELECT upper(name) FROM gold.users WHERE id > 1",
        ] {
            let (status, body) = run("ta", sql).await;
            assert_eq!(status, StatusCode::OK, "{sql}: {body}");
        }
        for sql in [
            "SELECT * FROM gold.users",
            "SELECT id FROM gold.users WHERE email = 'a@x'",
            "SELECT length(email) FROM gold.users",
            "SELECT id FROM gold.users WHERE id IN (SELECT id FROM gold.users WHERE email LIKE 'a%')",
        ] {
            let (status, body) = run("ta", sql).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{sql}: {body}");
            assert!(body.contains("gold.users.email (column)"), "{body}");
        }
        let (status, body) = run("tb", "SELECT * FROM gold.users").await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    /// `params` (ADR 0043): bound as values in every format; non-scalar
    /// params are a 400; the SQL text in the registry has no values.
    #[tokio::test]
    async fn params_bind_values_not_sql() {
        use plimsoll_core::engine::Engine;
        use plimsoll_core::memory::CeilingPool;
        let pool = Arc::new(CeilingPool::new(64 << 20, 1 << 20).unwrap());
        let mut st = Arc::into_inner(default_state()).unwrap();
        st.engine = Arc::new(crate::engine::CoreEngine(Arc::new(
            Engine::new(pool, None).unwrap(),
        )));
        let st = Arc::new(st);
        let q = "SELECT count(*) AS n FROM range(10) WHERE value >= $1 AND 'x' <> $2";
        for (format, params, n) in [
            ("json", json!([7, "y"]), "3"),
            ("ndjson", json!([7, "y"]), "3"),
            // An injection attempt is just a string that is not 'x'.
            ("json", json!([0, "x' OR '1'='1"]), "10"),
        ] {
            let (status, body) = call(
                router(st.clone()),
                sql_req(json!({"sql": q, "user": "ana", "format": format, "params": params})),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert!(body.contains(&format!("[{n}]")), "{format}: {body}");
        }
        let (status, body) = call(
            router(st.clone()),
            sql_req(json!({"sql": q, "user": "ana", "params": [[1], "y"]})),
        )
        .await;
        assert_eq!(
            (status, error_code(&body).as_str()),
            (StatusCode::BAD_REQUEST, "invalid_request")
        );
        let (status, body) = call(
            router(st.clone()),
            sql_req(json!({"sql": q, "user": "ana", "params": [1]})),
        )
        .await;
        assert_eq!(
            (status, error_code(&body).as_str()),
            (StatusCode::BAD_REQUEST, "invalid_query")
        );
        // The registry keeps the statement, never the values.
        let reg = st.admission.registry();
        assert!(reg.snapshot().iter().all(|q| !q.sql.contains("OR '1'")));
    }

    /// `format: arrow` (ADR 0037): a decodable IPC stream with the query id
    /// in a header; early errors keep their status; a mid-stream failure
    /// aborts the body so the stream lacks its end-of-stream marker.
    #[tokio::test]
    async fn arrow_results_stream_and_fail_loudly() {
        use plimsoll_core::datafusion::arrow::ipc::reader::StreamReader;
        use plimsoll_core::engine::Engine;
        use plimsoll_core::memory::CeilingPool;
        let pool = Arc::new(CeilingPool::new(64 << 20, 1 << 20).unwrap());
        let mut st = Arc::into_inner(default_state()).unwrap();
        st.engine = Arc::new(crate::engine::CoreEngine(Arc::new(
            Engine::new(pool.clone(), None).unwrap(),
        )));
        let st = Arc::new(st);
        let arrow = |sql: &str| sql_req(json!({"sql": sql, "user": "ana", "format": "arrow"}));

        let res = router(st.clone())
            .oneshot(arrow("SELECT value AS v FROM range(5000)"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()[header::CONTENT_TYPE],
            "application/vnd.apache.arrow.stream"
        );
        assert_eq!(res.headers()["x-plimsoll-query-id"], "1");
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let reader = StreamReader::try_new(std::io::Cursor::new(body.to_vec()), None).unwrap();
        assert_eq!(reader.map(|b| b.unwrap().num_rows()).sum::<usize>(), 5000);

        let (status, body) = call(router(st.clone()), arrow("SELEC")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(error_code(&body), "invalid_query");

        // Division by zero in the second batch: rows already went out.
        let res = router(st.clone())
            .oneshot(arrow("SELECT 1 / (value - 9000) AS x FROM range(20000)"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let mut body = res.into_body().into_data_stream();
        let (mut bytes, mut aborted) = (Vec::new(), false);
        while let Some(chunk) = body.next().await {
            match chunk {
                Ok(c) => bytes.extend_from_slice(&c),
                Err(_) => {
                    aborted = true;
                    break;
                }
            }
        }
        assert!(aborted, "the body ends in an error, not a clean EOF");
        assert!(
            !bytes.is_empty(),
            "some batches were sent before the failure"
        );
        assert_eq!(st.admission.load(), (0, 0));
        assert_eq!(pool.used(), 0);
    }

    /// A reader that goes away mid-stream cancels the query: the slot and
    /// every reserved chunk are released, and the registry says cancelled.
    #[tokio::test]
    async fn ndjson_client_disconnect_cancels_the_query() {
        use plimsoll_core::engine::Engine;
        use plimsoll_core::memory::CeilingPool;
        let pool = Arc::new(CeilingPool::new(64 << 20, 1 << 20).unwrap());
        let mut st = Arc::into_inner(default_state()).unwrap();
        st.engine = Arc::new(crate::engine::CoreEngine(Arc::new(
            Engine::new(pool.clone(), None).unwrap(),
        )));
        let st = Arc::new(st);
        let res = router(st.clone())
            .oneshot(sql_req(json!({
                "sql": "SELECT value, repeat('x', 100) FROM range(100000000)",
                "user": "ana", "format": "ndjson"})))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let mut body = res.into_body().into_data_stream();
        let first = body.next().await.unwrap().unwrap();
        let header: serde_json::Value = serde_json::from_slice(&first).unwrap();
        assert_eq!(header["query_id"], 1);
        assert!(body.next().await.unwrap().is_ok(), "rows are flowing");
        drop(body);
        let reg = st.admission.registry();
        for _ in 0..500 {
            if reg.get(1).is_some_and(|q| q.state.is_terminal()) && st.admission.load() == (0, 0) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(reg.get(1).unwrap().state, QueryState::Cancelled);
        assert_eq!(st.admission.load(), (0, 0));
        assert_eq!(pool.used(), 0, "chunks in flight released");
        assert_eq!(st.errors.snapshot().get("client_disconnected"), Some(&1));
    }

    #[tokio::test]
    async fn owners_and_operators_cancel_queries_others_see_nothing() {
        let mut st = Arc::into_inner(default_state()).unwrap();
        let digest = |t: &str| -> String {
            crate::auth::token_digest(t)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect()
        };
        let tokens = format!(
            "ana:sha256:{},bob:sha256:{},ops:sha256:{}",
            digest("ta"),
            digest("tb"),
            digest("to")
        );
        st.auth = Auth::from_lookup(
            &|k| match k {
                "PLIMSOLL_AUTH_TOKENS" => Some(tokens.clone()),
                "PLIMSOLL_AUTH_OPERATORS" => Some("ops".into()),
                _ => None,
            },
            &|g| g == "default",
        )
        .unwrap();
        let st = Arc::new(st);
        let run = |token: &str| {
            Request::post("/v1/sql")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(json!({"sql": "slow"}).to_string()))
                .unwrap()
        };
        let cancel = |token: &str, id: &str| {
            Request::delete(format!("/v1/queries/{id}"))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        let running = |id: u64| {
            st.admission
                .registry()
                .get(id)
                .is_some_and(|q| q.state == QueryState::Running)
        };
        let q1 = tokio::spawn(call(router(st.clone()), run("ta")));
        while !running(1) {
            tokio::task::yield_now().await;
        }
        // Another user: indistinguishable from a missing query.
        for (token, id) in [("tb", "1"), ("ta", "999"), ("ta", "x")] {
            let (status, body) = call(router(st.clone()), cancel(token, id)).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{token} {id}: {body}");
            assert_eq!(error_code(&body), "query_not_found");
        }
        let res = router(st.clone())
            .oneshot(
                Request::delete("/v1/queries/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert!(running(1), "nothing above touched the query");

        // The owner cancels; the query's own request ends with 409.
        let (status, body) = call(router(st.clone()), cancel("ta", "1")).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let (status, body) = q1.await.unwrap();
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(error_code(&body), "query_cancelled");
        assert_eq!(st.admission.load(), (0, 0), "slot released");
        let (status, body) = call(router(st.clone()), cancel("ta", "1")).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(error_code(&body), "query_not_running");

        // Operators cancel anyone's query.
        let q2 = tokio::spawn(call(router(st.clone()), run("tb")));
        while !running(2) {
            tokio::task::yield_now().await;
        }
        let (status, _) = call(router(st.clone()), cancel("to", "2")).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(q2.await.unwrap().0, StatusCode::CONFLICT);
        let reg = st.admission.registry();
        assert_eq!(reg.get(2).unwrap().state, QueryState::Cancelled);
        assert_eq!(reg.totals().cancelled, 2);
    }

    #[tokio::test]
    async fn sql_requires_a_bearer_token_and_runs_as_its_principal() {
        let mut st = Arc::into_inner(default_state()).unwrap();
        let hex: String = crate::auth::token_digest("t1")
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        st.auth = Auth::from_lookup(
            &|k| match k {
                "PLIMSOLL_AUTH_TOKENS" => Some(format!("ana:sha256:{hex}")),
                "PLIMSOLL_AUTH_GROUPS" => Some("ana=default".into()),
                _ => None,
            },
            &|g| ["default", "batch"].contains(&g),
        )
        .unwrap();
        let st = Arc::new(st);
        let req = |token: Option<&str>, body: serde_json::Value| {
            let mut r = Request::post("/v1/sql").header("content-type", "application/json");
            if let Some(t) = token {
                r = r.header("authorization", format!("Bearer {t}"));
            }
            r.body(Body::from(body.to_string())).unwrap()
        };
        // Missing or unknown token: 401 with a challenge, nothing executed.
        for token in [None, Some("nope")] {
            let res = router(st.clone())
                .oneshot(req(token, json!({"sql": "select 1"})))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(res.headers()[header::WWW_AUTHENTICATE], "Bearer");
        }
        // Impersonation is refused.
        let (status, body) = call(
            router(st.clone()),
            req(Some("t1"), json!({"sql": "select 1", "user": "bob"})),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        // The principal is the user; `user` may be omitted or match.
        for body in [
            json!({"sql": "select 1"}),
            json!({"sql": "select 1", "user": "ana"}),
        ] {
            let (status, out) = call(router(st.clone()), req(Some("t1"), body)).await;
            assert_eq!(status, StatusCode::OK, "{out}");
        }
        // Resource groups: bound groups pass, others 403, unknown 400.
        for (group, status) in [
            ("default", StatusCode::OK),
            ("batch", StatusCode::FORBIDDEN),
            ("nightly", StatusCode::BAD_REQUEST),
        ] {
            let body = json!({"sql": "select 1", "resource_group": group});
            let (got, out) = call(router(st.clone()), req(Some("t1"), body)).await;
            assert_eq!(got, status, "{group}: {out}");
        }
        let users: Vec<(String, String)> = st
            .admission
            .registry()
            .snapshot()
            .into_iter()
            .map(|q| (q.user, q.resource_group))
            .collect();
        let ana = |g: &str| ("ana".to_owned(), g.to_owned());
        assert_eq!(users, [ana("default"), ana("default"), ana("default")]);
        let (_, m) = call(router(st), get_req("/metrics")).await;
        assert!(
            m.contains("plimsoll_query_errors_total{code=\"unauthorized\"} 2"),
            "{m}"
        );
        assert!(
            m.contains("plimsoll_query_errors_total{code=\"forbidden\"} 2"),
            "{m}"
        );
    }

    #[tokio::test]
    async fn latency_histograms_split_by_phase() {
        let st = default_state();
        for sql in ["select 1", "s3 down"] {
            call(
                router(st.clone()),
                sql_req(json!({"sql": sql, "user": "ana"})),
            )
            .await;
        }
        let (_, m) = call(router(st), get_req("/metrics")).await;
        // Both reached the engine; only the success has plan/execute/serialize.
        for line in [
            "plimsoll_query_duration_seconds_count{phase=\"queue\"} 2",
            "plimsoll_query_duration_seconds_count{phase=\"total\"} 2",
            "plimsoll_query_duration_seconds_count{phase=\"planning\"} 1",
            "plimsoll_query_duration_seconds_count{phase=\"serialization\"} 1",
            "plimsoll_query_duration_seconds_bucket{phase=\"total\",le=\"+Inf\"} 2",
        ] {
            assert!(m.contains(line), "missing {line}\n{m}");
        }
    }

    #[tokio::test]
    async fn error_contract_carries_class_retryability_and_metrics() {
        let st = default_state();
        let cases = [
            (
                "deletes",
                StatusCode::UNPROCESSABLE_ENTITY,
                "unsupported_feature",
                false,
            ),
            (
                "s3 down",
                StatusCode::BAD_GATEWAY,
                "dependency_failed",
                true,
            ),
        ];
        for (sql, status, code, retryable) in cases {
            let (got, body) = call(
                router(st.clone()),
                sql_req(json!({"sql": sql, "user": "ana"})),
            )
            .await;
            assert_eq!(got, status, "{body}");
            let v: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(v["error"]["code"], code);
            assert_eq!(v["error"]["retryable"], retryable);
        }
        let (_, body) = call(
            router(st.clone()),
            sql_req(json!({"sql": "s3 down", "user": "ana"})),
        )
        .await;
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["error"]["dependency"], "object_store");

        let req = Request::get("/metrics").body(Body::empty()).unwrap();
        let (_, metrics) = call(router(st), req).await;
        assert!(
            metrics.contains("plimsoll_query_errors_total{code=\"dependency_failed\"} 2"),
            "{metrics}"
        );
        assert!(metrics.contains("plimsoll_query_errors_total{code=\"unsupported_feature\"} 1"));
    }

    #[tokio::test(start_paused = true)]
    async fn drain_unadvertises_but_keeps_serving_for_the_delay() {
        let st = default_state();
        let drain = tokio::spawn({
            let st = st.clone();
            async move { drain(&st, Duration::from_secs(5)).await }
        });
        tokio::task::yield_now().await;
        // Load balancers see "not ready" at once...
        assert_eq!(
            call(router(st.clone()), get_req("/readyz")).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        // ...but requests they still send are served during the delay.
        let (status, body) = call(
            router(st.clone()),
            sql_req(json!({"sql": "select 1", "user": "ana"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(!drain.is_finished());
        tokio::time::advance(Duration::from_secs(5)).await;
        drain.await.unwrap();
    }

    #[tokio::test]
    async fn sql_rejected_when_not_ready() {
        let st = default_state();
        st.serving.store(false, Ordering::Relaxed);
        let (status, body) = call(
            router(st),
            sql_req(json!({"sql": "select 1", "user": "ana"})),
        )
        .await;
        assert_eq!(
            (status, error_code(&body).as_str()),
            (StatusCode::SERVICE_UNAVAILABLE, "not_ready")
        );
    }

    #[tokio::test]
    async fn sql_timeout_fails_the_query_not_the_node() {
        let st = state(
            Limits {
                max_running: 1,
                max_queued: 0,
            },
            Duration::from_millis(20),
        );
        let (status, body) = call(
            router(st.clone()),
            sql_req(json!({"sql": "slow", "user": "ana"})),
        )
        .await;
        assert_eq!(
            (status, error_code(&body).as_str()),
            (StatusCode::GATEWAY_TIMEOUT, "query_timeout")
        );
        // Slot was freed: the next query runs.
        let (status, _) = call(
            router(st),
            sql_req(json!({"sql": "select 1", "user": "ana"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn sql_queue_full_returns_429() {
        let st = state(
            Limits {
                max_running: 1,
                max_queued: 0,
            },
            Duration::from_secs(5),
        );
        let slow = tokio::spawn(call(
            router(st.clone()),
            sql_req(json!({"sql": "slow", "user": "ana"})),
        ));
        while st.admission.load().0 == 0 {
            tokio::task::yield_now().await;
        }
        let (status, body) = call(
            router(st),
            sql_req(json!({"sql": "select 1", "user": "bob"})),
        )
        .await;
        assert_eq!(
            (status, error_code(&body).as_str()),
            (StatusCode::TOO_MANY_REQUESTS, "queue_full")
        );
        slow.abort();
    }
}
