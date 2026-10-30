//! HTTP routes. Every handler is a thin translation layer: governance lives
//! in `plimsoll-control`, execution behind [`QueryEngine`].

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use plimsoll_control::governance::{Admission, AdmissionError, GovernedError, QueryRequest};
use serde::Deserialize;
use serde_json::json;

use crate::auth::Auth;
use crate::engine::{EngineError, QueryEngine, QueryOutput};
use crate::metrics::{self, Latency, PoolStatsProvider};
use futures::StreamExt;

pub struct AppState {
    pub engine: Arc<dyn QueryEngine>,
    pub admission: Admission,
    pub pool_stats: PoolStatsProvider,
    pub query_timeout: Duration,
    /// False while starting and while draining after SIGTERM.
    pub ready: AtomicBool,
    pub started: Instant,
    /// `/v1/sql` errors by `code`, for `plimsoll_query_errors_total`.
    pub errors: ErrorCounts,
    pub latency: Latency,
    pub auth: Auth,
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
        let mut body = json!({
            "code": self.code,
            "message": message.to_string(),
            "retryable": self.retryable,
            "query_id": query_id,
        });
        if let Some(d) = self.dependency {
            body["dependency"] = json!(d);
        }
        (self.status, Json(json!({ "error": body }))).into_response()
    }
}

async fn sql(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Result<Json<SqlRequest>, JsonRejection>,
) -> Response {
    // Authenticate first: an anonymous caller learns nothing else (ADR 0021).
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let principal = match st.auth.principal(authorization) {
        Ok(p) => p,
        Err(e) => {
            let mut r = ApiError::new(StatusCode::UNAUTHORIZED, "unauthorized", false)
                .respond(&st, e, None);
            r.headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            return r;
        }
    };
    if !st.ready.load(Ordering::Relaxed) {
        return ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "not_ready", true).respond(
            &st,
            "server is not accepting queries",
            None,
        );
    }
    let req = match body {
        Ok(Json(r)) => r,
        Err(e) => {
            return ApiError::new(StatusCode::BAD_REQUEST, "invalid_request", false).respond(
                &st,
                e.body_text(),
                None,
            );
        }
    };
    let user = match (principal, req.user) {
        (Some(p), Some(u)) if u != p => {
            return ApiError::new(StatusCode::FORBIDDEN, "forbidden", false).respond(
                &st,
                format!("authenticated as {p:?}, cannot run as {u:?}"),
                None,
            );
        }
        (Some(p), _) => p.to_owned(),
        (None, Some(u)) => u,
        (None, None) => String::new(),
    };
    if req.sql.trim().is_empty() || user.trim().is_empty() {
        return ApiError::new(StatusCode::BAD_REQUEST, "invalid_request", false).respond(
            &st,
            "`sql` and `user` must be non-empty",
            None,
        );
    }
    let request = QueryRequest {
        user,
        resource_group: req.resource_group.unwrap_or_else(|| "default".to_owned()),
        sql: req.sql,
    };
    let sql_text = request.sql.clone();
    let viewer = st.auth.viewer(&request.user);
    let assigned = AtomicU64::new(0);
    let arrived = Instant::now();
    let result = st
        .admission
        .execute(request, st.query_timeout, |id| {
            assigned.store(id, Ordering::Relaxed);
            // Called once a slot is granted: everything before was queueing.
            st.latency.queue.observe(arrived.elapsed());
            st.engine.execute(&sql_text, viewer)
        })
        .await;
    let query_id = Some(assigned.load(Ordering::Relaxed)).filter(|id| *id != 0);
    if query_id.is_some() {
        st.latency.total.observe(arrived.elapsed());
    }

    match result {
        Ok(out) => {
            st.latency.planning.observe(out.phases.planning);
            st.latency.execution.observe(out.phases.execution);
            st.latency.serialization.observe(out.phases.serialization);
            json_body(query_id, out)
        }
        Err(e) => ApiError::of(&e).respond(&st, &e, query_id),
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
    use plimsoll_control::governance::{AdmissionConfig, Limits, QueryRegistry};
    use tower::ServiceExt;

    struct FakeEngine;

    #[async_trait]
    impl QueryEngine for FakeEngine {
        async fn execute(&self, sql: &str, _viewer: Viewer) -> Result<QueryOutput, EngineError> {
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
        };
        Arc::new(AppState {
            engine: Arc::new(FakeEngine),
            admission: Admission::new(cfg, Arc::new(QueryRegistry::new(100))),
            pool_stats: Arc::new(|| PoolStats {
                used_bytes: 5,
                limit_bytes: 1024,
                cache_reserve_bytes: 0,
                spill_bytes_total: 0,
            }),
            query_timeout: timeout,
            ready: AtomicBool::new(true),
            started: Instant::now(),
            errors: ErrorCounts::default(),
            latency: Latency::default(),
            auth: Auth::Disabled,
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
        st.auth = Auth::from_lookup(&|k| match k {
            "PLIMSOLL_AUTH_TOKENS" => Some(tokens.clone()),
            "PLIMSOLL_AUTH_OPERATORS" => Some("ops".into()),
            _ => None,
        })
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
    async fn sql_requires_a_bearer_token_and_runs_as_its_principal() {
        let mut st = Arc::into_inner(default_state()).unwrap();
        st.auth = Auth::from_lookup(&|k| {
            (k == "PLIMSOLL_AUTH_TOKENS").then(|| {
                let hex: String = crate::auth::token_digest("t1")
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect();
                format!("ana:sha256:{hex}")
            })
        })
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
        let users: Vec<String> = st
            .admission
            .registry()
            .snapshot()
            .into_iter()
            .map(|q| q.user)
            .collect();
        assert_eq!(users, ["ana", "ana"]);
        let (_, m) = call(router(st), get_req("/metrics")).await;
        assert!(
            m.contains("plimsoll_query_errors_total{code=\"unauthorized\"} 2"),
            "{m}"
        );
        assert!(
            m.contains("plimsoll_query_errors_total{code=\"forbidden\"} 1"),
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

    #[tokio::test]
    async fn sql_rejected_when_not_ready() {
        let st = default_state();
        st.ready.store(false, Ordering::Relaxed);
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
