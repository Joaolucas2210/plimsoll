//! The web console (ADR 0135): three static files compiled into the
//! binary, served at `/ui`. It holds no data and no session: it signs in
//! with a token and calls `/v1/sql` like any client, so authentication,
//! grants, admission and logs are the API's.

use axum::Router;
use axum::http::header;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;

const INDEX: &str = include_str!("../ui/index.html");
const SCRIPT: &str = include_str!("../ui/app.js");
const STYLE: &str = include_str!("../ui/app.css");

/// Only this origin's own script and styles run; the page cannot be
/// framed, and types are not sniffed.
const POLICY: &str = "default-src 'none'; script-src 'self'; style-src 'self'; \
                      connect-src 'self'; img-src 'self'; base-uri 'none'; \
                      form-action 'self'; frame-ancestors 'none'";

fn asset(content_type: &'static str, body: &'static str) -> Response {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CONTENT_SECURITY_POLICY, POLICY),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::REFERRER_POLICY, "no-referrer"),
            // Small, and changes with each release: always revalidated.
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response()
}

pub fn routes<S: Clone + Send + Sync + 'static>() -> Router<S> {
    Router::new()
        .route("/", get(|| async { Redirect::temporary("/ui") }))
        .route(
            "/ui",
            get(|| async { asset("text/html; charset=utf-8", INDEX) }),
        )
        .route(
            "/ui/app.js",
            get(|| async { asset("text/javascript; charset=utf-8", SCRIPT) }),
        )
        .route(
            "/ui/app.css",
            get(|| async { asset("text/css; charset=utf-8", STYLE) }),
        )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::*;

    async fn get_path(path: &str) -> Response {
        routes::<()>()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn serves_the_console_with_a_strict_policy() {
        for (path, kind, marker) in [
            ("/ui", "text/html", "<script src=\"/ui/app.js\">"),
            ("/ui/app.js", "text/javascript", "/v1/sql"),
            ("/ui/app.css", "text/css", "--accent"),
        ] {
            let res = get_path(path).await;
            assert_eq!(res.status(), StatusCode::OK, "{path}");
            let h = res.headers();
            assert!(h[header::CONTENT_TYPE].to_str().unwrap().starts_with(kind));
            let policy = h[header::CONTENT_SECURITY_POLICY].to_str().unwrap();
            assert!(policy.contains("script-src 'self'") && !policy.contains("unsafe"));
            assert_eq!(h[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
            let body = axum::body::to_bytes(res.into_body(), 1 << 20)
                .await
                .unwrap();
            assert!(String::from_utf8_lossy(&body).contains(marker), "{path}");
        }
        let root = get_path("/").await;
        assert_eq!(root.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(root.headers()[header::LOCATION], "/ui");
    }

    /// No inline script or style: the policy would block it.
    #[test]
    fn the_page_has_nothing_inline() {
        assert!(
            !INDEX.contains("<script>") && !INDEX.contains("<style") && !INDEX.contains(" style=")
        );
        assert!(!INDEX.contains(" onclick=") && !INDEX.contains(" onload="));
    }
}
