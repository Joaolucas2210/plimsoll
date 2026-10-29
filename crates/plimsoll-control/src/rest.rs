//! Iceberg REST catalog over HTTP (Polaris, the Apache REST fixture, ...).
//!
//! Follows the spec's handshake: `GET /v1/config` decides the URL `prefix`
//! and namespace separator; every other call goes to `/v1/{prefix}/...`.
//! See ADR 0008.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, RequestBuilder, StatusCode};
use serde_json::Value;
use tokio::sync::RwLock;

use crate::catalog::{
    Catalog, CatalogAuth, CatalogConfig, CatalogError, LoadedTable, Namespace, TableIdent, encode,
};

const TIMEOUT: Duration = Duration::from_secs(30);

pub struct RestCatalog {
    http: Client,
    cfg: CatalogConfig,
    /// `{uri}/v1` or `{uri}/v1/{prefix}`.
    base: String,
    /// Already URL-encoded; the spec default is `%1F`.
    separator: String,
    /// Current bearer token (static, or from client credentials).
    token: RwLock<Option<String>>,
}

impl RestCatalog {
    /// Authenticates and runs the `/v1/config` handshake. Fails fast so a
    /// misconfigured catalog stops the server at boot.
    pub async fn connect(cfg: CatalogConfig) -> Result<Self, CatalogError> {
        let http = Client::builder()
            .timeout(TIMEOUT)
            .build()
            .map_err(|e| CatalogError::Http(e.to_string()))?;
        let mut catalog = Self {
            http,
            base: format!("{}/v1", cfg.uri),
            separator: "%1F".to_owned(),
            token: RwLock::new(match &cfg.auth {
                CatalogAuth::Token(t) => Some(t.clone()),
                _ => None,
            }),
            cfg,
        };
        catalog.refresh_token().await?;
        let mut url = format!("{}/v1/config", catalog.cfg.uri);
        if let Some(w) = &catalog.cfg.warehouse {
            url = format!("{url}?warehouse={}", encode(w));
        }
        let config = catalog.get(&url).await?.1;
        // Server overrides win over defaults (spec: ConfigResponse).
        let setting = |key: &str| {
            config["overrides"][key]
                .as_str()
                .or_else(|| config["defaults"][key].as_str())
                .map(str::to_owned)
        };
        if let Some(prefix) = setting("prefix") {
            catalog.base = format!("{}/v1/{}", catalog.cfg.uri, encode(&prefix));
        }
        if let Some(sep) = setting("namespace-separator") {
            catalog.separator = sep;
        }
        Ok(catalog)
    }

    fn namespace_path(&self, ns: &Namespace) -> String {
        ns.0.iter()
            .map(|level| encode(level))
            .collect::<Vec<_>>()
            .join(&self.separator)
    }

    /// Client credentials -> bearer token. No-op for other auth modes.
    async fn refresh_token(&self) -> Result<(), CatalogError> {
        let CatalogAuth::ClientCredentials {
            client_id,
            client_secret,
            scope,
        } = &self.cfg.auth
        else {
            return Ok(());
        };
        // ponytail: the spec's (deprecated) `/v1/oauth/tokens` endpoint, which
        // Polaris serves. Upgrade path: an `oauth2-server-uri` setting for
        // external IdPs.
        let form = format!(
            "grant_type=client_credentials&client_id={}&client_secret={}&scope={}",
            encode(client_id),
            encode(client_secret),
            encode(scope)
        );
        let resp = self
            .http
            .post(format!("{}/v1/oauth/tokens", self.cfg.uri))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form)
            .send()
            .await
            .map_err(|e| CatalogError::Http(e.to_string()))?;
        let status = resp.status();
        let body: Value = json_value(resp)
            .await
            .map_err(|e| CatalogError::Auth(format!("{status}: {e}")))?;
        let token = body["access_token"]
            .as_str()
            .filter(|_| status.is_success())
            .ok_or_else(|| CatalogError::Auth(format!("{status}: {}", error_message(&body))))?;
        *self.token.write().await = Some(token.to_owned());
        Ok(())
    }

    fn authorized(&self, req: RequestBuilder, token: Option<&str>) -> RequestBuilder {
        match token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }

    /// GET returning `(status, json)`; a 401 renews client credentials once.
    async fn get(&self, url: &str) -> Result<(StatusCode, Value), CatalogError> {
        let mut retried = false;
        loop {
            let token = self.token.read().await.clone();
            let resp = self
                .authorized(self.http.get(url), token.as_deref())
                .send()
                .await
                .map_err(|e| CatalogError::Http(e.to_string()))?;
            let status = resp.status();
            if status == StatusCode::UNAUTHORIZED
                && !retried
                && matches!(self.cfg.auth, CatalogAuth::ClientCredentials { .. })
            {
                retried = true;
                self.refresh_token().await?;
                continue;
            }
            let body = json_value(resp).await?;
            return match status {
                s if s.is_success() || s == StatusCode::NOT_FOUND => Ok((s, body)),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(CatalogError::Auth(
                    format!("{status}: {}", error_message(&body)),
                )),
                s => Err(CatalogError::Status {
                    status: s.as_u16(),
                    message: error_message(&body),
                }),
            };
        }
    }

    /// Follows `next-page-token`, collecting `field` from every page.
    async fn get_all(
        &self,
        url: &str,
        field: &str,
        not_found: impl Fn() -> CatalogError,
    ) -> Result<Vec<Value>, CatalogError> {
        let mut out = Vec::new();
        let mut page: Option<String> = None;
        loop {
            let page_url = match &page {
                Some(token) => format!("{url}?pageToken={}", encode(token)),
                None => url.to_owned(),
            };
            let (status, body) = self.get(&page_url).await?;
            if status == StatusCode::NOT_FOUND {
                return Err(not_found());
            }
            let items = body[field]
                .as_array()
                .ok_or_else(|| CatalogError::Protocol(format!("missing `{field}` in {url}")))?;
            out.extend(items.iter().cloned());
            match body["next-page-token"].as_str() {
                Some(t) if !t.is_empty() => page = Some(t.to_owned()),
                _ => return Ok(out),
            }
        }
    }
}

#[async_trait]
impl Catalog for RestCatalog {
    async fn list_namespaces(&self) -> Result<Vec<Namespace>, CatalogError> {
        let url = format!("{}/namespaces", self.base);
        let items = self
            .get_all(&url, "namespaces", || {
                CatalogError::Protocol("namespaces endpoint not found".into())
            })
            .await?;
        items.iter().map(namespace).collect()
    }

    async fn list_tables(&self, ns: &Namespace) -> Result<Vec<TableIdent>, CatalogError> {
        let url = format!(
            "{}/namespaces/{}/tables",
            self.base,
            self.namespace_path(ns)
        );
        let items = self
            .get_all(&url, "identifiers", || {
                CatalogError::NamespaceNotFound(ns.clone())
            })
            .await?;
        items
            .iter()
            .map(|id| {
                let name = id["name"]
                    .as_str()
                    .ok_or_else(|| CatalogError::Protocol("identifier without name".into()))?;
                Ok(TableIdent::new(namespace(&id["namespace"])?, name))
            })
            .collect()
    }

    async fn load_table(&self, ident: &TableIdent) -> Result<LoadedTable, CatalogError> {
        let url = format!(
            "{}/namespaces/{}/tables/{}",
            self.base,
            self.namespace_path(&ident.namespace),
            encode(&ident.name)
        );
        let (status, mut body) = self.get(&url).await?;
        if status == StatusCode::NOT_FOUND {
            return Err(
                if body["error"]["type"]
                    .as_str()
                    .is_some_and(|t| t.contains("Namespace"))
                {
                    CatalogError::NamespaceNotFound(ident.namespace.clone())
                } else {
                    CatalogError::TableNotFound(ident.clone())
                },
            );
        }
        Ok(LoadedTable {
            ident: ident.clone(),
            metadata_location: body["metadata-location"].as_str().map(str::to_owned),
            metadata: body["metadata"].take(),
        })
    }
}

fn namespace(v: &Value) -> Result<Namespace, CatalogError> {
    v.as_array()
        .and_then(|levels| {
            levels
                .iter()
                .map(|l| l.as_str().map(str::to_owned))
                .collect()
        })
        .map(Namespace)
        .ok_or_else(|| CatalogError::Protocol(format!("invalid namespace {v}")))
}

/// Spec error body: `{"error": {"message", "type", "code"}}`.
fn error_message(body: &Value) -> String {
    body["error"]["message"]
        .as_str()
        .or_else(|| body["error_description"].as_str()) // OAuth errors
        .unwrap_or("no error message")
        .to_owned()
}

/// Empty bodies (e.g. a bare 404) become `null` instead of an error.
async fn json_value(resp: reqwest::Response) -> Result<Value, CatalogError> {
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| CatalogError::Http(e.to_string()))?;
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&bytes).map_err(|e| CatalogError::Protocol(e.to_string()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, Method, Uri};
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Spec-shaped stub. Matches on the raw (still encoded) URI so the test
    /// pins the exact wire format. Token `t1` is "expired" to force a renewal.
    async fn stub() -> (String, Arc<AtomicUsize>) {
        let tokens = Arc::new(AtomicUsize::new(0));
        let issued = tokens.clone();
        let app = axum::Router::new().fallback(
            move |method: Method, uri: Uri, headers: HeaderMap| {
                let issued = issued.clone();
                async move {
                    let path = uri.path_and_query().map(|p| p.as_str()).unwrap_or("");
                    let auth = headers.get("authorization").and_then(|v| v.to_str().ok());
                    let (status, body) = match (method, path) {
                        (Method::POST, "/api/v1/oauth/tokens") => {
                            let n = issued.fetch_add(1, Ordering::SeqCst) + 1;
                            (200, json!({"access_token": format!("t{n}"), "token_type": "bearer"}))
                        }
                        (_, "/api/v1/config?warehouse=lake") => {
                            (200, json!({"defaults": {}, "overrides": {"prefix": "lake prefix"}}))
                        }
                        _ if auth != Some("Bearer t2") => {
                            (401, json!({"error": {"message": "expired", "code": 401}}))
                        }
                        (_, "/api/v1/lake%20prefix/namespaces") => (
                            200,
                            json!({"namespaces": [["bronze"]], "next-page-token": "p 2"}),
                        ),
                        (_, "/api/v1/lake%20prefix/namespaces?pageToken=p%202") => {
                            (200, json!({"namespaces": [["gold", "fin"]], "next-page-token": null}))
                        }
                        (_, "/api/v1/lake%20prefix/namespaces/gold%1Ffin/tables") => (
                            200,
                            json!({"identifiers": [{"namespace": ["gold", "fin"], "name": "orders"}]}),
                        ),
                        (_, "/api/v1/lake%20prefix/namespaces/bronze/tables/trips") => (
                            200,
                            json!({"metadata-location": "file:///wh/m.metadata.json",
                                   "metadata": {"format-version": 2}}),
                        ),
                        (_, "/api/v1/lake%20prefix/namespaces/nope/tables") => (
                            404,
                            json!({"error": {"type": "NoSuchNamespaceException", "code": 404}}),
                        ),
                        _ => (404, json!({"error": {"type": "NoSuchTableException", "code": 404}})),
                    };
                    (axum::http::StatusCode::from_u16(status).unwrap(), axum::Json(body))
                }
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        (format!("http://{addr}/api"), tokens)
    }

    fn config(uri: String) -> CatalogConfig {
        CatalogConfig {
            uri,
            warehouse: Some("lake".into()),
            auth: CatalogAuth::ClientCredentials {
                client_id: "root".into(),
                client_secret: "s3cret".into(),
                scope: "PRINCIPAL_ROLE:ALL".into(),
            },
        }
    }

    #[tokio::test]
    async fn handshake_pagination_auth_renewal_and_not_found() {
        let (uri, tokens) = stub().await;
        let cat = RestCatalog::connect(config(uri)).await.unwrap();

        // t1 is rejected, renewed to t2 once, then pages are followed.
        let namespaces = cat.list_namespaces().await.unwrap();
        assert_eq!(
            namespaces,
            [
                Namespace::new(&["bronze"]),
                Namespace::new(&["gold", "fin"])
            ]
        );
        assert_eq!(tokens.load(Ordering::SeqCst), 2);

        let tables = cat
            .list_tables(&Namespace::new(&["gold", "fin"]))
            .await
            .unwrap();
        assert_eq!(tables[0].to_string(), "gold.fin.orders");

        let bronze = Namespace::new(&["bronze"]);
        let t = cat
            .load_table(&TableIdent::new(bronze.clone(), "trips"))
            .await
            .unwrap();
        assert_eq!(
            t.metadata_location.as_deref(),
            Some("file:///wh/m.metadata.json")
        );
        assert_eq!(t.metadata["format-version"], 2);

        assert!(matches!(
            cat.load_table(&TableIdent::new(bronze, "missing")).await,
            Err(CatalogError::TableNotFound(_))
        ));
        assert!(matches!(
            cat.list_tables(&Namespace::new(&["nope"])).await,
            Err(CatalogError::NamespaceNotFound(_))
        ));
    }

    #[tokio::test]
    async fn bad_static_token_is_an_auth_error() {
        let (uri, _) = stub().await;
        let mut cfg = config(uri);
        cfg.auth = CatalogAuth::Token("wrong".into());
        let cat = RestCatalog::connect(cfg).await.unwrap();
        assert!(matches!(
            cat.list_namespaces().await,
            Err(CatalogError::Auth(_))
        ));
    }

    /// Against a real REST catalog, e.g. `apache/iceberg-rest-fixture` with a
    /// table written by `scripts/iceberg_fixture.py http://...`.
    #[tokio::test]
    #[ignore = "needs PLIMSOLL_CATALOG_URI pointing at a live Iceberg REST catalog"]
    async fn live_rest_catalog() {
        let Some(cfg) = CatalogConfig::from_lookup(|k| std::env::var(k).ok()).unwrap() else {
            return;
        };
        let cat = RestCatalog::connect(cfg).await.unwrap();
        let bronze = Namespace::new(&["bronze"]);
        assert!(cat.list_namespaces().await.unwrap().contains(&bronze));
        let t = cat
            .load_table(&TableIdent::new(bronze, "trips"))
            .await
            .unwrap();
        assert!(t.metadata_location.unwrap().ends_with(".metadata.json"));
    }
}
