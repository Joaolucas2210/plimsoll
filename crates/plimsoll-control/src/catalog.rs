//! Iceberg REST catalog client contract (Polaris-compatible).
//!
//! The catalog is control plane: it answers "which tables exist and where is
//! their metadata", never reads data files. Endpoints follow the Iceberg REST
//! OpenAPI spec (`/v1/{prefix}/namespaces/...`).
//!
//! ponytail: only the trait, the config, URL building and an in-memory
//! implementation exist. The HTTP implementation is deliberately absent to keep
//! `reqwest` + TLS out of the dependency tree until the real Iceberg
//! TableProvider lands (see ROADMAP "next PR"). Upgrade path: a `RestCatalog`
//! that issues `GET` on the URLs built by [`CatalogConfig`] with
//! [`CatalogConfig::bearer_token`], and deserializes the JSON bodies into the
//! types below (shapes already match the spec's response fields).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;

/// Env var names. Secrets (the token) are read from env only, never from files
/// or CLI args, and never printed (see the `Debug` impl).
pub const ENV_URI: &str = "PLIMSOLL_CATALOG_URI";
pub const ENV_WAREHOUSE: &str = "PLIMSOLL_CATALOG_WAREHOUSE";
pub const ENV_TOKEN: &str = "PLIMSOLL_CATALOG_TOKEN";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CatalogError {
    #[error("invalid catalog config: {0}")]
    Config(String),
    #[error("namespace not found: {0}")]
    NamespaceNotFound(Namespace),
    #[error("table not found: {0}")]
    TableNotFound(TableIdent),
}

/// Multi-level namespace, e.g. `["gold", "finance"]`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Namespace(pub Vec<String>);

impl Namespace {
    pub fn new(levels: &[&str]) -> Self {
        Self(levels.iter().map(|s| (*s).to_owned()).collect())
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.join("."))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableIdent {
    pub namespace: Namespace,
    pub name: String,
}

impl TableIdent {
    pub fn new(namespace: Namespace, name: &str) -> Self {
        Self {
            namespace,
            name: name.to_owned(),
        }
    }
}

impl fmt::Display for TableIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

/// Result of `loadTable`.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadedTable {
    pub ident: TableIdent,
    /// `metadata-location` from the spec: the current metadata JSON file.
    pub metadata_location: Option<String>,
    /// ponytail: raw Iceberg table metadata JSON, untyped until the Iceberg
    /// TableProvider needs specific fields (schema, snapshots, partition spec).
    pub metadata: serde_json::Value,
}

/// The one abstraction in this module: real REST client vs in-memory stub.
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Top-level namespaces. ponytail: no `parent` filter yet.
    async fn list_namespaces(&self) -> Result<Vec<Namespace>, CatalogError>;
    async fn list_tables(&self, namespace: &Namespace) -> Result<Vec<TableIdent>, CatalogError>;
    async fn load_table(&self, ident: &TableIdent) -> Result<LoadedTable, CatalogError>;
}

/// REST catalog connection settings.
#[derive(Clone, PartialEq, Eq)]
pub struct CatalogConfig {
    /// Base URI without the `/v1` suffix, e.g. `http://polaris:8181/api/catalog`.
    pub uri: String,
    /// Warehouse / Polaris catalog name. Used as the URL `prefix`.
    /// ponytail: the spec lets `GET /v1/config` override the prefix; Polaris
    /// returns the catalog name, which is what we use directly.
    pub warehouse: String,
    pub bearer_token: Option<String>,
}

impl fmt::Debug for CatalogConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogConfig")
            .field("uri", &self.uri)
            .field("warehouse", &self.warehouse)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl CatalogConfig {
    /// Reads config through `lookup` (pass `|k| std::env::var(k).ok()` in
    /// production). Returns `Ok(None)` when no catalog URI is configured;
    /// fails fast when the URI is set but the rest is invalid.
    pub fn from_lookup(
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<Self>, CatalogError> {
        let Some(uri) = lookup(ENV_URI) else {
            return Ok(None);
        };
        if !(uri.starts_with("http://") || uri.starts_with("https://")) {
            return Err(CatalogError::Config(format!(
                "{ENV_URI} must start with http:// or https://"
            )));
        }
        let warehouse = lookup(ENV_WAREHOUSE)
            .filter(|w| !w.is_empty())
            .ok_or_else(|| CatalogError::Config(format!("{ENV_WAREHOUSE} is required")))?;
        Ok(Some(Self {
            uri: uri.trim_end_matches('/').to_owned(),
            warehouse,
            bearer_token: lookup(ENV_TOKEN).filter(|t| !t.is_empty()),
        }))
    }

    /// `GET /v1/config?warehouse=...`
    pub fn config_url(&self) -> String {
        format!(
            "{}/v1/config?warehouse={}",
            self.uri,
            encode(&self.warehouse)
        )
    }

    /// `GET /v1/{prefix}/namespaces`
    pub fn namespaces_url(&self) -> String {
        format!("{}/v1/{}/namespaces", self.uri, encode(&self.warehouse))
    }

    /// `GET /v1/{prefix}/namespaces/{namespace}/tables`
    pub fn tables_url(&self, ns: &Namespace) -> String {
        format!("{}/{}/tables", self.namespaces_url(), encode_namespace(ns))
    }

    /// `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`
    pub fn table_url(&self, ident: &TableIdent) -> String {
        format!(
            "{}/{}",
            self.tables_url(&ident.namespace),
            encode(&ident.name)
        )
    }
}

/// Multi-level namespaces are joined by the unit separator (0x1F) per spec.
fn encode_namespace(ns: &Namespace) -> String {
    encode(&ns.0.join("\u{1f}"))
}

/// Percent-encodes everything outside RFC 3986 "unreserved".
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// In-memory catalog for tests and local development.
#[derive(Default)]
pub struct InMemoryCatalog {
    namespaces: Mutex<BTreeMap<Namespace, BTreeMap<String, LoadedTable>>>,
}

impl InMemoryCatalog {
    pub fn create_namespace(&self, ns: Namespace) {
        self.lock().entry(ns).or_default();
    }

    pub fn put_table(&self, ident: TableIdent, metadata: serde_json::Value) {
        let table = LoadedTable {
            ident: ident.clone(),
            metadata_location: None,
            metadata,
        };
        self.lock()
            .entry(ident.namespace)
            .or_default()
            .insert(ident.name, table);
    }

    fn lock(
        &self,
    ) -> std::sync::MutexGuard<'_, BTreeMap<Namespace, BTreeMap<String, LoadedTable>>> {
        self.namespaces
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[async_trait]
impl Catalog for InMemoryCatalog {
    async fn list_namespaces(&self) -> Result<Vec<Namespace>, CatalogError> {
        Ok(self.lock().keys().cloned().collect())
    }

    async fn list_tables(&self, namespace: &Namespace) -> Result<Vec<TableIdent>, CatalogError> {
        let guard = self.lock();
        let tables = guard
            .get(namespace)
            .ok_or_else(|| CatalogError::NamespaceNotFound(namespace.clone()))?;
        Ok(tables.values().map(|t| t.ident.clone()).collect())
    }

    async fn load_table(&self, ident: &TableIdent) -> Result<LoadedTable, CatalogError> {
        self.lock()
            .get(&ident.namespace)
            .and_then(|tables| tables.get(&ident.name))
            .cloned()
            .ok_or_else(|| CatalogError::TableNotFound(ident.clone()))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |k| map.get(k).cloned()
    }

    #[test]
    fn config_absent_is_none_and_invalid_fails_fast() {
        assert_eq!(CatalogConfig::from_lookup(env(&[])).unwrap(), None);
        assert!(CatalogConfig::from_lookup(env(&[(ENV_URI, "polaris:8181")])).is_err());
        assert!(CatalogConfig::from_lookup(env(&[(ENV_URI, "http://p")])).is_err());
    }

    #[test]
    fn builds_spec_urls_and_redacts_token() {
        let cfg = CatalogConfig::from_lookup(env(&[
            (ENV_URI, "http://polaris:8181/api/catalog/"),
            (ENV_WAREHOUSE, "lake"),
            (ENV_TOKEN, "s3cret"),
        ]))
        .unwrap()
        .unwrap();
        let ident = TableIdent::new(Namespace::new(&["gold", "fin ance"]), "orders");
        assert_eq!(
            cfg.table_url(&ident),
            "http://polaris:8181/api/catalog/v1/lake/namespaces/gold%1Ffin%20ance/tables/orders"
        );
        assert_eq!(
            cfg.config_url(),
            "http://polaris:8181/api/catalog/v1/config?warehouse=lake"
        );
        assert!(!format!("{cfg:?}").contains("s3cret"));
    }

    #[tokio::test]
    async fn in_memory_catalog_lists_and_loads() {
        let cat = InMemoryCatalog::default();
        let bronze = Namespace::new(&["bronze"]);
        cat.create_namespace(Namespace::new(&["gold"]));
        let ident = TableIdent::new(bronze.clone(), "events");
        cat.put_table(ident.clone(), serde_json::json!({"format-version": 2}));

        assert_eq!(cat.list_namespaces().await.unwrap().len(), 2);
        assert_eq!(cat.list_tables(&bronze).await.unwrap(), vec![ident.clone()]);
        assert_eq!(
            cat.load_table(&ident).await.unwrap().metadata["format-version"],
            2
        );
        assert!(matches!(
            cat.load_table(&TableIdent::new(bronze, "nope")).await,
            Err(CatalogError::TableNotFound(_))
        ));
        assert!(matches!(
            cat.list_tables(&Namespace::new(&["silver"])).await,
            Err(CatalogError::NamespaceNotFound(_))
        ));
    }
}
