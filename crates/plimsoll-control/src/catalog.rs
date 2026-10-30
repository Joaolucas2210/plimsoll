//! Iceberg REST catalog client contract (Polaris-compatible).
//!
//! The catalog is control plane: it answers "which tables exist and where is
//! their metadata", never reads data files. Endpoints follow the Iceberg REST
//! OpenAPI spec (`/v1/{prefix}/namespaces/...`).
//!
//! [`crate::rest::RestCatalog`] is the HTTP implementation; [`InMemoryCatalog`]
//! serves tests and local development.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;

/// Env var names. Secrets (the token) are read from env only, never from files
/// or CLI args, and never printed (see the `Debug` impl).
pub const ENV_URI: &str = "PLIMSOLL_CATALOG_URI";
pub const ENV_WAREHOUSE: &str = "PLIMSOLL_CATALOG_WAREHOUSE";
pub const ENV_TOKEN: &str = "PLIMSOLL_CATALOG_TOKEN";
/// OAuth2 client credentials as `client_id:client_secret` (Polaris).
pub const ENV_CREDENTIAL: &str = "PLIMSOLL_CATALOG_CREDENTIAL";
pub const ENV_SCOPE: &str = "PLIMSOLL_CATALOG_SCOPE";
/// `true` to ask for catalog-vended storage credentials on `loadTable`.
pub const ENV_VENDED: &str = "PLIMSOLL_CATALOG_VENDED_CREDENTIALS";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CatalogError {
    #[error("invalid catalog config: {0}")]
    Config(String),
    #[error("namespace not found: {0}")]
    NamespaceNotFound(Namespace),
    #[error("table not found: {0}")]
    TableNotFound(TableIdent),
    #[error("catalog unreachable: {0}")]
    Http(String),
    #[error("catalog authentication failed: {0}")]
    Auth(String),
    #[error("catalog returned {status}: {message}")]
    Status { status: u16, message: String },
    #[error("unexpected catalog response: {0}")]
    Protocol(String),
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
#[derive(Clone, PartialEq)]
pub struct LoadedTable {
    pub ident: TableIdent,
    /// `metadata-location` from the spec: the current metadata JSON file.
    pub metadata_location: Option<String>,
    /// ponytail: raw Iceberg table metadata JSON, untyped until the Iceberg
    /// TableProvider needs specific fields (schema, snapshots, partition spec).
    pub metadata: serde_json::Value,
    /// Storage (FileIO) properties for this table: the response `config`
    /// plus the best-matching `storage-credentials` entry. May hold vended
    /// secrets (`s3.secret-access-key`, ...): redacted in `Debug`.
    pub storage: BTreeMap<String, String>,
}

impl fmt::Debug for LoadedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadedTable")
            .field("ident", &self.ident)
            .field("metadata_location", &self.metadata_location)
            .field("storage_keys", &self.storage.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
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
    /// Sent as `?warehouse=` to `/v1/config` (Polaris: the catalog name).
    pub warehouse: Option<String>,
    pub auth: CatalogAuth,
    /// Send `X-Iceberg-Access-Delegation: vended-credentials` on `loadTable`.
    pub vended_credentials: bool,
}

#[derive(Clone, PartialEq, Eq)]
pub enum CatalogAuth {
    None,
    /// Static bearer token.
    Token(String),
    /// OAuth2 client-credentials exchanged at `{uri}/v1/oauth/tokens`.
    ClientCredentials {
        client_id: String,
        client_secret: String,
        scope: String,
    },
}

impl fmt::Debug for CatalogConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let auth = match &self.auth {
            CatalogAuth::None => "none".to_owned(),
            CatalogAuth::Token(_) => "token <redacted>".to_owned(),
            CatalogAuth::ClientCredentials {
                client_id, scope, ..
            } => format!("client {client_id} <redacted> scope {scope}"),
        };
        f.debug_struct("CatalogConfig")
            .field("uri", &self.uri)
            .field("warehouse", &self.warehouse)
            .field("auth", &auth)
            .field("vended_credentials", &self.vended_credentials)
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
        let get = |k| lookup(k).filter(|v: &String| !v.is_empty());
        let Some(uri) = get(ENV_URI) else {
            return Ok(None);
        };
        if !(uri.starts_with("http://") || uri.starts_with("https://")) {
            return Err(CatalogError::Config(format!(
                "{ENV_URI} must start with http:// or https://"
            )));
        }
        let auth = match (get(ENV_TOKEN), get(ENV_CREDENTIAL)) {
            (Some(_), Some(_)) => {
                return Err(CatalogError::Config(format!(
                    "set either {ENV_TOKEN} or {ENV_CREDENTIAL}, not both"
                )));
            }
            (Some(token), None) => CatalogAuth::Token(token),
            (None, Some(cred)) => {
                let (id, secret) = cred.split_once(':').ok_or_else(|| {
                    CatalogError::Config(format!(
                        "{ENV_CREDENTIAL} must be client_id:client_secret"
                    ))
                })?;
                CatalogAuth::ClientCredentials {
                    client_id: id.to_owned(),
                    client_secret: secret.to_owned(),
                    scope: get(ENV_SCOPE).unwrap_or_else(|| "PRINCIPAL_ROLE:ALL".to_owned()),
                }
            }
            (None, None) => CatalogAuth::None,
        };
        let vended_credentials = match get(ENV_VENDED).as_deref() {
            None | Some("false") => false,
            Some("true") => true,
            Some(v) => {
                return Err(CatalogError::Config(format!(
                    "{ENV_VENDED} must be true or false, got {v:?}"
                )));
            }
        };
        Ok(Some(Self {
            uri: uri.trim_end_matches('/').to_owned(),
            warehouse: get(ENV_WAREHOUSE),
            auth,
            vended_credentials,
        }))
    }
}

/// Percent-encodes everything outside RFC 3986 "unreserved".
pub(crate) fn encode(s: &str) -> String {
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

    pub fn put_table(
        &self,
        ident: TableIdent,
        metadata_location: Option<String>,
        metadata: serde_json::Value,
    ) {
        let table = LoadedTable {
            ident: ident.clone(),
            metadata_location,
            metadata,
            storage: BTreeMap::new(),
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
        assert!(CatalogConfig::from_lookup(env(&[(ENV_URI, "http://p")])).is_ok());
    }

    #[test]
    fn auth_modes_and_redaction() {
        let cfg = |pairs: &[(&str, &str)]| {
            let mut all = vec![(ENV_URI, "http://polaris:8181/api/catalog/")];
            all.extend_from_slice(pairs);
            CatalogConfig::from_lookup(env(&all))
        };
        let token = cfg(&[(ENV_TOKEN, "s3cret")]).unwrap().unwrap();
        assert_eq!(token.uri, "http://polaris:8181/api/catalog");
        assert!(!format!("{token:?}").contains("s3cret"));
        let client = cfg(&[(ENV_CREDENTIAL, "root:s3cret")]).unwrap().unwrap();
        assert!(
            matches!(&client.auth, CatalogAuth::ClientCredentials { scope, .. } if scope == "PRINCIPAL_ROLE:ALL")
        );
        assert!(!format!("{client:?}").contains("s3cret"));
        assert!(cfg(&[(ENV_CREDENTIAL, "no-colon")]).is_err());
        assert!(cfg(&[(ENV_TOKEN, "a"), (ENV_CREDENTIAL, "b:c")]).is_err());
        assert!(
            cfg(&[(ENV_VENDED, "true")])
                .unwrap()
                .unwrap()
                .vended_credentials
        );
        assert!(cfg(&[(ENV_VENDED, "yes")]).is_err());
        assert_eq!(encode("gold\u{1f}fin ance"), "gold%1Ffin%20ance");
    }

    #[tokio::test]
    async fn in_memory_catalog_lists_and_loads() {
        let cat = InMemoryCatalog::default();
        let bronze = Namespace::new(&["bronze"]);
        cat.create_namespace(Namespace::new(&["gold"]));
        let ident = TableIdent::new(bronze.clone(), "events");
        cat.put_table(
            ident.clone(),
            None,
            serde_json::json!({"format-version": 2}),
        );

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
