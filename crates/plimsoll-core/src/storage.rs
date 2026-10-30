//! Object stores resolved on first use.
//!
//! `file://` is always there. `s3://bucket` (and `s3a://`) get an S3 store
//! built from the standard `AWS_*` env (`AWS_ENDPOINT`, `AWS_REGION`,
//! `AWS_ACCESS_KEY_ID`, ..., or the instance/pod role), which also covers
//! S3-compatible stores such as MinIO (`AWS_ALLOW_HTTP=true` for plain HTTP).
//! Secrets only ever come from env. See ADR 0009.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry, ObjectStoreUrl,
};
use datafusion::object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use url::Url;

#[derive(Debug, Default)]
pub struct LazyObjectStores {
    stores: DefaultObjectStoreRegistry,
}

impl ObjectStoreRegistry for LazyObjectStores {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.stores.register_store(url, store)
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        if let Ok(store) = self.stores.get_store(url) {
            return Ok(store);
        }
        let store: Arc<dyn ObjectStore> = match url.scheme() {
            "s3" | "s3a" => Arc::new(
                AmazonS3Builder::from_env()
                    .with_url(url.as_str())
                    .build()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            ),
            // ponytail: GCS/ADLS are the same few lines behind the `gcp` /
            // `azure` object_store features; add when a deployment needs them.
            _ => return self.stores.get_store(url),
        };
        // Two racing first uses both build a store; the last one wins. Harmless.
        self.stores.register_store(url, store.clone());
        Ok(store)
    }
}

/// An S3 store scoped to one set of credentials (e.g. vended by an Iceberg
/// REST catalog for one table), registered under its own URL so it never
/// replaces the process-wide store for the same bucket.
#[derive(Clone)]
pub struct ScopedStore {
    /// The real bucket URL, e.g. `s3://warehouse`.
    pub bucket: ObjectStoreUrl,
    /// Synthetic key in the registry, e.g. `s3-1a2b3c4d5e6f7a8b://warehouse`.
    pub url: ObjectStoreUrl,
    pub store: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for ScopedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ScopedStore({} as {})",
            self.bucket.as_str(),
            self.url.as_str()
        )
    }
}

/// Builds a [`ScopedStore`] from Iceberg FileIO properties (`s3.access-key-id`,
/// `s3.secret-access-key`, `s3.session-token`, `s3.endpoint`, `client.region`,
/// `s3.path-style-access`). `None` when `props` carry no S3 credentials or
/// `location` is not on S3. Unset properties fall back to the `AWS_*` env.
pub fn scoped_s3_store(
    location: &str,
    props: &BTreeMap<String, String>,
) -> Result<Option<ScopedStore>> {
    let Some(key_id) = props.get("s3.access-key-id") else {
        return Ok(None);
    };
    let url = Url::parse(location).map_err(|e| DataFusionError::External(Box::new(e)))?;
    if !matches!(url.scheme(), "s3" | "s3a") {
        return Ok(None);
    }
    let bucket = url.host_str().unwrap_or_default();
    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_access_key_id(key_id);
    if let Some(v) = props.get("s3.secret-access-key") {
        builder = builder.with_secret_access_key(v);
    }
    if let Some(v) = props.get("s3.session-token") {
        builder = builder.with_token(v);
    }
    if let Some(v) = props.get("s3.endpoint") {
        builder = builder
            .with_endpoint(v)
            .with_allow_http(v.starts_with("http://"));
    }
    if let Some(v) = props
        .get("client.region")
        .or_else(|| props.get("s3.region"))
    {
        builder = builder.with_region(v);
    }
    if let Some(v) = props.get("s3.path-style-access") {
        builder = builder.with_virtual_hosted_style_request(v != "true");
    }
    let store = builder
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    // Same credentials -> same key, so per-query resolution reuses the entry.
    // ponytail: rotated credentials leave old entries in the registry (one
    // small client each). Upgrade path: evict on expiry
    // (`s3.session-token-expires-at-ms`).
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    (
        key_id,
        props.get("s3.session-token"),
        props.get("s3.endpoint"),
    )
        .hash(&mut hasher);
    Ok(Some(ScopedStore {
        bucket: ObjectStoreUrl::parse(format!("s3://{bucket}"))?,
        url: ObjectStoreUrl::parse(format!("s3-{:016x}://{bucket}", hasher.finish()))?,
        store: Arc::new(store),
    }))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn s3_is_built_on_demand_and_cached_unknown_schemes_fail() {
        let reg = LazyObjectStores::default();
        let url = Url::parse("s3://bucket/a/b.parquet").unwrap();
        let first = reg.get_store(&url).unwrap();
        let again = reg
            .get_store(&Url::parse("s3://bucket/other").unwrap())
            .unwrap();
        assert!(Arc::ptr_eq(&first, &again));
        assert!(reg.get_store(&Url::parse("file:///tmp").unwrap()).is_ok());
        assert!(
            reg.get_store(&Url::parse("gs://bucket/x").unwrap())
                .is_err()
        );
    }

    #[test]
    fn scoped_store_from_vended_properties() {
        let props: BTreeMap<String, String> = [
            ("s3.access-key-id", "AKIA"),
            ("s3.secret-access-key", "secret"),
            ("s3.session-token", "tok"),
            ("s3.endpoint", "http://localhost:8333"),
        ]
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .into();
        let a = scoped_s3_store("s3://wh/bronze/t", &props)
            .unwrap()
            .unwrap();
        let b = scoped_s3_store("s3://wh/other", &props).unwrap().unwrap();
        assert_eq!(a.bucket.as_str(), "s3://wh/");
        assert_eq!(a.url, b.url, "same credentials, same registry key");
        assert_ne!(a.url, a.bucket);
        assert!(!format!("{a:?}").contains("secret"));
        assert!(
            scoped_s3_store("s3://wh/t", &BTreeMap::new())
                .unwrap()
                .is_none()
        );
        assert!(scoped_s3_store("file:///wh/t", &props).unwrap().is_none());
    }
}
