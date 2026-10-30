//! Object stores resolved on first use.
//!
//! `file://` is always there. `s3://bucket` (and `s3a://`) get an S3 store
//! built from the standard `AWS_*` env (`AWS_ENDPOINT`, `AWS_REGION`,
//! `AWS_ACCESS_KEY_ID`, ..., or the instance/pod role), which also covers
//! S3-compatible stores such as MinIO (`AWS_ALLOW_HTTP=true` for plain HTTP).
//! Secrets only ever come from env. See ADR 0009.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
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
}
