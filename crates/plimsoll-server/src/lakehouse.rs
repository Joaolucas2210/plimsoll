//! Iceberg REST catalog namespaces exposed as DataFusion schemas.
//!
//! `SELECT * FROM bronze.trips` resolves `bronze.trips` through the catalog's
//! `loadTable` on *every* query, so new commits are visible without a
//! restart and a dropped table disappears. Read-only: DDL goes to the catalog.

use std::sync::Arc;

use async_trait::async_trait;
use plimsoll_connectors::{Connector, IcebergConnector};
use plimsoll_control::catalog::{Catalog, CatalogError, Namespace, TableIdent};
use plimsoll_core::datafusion::catalog::{SchemaProvider, TableProvider};
use plimsoll_core::datafusion::error::{DataFusionError, Result};
use plimsoll_core::datafusion::prelude::SessionContext;

struct CatalogSchema {
    catalog: Arc<dyn Catalog>,
    namespace: Namespace,
    /// Names listed at boot, for `SHOW TABLES` / `information_schema`.
    /// ponytail: tables created later still resolve via `table()`, they just
    /// don't show up in listings until restart.
    names: Vec<String>,
}

impl std::fmt::Debug for CatalogSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CatalogSchema({})", self.namespace)
    }
}

#[async_trait]
impl SchemaProvider for CatalogSchema {
    fn table_names(&self) -> Vec<String> {
        self.names.clone()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let ident = TableIdent::new(self.namespace.clone(), name);
        let loaded = match self.catalog.load_table(&ident).await {
            Ok(t) => t,
            Err(CatalogError::TableNotFound(_)) => return Ok(None),
            Err(e) => return Err(DataFusionError::External(Box::new(e))),
        };
        let location = loaded.metadata_location.ok_or_else(|| {
            DataFusionError::Plan(format!("{ident} has no metadata-location (staged table?)"))
        })?;
        // Vended credentials (if the catalog sent any) scope this table's
        // reads; they are fetched fresh with every query.
        let connector =
            IcebergConnector::from_metadata(ident.to_string(), &location, &loaded.metadata)?
                .with_storage_properties(&loaded.storage)?;
        Ok(Some(connector.provider()))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.names.iter().any(|n| n == name)
    }
}

/// Registers every single-level namespace of `catalog` as a schema of the
/// default DataFusion catalog (replacing the empty medallion placeholders).
/// Returns the namespaces registered.
pub async fn register(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let default = ctx
        .catalog("datafusion")
        .ok_or("default DataFusion catalog missing")?;
    let mut registered = Vec::new();
    for namespace in catalog.list_namespaces().await? {
        // ponytail: `a.b.c` namespaces need nested catalogs in DataFusion;
        // skipped until someone has one.
        let [name] = namespace.0.as_slice() else {
            eprintln!("catalog: skipping multi-level namespace {namespace}");
            continue;
        };
        let names = catalog
            .list_tables(&namespace)
            .await?
            .into_iter()
            .map(|t| t.name)
            .collect();
        let schema = CatalogSchema {
            catalog: catalog.clone(),
            namespace: namespace.clone(),
            names,
        };
        default.register_schema(name, Arc::new(schema))?;
        registered.push(name.clone());
    }
    Ok(registered)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use plimsoll_control::catalog::InMemoryCatalog;
    use serde_json::json;

    fn empty_table(column: &str) -> serde_json::Value {
        json!({"format-version": 2, "current-schema-id": 0, "current-snapshot-id": -1,
               "schemas": [{"schema-id": 0, "fields": [
                   {"id": 1, "name": column, "type": "long", "required": true}]}]})
    }

    /// Live: REST catalog + S3 that enforces credentials. The process env has
    /// *wrong* AWS keys; `PLIMSOLL_TEST_VENDED=key:secret` are valid ones that
    /// a vending catalog (simulated here) hands out with `loadTable`.
    #[tokio::test]
    #[ignore = "needs PLIMSOLL_CATALOG_URI, an S3 warehouse and PLIMSOLL_TEST_VENDED"]
    async fn reads_with_catalog_vended_credentials() {
        use plimsoll_control::catalog::{CatalogConfig, LoadedTable};
        use plimsoll_control::rest::RestCatalog;
        use plimsoll_core::engine::Engine;
        use plimsoll_core::memory::CeilingPool;

        let (Some(cfg), Ok(vended)) = (
            CatalogConfig::from_lookup(|k| std::env::var(k).ok()).unwrap(),
            std::env::var("PLIMSOLL_TEST_VENDED"),
        ) else {
            return;
        };
        let (key, secret) = vended.split_once(':').unwrap();

        struct Vending(RestCatalog, String, String);
        #[async_trait]
        impl Catalog for Vending {
            async fn list_namespaces(&self) -> Result<Vec<Namespace>, CatalogError> {
                self.0.list_namespaces().await
            }
            async fn list_tables(&self, ns: &Namespace) -> Result<Vec<TableIdent>, CatalogError> {
                self.0.list_tables(ns).await
            }
            async fn load_table(&self, id: &TableIdent) -> Result<LoadedTable, CatalogError> {
                let mut t = self.0.load_table(id).await?;
                t.storage.insert("s3.access-key-id".into(), self.1.clone());
                t.storage
                    .insert("s3.secret-access-key".into(), self.2.clone());
                Ok(t)
            }
        }

        let run = async |catalog: Arc<dyn Catalog>| {
            let pool = Arc::new(CeilingPool::new(256 << 20, 32 << 20).unwrap());
            let engine = Engine::new(pool, None).unwrap();
            register(engine.context(), catalog).await.unwrap();
            engine.sql("SELECT count(*) FROM bronze.trips").await
        };
        let plain = Arc::new(RestCatalog::connect(cfg.clone()).await.unwrap());
        let err = run(plain).await.unwrap_err();
        // Wrong storage credentials are the object store's refusal, not a bug.
        assert!(
            matches!(
                crate::engine::classify(&err),
                crate::engine::EngineError::Dependency {
                    dependency: "object_store",
                    ..
                }
            ),
            "{err}"
        );
        let vending = Vending(
            RestCatalog::connect(cfg).await.unwrap(),
            key.to_owned(),
            secret.to_owned(),
        );
        let result = run(Arc::new(vending)).await.unwrap();
        assert_eq!(result.batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn tables_resolve_per_query_through_the_catalog() {
        let cat = Arc::new(InMemoryCatalog::default());
        let ident = TableIdent::new(Namespace::new(&["silver"]), "users");
        let loc = Some("file:///wh/silver/users/metadata/1.metadata.json".to_owned());
        cat.put_table(ident.clone(), loc.clone(), empty_table("a"));
        cat.create_namespace(Namespace::new(&["deep", "er"]));

        let ctx = SessionContext::new();
        let schemas = register(&ctx, cat.clone()).await.unwrap();
        assert_eq!(schemas, ["silver"]);
        assert!(ctx.sql("SELECT a FROM silver.users").await.is_ok());

        // A new commit (here: schema change) is seen by the next query.
        cat.put_table(ident, loc, empty_table("b"));
        assert!(ctx.sql("SELECT a FROM silver.users").await.is_err());
        let rows = ctx
            .sql("SELECT b FROM silver.users")
            .await
            .unwrap()
            .collect()
            .await;
        assert_eq!(rows.unwrap().iter().map(|b| b.num_rows()).sum::<usize>(), 0);

        let err = ctx.sql("SELECT * FROM silver.nope").await.unwrap_err();
        assert!(err.to_string().contains("not found"), "{err}");
    }
}
