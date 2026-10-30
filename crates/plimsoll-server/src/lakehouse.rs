//! Iceberg REST catalog namespaces exposed as DataFusion schemas.
//!
//! `SELECT * FROM bronze.trips` resolves `bronze.trips` through the catalog's
//! `loadTable` on *every* query, so new commits are visible without a
//! restart and a dropped table disappears. Read-only: DDL goes to the catalog.
//!
//! Time travel (ADR 0028): `bronze."trips@<snapshot-id>"`,
//! `bronze."trips@2024-05-01T00:00:00Z"` or, by branch/tag (ADR 0029),
//! `bronze."trips@audit"` reads the table as of that snapshot. The reference stays in its schema, so grants apply unchanged.

use std::sync::Arc;

use async_trait::async_trait;
use plimsoll_connectors::iceberg;
use plimsoll_connectors::{AsOf, Connector, IcebergConnector};
use plimsoll_control::catalog::{Catalog, CatalogError, LoadedTable, Namespace, TableIdent};
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

impl CatalogSchema {
    /// `loadTable` for `name`; `None` when the catalog has no such table.
    async fn load(&self, name: &str) -> Result<Option<(TableIdent, LoadedTable)>> {
        let ident = TableIdent::new(self.namespace.clone(), name);
        match self.catalog.load_table(&ident).await {
            Ok(t) => Ok(Some((ident, t))),
            Err(CatalogError::TableNotFound(_)) => Ok(None),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }
    }

    /// A provider over `metadata` (possibly pinned) of a loaded table, named
    /// `schema.name` as queried (the label shows the `@` spec).
    fn provider(
        &self,
        name: &str,
        (ident, loaded): (TableIdent, LoadedTable),
        metadata: &serde_json::Value,
    ) -> Result<Arc<dyn TableProvider>> {
        let location = loaded.metadata_location.ok_or_else(|| {
            DataFusionError::Plan(format!("{ident} has no metadata-location (staged table?)"))
        })?;
        // Vended credentials (if the catalog sent any) scope this table's
        // reads; they are fetched fresh with every query.
        let connector = IcebergConnector::from_metadata(
            format!("{}.{name}", self.namespace),
            &location,
            metadata,
        )?
        .with_storage_properties(&loaded.storage)?;
        Ok(connector.provider())
    }
}

#[async_trait]
impl SchemaProvider for CatalogSchema {
    fn table_names(&self) -> Vec<String> {
        self.names.clone()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // `table@spec`: a snapshot id, an instant, or a branch/tag of `table`
        // (ADR 0028/0029). Anything else means `@` is part of the name.
        if let Some((base, spec)) = name.rsplit_once('@') {
            if let Some(loaded) = self.load(base).await? {
                let pinned = match AsOf::parse(spec) {
                    Some(as_of) => Some(iceberg::at(&loaded.1.metadata, as_of)),
                    None => iceberg::ref_snapshot(&loaded.1.metadata, spec)
                        .map(|id| iceberg::at(&loaded.1.metadata, AsOf::Snapshot(id))),
                };
                if let Some(pinned) = pinned {
                    let metadata = pinned
                        .map_err(|m| DataFusionError::Plan(format!("{}@{spec}: {m}", loaded.0)))?;
                    return self.provider(name, loaded, &metadata).map(Some);
                }
            }
        }
        match self.load(name).await? {
            Some(loaded) => {
                let metadata = loaded.1.metadata.clone();
                self.provider(name, loaded, &metadata).map(Some)
            }
            None => Ok(None),
        }
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
            crate::log::emit(
                crate::log::Level::Warn,
                "namespace_skipped",
                serde_json::json!({ "namespace": namespace.to_string(), "reason": "multi-level" }),
            );
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

    /// Two snapshots with different schemas: `a` (snapshot 1, t=100 ms),
    /// then `b` (snapshot 2, t=200 ms, current).
    fn two_snapshots() -> serde_json::Value {
        let schema = |id, col: &str| {
            json!({"schema-id": id, "fields": [
            {"id": id + 1, "name": col, "type": "long", "required": true}]})
        };
        json!({"format-version": 2, "current-schema-id": 1, "current-snapshot-id": 2,
               "schemas": [schema(0, "a"), schema(1, "b")],
               "snapshots": [
                   {"snapshot-id": 1, "schema-id": 0, "timestamp-ms": 100, "manifest-list": "file:///l1.avro"},
                   {"snapshot-id": 2, "schema-id": 1, "timestamp-ms": 200, "manifest-list": "file:///l2.avro"}],
               "snapshot-log": [
                   {"snapshot-id": 1, "timestamp-ms": 100},
                   {"snapshot-id": 2, "timestamp-ms": 200}],
               "refs": {"main": {"snapshot-id": 2, "type": "branch"},
                        "v1": {"snapshot-id": 1, "type": "tag"}}})
    }

    #[tokio::test]
    async fn time_travel_by_snapshot_id_or_instant() {
        let cat = Arc::new(InMemoryCatalog::default());
        let ident = TableIdent::new(Namespace::new(&["silver"]), "users");
        cat.put_table(
            ident,
            Some("file:///wh/silver/users/metadata/2.metadata.json".to_owned()),
            two_snapshots(),
        );
        let ctx = SessionContext::new();
        register(&ctx, cat).await.unwrap();
        // Logical planning resolves the table and its schema at that point.
        let plans = async |sql: &str| ctx.sql(sql).await.map(|_| ()).map_err(|e| e.to_string());
        assert!(plans("SELECT b FROM silver.users").await.is_ok());
        assert!(plans("SELECT a FROM silver.\"users@1\"").await.is_ok());
        assert!(
            plans("SELECT b FROM silver.\"users@1\"").await.is_err(),
            "old schema"
        );
        assert!(
            plans("SELECT a FROM silver.\"users@1970-01-01T00:00:00.150Z\"")
                .await
                .is_ok()
        );
        assert!(
            plans("SELECT b FROM silver.\"users@1970-01-01T00:00:00.250Z\"")
                .await
                .is_ok()
        );
        let err = plans("SELECT * FROM silver.\"users@1970-01-01T00:00:00.050Z\"")
            .await
            .unwrap_err();
        assert!(err.contains("no snapshot at or before"), "{err}");
        let err = plans("SELECT * FROM silver.\"users@999\"")
            .await
            .unwrap_err();
        assert!(err.contains("snapshot 999 not found"), "{err}");
        // Branches and tags (ADR 0029).
        assert!(plans("SELECT a FROM silver.\"users@v1\"").await.is_ok());
        assert!(plans("SELECT b FROM silver.\"users@main\"").await.is_ok());
        // Not a time-travel spec: the whole name is looked up (and missing).
        let err = plans("SELECT * FROM silver.\"users@yesterday\"")
            .await
            .unwrap_err();
        assert!(err.contains("not found"), "{err}");
    }
}
