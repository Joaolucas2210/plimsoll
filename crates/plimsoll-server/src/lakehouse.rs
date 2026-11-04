//! Iceberg REST catalog namespaces exposed as DataFusion schemas.
//!
//! `SELECT * FROM bronze.trips` resolves `bronze.trips` through the catalog's
//! `loadTable` on *every* query, so new commits are visible without a
//! restart and a dropped table disappears. Read-only: DDL goes to the catalog.
//!
//! Time travel (ADR 0028): `bronze."trips@<snapshot-id>"`,
//! `bronze."trips@2024-05-01T00:00:00Z"` or, by branch/tag (ADR 0029),
//! `bronze."trips@audit"` reads the table as of that snapshot. The reference stays in its schema, so grants apply unchanged.
//! `bronze."trips$snapshots"`, `$history` and `$refs` list them (ADR 0030);
//! `$files` and `$manifests` list the current snapshot's files (ADR 0032).
//! `CREATE TABLE ns.t (…)` creates through the catalog (ADR 0083; `… AS
//! SELECT` staged, then committed with its rows, ADR 0090), and
//! `ALTER TABLE` changes its schema and properties there (ADR 0084).

use std::sync::{Arc, PoisonError, RwLock};

use async_trait::async_trait;
use plimsoll_connectors::iceberg;
use plimsoll_connectors::{AsOf, Connector, IcebergConnector};
use plimsoll_control::catalog::{Catalog, CatalogError, LoadedTable, Namespace, TableIdent};
use plimsoll_core::datafusion::catalog::{SchemaProvider, TableProvider};
use plimsoll_core::datafusion::datasource::MemTable;
use plimsoll_core::datafusion::error::{DataFusionError, Result};
use plimsoll_core::datafusion::prelude::SessionContext;

struct CatalogSchema {
    catalog: Arc<dyn Catalog>,
    namespace: Namespace,
    /// Listed names, for `SHOW TABLES` / `information_schema`. Refreshed by
    /// [`refresh`] (ADR 0033); lookups never depend on it: `table()` asks
    /// the catalog every time.
    names: RwLock<Vec<String>>,
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
        let started = std::time::Instant::now();
        let loaded = self.catalog.load_table(&ident).await;
        // A span in the query's trace (ADR 0056).
        plimsoll_core::plan_trace::record(
            "catalog.load_table",
            started,
            vec![
                ("table", ident.to_string()),
                (
                    "found",
                    (!matches!(loaded, Err(CatalogError::TableNotFound(_)))).to_string(),
                ),
            ],
        );
        match loaded {
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
        loaded: (TableIdent, LoadedTable),
        metadata: &serde_json::Value,
    ) -> Result<Arc<dyn TableProvider>> {
        Ok(self.connector(name, loaded, metadata)?.provider())
    }

    fn connector(
        &self,
        name: &str,
        (ident, loaded): (TableIdent, LoadedTable),
        metadata: &serde_json::Value,
    ) -> Result<IcebergConnector> {
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
        .with_storage_properties(&loaded.storage)?
        // `INSERT INTO` commits back through this catalog (ADR 0075).
        .with_committer(Arc::new(CatalogCommitter {
            catalog: self.catalog.clone(),
            ident,
        }));
        Ok(connector)
    }
}

/// Commits an Iceberg append as a REST `updateTable` (ADR 0075).
struct CatalogCommitter {
    catalog: Arc<dyn Catalog>,
    ident: TableIdent,
}

impl std::fmt::Debug for CatalogCommitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CatalogCommitter({})", self.ident)
    }
}

#[async_trait]
impl iceberg::write::Committer for CatalogCommitter {
    async fn current(&self) -> std::result::Result<serde_json::Value, String> {
        self.catalog
            .load_table(&self.ident)
            .await
            .map(|t| t.metadata)
            .map_err(|e| e.to_string())
    }

    async fn commit(
        &self,
        requirements: Vec<serde_json::Value>,
        updates: Vec<serde_json::Value>,
    ) -> std::result::Result<(), iceberg::write::CommitError> {
        match self
            .catalog
            .update_table(&self.ident, requirements, updates)
            .await
        {
            Ok(_) => Ok(()),
            Err(CatalogError::CommitConflict(m)) => Err(iceberg::write::CommitError::Conflict(m)),
            Err(e) => Err(iceberg::write::CommitError::Failed(e.to_string())),
        }
    }
}

/// `CREATE TABLE` (ADR 0083), `ALTER TABLE` (ADR 0084) and `DROP TABLE`
/// (ADR 0089) in the REST catalog: the table's schema is the catalog
/// namespace.
struct CatalogDdl(Arc<dyn Catalog>);

impl CatalogDdl {
    /// `createTable` (`stage-create` when `staged`, ADR 0090): the table as
    /// created or staged; `None` when it exists and `IF NOT EXISTS`.
    async fn create(
        &self,
        table: &plimsoll_core::datafusion::common::ResolvedTableReference,
        new: &plimsoll_core::ddl::NewTable,
        staged: bool,
    ) -> Result<Option<LoadedTable>> {
        let ident = TableIdent::new(Namespace::new(&[&table.schema]), &table.table);
        let mut request =
            iceberg::create_request(&table.table, &new.schema, &new.partition, &new.properties)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if staged {
            request["stage-create"] = serde_json::json!(true);
        }
        match self.0.create_table(&ident, request).await {
            Ok(t) => Ok(Some(t)),
            Err(CatalogError::TableExists(_)) if new.if_not_exists => Ok(None),
            Err(CatalogError::TableExists(_)) => Err(DataFusionError::Plan(format!(
                "table {ident} already exists"
            ))),
            // The catalog refused the definition (a transform on the
            // wrong type, a bad property): the statement's fault.
            Err(CatalogError::Status {
                status: 400,
                message,
            }) => Err(DataFusionError::Plan(format!(
                "cannot create {ident}: {message}"
            ))),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }
    }
}

/// Commit attempts of an `ALTER TABLE` racing other writers.
const ALTER_ATTEMPTS: usize = 4;

#[async_trait]
impl plimsoll_core::ddl::TableDdl for CatalogDdl {
    async fn create_table(
        &self,
        table: &plimsoll_core::datafusion::common::ResolvedTableReference,
        new: plimsoll_core::ddl::NewTable,
    ) -> Result<()> {
        self.create(table, &new, false).await.map(|_| ())
    }

    async fn create_table_as(
        &self,
        table: &plimsoll_core::datafusion::common::ResolvedTableReference,
        new: plimsoll_core::ddl::NewTable,
        plan: Arc<dyn plimsoll_core::datafusion::physical_plan::ExecutionPlan>,
        state: &dyn plimsoll_core::datafusion::catalog::Session,
    ) -> Result<u64> {
        // Staged: nothing is visible until the data commits with it.
        let Some(staged) = self.create(table, &new, true).await? else {
            return Ok(0); // IF NOT EXISTS, and it does
        };
        let ident = TableIdent::new(Namespace::new(&[&table.schema]), &table.table);
        let location = staged.metadata["location"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let connector = IcebergConnector::from_metadata(
            format!("{}.{}", table.schema, table.table),
            &location,
            &staged.metadata,
        )
        .and_then(|c| c.with_storage_properties(&staged.storage))
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .with_committer(Arc::new(CatalogCommitter {
            catalog: self.0.clone(),
            ident,
        }));
        connector
            .create_as(state, &staged.metadata, plan)
            .await
            .map_err(DataFusionError::from)
    }

    async fn alter_table(
        &self,
        table: &plimsoll_core::datafusion::common::ResolvedTableReference,
        changes: Vec<plimsoll_core::ddl::Alteration>,
        if_exists: bool,
    ) -> Result<()> {
        let ident = TableIdent::new(Namespace::new(&[&table.schema]), &table.table);
        let external = |e: CatalogError| DataFusionError::External(Box::new(e));
        let mut attempt = 0;
        loop {
            attempt += 1;
            // Changes apply to the metadata as of now; on a conflict
            // (another commit in between), to the fresh metadata.
            let meta = match self.0.load_table(&ident).await {
                Ok(t) => t.metadata,
                Err(CatalogError::TableNotFound(_)) if if_exists => return Ok(()),
                Err(e) => return Err(external(e)),
            };
            let Some((requirements, updates)) = iceberg::alter_updates(&meta, &changes)? else {
                return Ok(());
            };
            match self.0.update_table(&ident, requirements, updates).await {
                Ok(_) => return Ok(()),
                Err(CatalogError::CommitConflict(_)) if attempt < ALTER_ATTEMPTS => {}
                Err(CatalogError::Status {
                    status: 400,
                    message,
                }) => {
                    return Err(DataFusionError::Plan(format!(
                        "cannot alter {ident}: {message}"
                    )));
                }
                Err(e) => return Err(external(e)),
            }
        }
    }

    async fn drop_table(
        &self,
        table: &plimsoll_core::datafusion::common::ResolvedTableReference,
        if_exists: bool,
        purge: bool,
    ) -> Result<()> {
        let ident = TableIdent::new(Namespace::new(&[&table.schema]), &table.table);
        match self.0.drop_table(&ident, purge).await {
            Ok(()) => Ok(()),
            Err(CatalogError::TableNotFound(_)) if if_exists => Ok(()),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }
    }
}

#[async_trait]
impl SchemaProvider for CatalogSchema {
    fn table_names(&self) -> Vec<String> {
        self.names
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // `table$snapshots|history|refs` (ADR 0030): built from the metadata
        // just loaded, so it is as fresh as a data read. Other suffixes mean
        // `$` is part of the name.
        if let Some((base, kind)) = name.rsplit_once('$')
            && let Some(loaded) = self.load(base).await?
        {
            if let Some(batch) = iceberg::meta::table(&loaded.1.metadata, kind) {
                let batch = batch?;
                let mem = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
                return Ok(Some(Arc::new(mem)));
            }
            // `$files` / `$manifests` read manifests (ADR 0032).
            if matches!(kind, "files" | "manifests" | "partitions") {
                let metadata = loaded.1.metadata.clone();
                let connector = self.connector(name, loaded, &metadata)?;
                if let Some(t) = connector.metadata_table(kind) {
                    return t.map(Some);
                }
            }
        }
        // `table@spec`: a snapshot id, an instant, or a branch/tag of `table`
        // (ADR 0028/0029). Anything else means `@` is part of the name.
        if let Some((base, spec)) = name.rsplit_once('@')
            && let Some(loaded) = self.load(base).await?
        {
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
        match self.load(name).await? {
            Some(loaded) => {
                let metadata = loaded.1.metadata.clone();
                self.provider(name, loaded, &metadata).map(Some)
            }
            None => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names().iter().any(|n| n == name)
    }
}

/// Registers every single-level namespace of `catalog` as a schema of the
/// default DataFusion catalog (replacing the empty medallion placeholders).
/// Returns the namespaces registered.
pub async fn register(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let r = refresh(ctx, catalog.clone()).await?;
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(plimsoll_core::ddl::Ddl(Arc::new(CatalogDdl(
            catalog,
        )))));
    for (namespace, reason) in &r.skipped {
        crate::log::emit(
            crate::log::Level::Warn,
            "namespace_skipped",
            serde_json::json!({ "namespace": namespace, "reason": reason }),
        );
    }
    Ok(r.namespaces)
}

/// The connector of `table` (`schema.name`), a catalog-served Iceberg
/// table, loaded fresh, for maintenance.
async fn maintained(ctx: &SessionContext, table: &str) -> Result<IcebergConnector> {
    let plan = |m: String| DataFusionError::Plan(m);
    let (schema, name) = table
        .rsplit_once('.')
        .ok_or_else(|| plan(format!("expected `schema.table`, got {table:?}")))?;
    let state = ctx.state();
    let default = &state.config_options().catalog.default_catalog;
    let provider = ctx
        .catalog(default)
        .and_then(|c| c.schema(schema))
        .ok_or_else(|| plan(format!("no schema {schema}")))?;
    let Some(catalog) = provider.as_ref().downcast_ref::<CatalogSchema>() else {
        return Err(plan(format!(
            "{table}: only tables served by a REST catalog can be maintained"
        )));
    };
    let Some(loaded) = catalog.load(name).await? else {
        return Err(plan(format!("no table {table}")));
    };
    let metadata = loaded.1.metadata.clone();
    catalog.connector(name, loaded, &metadata)
}

/// Compacts `table` in place (ADR 0078): rewrites its small files and
/// files carrying deletes, and commits the replacement through the
/// catalog.
pub async fn compact(ctx: &SessionContext, table: &str) -> Result<plimsoll_connectors::Compaction> {
    maintained(ctx, table)
        .await?
        .compact(&ctx.state())
        .await
        .map_err(DataFusionError::from)
}

/// Expires `table`'s old snapshots and deletes the files only they
/// reached (ADR 0079).
pub async fn expire_snapshots(
    ctx: &SessionContext,
    table: &str,
    opts: iceberg::expire::ExpireOptions,
) -> Result<iceberg::expire::Expiration> {
    maintained(ctx, table)
        .await?
        .expire_snapshots(&ctx.state(), opts)
        .await
        .map_err(DataFusionError::from)
}

/// Claims `table` for `ttl_ms` of scheduled maintenance (ADR 0101);
/// `false` when another coordinator holds it.
pub async fn claim_maintenance(
    ctx: &SessionContext,
    table: &str,
    holder: &str,
    ttl_ms: i64,
) -> Result<bool> {
    maintained(ctx, table)
        .await?
        .claim_maintenance(&ctx.state(), holder, ttl_ms)
        .await
        .map_err(DataFusionError::from)
}

/// Finds (and unless `dry_run`, deletes) `table`'s orphan files (ADR
/// 0080).
pub async fn remove_orphans(
    ctx: &SessionContext,
    table: &str,
    opts: iceberg::expire::OrphanOptions,
) -> Result<iceberg::expire::Orphans> {
    maintained(ctx, table)
        .await?
        .remove_orphans(&ctx.state(), opts)
        .await
        .map_err(DataFusionError::from)
}

/// What a [`refresh`] saw and changed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Refreshed {
    /// Every catalog-backed schema, sorted.
    pub namespaces: Vec<String>,
    /// Namespaces registered by this call.
    pub new_namespaces: Vec<String>,
    /// Schemas whose table list changed.
    pub changed: Vec<String>,
    /// Namespaces not exposed, with the reason.
    pub skipped: Vec<(String, &'static str)>,
}

/// Re-lists the catalog (ADR 0033): updates each catalog schema's table
/// names and registers namespaces created since. A namespace is only
/// registered where no schema with tables exists, so env-registered tables
/// are never shadowed. Any catalog error leaves every listing as it was.
/// ponytail: dropped namespaces keep an (emptied) schema until restart.
pub async fn refresh(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
) -> Result<Refreshed, Box<dyn std::error::Error>> {
    let default = ctx
        .catalog("datafusion")
        .ok_or("default DataFusion catalog missing")?;
    // Everything is listed before anything changes.
    let mut listed = Vec::new();
    let mut skipped = Vec::new();
    let namespaces = catalog.list_namespaces().await?;
    for namespace in &namespaces {
        // ponytail: `a.b.c` namespaces need nested catalogs in DataFusion;
        // skipped until someone has one.
        let [name] = namespace.0.as_slice() else {
            skipped.push((namespace.to_string(), "multi-level"));
            continue;
        };
        let mut names: Vec<String> = catalog
            .list_tables(namespace)
            .await?
            .into_iter()
            .map(|t| t.name)
            .collect();
        names.sort();
        listed.push((name.clone(), namespace.clone(), names));
    }
    let mut out = Refreshed {
        skipped,
        ..Default::default()
    };
    for (name, namespace, names) in listed {
        let existing = default.schema(&name);
        match existing
            .as_ref()
            .and_then(|s| s.downcast_ref::<CatalogSchema>())
        {
            Some(schema) => {
                let mut current = schema.names.write().unwrap_or_else(PoisonError::into_inner);
                if *current != names {
                    *current = names;
                    out.changed.push(name.clone());
                }
            }
            None if existing.as_ref().is_none_or(|s| s.table_names().is_empty()) => {
                let schema = CatalogSchema {
                    catalog: catalog.clone(),
                    namespace,
                    names: RwLock::new(names),
                };
                default.register_schema(&name, Arc::new(schema))?;
                out.new_namespaces.push(name.clone());
            }
            None => {
                out.skipped.push((name, "schema has env tables"));
                continue;
            }
        }
        out.namespaces.push(name);
    }
    // Namespaces gone from the catalog: their tables no longer resolve, so
    // stop listing them.
    for name in default.schema_names() {
        if out.namespaces.contains(&name) {
            continue;
        }
        if let Some(schema) = default.schema(&name)
            && let Some(schema) = schema.downcast_ref::<CatalogSchema>()
        {
            let mut current = schema.names.write().unwrap_or_else(PoisonError::into_inner);
            if !current.is_empty() {
                current.clear();
                out.changed.push(name);
            }
        }
    }
    out.namespaces.sort();
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use plimsoll_control::catalog::InMemoryCatalog;
    use plimsoll_core::datafusion::arrow::array::{Int64Array as arrow_i64, RecordBatch};
    use serde_json::json;

    fn empty_table(column: &str) -> serde_json::Value {
        json!({"format-version": 2, "current-schema-id": 0, "current-snapshot-id": -1,
               "schemas": [{"schema-id": 0, "fields": [
                   {"id": 1, "name": column, "type": "long", "required": true}]}]})
    }

    /// Live: REST catalog + S3 that enforces credentials. The process env has
    /// *wrong* AWS keys; `PLIMSOLL_TEST_VENDED=key:secret` are valid ones that
    /// a vending catalog (simulated here) hands out with `loadTable`.
    /// `INSERT INTO` through a real REST catalog (ADR 0075): data files
    /// on the table's storage, a snapshot committed by the catalog, read
    /// back by a fresh `loadTable`. Uses its own table, so the shared
    /// fixtures stay as other live tests expect them.
    #[tokio::test]
    #[ignore = "needs PLIMSOLL_CATALOG_URI (iceberg-rest-fixture) and its warehouse's storage"]
    /// Its debug-build futures need more than the default 2 MiB test
    /// stack: CI runs it with `RUST_MIN_STACK=8388608`.
    async fn writes_through_the_rest_catalog() {
        use plimsoll_control::catalog::CatalogConfig;
        use plimsoll_control::rest::RestCatalog;
        use plimsoll_core::datafusion::arrow::util::pretty::pretty_format_batches;
        use plimsoll_core::engine::Engine;
        use plimsoll_core::memory::CeilingPool;

        let Some(cfg) = CatalogConfig::from_lookup(|k| std::env::var(k).ok()).unwrap() else {
            return;
        };
        let name = format!("appends_{:x}", std::process::id());
        let http = reqwest::Client::new();
        let base = format!("{}/v1/namespaces/bronze/tables", cfg.uri);
        let created = http
            .post(&base)
            .header("content-type", "application/json")
            .body(
                serde_json::json!({
                    "name": name,
                    "schema": {"type": "struct", "schema-id": 0, "fields": [
                        {"id": 1, "name": "id", "required": true, "type": "long"},
                        {"id": 2, "name": "name", "required": false, "type": "string"},
                    ]},
                    "properties": {"format-version": "2"},
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();
        assert!(
            created.status().is_success(),
            "{}",
            created.text().await.unwrap()
        );

        let catalog: Arc<dyn Catalog> = Arc::new(RestCatalog::connect(cfg).await.unwrap());
        let pool = Arc::new(CeilingPool::new(256 << 20, 32 << 20).unwrap());
        let engine = Engine::new(pool, None).unwrap();
        register(engine.context(), catalog.clone()).await.unwrap();
        let text = async |sql: &str| {
            pretty_format_batches(&engine.sql(sql).await.unwrap().batches)
                .unwrap()
                .to_string()
        };
        let t = format!("bronze.{name}");
        // An empty table, then two appends: the second carries the first
        // snapshot's manifest over.
        assert!(
            text(&format!("INSERT INTO {t} VALUES (1, 'a'), (2, NULL)"))
                .await
                .contains("| 2 ")
        );
        assert!(
            text(&format!("INSERT INTO {t} SELECT id + 10, name FROM {t}"))
                .await
                .contains("| 2 ")
        );
        let all = text(&format!("SELECT id, name FROM {t} ORDER BY id")).await;
        for row in [
            "| 1  | a    |",
            "| 2  |      |",
            "| 11 | a    |",
            "| 12 |      |",
        ] {
            assert!(all.contains(row), "{all}");
        }
        // The catalog committed both, parent to child.
        let meta = catalog
            .load_table(&TableIdent::new(Namespace::new(&["bronze"]), &name))
            .await
            .unwrap()
            .metadata;
        let snapshots = meta["snapshots"].as_array().unwrap();
        assert_eq!(snapshots.len(), 2);
        assert_eq!(
            snapshots[1]["parent-snapshot-id"],
            snapshots[0]["snapshot-id"]
        );
        assert_eq!(snapshots[1]["summary"]["engine-name"], "plimsoll");
        assert_eq!(meta["last-sequence-number"], 2);
        // A NULL into a required column fails, and commits nothing.
        assert!(
            engine
                .sql(&format!("INSERT INTO {t} VALUES (NULL, 'x')"))
                .await
                .is_err()
        );
        // Partitioned (ADR 0076): rows routed by `identity(region)`, and
        // column bounds recorded, so both prune the new files.
        // Created through SQL (ADR 0083), as a caller would.
        let parted = format!("{name}_p");
        let p = format!("bronze.{parted}");
        let create =
            format!("CREATE TABLE {p} (id BIGINT NOT NULL, region VARCHAR) PARTITION BY (region)");
        text(&create).await;
        assert!(engine.sql(&create).await.is_err(), "already exists");
        text(&create.replace("TABLE", "TABLE IF NOT EXISTS")).await;
        text(&format!(
            "INSERT INTO {p} VALUES (1, 'eu'), (2, 'eu'), (3, 'us')"
        ))
        .await;
        text(&format!("INSERT INTO {p} VALUES (10, 'us')")).await;
        for filter in ["region = 'eu'", "id = 10"] {
            let plan = text(&format!("EXPLAIN SELECT id FROM {p} WHERE {filter}")).await;
            // One file read: by the manifest list's partition summaries,
            // the files' partition values or their column bounds.
            assert!(plan.contains(" files=1/"), "{filter}: {plan}");
        }
        let rows = text(&format!(
            "SELECT region, count(*) AS n FROM {p} GROUP BY region ORDER BY region"
        ))
        .await;
        assert!(
            rows.contains("| eu     | 2 |") && rows.contains("| us     | 2 |"),
            "{rows}"
        );
        // DELETE (ADR 0077): position deletes, across partitions; rows
        // already deleted are not deleted again; appends go on after.
        let deleted = async |sql: String| {
            let out = text(&sql).await;
            out.lines().nth(3).unwrap_or_default().to_owned()
        };
        assert!(
            deleted(format!("DELETE FROM {p} WHERE id IN (2, 10)"))
                .await
                .contains("| 2 ")
        );
        assert!(
            deleted(format!("DELETE FROM {p} WHERE id IN (2, 10)"))
                .await
                .contains("| 0 ")
        );
        assert!(
            deleted(format!("DELETE FROM {t} WHERE name IS NULL"))
                .await
                .contains("| 2 ")
        );
        text(&format!("INSERT INTO {p} VALUES (11, 'eu')")).await;
        let ids = text(&format!("SELECT id FROM {p} ORDER BY id")).await;
        let ids: Vec<&str> = ids
            .lines()
            .skip(3)
            .filter(|l| l.starts_with("| "))
            .collect();
        assert_eq!(ids, ["| 1  |", "| 3  |", "| 11 |"]);
        let left = text(&format!("SELECT id FROM {t} ORDER BY id")).await;
        assert!(
            left.contains("| 1  |") && left.contains("| 11 |") && !left.contains("| 12 |"),
            "{left}"
        );
        // ALTER TABLE (ADR 0084): a column added and one renamed; old files
        // read the new column as NULL and the renamed one by field id; a
        // promotion reads old int files as long.
        // Boxed: the test's future would otherwise outgrow the stack.
        Box::pin(async {
            text(&format!(
                "ALTER TABLE {t} ADD COLUMN score INT, RENAME COLUMN name TO label"
            ))
            .await;
            text(&format!("INSERT INTO {t} VALUES (30, 'z', 7)")).await;
            text(&format!(
                "ALTER TABLE {t} ALTER COLUMN score SET DATA TYPE BIGINT"
            ))
            .await;
            let evolved = text(&format!("SELECT id, label, score FROM {t} ORDER BY id")).await;
            for row in ["| 1  | a     |       |", "| 30 | z     | 7     |"] {
                assert!(evolved.contains(row), "{evolved}");
            }
            assert!(
                engine
                    .sql(&format!(
                        "ALTER TABLE {t} ALTER COLUMN score SET DATA TYPE INT"
                    ))
                    .await
                    .is_err()
            );
            text(&format!("DELETE FROM {t} WHERE id = 30")).await;
        })
        .await;
        let meta = catalog
            .load_table(&TableIdent::new(Namespace::new(&["bronze"]), &parted))
            .await
            .unwrap()
            .metadata;
        let ops: Vec<&str> = meta["snapshots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["summary"]["operation"].as_str().unwrap())
            .collect();
        assert_eq!(ops, ["append", "append", "delete", "append"]);
        // UPDATE (ADR 0081): a row moved to another partition, in one
        // overwrite snapshot; filters on either side see it right.
        assert!(
            deleted(format!("UPDATE {p} SET region = 'us' WHERE id = 1"))
                .await
                .contains("| 1 ")
        );
        let us = text(&format!(
            "SELECT id FROM {p} WHERE region = 'us' ORDER BY id"
        ))
        .await;
        assert!(us.contains("| 1  |") && us.contains("| 3  |"), "{us}");
        let eu = text(&format!(
            "SELECT id FROM {p} WHERE region = 'eu' ORDER BY id"
        ))
        .await;
        assert!(!eu.contains("| 1  |") && eu.contains("| 11 |"), "{eu}");
        let meta = catalog
            .load_table(&TableIdent::new(Namespace::new(&["bronze"]), &parted))
            .await
            .unwrap()
            .metadata;
        let last = meta["snapshots"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()
            .clone();
        assert_eq!(last["summary"]["operation"], "overwrite");
        assert_eq!(last["summary"]["added-position-deletes"], "1");
        // MERGE INTO (ADR 0082): an insert, then a delete of it with an
        // update of a row the source does not match; rows end as before.
        assert!(
            deleted(format!(
                "MERGE INTO {p} t USING (SELECT 20 AS id, 'eu' AS region) s ON t.id = s.id \
                 WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.region)"
            ))
            .await
            .contains("| 1 ")
        );
        assert!(
            deleted(format!(
                "MERGE INTO {p} t USING (SELECT 20 AS id) s ON t.id = s.id \
                 WHEN MATCHED THEN DELETE \
                 WHEN NOT MATCHED BY SOURCE AND t.id = 3 THEN UPDATE SET region = 'us'"
            ))
            .await
            .contains("| 2 ")
        );
        let duplicate = engine
            .sql(&format!(
                "MERGE INTO {p} t USING (SELECT 3 AS id UNION ALL SELECT 3) s \
                 ON t.id = s.id WHEN MATCHED THEN DELETE"
            ))
            .await
            .unwrap_err();
        assert!(
            duplicate.to_string().contains("more than one"),
            "{duplicate}"
        );
        // Partition evolution (ADR 0085): a second spec; a delete on a file
        // of each spec, each delete in its file's spec; compaction below
        // moves the old spec's files to the new one.
        Box::pin(async {
            text(&format!(
                "ALTER TABLE {p} ADD PARTITION FIELD bucket(2, id)"
            ))
            .await;
            text(&format!("INSERT INTO {p} VALUES (40, 'us')")).await;
            text(&format!("DELETE FROM {p} WHERE id = 40")).await;
            text(&format!("UPDATE {p} SET region = 'us' WHERE id = 3")).await;
            let specs = text(&format!(
                "SELECT DISTINCT content, spec_id FROM bronze.\"{parted}$files\" ORDER BY 1, 2"
            ))
            .await;
            for row in ["| 1       | 0       |", "| 1       | 1       |"] {
                assert!(specs.contains(row), "{specs}");
            }
        })
        .await;
        // Compaction (ADR 0078): the partition with deletes, and the one
        // with small files, rewritten; the rows unchanged; the position
        // deletes gone with the files they referenced.
        let files = async |sql: String| text(&sql).await;
        let before = files(format!("SELECT content, count(*) AS n FROM bronze.\"{parted}$files\" GROUP BY content ORDER BY content")).await;
        assert!(
            before.contains("| 1       |"),
            "position deletes before: {before}"
        );
        let report = compact(engine.context(), &p).await.unwrap();
        assert!(report.files_rewritten > report.files_written, "{report:?}");
        assert_eq!(report.rows, 3);
        let ids = text(&format!("SELECT id FROM {p} ORDER BY id")).await;
        let ids: Vec<&str> = ids
            .lines()
            .skip(3)
            .filter(|l| l.starts_with("| "))
            .collect();
        assert_eq!(ids, ["| 1  |", "| 3  |", "| 11 |"]);
        let after = files(format!("SELECT content, count(*) AS n FROM bronze.\"{parted}$files\" GROUP BY content ORDER BY content")).await;
        let spec = files(format!(
            "SELECT DISTINCT spec_id FROM bronze.\"{parted}$files\""
        ))
        .await;
        assert!(
            spec.contains("| 1       |") && !spec.contains("| 0       |"),
            "{spec}"
        );
        assert!(
            !after.contains("| 1       |"),
            "no position deletes after: {after}"
        );
        // Nothing left to do.
        assert_eq!(
            compact(engine.context(), &p).await.unwrap().files_rewritten,
            0
        );
        let last = catalog
            .load_table(&TableIdent::new(Namespace::new(&["bronze"]), &parted))
            .await
            .unwrap()
            .metadata;
        let last = last["snapshots"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()
            .clone();
        assert_eq!(last["summary"]["operation"], "replace");
        // Snapshot expiration (ADR 0079): a tag on the first snapshot keeps
        // it (and its files) alive; everything else but the head goes, and
        // the files only those snapshots reached are deleted.
        let ident = TableIdent::new(Namespace::new(&["bronze"]), &parted);
        let meta = catalog.load_table(&ident).await.unwrap().metadata;
        let first = meta["snapshots"][0]["snapshot-id"].as_i64().unwrap();
        catalog
            .update_table(
                &ident,
                vec![],
                vec![serde_json::json!({"action": "set-snapshot-ref", "ref-name": "keep", "type": "tag", "snapshot-id": first})],
            )
            .await
            .unwrap();
        let tagged = text(&format!(
            "SELECT id FROM bronze.\"{parted}@keep\" ORDER BY id"
        ))
        .await;
        let opts = iceberg::expire::ExpireOptions {
            older_than_ms: i64::MAX,
            retain_last: 1,
        };
        let report = expire_snapshots(engine.context(), &p, opts).await.unwrap();
        let total = meta["snapshots"].as_array().unwrap().len();
        assert_eq!(report.snapshots_expired, total - 2, "{report:?}");
        assert!(
            report.data_files_deleted > 0 && report.manifest_lists_deleted > 0,
            "{report:?}"
        );
        let ids = text(&format!("SELECT id FROM {p} ORDER BY id")).await;
        let ids: Vec<&str> = ids
            .lines()
            .skip(3)
            .filter(|l| l.starts_with("| "))
            .collect();
        assert_eq!(ids, ["| 1  |", "| 3  |", "| 11 |"]);
        // The tagged snapshot still reads, from files that were kept.
        let still = text(&format!(
            "SELECT id FROM bronze.\"{parted}@keep\" ORDER BY id"
        ))
        .await;
        assert_eq!(still, tagged);
        // Nothing left to expire.
        let again = expire_snapshots(engine.context(), &p, opts).await.unwrap();
        assert_eq!(again.snapshots_expired, 0);
        // Orphan removal (ADR 0080): a file nothing references, as a
        // failed write leaves, is found by a dry run, then deleted; every
        // referenced file stays. (No age window here: a fresh table, and
        // the store cannot backdate files.)
        let location = catalog.load_table(&ident).await.unwrap().metadata["location"]
            .as_str()
            .unwrap()
            .trim_end_matches('/')
            .to_owned();
        use plimsoll_core::datafusion::object_store::ObjectStoreExt;
        let orphan = format!("{location}/data/failed-write-00000.parquet");
        let url = plimsoll_core::datafusion::datasource::listing::ListingTableUrl::parse(&orphan)
            .unwrap();
        engine
            .context()
            .runtime_env()
            .object_store(url.object_store())
            .unwrap()
            .put(url.prefix(), b"not a commit".to_vec().into())
            .await
            .unwrap();
        let window = |dry_run| iceberg::expire::OrphanOptions {
            older_than_ms: i64::MAX,
            dry_run,
        };
        let found = remove_orphans(engine.context(), &p, window(true))
            .await
            .unwrap();
        assert_eq!(found.orphans.len(), 1, "{found:?}");
        assert!(
            found.orphans[0].ends_with("data/failed-write-00000.parquet"),
            "{found:?}"
        );
        assert_eq!(found.deleted, 0);
        let removed = remove_orphans(engine.context(), &p, window(false))
            .await
            .unwrap();
        assert_eq!(removed.deleted, 1);
        assert!(
            remove_orphans(engine.context(), &p, window(false))
                .await
                .unwrap()
                .orphans
                .is_empty()
        );
        let ids = text(&format!("SELECT id FROM {p} ORDER BY id")).await;
        assert!(ids.contains("| 11 |"), "{ids}");
        assert_eq!(
            text(&format!(
                "SELECT id FROM bronze.\"{parted}@keep\" ORDER BY id"
            ))
            .await,
            tagged
        );
        // Scheduled maintenance (ADR 0086): the table opts in through its
        // properties; one pass compacts two small appends and expires all
        // but the head and the tag.
        Box::pin(async {
            text(&format!(
                "ALTER TABLE {p} SET TBLPROPERTIES ('plimsoll.maintenance.enabled' = 'true', \
                 'history.expire.max-snapshot-age-ms' = '0')"
            ))
            .await;
            for _ in 0..2 {
                text(&format!("INSERT INTO {p} VALUES (50, 'eu')")).await;
            }
            let hour = std::time::Duration::from_secs(3600);
            let pass = |holder| {
                crate::schedule::pass(
                    engine.context(),
                    catalog.as_ref(),
                    None,
                    Some((holder, hour)),
                )
            };
            assert!(pass("a").await >= 1);
            // ADR 0101: another coordinator finds every table claimed.
            assert_eq!(pass("b").await, 0);
            let meta = catalog
                .load_table(&TableIdent::new(Namespace::new(&["bronze"]), &parted))
                .await
                .unwrap()
                .metadata;
            assert_eq!(
                meta["snapshots"].as_array().unwrap().len(),
                2,
                "head and tag"
            );
            assert_eq!(
                meta["snapshots"].as_array().unwrap().last().unwrap()["summary"]["operation"],
                "replace"
            );
            let fifty = text(&format!("SELECT count(*) AS n FROM {p} WHERE id = 50")).await;
            assert!(fifty.contains("| 2 |"), "{fifty}");
        })
        .await;
        // Nested columns (ADR 0087): created, written, updated and merged
        // through SQL; every nested field written with its field id, so
        // the files read back by id.
        let nested = format!("{name}_n");
        Box::pin(async {
            let n = format!("bronze.{nested}");
            text(&format!(
                "CREATE TABLE {n} (id BIGINT NOT NULL, s STRUCT<a INT, tags ARRAY<VARCHAR>>)"
            ))
            .await;
            text(&format!(
                "INSERT INTO {n} VALUES (1, named_struct('a', 1, 'tags', make_array('x', 'y'))), \
                 (2, NULL)"
            ))
            .await;
            text(&format!(
                "UPDATE {n} SET s = named_struct('a', 2, 'tags', make_array('z')) WHERE id = 2"
            ))
            .await;
            text(&format!(
                "MERGE INTO {n} t USING (SELECT 1 AS id) s ON t.id = s.id \
                 WHEN MATCHED THEN DELETE"
            ))
            .await;
            let rows = text(&format!(
                "SELECT id, s['a'] AS a, s['tags'] AS tags FROM {n}"
            ))
            .await;
            assert!(rows.contains("| 2  | 2 | [z]  |"), "{rows}");
            assert!(!rows.contains("| 1  |"), "{rows}");
            // Nested evolution (ADR 0088): a field renamed, one dropped and
            // re-added (a new id, so NULL in old files), one added; old
            // files read by id.
            for change in [
                "RENAME COLUMN s.a TO alpha",
                "DROP COLUMN s.tags",
                "ADD COLUMN s.tags VARCHAR",
                "ADD COLUMN s.b DOUBLE",
            ] {
                text(&format!("ALTER TABLE {n} {change}")).await;
            }
            text(&format!(
                "INSERT INTO {n} VALUES (3, named_struct('alpha', 3, 'tags', 't', 'b', 0.5))"
            ))
            .await;
            let rows = text(&format!(
                "SELECT id, s['alpha'] AS alpha, s['tags'] AS tags, s['b'] AS b FROM {n} ORDER BY id"
            ))
            .await;
            for row in ["| 2  | 2     |      |     |", "| 3  | 3     | t    | 0.5 |"] {
                assert!(rows.contains(row), "{rows}");
            }
        })
        .await;
        // CTAS (ADR 0090): table and rows in one commit; a query failing
        // midway leaves no table behind.
        let ctas = format!("{name}_c");
        Box::pin(async {
            let out = text(&format!(
                "CREATE TABLE bronze.{ctas} PARTITION BY (region) AS SELECT id, region FROM {p}"
            ))
            .await;
            // 1, 3, 11, and the two scheduled maintenance appended.
            assert!(out.contains("| 5     |"), "{out}");
            let meta = catalog
                .load_table(&TableIdent::new(Namespace::new(&["bronze"]), &ctas))
                .await
                .unwrap()
                .metadata;
            assert_eq!(meta["snapshots"].as_array().unwrap().len(), 1);
            let failed = format!("{name}_f");
            assert!(
                engine
                    .sql(&format!(
                        "CREATE TABLE bronze.{failed} AS SELECT 1 / (id - id) AS boom FROM {p}"
                    ))
                    .await
                    .is_err()
            );
            assert!(matches!(
                catalog
                    .load_table(&TableIdent::new(Namespace::new(&["bronze"]), &failed))
                    .await,
                Err(CatalogError::TableNotFound(_))
            ));
        })
        .await;
        // Format v3 (ADR 0095): appends carry row lineage, deletes are
        // deletion vectors, one per data file (a second delete merges the
        // first); rewriting rows is refused.
        let v3 = format!("{name}_v3");
        Box::pin(async {
            let t3 = format!("bronze.{v3}");
            text(&format!(
                "CREATE TABLE {t3} (id BIGINT NOT NULL) OPTIONS ('format-version' = '3')"
            ))
            .await;
            text(&format!(
                "INSERT INTO {t3} SELECT value FROM generate_series(1, 10)"
            ))
            .await;
            for sql in [
                format!("DELETE FROM {t3} WHERE id = 1"),
                format!("DELETE FROM {t3} WHERE id = 2"),
            ] {
                assert!(text(&sql).await.contains("| 1     |"), "{sql}");
            }
            let left = text(&format!("SELECT count(*) AS n FROM {t3}")).await;
            assert!(left.contains("| 8 |"), "{left}");
            let dvs = text(&format!(
                "SELECT file_format, record_count FROM bronze.\"{v3}$files\" WHERE content = 1"
            ))
            .await;
            assert!(
                dvs.contains("| PUFFIN      | 2            |"),
                "one merged vector: {dvs}"
            );
            assert_eq!(
                dvs.lines().filter(|l| l.contains("PUFFIN")).count(),
                1,
                "{dvs}"
            );
            let meta = catalog
                .load_table(&TableIdent::new(Namespace::new(&["bronze"]), &v3))
                .await
                .unwrap()
                .metadata;
            assert_eq!(meta["next-row-id"], 10);
            assert_eq!(meta["snapshots"][0]["first-row-id"], 0);
            // Rewrites keep row lineage (ADR 0096): UPDATE, a merge's update
            // and compaction all run, and the rows come out right.
            assert!(
                text(&format!("UPDATE {t3} SET id = 30 WHERE id = 3"))
                    .await
                    .contains("| 1     |")
            );
            text(&format!(
                "MERGE INTO {t3} t USING (SELECT 4 AS id) s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET id = 40"
            ))
            .await;
            let report = compact(engine.context(), &t3).await.unwrap();
            assert!(report.files_rewritten > 0, "{report:?}");
            let ids = text(&format!("SELECT sum(id) AS s, count(*) AS n FROM {t3}")).await;
            // 5..10, 30 and 40: 45 + 70.
            assert!(ids.contains("| 115 | 8 |"), "{ids}");
        })
        .await;
        // DROP TABLE (ADR 0089) cleans up: gone for queries, and a second
        // drop only passes with IF EXISTS.
        for t in [name, parted, nested, ctas, v3] {
            text(&format!("DROP TABLE bronze.{t} PURGE")).await;
            assert!(
                engine
                    .sql(&format!("SELECT * FROM bronze.{t}"))
                    .await
                    .is_err()
            );
            assert!(engine.sql(&format!("DROP TABLE bronze.{t}")).await.is_err());
            text(&format!("DROP TABLE IF EXISTS bronze.{t}")).await;
        }
    }

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

        // Each loadTable is a planning event for the trace (ADR 0056).
        let (_, events) =
            plimsoll_core::plan_trace::collect(ctx.sql("SELECT b FROM silver.users")).await;
        let load = events
            .iter()
            .find(|e| e.name == "catalog.load_table")
            .unwrap();
        assert!(
            load.attrs.contains(&("table", "silver.users".to_owned())),
            "{load:?}"
        );
        assert!(load.attrs.contains(&("found", "true".to_owned())));
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

    #[tokio::test]
    async fn metadata_tables_list_history_through_sql() {
        let cat = Arc::new(InMemoryCatalog::default());
        let ident = TableIdent::new(Namespace::new(&["silver"]), "users");
        cat.put_table(
            ident,
            Some("file:///wh/silver/users/metadata/2.metadata.json".to_owned()),
            two_snapshots(),
        );
        let ctx = SessionContext::new();
        register(&ctx, cat).await.unwrap();
        let ints = async |sql: &str| {
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            let mut out = Vec::new();
            for b in batches {
                let col = b.column(0).as_any().downcast_ref::<arrow_i64>().unwrap();
                out.extend(col.iter().flatten());
            }
            out
        };
        assert_eq!(
            ints("SELECT snapshot_id FROM silver.\"users$snapshots\" ORDER BY committed_at").await,
            [1, 2]
        );
        assert_eq!(
            ints("SELECT snapshot_id FROM silver.\"users$refs\" WHERE name = 'v1'").await,
            [1]
        );
        assert_eq!(
            ints("SELECT count(*) FROM silver.\"users$history\"").await,
            [2]
        );
        // Unknown kinds fall back to a whole-name lookup (and are missing).
        assert!(
            ctx.sql("SELECT * FROM silver.\"users$nope\"")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn refresh_tracks_tables_and_namespaces_created_after_boot() {
        use std::sync::atomic::{AtomicBool, Ordering};
        /// The in-memory catalog, unreachable while `down` is set.
        #[derive(Default)]
        struct Flaky(InMemoryCatalog, AtomicBool);
        #[async_trait]
        impl Catalog for Flaky {
            async fn list_namespaces(&self) -> Result<Vec<Namespace>, CatalogError> {
                if self.1.load(Ordering::Relaxed) {
                    return Err(CatalogError::Http("down".into()));
                }
                self.0.list_namespaces().await
            }
            async fn list_tables(&self, ns: &Namespace) -> Result<Vec<TableIdent>, CatalogError> {
                self.0.list_tables(ns).await
            }
            async fn load_table(&self, id: &TableIdent) -> Result<LoadedTable, CatalogError> {
                self.0.load_table(id).await
            }
        }
        let cat = Arc::new(Flaky::default());
        let put = |ns: &str, t: &str| {
            cat.0.put_table(
                TableIdent::new(Namespace::new(&[ns]), t),
                Some(format!("file:///wh/{ns}/{t}/metadata/1.metadata.json")),
                empty_table("a"),
            )
        };
        put("silver", "users");
        let ctx = SessionContext::new_with_config(
            plimsoll_core::datafusion::prelude::SessionConfig::new().with_information_schema(true),
        );
        ctx.catalog("datafusion")
            .unwrap()
            .register_schema(
                "gold",
                Arc::new(plimsoll_core::datafusion::catalog::MemorySchemaProvider::new()),
            )
            .unwrap();
        // An env-registered table in `gold`: a later `gold` namespace must
        // not shadow it.
        ctx.register_batch(
            "gold.kpis",
            RecordBatch::try_from_iter([("x", Arc::new(arrow_i64::from(vec![1])) as _)]).unwrap(),
        )
        .unwrap();
        register(&ctx, cat.clone()).await.unwrap();
        let tables = async || {
            let b = ctx
                .sql(
                    "SELECT table_schema || '.' || table_name AS t FROM information_schema.tables \
                      WHERE table_schema NOT IN ('information_schema') ORDER BY 1",
                )
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            plimsoll_core::datafusion::arrow::util::pretty::pretty_format_batches(&b)
                .unwrap()
                .to_string()
                .lines()
                .filter_map(|l| {
                    l.strip_prefix("| ")
                        .map(|l| l.trim_end_matches(" |").trim().to_owned())
                })
                .filter(|l| l.contains('.'))
                .collect::<Vec<_>>()
        };
        assert_eq!(tables().await, ["gold.kpis", "silver.users"]);

        put("silver", "orders");
        put("platinum", "facts");
        put("gold", "shadow");
        let r = refresh(&ctx, cat.clone()).await.unwrap();
        assert_eq!(r.new_namespaces, ["platinum"]);
        assert_eq!(r.changed, ["silver"]);
        assert_eq!(r.skipped, [("gold".to_owned(), "schema has env tables")]);
        assert_eq!(
            tables().await,
            [
                "gold.kpis",
                "platinum.facts",
                "silver.orders",
                "silver.users"
            ]
        );
        // Unchanged catalog: nothing to report.
        let r = refresh(&ctx, cat.clone()).await.unwrap();
        assert!(r.changed.is_empty() && r.new_namespaces.is_empty());

        // Catalog down: the listing stays as it was.
        cat.1.store(true, Ordering::Relaxed);
        assert!(refresh(&ctx, cat.clone()).await.is_err());
        assert_eq!(tables().await.len(), 4);
        cat.0.drop_namespace(&Namespace::new(&["platinum"]));
        // Back up: the dropped namespace stops listing tables.
        cat.1.store(false, Ordering::Relaxed);
        let r = refresh(&ctx, cat.clone()).await.unwrap();
        assert_eq!(r.changed, ["platinum"]);
        assert_eq!(
            tables().await,
            ["gold.kpis", "silver.orders", "silver.users"]
        );
    }
}
