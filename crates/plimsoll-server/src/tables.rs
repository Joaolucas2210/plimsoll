//! Boot-time table registration from env (fail fast on bad entries).
//!
//! - `PLIMSOLL_PARQUET_TABLES=bronze.trips=/data/trips/,gold.daily=/data/d.parquet`
//! - `PLIMSOLL_ICEBERG_TABLES=bronze.trips=s3://wh/bronze/trips` (table dir or `*.metadata.json`)
//! - `PLIMSOLL_PG_TABLES=silver.users=public.users` (needs `PLIMSOLL_PG_URL`)
//!
//! Names may use the medallion schemas (`bronze`/`silver`/`gold`).
//! ponytail: static list at boot; upgrade path is the Iceberg REST catalog
//! (`load_table` returns the `metadata-location` IcebergConnector takes).

use plimsoll_connectors::{Connector, IcebergConnector, ParquetConnector, PostgresConnector};
use plimsoll_core::datafusion::catalog::MemorySchemaProvider;
use plimsoll_core::datafusion::error::DataFusionError;
use plimsoll_core::datafusion::prelude::SessionContext;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum TablesError {
    #[error("{var}: invalid entry {entry:?}, expected name=target")]
    Invalid { var: &'static str, entry: String },
    #[error("{var}: table {name}: {source}")]
    Register {
        var: &'static str,
        name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// `a=b,c=d` -> `[(a, b), (c, d)]`.
fn entries(var: &'static str, value: &str) -> Result<Vec<(String, String)>, TablesError> {
    value
        .split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(|e| match e.split_once('=') {
            Some((n, t)) if !n.trim().is_empty() && !t.trim().is_empty() => {
                Ok((n.trim().to_owned(), t.trim().to_owned()))
            }
            _ => Err(TablesError::Invalid {
                var,
                entry: e.to_owned(),
            }),
        })
        .collect()
}

/// A `schema.table` name may name a schema nothing created (not a
/// medallion one, not a catalog namespace): it is created, in memory, so
/// `tpch.lineitem=/data/lineitem.parquet` works like `bronze.trips`.
fn ensure_schema(ctx: &SessionContext, name: &str) -> Result<(), DataFusionError> {
    let parts: Vec<&str> = name.split('.').collect();
    let [schema, _table] = parts.as_slice() else {
        return Ok(());
    };
    let default = ctx
        .state()
        .config()
        .options()
        .catalog
        .default_catalog
        .clone();
    let Some(catalog) = ctx.catalog(&default) else {
        return Ok(());
    };
    if catalog.schema(schema).is_none() {
        catalog.register_schema(schema, Arc::new(MemorySchemaProvider::new()))?;
    }
    Ok(())
}

pub async fn register_from_env(
    ctx: &SessionContext,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<usize, TablesError> {
    let mut count = 0;
    const PQ: &str = "PLIMSOLL_PARQUET_TABLES";
    for (name, path) in entries(PQ, &lookup(PQ).unwrap_or_default())? {
        let reg = |e: Box<dyn std::error::Error + Send + Sync>| TablesError::Register {
            var: PQ,
            name: name.clone(),
            source: e,
        };
        ensure_schema(ctx, &name).map_err(|e| reg(e.into()))?;
        ParquetConnector::try_new(name.as_str(), &path, &ctx.state())
            .await
            .map_err(|e| reg(e.into()))?
            .register(ctx)
            .map_err(|e| reg(e.into()))?;
        count += 1;
    }
    const ICE: &str = "PLIMSOLL_ICEBERG_TABLES";
    for (name, location) in entries(ICE, &lookup(ICE).unwrap_or_default())? {
        let reg = |e: Box<dyn std::error::Error + Send + Sync>| TablesError::Register {
            var: ICE,
            name: name.clone(),
            source: e,
        };
        ensure_schema(ctx, &name).map_err(|e| reg(e.into()))?;
        IcebergConnector::try_new(name.as_str(), &location, &ctx.state())
            .await
            .map_err(|e| reg(e.into()))?
            .register(ctx)
            .map_err(|e| reg(e.into()))?;
        count += 1;
    }
    const PG: &str = "PLIMSOLL_PG_TABLES";
    for (name, target) in entries(PG, &lookup(PG).unwrap_or_default())? {
        let Some((schema, table)) = target.split_once('.') else {
            return Err(TablesError::Invalid {
                var: PG,
                entry: target,
            });
        };
        let reg = |e: Box<dyn std::error::Error + Send + Sync>| TablesError::Register {
            var: PG,
            name: name.clone(),
            source: e,
        };
        ensure_schema(ctx, &name).map_err(|e| reg(e.into()))?;
        PostgresConnector::from_env(name.as_str(), schema, table)
            .await
            .map_err(|e| reg(e.into()))?
            .register(ctx)
            .map_err(|e| reg(e.into()))?;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn env_tables_create_their_schema() {
        let ctx = SessionContext::new();
        ensure_schema(&ctx, "tpch.lineitem").unwrap();
        let catalog = ctx.catalog("datafusion").unwrap();
        assert!(catalog.schema("tpch").is_some());
        // Existing schemas are kept as they are; bare names need none.
        let public = catalog.schema("public").unwrap();
        ensure_schema(&ctx, "public.t").unwrap();
        assert!(Arc::ptr_eq(&public, &catalog.schema("public").unwrap()));
        ensure_schema(&ctx, "t").unwrap();
        ensure_schema(&ctx, "a.b.c").unwrap();
        let mut names = catalog.schema_names();
        names.sort();
        assert_eq!(names, ["public", "tpch"]);
    }

    #[test]
    fn parses_entries_and_rejects_garbage() {
        let ok = entries("V", " bronze.a=/x/ , b=y.z,").ok();
        assert_eq!(
            ok,
            Some(vec![
                ("bronze.a".into(), "/x/".into()),
                ("b".into(), "y.z".into())
            ])
        );
        assert!(entries("V", "no_equals").is_err());
        assert!(entries("V", "=x").is_err());
    }
}
