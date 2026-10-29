//! Boot-time table registration from env (fail fast on bad entries).
//!
//! - `PLIMSOLL_PARQUET_TABLES=bronze.trips=/data/trips/,gold.daily=/data/d.parquet`
//! - `PLIMSOLL_PG_TABLES=silver.users=public.users` (needs `PLIMSOLL_PG_URL`)
//!
//! Names may use the medallion schemas (`bronze`/`silver`/`gold`).
//! ponytail: static list at boot; upgrade path is the Iceberg REST catalog.

use plimsoll_connectors::{Connector, ParquetConnector, PostgresConnector};
use plimsoll_core::datafusion::prelude::SessionContext;

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
        ParquetConnector::try_new(name.as_str(), &path, &ctx.state())
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
mod tests {
    use super::*;

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
