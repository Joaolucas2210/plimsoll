//! `system.runtime.queries`: schema and snapshot source.
//!
//! Plain Rust on purpose: the control plane does not depend on DataFusion.
//! The compute plane maps [`QUERIES_COLUMNS`] to an Arrow schema and builds one
//! `RecordBatch` per scan from [`queries_snapshot`] (column order = field order
//! of [`QueryInfo`]).

use crate::governance::{QueryInfo, QueryRegistry};

pub const CATALOG: &str = "system";
pub const SCHEMA: &str = "runtime";
pub const QUERIES_TABLE: &str = "queries";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// Arrow `UInt64`.
    UInt64,
    /// Arrow `Utf8`.
    Utf8,
    /// Arrow `Timestamp(Millisecond, Some("UTC"))`.
    TimestampMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Column {
    pub name: &'static str,
    pub ty: ColumnType,
    pub nullable: bool,
}

const fn col(name: &'static str, ty: ColumnType, nullable: bool) -> Column {
    Column { name, ty, nullable }
}

pub const QUERIES_COLUMNS: &[Column] = &[
    col("query_id", ColumnType::UInt64, false),
    col("user", ColumnType::Utf8, false),
    col("resource_group", ColumnType::Utf8, false),
    col("sql", ColumnType::Utf8, false),
    col("state", ColumnType::Utf8, false), // QueryState::as_str()
    col("created_at", ColumnType::TimestampMillis, false),
    col("started_at", ColumnType::TimestampMillis, true),
    col("finished_at", ColumnType::TimestampMillis, true),
    col("error", ColumnType::Utf8, true),
];

/// Point-in-time rows for `system.runtime.queries`.
///
/// ponytail: full copy per scan; no filter pushdown. Fine while the registry
/// is bounded (`QueryRegistry::new(retain_terminal)`).
pub fn queries_snapshot(registry: &QueryRegistry) -> Vec<QueryInfo> {
    registry.snapshot()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::governance::{QueryRequest, QueryState};

    #[test]
    fn snapshot_reflects_registry_states() {
        let reg = QueryRegistry::new(10);
        let r = QueryRequest {
            user: "ana".into(),
            resource_group: "etl".into(),
            sql: "select * from gold.orders".into(),
        };
        let a = reg.register(&r);
        let b = reg.register(&r);
        reg.mark_running(b);
        reg.finish(a, QueryState::Failed, Some("memory limit".into()));

        let rows = queries_snapshot(&reg);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].state.as_str(), "failed");
        assert_eq!(rows[0].error.as_deref(), Some("memory limit"));
        assert_eq!(rows[1].state, QueryState::Running);
        assert!(rows[1].started_at_ms.is_some());
        assert_eq!(QUERIES_COLUMNS.len(), 9);
        assert_eq!(QUERIES_COLUMNS[0].name, "query_id");
    }
}
