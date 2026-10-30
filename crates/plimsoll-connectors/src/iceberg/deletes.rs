//! Merge-on-read deletes.
//!
//! Position deletes: `(file_path, pos)` pairs. Spec v2: they apply to a data
//! file when `file_path` matches and the data file's sequence number is <= the
//! delete file's. Plimsoll turns the surviving positions of each data file
//! into a file-level `RowSelection`, so DataFusion's Parquet scan never
//! decodes deleted rows (ADR 0014).
//!
//! Equality deletes: rows of key values. Spec v2: they apply to a data file
//! with a *strictly lower* sequence number in the same partition (or to every
//! partition when the delete's spec is unpartitioned). Loaded here as key
//! batches; the scan anti-joins against them (ADR 0016).

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::types::Value as Avro;
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{Field, Int64Type, Schema};
use datafusion::catalog::Session;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::{
    ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use plimsoll_core::storage::ScopedStore;

use crate::ConnectorError;

/// A live position delete file from a delete manifest.
pub(super) struct DeleteFile {
    pub path: String,
    /// Data sequence number (inherited from the manifest when absent).
    pub seq: i64,
    /// v2 `referenced_data_file`: when set, the only data file it touches.
    pub referenced: Option<String>,
}

/// A live equality delete file from a delete manifest.
pub(super) struct EqualityDelete {
    pub path: String,
    pub seq: i64,
    /// Partition spec id and partition tuple of the delete file.
    pub spec: i64,
    pub partition: Option<Avro>,
    /// `equality_ids`: the key columns, as field ids.
    pub ids: Vec<i64>,
}

impl EqualityDelete {
    /// Spec v2 applicability to a data file (`unpartitioned`: the delete's
    /// spec has no non-void field, so it applies across partitions).
    pub fn applies(
        &self,
        seq: i64,
        spec: i64,
        partition: Option<&Avro>,
        unpartitioned: bool,
    ) -> bool {
        self.seq > seq
            && (unpartitioned || (self.spec == spec && self.partition.as_ref() == partition))
    }
}

/// Key rows of an equality delete file, cast to the table's types, columns
/// named after the table's `key` fields. The whole file is read; its bytes and
/// the decoded keys are reserved while planning (the hash join reserves them
/// again when it builds, ADR 0016).
pub(super) async fn equality_keys(
    state: &dyn Session,
    scoped: Option<&ScopedStore>,
    path: &str,
    key: &[Field],
    reservation: &MemoryReservation,
) -> Result<Vec<RecordBatch>> {
    let bytes = super::read(path, state, scoped).await?;
    reservation.try_grow(bytes.len())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?;
    let mask = ProjectionMask::columns(
        builder.parquet_schema(),
        key.iter().map(|f| f.name().as_str()),
    );
    let schema = Arc::new(Schema::new(
        key.iter()
            .map(|f| f.clone().with_nullable(true))
            .collect::<Vec<_>>(),
    ));
    let mut out = Vec::new();
    for batch in builder.with_projection(mask).build()? {
        let batch = batch?;
        let columns = key
            .iter()
            .map(|f| {
                let c = batch.column_by_name(f.name()).ok_or_else(|| {
                    invalid_delete(
                        path,
                        "equality delete",
                        &format!("missing key column {}", f.name()),
                    )
                })?;
                Ok(cast(c, f.data_type())?)
            })
            .collect::<Result<Vec<_>>>()?;
        let keys = RecordBatch::try_new(schema.clone(), columns)?;
        reservation.try_grow(keys.get_array_memory_size())?;
        out.push(keys);
    }
    reservation.shrink(bytes.len());
    Ok(out)
}

/// Data file path -> its data sequence number, for the files being scanned.
pub(super) type Targets<'a> = HashMap<&'a str, i64>;

/// Deleted positions per data file (sorted, unique), read from the delete
/// files that apply to `targets`. Positions are reserved in `reservation`
/// (8 bytes each) because they grow with the number of deletes.
pub(super) async fn positions(
    state: &dyn Session,
    scoped: Option<&ScopedStore>,
    deletes: &[DeleteFile],
    targets: &Targets<'_>,
    reservation: &MemoryReservation,
) -> Result<HashMap<String, Vec<u64>>> {
    let mut out: HashMap<String, Vec<u64>> = HashMap::new();
    for delete in deletes {
        // `referenced_data_file` lets us skip delete files for pruned data.
        if delete
            .referenced
            .as_deref()
            .is_some_and(|r| !targets.contains_key(r))
        {
            continue;
        }
        // Whole file in memory, reserved while decoded. Position delete files
        // are small relative to data (two narrow columns).
        let bytes = super::read(&delete.path, state, scoped).await?;
        reservation.try_grow(bytes.len())?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?;
        let mask = ProjectionMask::columns(builder.parquet_schema(), ["file_path", "pos"]);
        let batches = builder.with_projection(mask).build()?;
        for batch in batches {
            let batch = batch?;
            let (Some(paths), Some(pos)) = (
                batch.column_by_name("file_path"),
                batch.column_by_name("pos"),
            ) else {
                return Err(invalid(&delete.path, "missing file_path/pos columns"));
            };
            let paths = paths
                .as_string_opt::<i32>()
                .ok_or_else(|| invalid(&delete.path, "file_path is not a string column"))?;
            let pos = pos
                .as_primitive_opt::<Int64Type>()
                .ok_or_else(|| invalid(&delete.path, "pos is not a long column"))?;
            for i in 0..batch.num_rows() {
                if paths.is_null(i) || pos.is_null(i) {
                    return Err(invalid(&delete.path, "null file_path/pos"));
                }
                let path = paths.value(i);
                // Spec: applies only when data seq <= delete seq.
                let Some(&data_seq) = targets.get(path) else {
                    continue;
                };
                if data_seq > delete.seq {
                    continue;
                }
                let p = u64::try_from(pos.value(i))
                    .map_err(|_| invalid(&delete.path, "negative pos"))?;
                reservation.try_grow(std::mem::size_of::<u64>())?;
                out.entry(path.to_owned()).or_default().push(p);
            }
        }
        reservation.shrink(bytes.len());
    }
    for positions in out.values_mut() {
        positions.sort_unstable();
        positions.dedup();
    }
    Ok(out)
}

/// Rows `0..rows` minus the sorted, unique `deleted` positions.
pub(super) fn selection(deleted: &[u64], rows: u64) -> Result<RowSelection> {
    let mut selectors = Vec::new();
    let mut next = 0u64;
    for &p in deleted {
        if p >= rows {
            return Err(DataFusionError::from(ConnectorError::InvalidMetadata(
                format!("position delete {p} beyond the file's {rows} rows"),
            )));
        }
        if p > next {
            selectors.push(RowSelector::select(to_usize(p - next)?));
        }
        selectors.push(RowSelector::skip(1));
        next = p + 1;
    }
    if rows > next {
        selectors.push(RowSelector::select(to_usize(rows - next)?));
    }
    Ok(RowSelection::from(selectors))
}

fn to_usize(n: u64) -> Result<usize> {
    usize::try_from(n).map_err(|e| DataFusionError::External(Box::new(e)))
}

fn invalid(path: &str, what: &str) -> DataFusionError {
    invalid_delete(path, "position delete", what)
}

fn invalid_delete(path: &str, kind: &str, what: &str) -> DataFusionError {
    DataFusionError::from(ConnectorError::InvalidMetadata(format!(
        "{kind} file {path}: {what}"
    )))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn runs(s: &RowSelection) -> Vec<(bool, usize)> {
        s.iter().map(|r| (r.skip, r.row_count)).collect()
    }

    #[test]
    fn selection_skips_exactly_the_deleted_rows() {
        let s = selection(&[0, 3, 4, 9], 10).unwrap();
        // Adjacent skips (3, 4) merge into one run.
        assert_eq!(
            runs(&s),
            [(true, 1), (false, 2), (true, 2), (false, 4), (true, 1)]
        );
        assert_eq!(s.row_count(), 6);
        assert_eq!(selection(&[], 5).unwrap().row_count(), 5);
        assert_eq!(selection(&[0, 1, 2], 3).unwrap().row_count(), 0);
        assert!(selection(&[5], 5).is_err(), "position past the end");
    }
}
