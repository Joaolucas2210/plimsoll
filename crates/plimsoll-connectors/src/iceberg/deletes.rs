//! Merge-on-read deletes.
//!
//! Decoded delete files are cached in DataFusion's file metadata cache, sized
//! to the pool's cache reserve (ADR 0018): they are immutable, so a path is a
//! complete key and repeated queries skip the read and the decode.
//!
//! Position deletes: `(file_path, pos)` pairs. Spec v2: they apply to a data
//! file when `file_path` matches and the data file's sequence number is <= the
//! delete file's. Plimsoll turns the surviving positions of each data file
//! into a file-level `RowSelection`, so DataFusion's Parquet scan never
//! decodes deleted rows (ADR 0014).
//!
//! Deletion vectors (v3): the same positions as a Roaring bitmap blob inside a
//! Puffin file, one per data file, applied like position deletes (ADR 0017).
//!
//! Equality deletes: rows of key values. Spec v2: they apply to a data file
//! with a *strictly lower* sequence number in the same partition (or to every
//! partition when the delete's spec is unpartitioned). Loaded here as key
//! batches; the scan anti-joins against them (ADR 0016).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use apache_avro::types::Value as Avro;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{Field, Int64Type, Schema};
use datafusion::catalog::Session;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::cache::cache_manager::{CachedFileMetadataEntry, FileMetadata};
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::object_store::ObjectMeta;
use datafusion::object_store::path::Path;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::{
    ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use futures::StreamExt;
use plimsoll_core::storage::ScopedStore;
use roaring::RoaringTreemap;

use crate::ConnectorError;

/// A live position delete file (or v3 deletion vector) from a delete manifest.
pub(super) struct DeleteFile {
    pub path: String,
    /// Data sequence number (inherited from the manifest when absent).
    pub seq: i64,
    /// v2 `referenced_data_file`: when set, the only data file it touches.
    pub referenced: Option<String>,
    /// Deletion vector: `(content_offset, content_size_in_bytes)` of its blob
    /// in the Puffin file at `path`; `referenced` is then required.
    pub blob: Option<(u64, u64)>,
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
/// named after the table's `key` fields. Cached per (file, key types); on a
/// miss the file is read whole. The keys are reserved while planning (the hash
/// join reserves them again when it builds, ADR 0016).
// ponytail: eight arguments; a struct for the key columns (fields, ids,
// mapping) if a ninth ever arrives.
#[allow(clippy::too_many_arguments)]
pub(super) async fn equality_keys(
    state: &dyn Session,
    scoped: Option<&ScopedStore>,
    path: &str,
    key: &[Field],
    ids: &[i64],
    mapping: Option<&super::field_ids::NameMapping>,
    reservation: &MemoryReservation,
    hits: &AtomicUsize,
) -> Result<Vec<RecordBatch>> {
    let types = key
        .iter()
        .map(|f| format!("{}:{}", f.name(), f.data_type()))
        .collect::<Vec<_>>()
        .join(",");
    let (keys, held) = cached(
        state,
        &format!("{path}?eq={types}"),
        reservation,
        hits,
        async {
            let bytes = super::read(path, state, scoped).await?;
            reservation.try_grow(bytes.len())?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?;
            // Key columns by field id when the file carries ids (a delete
            // written before a rename uses the old name, ADR 0040), else by
            // the current name.
            let file_names: Vec<String> = key
                .iter()
                .zip(ids)
                .map(|(f, id)| {
                    let fields = builder.schema().fields();
                    // By id, then through the name mapping (ADR 0042), then
                    // by the current name.
                    fields
                        .iter()
                        .find(|c| super::field_ids::field_id(c) == Some(*id))
                        .or_else(|| {
                            let m = mapping?;
                            fields.iter().find(|c| m.get(c.name()) == Some(id))
                        })
                        .map_or_else(|| f.name().clone(), |c| c.name().clone())
                })
                .collect();
            let mask = ProjectionMask::columns(
                builder.parquet_schema(),
                file_names.iter().map(String::as_str),
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
                    .zip(&file_names)
                    .map(|(f, name)| {
                        let c = batch.column_by_name(name).ok_or_else(|| {
                            invalid_delete(
                                path,
                                "equality delete",
                                &format!("missing key column {} (field {name:?})", f.name()),
                            )
                        })?;
                        Ok(cast(c, f.data_type())?)
                    })
                    .collect::<Result<Vec<_>>>()?;
                out.push(RecordBatch::try_new(schema.clone(), columns)?);
            }
            reservation.shrink(bytes.len());
            let size = out.iter().map(RecordBatch::get_array_memory_size).sum();
            Ok((out, size))
        },
    )
    .await?;
    // Kept reserved until planning ends, hit or miss.
    if held == 0 {
        reservation.try_grow(keys.iter().map(RecordBatch::get_array_memory_size).sum())?;
    }
    Ok(keys.as_ref().clone())
}

/// Data file path -> its data sequence number, for the files being scanned.
pub(super) type Targets<'a> = HashMap<&'a str, i64>;

/// Deleted positions per data file (sorted, unique), read from the delete
/// files that apply to `targets`, `concurrency` at a time. Decoded delete
/// files are cached (ADR 0018); the positions a query keeps are reserved in
/// `reservation` (8 bytes each) because they grow with the number of deletes.
pub(super) async fn positions(
    state: &dyn Session,
    scoped: Option<&ScopedStore>,
    deletes: &[DeleteFile],
    targets: &Targets<'_>,
    reservation: &MemoryReservation,
    concurrency: usize,
    hits: &AtomicUsize,
) -> Result<HashMap<String, Vec<u64>>> {
    // Fetch + decode concurrently; merge sequentially in manifest order.
    // Indices, not references: a borrowed stream item trips the `Send`
    // check of the `async_trait` scan future (higher-ranked lifetimes).
    let mut loaded = futures::stream::iter(0..deletes.len())
        .map(|i| load(state, scoped, &deletes[i], targets, reservation, hits))
        .buffered(concurrency);
    let mut out: HashMap<String, Vec<u64>> = HashMap::new();
    let word = std::mem::size_of::<u64>();
    while let Some(next) = loaded.next().await {
        let (delete, decoded, held) = next?;
        match decoded {
            Decoded::Skip => {}
            Decoded::Vector(target, bitmap) => {
                let n = usize::try_from(bitmap.len())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                reservation.try_grow(n * word)?;
                out.entry(target).or_default().extend(bitmap.iter());
            }
            Decoded::Positions(all) => {
                for (path, positions) in all.iter() {
                    // Spec: applies only when data seq <= delete seq.
                    match targets.get(path.as_str()) {
                        Some(&data_seq) if data_seq <= delete.seq => {}
                        _ => continue,
                    }
                    reservation.try_grow(positions.len() * word)?;
                    out.entry(path.clone()).or_default().extend(positions);
                }
            }
        }
        reservation.shrink(held);
    }
    drop(loaded);
    for positions in out.values_mut() {
        positions.sort_unstable();
        positions.dedup();
    }
    Ok(out)
}

/// One delete file's decoded content, before filtering by target.
enum Decoded {
    /// Applies to nothing scanned.
    Skip,
    /// A deletion vector for one data file.
    Vector(String, Arc<RoaringTreemap>),
    /// Every position of a position delete file, by data file.
    Positions(Arc<HashMap<String, Vec<u64>>>),
}

/// Fetches and decodes one delete file (or DV blob), via the cache. `held`
/// bytes are reserved on a miss until the caller has merged the result.
async fn load<'a>(
    state: &dyn Session,
    scoped: Option<&ScopedStore>,
    delete: &'a DeleteFile,
    targets: &Targets<'_>,
    reservation: &MemoryReservation,
    hits: &AtomicUsize,
) -> Result<(&'a DeleteFile, Decoded, usize)> {
    // `referenced_data_file` lets us skip delete files for pruned data.
    if delete
        .referenced
        .as_deref()
        .is_some_and(|r| !targets.contains_key(r))
    {
        return Ok((delete, Decoded::Skip, 0));
    }
    let Some((offset, size)) = delete.blob else {
        let (all, held) = cached(state, &delete.path, reservation, hits, async {
            decode_positions(&delete.path, state, scoped, reservation).await
        })
        .await?;
        return Ok((delete, Decoded::Positions(all), held));
    };
    let Some(target) = delete.referenced.as_deref() else {
        return Err(invalid_delete(
            &delete.path,
            "deletion vector",
            "no referenced_data_file",
        ));
    };
    match targets.get(target) {
        Some(&data_seq) if data_seq <= delete.seq => {}
        _ => return Ok((delete, Decoded::Skip, 0)),
    }
    let key = format!("{}#{offset}+{size}", delete.path);
    let (bitmap, held) = cached(state, &key, reservation, hits, async {
        let blob = super::read_range(&delete.path, offset..offset + size, state, scoped).await?;
        reservation.try_grow(blob.len())?;
        let bitmap =
            deletion_vector(&blob).map_err(|e| invalid_delete(&delete.path, "deletion vector", &e));
        reservation.shrink(blob.len());
        let bitmap = bitmap?;
        let bytes = bitmap.serialized_size();
        Ok((bitmap, bytes))
    })
    .await?;
    Ok((delete, Decoded::Vector(target.to_owned(), bitmap), held))
}

/// Every `(file_path, pos)` of a position delete file, read whole. Returns
/// the map and its size in bytes.
async fn decode_positions(
    path: &str,
    state: &dyn Session,
    scoped: Option<&ScopedStore>,
    reservation: &MemoryReservation,
) -> Result<(HashMap<String, Vec<u64>>, usize)> {
    let bytes = super::read(path, state, scoped).await?;
    reservation.try_grow(bytes.len())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?;
    let mask = ProjectionMask::columns(builder.parquet_schema(), ["file_path", "pos"]);
    let mut out: HashMap<String, Vec<u64>> = HashMap::new();
    let mut size = 0;
    for batch in builder.with_projection(mask).build()? {
        let batch = batch?;
        let (Some(paths), Some(pos)) = (
            batch.column_by_name("file_path"),
            batch.column_by_name("pos"),
        ) else {
            return Err(invalid(path, "missing file_path/pos columns"));
        };
        let paths = paths
            .as_string_opt::<i32>()
            .ok_or_else(|| invalid(path, "file_path is not a string column"))?;
        let pos = pos
            .as_primitive_opt::<Int64Type>()
            .ok_or_else(|| invalid(path, "pos is not a long column"))?;
        for i in 0..batch.num_rows() {
            if paths.is_null(i) || pos.is_null(i) {
                return Err(invalid(path, "null file_path/pos"));
            }
            let p = u64::try_from(pos.value(i)).map_err(|_| invalid(path, "negative pos"))?;
            let file = paths.value(i);
            reservation.try_grow(std::mem::size_of::<u64>())?;
            size += std::mem::size_of::<u64>();
            if let Some(v) = out.get_mut(file) {
                v.push(p);
            } else {
                reservation.try_grow(file.len())?;
                size += file.len();
                out.insert(file.to_owned(), vec![p]);
            }
        }
    }
    reservation.shrink(bytes.len() + size);
    Ok((out, size))
}

/// Decoded delete state in DataFusion's file metadata cache, whose budget is
/// the pool's cache reserve (ADR 0018).
struct Cached<T> {
    value: Arc<T>,
    bytes: usize,
}

impl<T: Send + Sync + 'static> FileMetadata for Cached<T> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn memory_size(&self) -> usize {
        self.bytes
    }
    fn extra_info(&self) -> datafusion::common::HashMap<String, String> {
        Default::default()
    }
}

/// `load`ed value for `key`, or the cached one. Delete files and manifests
/// are immutable, so the path (plus blob range / key types) is a complete
/// key. On a miss
/// the value's bytes are reserved and returned as `held`: the caller shrinks
/// them once done (the cache, if it kept the value, accounts for it).
pub(super) async fn cached<T: Send + Sync + 'static>(
    state: &dyn Session,
    key: &str,
    reservation: &MemoryReservation,
    hits: &AtomicUsize,
    load: impl Future<Output = Result<(T, usize)>>,
) -> Result<(Arc<T>, usize)> {
    let cache = state.runtime_env().cache_manager.get_file_metadata_cache();
    let meta = ObjectMeta {
        location: Path::from(format!("plimsoll-deletes/{key}")),
        last_modified: DateTime::<Utc>::UNIX_EPOCH,
        size: 0,
        e_tag: None,
        version: None,
    };
    if let Some(entry) = cache.get(&meta.location)
        && entry.is_valid_for(&meta)
        && let Some(hit) = entry.file_metadata.as_any().downcast_ref::<Cached<T>>()
    {
        hits.fetch_add(1, Ordering::Relaxed);
        return Ok((hit.value.clone(), 0));
    }
    let (value, bytes) = load.await?;
    reservation.try_grow(bytes)?;
    let value = Arc::new(value);
    let entry = Cached {
        value: value.clone(),
        bytes,
    };
    cache.put(
        &meta.location.clone(),
        CachedFileMetadataEntry::new(meta, Arc::new(entry)),
    );
    Ok((value, bytes))
}

/// Magic of a `deletion-vector-v1` blob.
const DV_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

/// Decodes a `deletion-vector-v1` blob: big-endian length of magic + bitmap,
/// magic, portable 64-bit Roaring bitmap, big-endian CRC-32 of magic + bitmap.
fn deletion_vector(blob: &[u8]) -> std::result::Result<RoaringTreemap, String> {
    let n = blob.len();
    if n < 12 {
        return Err(format!("blob of {n} bytes"));
    }
    let be = |b: &[u8]| u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let body = &blob[4..n - 4];
    if usize::try_from(be(&blob[..4])).ok() != Some(body.len()) {
        return Err("length prefix does not match the blob size".to_owned());
    }
    if body[..4] != DV_MAGIC {
        return Err("bad magic".to_owned());
    }
    if crc32(body) != be(&blob[n - 4..]) {
        return Err("CRC mismatch".to_owned());
    }
    RoaringTreemap::deserialize_from(&body[4..]).map_err(|e| e.to_string())
}

/// CRC-32 (IEEE), bitwise. ponytail: ~10 ms/MB; DV blobs are KBs. Table-driven
/// or `crc32fast` if profiles ever show it.
fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & (crc & 1).wrapping_neg());
        }
    }
    !crc
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

    fn blob(bitmap: &RoaringTreemap) -> Vec<u8> {
        let mut body = DV_MAGIC.to_vec();
        bitmap.serialize_into(&mut body).unwrap();
        let mut out = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
        out.extend(&body);
        out.extend(crc32(&body).to_be_bytes());
        out
    }

    #[test]
    fn deletion_vectors_decode_and_are_verified() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926, "IEEE check value");
        // Positions beyond 2^32 exercise the 64-bit (treemap) layout.
        let bitmap: RoaringTreemap = [0u64, 7, 70_000, (1 << 32) + 3].into_iter().collect();
        let mut b = blob(&bitmap);
        assert_eq!(deletion_vector(&b).unwrap(), bitmap);
        let last = b.len() - 1;
        b[last] ^= 1;
        assert!(deletion_vector(&b).unwrap_err().contains("CRC"));
        let mut b = blob(&bitmap);
        b[4] = 0;
        assert!(deletion_vector(&b).unwrap_err().contains("magic"));
        assert!(deletion_vector(&blob(&bitmap)[..20]).is_err(), "truncated");
    }
}
