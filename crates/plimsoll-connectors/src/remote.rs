//! Scan fragments on worker nodes (ADR 0058).
//!
//! A coordinator plans an Iceberg scan as usual (pruning, exact filters),
//! then splits the kept data files into fragments, one per worker. Each
//! worker re-opens the table at the same snapshot, re-plans the scan for
//! its files (deletes included) and streams the rows back as Arrow IPC
//! (ADR 0037). [`RemoteScanExec`] has one partition per fragment and
//! decodes as bytes arrive, so the coordinator holds a window, not a
//! result; dropping the stream (cancel, timeout, client gone) closes the
//! request and the worker stops.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamDecoder;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::stream::{self, BoxStream, StreamExt};
use plimsoll_core::engine::QueryTag;
use plimsoll_core::memory::AccountedExec;
use serde_json::{Value as Json, json};

/// Certificates trusted for HTTPS to workers besides the system roots
/// (`PLIMSOLL_TLS_CA`, ADR 0099): set once, at boot.
static TRUSTED: std::sync::OnceLock<Vec<reqwest::Certificate>> = std::sync::OnceLock::new();

/// Trusts the certificates of `pem` (a bundle) for every worker client
/// made from now on; how many. Call before any `Workers` is built.
pub fn trust(pem: &[u8]) -> Result<usize, reqwest::Error> {
    let certs = reqwest::Certificate::from_pem_bundle(pem)?;
    let n = certs.len();
    let _ = TRUSTED.set(certs);
    Ok(n)
}

/// How long a worker connection may take to open.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Worker endpoints and the shared secret they accept, set as a session
/// config extension on coordinators.
#[derive(Clone)]
pub struct Workers {
    /// As configured: worker base URLs, or `dns+http(s)://name:port`
    /// entries whose addresses are workers (ADR 0070).
    seeds: Vec<String>,
    token: String,
    client: reqwest::Client,
    /// The current workers. Static seeds start healthy; discovered ones
    /// join unhealthy and are checked in the round that found them.
    members: Arc<std::sync::RwLock<Vec<Member>>>,
    /// The largest build side sent to workers, as Arrow IPC (ADR 0099);
    /// a larger one is joined here. 0: no broadcasts.
    broadcast_limit: usize,
    /// Build sides already collected, by what they read (ADR 0116).
    builds: Arc<BuildCache>,
    /// A join bucket over this many bytes, and four times its side's
    /// median bucket, is split across reducers (ADR 0117). 0: never.
    skew_bytes: u64,
    /// How many hot buckets were split (ADR 0117).
    splits: Arc<std::sync::atomic::AtomicU64>,
}

/// The default [`Workers::with_skew`]: 64 MiB.
pub const DEFAULT_SKEW_BYTES: u64 = 64 << 20;

/// Collected broadcast build sides, as the Arrow IPC sent to workers, by
/// [`build_key`] (ADR 0116): a build side reading only Iceberg tables at
/// pinned snapshots is the same rows every time. Each entry holds its own
/// reservation in the memory pool, released when it is evicted. Oldest
/// used first out, within `max` bytes; 0 keeps nothing.
#[derive(Debug, Default)]
struct BuildCache {
    max: usize,
    entries: std::sync::Mutex<
        std::collections::VecDeque<(
            String,
            Bytes,
            Arc<datafusion::execution::memory_pool::MemoryReservation>,
        )>,
    >,
    hits: std::sync::atomic::AtomicU64,
}

impl BuildCache {
    fn get(
        &self,
        key: &str,
    ) -> Option<(
        Bytes,
        Arc<datafusion::execution::memory_pool::MemoryReservation>,
    )> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let i = entries.iter().position(|(k, _, _)| k == key)?;
        let entry = entries.remove(i)?;
        let found = (entry.1.clone(), entry.2.clone());
        entries.push_back(entry);
        self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Some(found)
    }

    fn put(
        &self,
        key: String,
        bytes: Bytes,
        memory: Arc<datafusion::execution::memory_pool::MemoryReservation>,
    ) {
        if bytes.len() > self.max {
            return;
        }
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.retain(|(k, _, _)| *k != key);
        let mut held: usize = entries.iter().map(|(_, b, _)| b.len()).sum();
        while held + bytes.len() > self.max {
            let Some((_, evicted, _)) = entries.pop_front() else {
                break;
            };
            held -= evicted.len();
        }
        entries.push_back((key, bytes, memory));
    }
}

/// What a build side reads, as a cache key (ADR 0116), or `None` unless
/// every source is an Iceberg scan pinned to a snapshot. Each node by its
/// one-line display and output fields (filters, projections and joins
/// all show there), except accounted scans, whose labels carry planning
/// counters: those by their pinned table, snapshot and schema.
fn build_key(plan: &Arc<dyn ExecutionPlan>) -> Option<String> {
    use datafusion::physical_plan::displayable;
    plimsoll_core::result_cache::key("", plan)?;
    fn walk(plan: &Arc<dyn ExecutionPlan>, depth: usize, out: &mut String) {
        use std::fmt::Write;
        let line = match plan.as_ref().downcast_ref::<AccountedExec>() {
            Some(a) => format!("Accounted[{}]", a.pinned().unwrap_or("-")),
            None => displayable(plan.as_ref()).one_line().to_string(),
        };
        let fields: Vec<String> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}:{}", f.name(), f.data_type()))
            .collect();
        let _ = writeln!(out, "{depth} {} [{}]", line.trim_end(), fields.join(","));
        for c in plan.children() {
            walk(c, depth + 1, out);
        }
    }
    let mut out = String::new();
    walk(plan, 0, &mut out);
    Some(out)
}

/// The default [`Workers::with_broadcast_limit`]: 64 MiB.
pub const DEFAULT_BROADCAST_LIMIT: usize = 64 << 20;

#[derive(Debug, Clone)]
struct Member {
    url: String,
    /// Index of the seed it came from.
    seed: usize,
    /// Last health check passed (ADR 0059).
    healthy: bool,
}

impl fmt::Debug for Workers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never the token.
        f.debug_struct("Workers")
            .field("seeds", &self.seeds)
            .finish()
    }
}

impl Workers {
    pub fn new(seeds: Vec<String>, token: String) -> Result<Self, reqwest::Error> {
        let seeds: Vec<String> = seeds
            .into_iter()
            .map(|u| u.trim_end_matches('/').to_owned())
            .collect();
        let members = seeds
            .iter()
            .enumerate()
            .filter(|(_, s)| !s.starts_with("dns+"))
            .map(|(seed, url)| Member {
                url: url.clone(),
                seed,
                healthy: true,
            })
            .collect();
        Ok(Self {
            seeds,
            token,
            // A dead host often drops the connection attempt instead of
            // refusing it: without a bound, a lost worker would hold the
            // query until its deadline instead of failing over (ADR 0072).
            // ponytail: fixed; a knob if workers sit behind slow links.
            client: TRUSTED
                .get()
                .into_iter()
                .flatten()
                .fold(
                    reqwest::Client::builder().connect_timeout(CONNECT_TIMEOUT),
                    |b, ca| b.add_root_certificate(ca.clone()),
                )
                .build()?,
            members: Arc::new(std::sync::RwLock::new(members)),
            broadcast_limit: DEFAULT_BROADCAST_LIMIT,
            builds: Arc::default(),
            skew_bytes: DEFAULT_SKEW_BYTES,
            splits: Arc::default(),
        })
    }

    /// Splits join buckets over `bytes` (and four times their side's
    /// median) across reducers (ADR 0117). 0 never splits.
    pub fn with_skew(mut self, bytes: u64) -> Self {
        self.skew_bytes = bytes;
        self
    }

    /// How many hot join buckets were split (ADR 0117).
    pub fn skew_splits(&self) -> u64 {
        self.splits.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Keeps up to `bytes` of collected build sides that read pinned
    /// snapshots only, and sends them again instead of collecting them
    /// again (ADR 0116). 0, the default, keeps none.
    pub fn with_build_cache(mut self, bytes: usize) -> Self {
        self.builds = Arc::new(BuildCache {
            max: bytes,
            ..BuildCache::default()
        });
        self
    }

    /// How many broadcasts reused a cached build side (ADR 0116).
    pub fn build_cache_hits(&self) -> u64 {
        self.builds.hits.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Caps the build side a broadcast join sends (ADR 0099): one larger
    /// runs on the coordinator instead, its build side as collected.
    pub fn with_broadcast_limit(mut self, bytes: usize) -> Self {
        self.broadcast_limit = bytes;
        self
    }

    fn members(&self) -> Vec<Member> {
        self.members
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// A request to a member, authenticated with the shared token: also
    /// how coordinators reach each other (ADR 0102).
    pub fn request(&self, method: reqwest::Method, url: &str) -> reqwest::RequestBuilder {
        self.client.request(method, url).bearer_auth(&self.token)
    }

    /// Every current worker, healthy or not.
    pub fn urls(&self) -> Vec<String> {
        self.members().into_iter().map(|m| m.url).collect()
    }

    /// Workers whose last `/readyz` answered 200: the ones fragments go
    /// to. A draining or dead worker drops out until it answers again.
    pub fn healthy_urls(&self) -> Vec<String> {
        self.members()
            .into_iter()
            .filter(|m| m.healthy)
            .map(|m| m.url)
            .collect()
    }

    /// Re-resolves `dns+` seeds (ADR 0070): each address is a worker at
    /// the seed's scheme and port. A name that fails to resolve keeps its
    /// last known workers; one that resolves to nothing has none.
    async fn discover(&self) {
        let current = self.members();
        let mut next = Vec::new();
        for (seed, entry) in self.seeds.iter().enumerate() {
            let Some(name) = entry.strip_prefix("dns+") else {
                next.extend(current.iter().filter(|m| m.seed == seed).cloned());
                continue;
            };
            let resolved = match reqwest::Url::parse(name) {
                Ok(url) => match (url.host_str(), url.port_or_known_default()) {
                    (Some(host), Some(port)) => {
                        let host = host.trim_start_matches('[').trim_end_matches(']');
                        tokio::net::lookup_host((host, port))
                            .await
                            .ok()
                            .map(|addrs| (url.scheme().to_owned(), addrs.collect::<Vec<_>>()))
                    }
                    _ => None,
                },
                Err(_) => None,
            };
            let Some((scheme, mut addrs)) = resolved else {
                next.extend(current.iter().filter(|m| m.seed == seed).cloned());
                continue;
            };
            addrs.sort();
            addrs.dedup();
            for addr in addrs {
                let url = format!("{scheme}://{addr}");
                let healthy = current.iter().any(|m| m.url == url && m.healthy);
                next.push(Member { url, seed, healthy });
            }
        }
        *self.members.write().unwrap_or_else(|e| e.into_inner()) = next;
    }

    /// One round: discovery, then health checks of every current worker;
    /// returns how many are healthy.
    pub async fn check_health(&self, timeout: Duration) -> usize {
        self.discover().await;
        let urls = self.urls();
        let checks = urls.iter().map(|url| {
            let request = self
                .client
                .get(format!("{url}/readyz"))
                .timeout(timeout)
                .send();
            async move { request.await.is_ok_and(|r| r.status().is_success()) }
        });
        let results = futures::future::join_all(checks).await;
        let mut members = self.members.write().unwrap_or_else(|e| e.into_inner());
        for m in members.iter_mut() {
            if let Some(i) = urls.iter().position(|u| *u == m.url) {
                m.healthy = results[i];
            }
        }
        members.iter().filter(|m| m.healthy).count()
    }
}

/// What a worker scans: a table at a snapshot, some of its data files and
/// the columns to return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fragment {
    pub metadata_location: String,
    pub snapshot_id: Option<i64>,
    pub files: Vec<String>,
    pub columns: Vec<String>,
    /// Aggregate on the worker and return partial states (ADR 0061).
    pub aggregate: Option<AggregateSpec>,
    /// Row filters as SQL over the table's columns, all to be applied
    /// before projecting or aggregating (ADR 0062).
    pub filters: Vec<String>,
    /// Shuffle map (ADR 0064): hash-partition the aggregated fragment
    /// into `partitions` buckets, keep them, and return bucket `index`.
    pub bucket: Option<Bucket>,
    /// Join the fragment's rows (the probe side) with the build side sent
    /// alongside (ADR 0066).
    pub broadcast: Option<Broadcast>,
    /// Write the rows as data files of this table and return the files
    /// written (ADR 0091), one JSON per row.
    pub write: Option<WriteTo>,
    /// A `DELETE` or `UPDATE` of this fragment's files (ADR 0093): their
    /// position deletes (and rewritten rows) written, the files returned
    /// as for `write`.
    pub change: Option<RowChange>,
    /// A `MERGE`'s pass over this fragment's files (ADR 0094), with the
    /// source attached: `WHEN MATCHED` and `NOT MATCHED BY SOURCE` applied,
    /// the files written returned, then which source rows paired.
    pub merge: Option<MergeSpec>,
    /// Columns computed after `columns`, `(name, SQL over them)`: the
    /// expression join keys DataFusion projects below a join (ADR 0111).
    /// Sent only when set, so a worker that does not know them refuses the
    /// fragment.
    pub computed: Vec<(String, String)>,
}

/// A `MERGE` as a worker runs it (ADR 0094): SQL over the merged row,
/// whose columns are named `t0…` (target, in table order) then `s0…`
/// (source, in the attachment's order), so equal names on both sides
/// stay apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeSpec {
    pub target_keys: Vec<String>,
    pub source_keys: Vec<String>,
    pub residual: Option<String>,
    pub matched: Vec<ClauseSpec>,
    pub not_matched_by_source: Vec<ClauseSpec>,
}

/// One `WHEN` clause: its condition, and `DELETE` or the updated row (a
/// value per target column).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClauseSpec {
    pub predicate: Option<String>,
    pub update: Option<Vec<String>>,
}

impl MergeSpec {
    fn to_json(&self) -> Json {
        let clauses = |c: &[ClauseSpec]| {
            c.iter()
                .map(|c| json!({"predicate": c.predicate, "update": c.update}))
                .collect::<Vec<_>>()
        };
        json!({
            "target_keys": self.target_keys,
            "source_keys": self.source_keys,
            "residual": self.residual,
            "matched": clauses(&self.matched),
            "not_matched_by_source": clauses(&self.not_matched_by_source),
        })
    }

    fn from_json(v: &Json) -> Result<Option<Self>, String> {
        if v.is_null() {
            return Ok(None);
        }
        let clauses = |key: &str| -> Result<Vec<ClauseSpec>, String> {
            v[key]
                .as_array()
                .ok_or(format!("`merge.{key}` must be an array"))?
                .iter()
                .map(|c| {
                    Ok(ClauseSpec {
                        predicate: c["predicate"].as_str().map(str::to_owned),
                        update: match &c["update"] {
                            Json::Null => None,
                            u => Some(names(u, "merge clause update")?),
                        },
                    })
                })
                .collect()
        };
        Ok(Some(Self {
            target_keys: names(&v["target_keys"], "merge.target_keys")?,
            source_keys: names(&v["source_keys"], "merge.source_keys")?,
            residual: v["residual"].as_str().map(str::to_owned),
            matched: clauses("matched")?,
            not_matched_by_source: clauses("not_matched_by_source")?,
        }))
    }
}

/// A row-level change of a fragment's files (ADR 0093), as SQL over the
/// table's columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowChange {
    /// The rows changed: the `WHERE` (row filters included).
    pub predicate: String,
    /// `UPDATE`: every column's new value, in table order. `None`: a
    /// `DELETE`.
    pub update: Option<Vec<String>>,
}

/// The table a worker writes its rows into (ADR 0091, 0092).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTo {
    /// The table's metadata file, as the coordinator loaded it.
    pub metadata_location: String,
    /// A staged create's metadata, which has no file yet (CTAS, ADR
    /// 0092): used instead of reading `metadata_location`.
    pub metadata: Option<Json>,
    /// The output columns written, by position, in the table's order (a
    /// reduce's output, ADR 0092); `None`: all, as they are.
    pub picks: Option<Vec<usize>>,
}

impl WriteTo {
    fn to_json(&self) -> Json {
        json!({"metadata_location": self.metadata_location, "metadata": self.metadata, "picks": self.picks})
    }

    fn from_json(v: &Json) -> Result<Option<Self>, String> {
        if v.is_null() {
            return Ok(None);
        }
        Ok(Some(Self {
            metadata_location: v["metadata_location"]
                .as_str()
                .ok_or("`write.metadata_location` is required")?
                .to_owned(),
            metadata: Some(v["metadata"].clone()).filter(|m| !m.is_null()),
            picks: projection(&v["picks"], "write.picks")?,
        }))
    }
}

/// A broadcast hash join on a worker: the build side (attached to the
/// request) joined with the fragment, build side on the left, as the
/// coordinator's `CollectLeft` join planned it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Broadcast {
    pub join_type: String,
    /// Build-side columns, paired with the fragment's `probe_keys`.
    pub build_keys: Vec<String>,
    pub probe_keys: Vec<String>,
    /// Output columns kept, by position in build ++ probe.
    pub projection: Option<Vec<usize>>,
    /// A join whose build-only rows the coordinator adds (left, full,
    /// left semi and anti, ADR 0097): the worker joins (inner, or right
    /// for full) and returns each output row's build row index as a last
    /// `__b_row` column; for semi and anti joins, only the indices.
    pub matched: bool,
    /// The join's residual filter (ADR 0098).
    pub filter: Option<JoinFilter>,
}

/// Which bucket of which shuffle map a reducing worker asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bucket {
    pub shuffle: String,
    /// This map's position among the shuffle's maps.
    pub map: usize,
    pub partitions: usize,
    pub index: usize,
    /// Output columns hashed (by position): group keys or join keys.
    pub keys: Vec<usize>,
    /// Keep only bucket `index`, dropping the rest as the map runs: a map
    /// re-run for one reducer (ADR 0072).
    pub only: bool,
    /// How many reduces read this bucket (ADR 0117): the other side of a
    /// split hot bucket is read by each of its parts. 1 normally.
    pub readers: usize,
    /// Run the map, take nothing, and return each bucket's size in bytes
    /// (one `bytes` row per bucket), so the coordinator can find hot ones
    /// (ADR 0117).
    pub sizes: bool,
}

/// A hot join bucket's part (ADR 0117): this reduce reads, on the split
/// side, only `maps` (positions among that side's maps), and the other
/// side's bucket, which `readers` parts read in all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Split {
    pub left: bool,
    pub maps: Vec<usize>,
    pub readers: usize,
}

/// A shuffle reduce (ADR 0064, 0065): fetch bucket `partition` (of
/// `partitions`) of every map, each kept by the worker at its URL. Without
/// `join`, the maps are aggregated fragments: merge their partial states
/// into final values. With it, the maps are the join's left side and
/// `join.right` its right side: join the two buckets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reduce {
    pub shuffle: String,
    pub partition: usize,
    pub partitions: usize,
    pub maps: Vec<(String, Fragment)>,
    pub join: Option<JoinSpec>,
    /// A retried reduce (ADR 0072): every map runs again for this bucket
    /// only, under this reduce's own shuffle id.
    pub only: bool,
    /// Write the reduced rows as data files and return the files written
    /// (ADR 0092).
    pub write: Option<WriteTo>,
    /// One part of a hot join bucket (ADR 0117).
    pub split: Option<Split>,
}

/// An equi-join of two shuffled sides, on columns of their fragments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSpec {
    /// `JoinType`'s name (`Inner`, `Left`, `LeftSemi`, …).
    pub join_type: String,
    pub right: Vec<(String, Fragment)>,
    pub left_keys: Vec<String>,
    pub right_keys: Vec<String>,
    /// Output columns kept, by position in left ++ right.
    pub projection: Option<Vec<usize>>,
    /// The join's residual filter (ADR 0098).
    pub filter: Option<JoinFilter>,
    /// A side whose maps are partial aggregates (ADR 0112): the reducer
    /// merges them, then keeps these columns of the merged output, named
    /// `c0…`, as the join's side. Keys and filter columns on that side
    /// use those names.
    pub left_picks: Option<Vec<usize>>,
    pub right_picks: Option<Vec<usize>>,
}

impl JoinSpec {
    /// The columns a side's maps hash, by position in their output: its
    /// keys among the fragment's columns, then its computed ones (ADR
    /// 0065, 0111); for a merged aggregation, the picked group columns its
    /// `c{i}` keys name (ADR 0112). The reducers and the coordinator's size
    /// round (ADR 0117) must hash alike.
    pub fn hash_keys(&self, left: bool, f: &Fragment) -> Result<Vec<usize>, String> {
        let (names, picks) = match left {
            true => (&self.left_keys, &self.left_picks),
            false => (&self.right_keys, &self.right_picks),
        };
        names
            .iter()
            .map(|k| match picks {
                Some(picks) => k
                    .strip_prefix('c')
                    .and_then(|i| i.parse::<usize>().ok())
                    .and_then(|i| picks.get(i).copied())
                    .ok_or_else(|| format!("join key {k} is not a picked column")),
                None => f
                    .columns
                    .iter()
                    .chain(f.computed.iter().map(|(n, _)| n))
                    .position(|c| c == k)
                    .ok_or_else(|| format!("join key {k} is not a map column")),
            })
            .collect()
    }
}

/// A join's non-equi filter as a worker plans it (ADR 0098): portable SQL
/// over columns named `f0…`, each a column of one side, by name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinFilter {
    pub sql: String,
    /// Per `f{i}`: whether it is on the left (build, for a broadcast)
    /// side, and its name there.
    pub columns: Vec<(bool, String)>,
}

impl JoinFilter {
    fn to_json(&self) -> Json {
        json!({"sql": self.sql, "columns": self.columns.iter().map(|(l, n)| json!([l, n])).collect::<Vec<_>>()})
    }

    fn from_json(v: &Json) -> Result<Option<Self>, String> {
        if v.is_null() {
            return Ok(None);
        }
        let columns = v["columns"]
            .as_array()
            .ok_or("`filter.columns` must be an array")?
            .iter()
            .map(|c| match (c[0].as_bool(), c[1].as_str()) {
                (Some(l), Some(n)) => Ok((l, n.to_owned())),
                _ => Err("`filter.columns[]` is [left, name]".to_owned()),
            })
            .collect::<Result<_, String>>()?;
        Ok(Some(Self {
            sql: v["sql"]
                .as_str()
                .ok_or("`filter.sql` is required")?
                .to_owned(),
            columns,
        }))
    }
}

/// The join's filter for workers (ADR 0098): `Some(None)` without one,
/// `None` when it does not travel exactly as SQL.
fn join_filter(
    filter: Option<&datafusion::physical_plan::joins::utils::JoinFilter>,
    (ls, rs): (SchemaRef, SchemaRef),
    config: &ConfigOptions,
    // Sides whose columns a reducer names by position, `c{i}`: merged
    // aggregations (ADR 0112).
    positional: (bool, bool),
) -> Option<Option<JoinFilter>> {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::common::JoinSide;
    use datafusion::physical_expr::expressions::Column;
    let Some(filter) = filter else {
        return Some(None);
    };
    let mut columns = Vec::new();
    for c in filter.column_indices() {
        let (left, schema, by_position) = match c.side {
            JoinSide::Left => (true, &ls, positional.0),
            JoinSide::Right => (false, &rs, positional.1),
            JoinSide::None => return None,
        };
        let name = schema.fields().get(c.index)?.name().clone();
        columns.push((
            left,
            if by_position {
                format!("c{}", c.index)
            } else {
                name
            },
        ));
    }
    let named = Arc::new(Schema::new(
        filter
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| Field::new(format!("f{i}"), f.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let expr = filter
        .expression()
        .clone()
        .transform(|e| {
            Ok(match e.as_ref().downcast_ref::<Column>() {
                Some(c) => {
                    Transformed::yes(Arc::new(Column::new(&format!("f{}", c.index()), c.index()))
                        as Arc<dyn datafusion::physical_expr::PhysicalExpr>)
                }
                None => Transformed::no(e),
            })
        })
        .data()
        .ok()?;
    let sql = crate::portable::physical_sql_equivalent(&expr, &named, config)?;
    Some(Some(JoinFilter { sql, columns }))
}

fn maps_json(maps: &[(String, Fragment)]) -> Json {
    maps.iter()
        .map(|(url, f)| json!({"url": url, "fragment": f.to_json()}))
        .collect()
}

fn maps_from_json(v: &Json, key: &str) -> Result<Vec<(String, Fragment)>, String> {
    v.as_array()
        .ok_or(format!("`{key}` must be an array"))?
        .iter()
        .map(|m| {
            let url = m["url"].as_str().ok_or(format!("`{key}[].url`"))?;
            Ok((url.to_owned(), Fragment::from_json(&m["fragment"])?))
        })
        .collect()
}

fn names(v: &Json, key: &str) -> Result<Vec<String>, String> {
    v.as_array()
        .ok_or(format!("`{key}` must be an array"))?
        .iter()
        .map(|s| {
            s.as_str()
                .map(str::to_owned)
                .ok_or(format!("`{key}`: strings"))
        })
        .collect()
}

fn projection(v: &Json, key: &str) -> Result<Option<Vec<usize>>, String> {
    match v {
        Json::Null => Ok(None),
        p => p
            .as_array()
            .ok_or(format!("`{key}` must be an array"))?
            .iter()
            .map(|i| count(i, key))
            .collect::<Result<_, _>>()
            .map(Some),
    }
}

fn count(v: &Json, key: &str) -> Result<usize, String> {
    v.as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .ok_or(format!("`{key}` must be a count"))
}

impl Reduce {
    pub fn to_json(&self) -> Json {
        json!({"reduce": {
            "shuffle": self.shuffle,
            "partition": self.partition,
            "partitions": self.partitions,
            "only": self.only,
            "write": self.write.as_ref().map(WriteTo::to_json),
            "split": self.split.as_ref().map(|s| json!({
                "left": s.left, "maps": s.maps, "readers": s.readers,
            })),
            "maps": maps_json(&self.maps),
            "join": self.join.as_ref().map(|j| json!({
                "join_type": j.join_type,
                "right": maps_json(&j.right),
                "left_keys": j.left_keys,
                "right_keys": j.right_keys,
                "projection": j.projection,
                "filter": j.filter.as_ref().map(JoinFilter::to_json),
                "left_picks": j.left_picks,
                "right_picks": j.right_picks,
            })),
        }})
    }

    pub fn from_json(v: &Json) -> Result<Self, String> {
        let r = &v["reduce"];
        let maps = maps_from_json(&r["maps"], "reduce.maps")?;
        let join = match &r["join"] {
            Json::Null => None,
            j => Some(JoinSpec {
                join_type: j["join_type"]
                    .as_str()
                    .ok_or("`reduce.join.join_type` is required")?
                    .to_owned(),
                right: maps_from_json(&j["right"], "reduce.join.right")?,
                left_keys: names(&j["left_keys"], "reduce.join.left_keys")?,
                right_keys: names(&j["right_keys"], "reduce.join.right_keys")?,
                projection: projection(&j["projection"], "reduce.join.projection")?,
                filter: JoinFilter::from_json(&j["filter"])?,
                left_picks: projection(&j["left_picks"], "reduce.join.left_picks")?,
                right_picks: projection(&j["right_picks"], "reduce.join.right_picks")?,
            }),
        };
        if join.is_none() && maps.iter().any(|(_, f)| f.aggregate.is_none()) {
            return Err("an aggregation shuffle's maps must aggregate".into());
        }
        let partitions = count(&r["partitions"], "reduce.partitions")?;
        let partition = count(&r["partition"], "reduce.partition")?;
        if partition >= partitions {
            return Err("`reduce.partition` must be below `reduce.partitions`".into());
        }
        Ok(Self {
            shuffle: r["shuffle"]
                .as_str()
                .ok_or("`reduce.shuffle` is required")?
                .to_owned(),
            partition,
            partitions,
            maps,
            join,
            only: r["only"].as_bool().unwrap_or(false),
            write: WriteTo::from_json(&r["write"])?,
            split: match &r["split"] {
                Json::Null => None,
                s => Some(Split {
                    left: s["left"]
                        .as_bool()
                        .ok_or("`reduce.split.left` is a boolean")?,
                    maps: projection(&s["maps"], "reduce.split.maps")?.unwrap_or_default(),
                    readers: count(&s["readers"], "reduce.split.readers")?,
                }),
            },
        })
    }
}

/// What `/v1/fragment` is asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
// One per request, parsed and consumed at once: its size does not matter.
#[allow(clippy::large_enum_variant)]
pub enum Task {
    Scan(Fragment),
    Reduce(Reduce),
    /// Drop every map of this shuffle, and of the shuffles derived from
    /// it by retries (`<id>~…`): the query is over (ADR 0073).
    Release(String),
}

impl Task {
    pub fn from_json(v: &Json) -> Result<Self, String> {
        if let Some(r) = v.get("release") {
            return r["shuffle"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(|s| Self::Release(s.to_owned()))
                .ok_or_else(|| "`release.shuffle` is required".to_owned());
        }
        if v.get("reduce").is_some() {
            Reduce::from_json(v).map(Self::Reduce)
        } else {
            Fragment::from_json(v).map(Self::Scan)
        }
    }
}

/// `<aggregates> GROUP BY <group_by>` over the fragment: what a worker
/// computes for a partial aggregation (ADR 0061). Keys and arguments are
/// SQL expressions over the table's columns (ADR 0063). Each aggregate is
/// (`count` | `sum` | `min` | `max`, argument), `count` with no argument
/// meaning `count(*)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateSpec {
    pub group_by: Vec<String>,
    pub aggregates: Vec<(String, Option<String>)>,
}

impl Fragment {
    pub fn to_json(&self) -> Json {
        let mut v = json!({
            "metadata_location": self.metadata_location,
            "snapshot_id": self.snapshot_id,
            "files": self.files,
            "columns": self.columns,
            "filters": self.filters,
            "bucket": self.bucket.as_ref().map(|b| json!({
                "shuffle": b.shuffle, "map": b.map, "partitions": b.partitions, "index": b.index, "keys": b.keys, "only": b.only,
                "readers": b.readers, "sizes": b.sizes,
            })),
            "broadcast": self.broadcast.as_ref().map(|b| json!({
                "join_type": b.join_type,
                "build_keys": b.build_keys,
                "probe_keys": b.probe_keys,
                "projection": b.projection,
                "matched": b.matched,
                "filter": b.filter.as_ref().map(JoinFilter::to_json),
            })),
            "write": self.write.as_ref().map(WriteTo::to_json),
            "change": self.change.as_ref().map(|c| json!({"predicate": c.predicate, "update": c.update})),
            "merge": self.merge.as_ref().map(MergeSpec::to_json),
            "aggregate": self.aggregate.as_ref().map(|a| json!({
                "group_by": a.group_by,
                "aggregates": a.aggregates.iter().map(|(f, c)| json!({"fn": f, "column": c})).collect::<Vec<_>>(),
            })),
        });
        if !self.computed.is_empty() {
            v["computed"] = self.computed.iter().map(|(n, e)| json!([n, e])).collect();
        }
        v
    }

    pub fn from_json(v: &Json) -> Result<Self, String> {
        // A field this worker does not know could change the answer (a
        // filter it would skip): refuse it rather than ignore it.
        const KNOWN: [&str; 12] = [
            "computed",
            "merge",
            "change",
            "write",
            "metadata_location",
            "snapshot_id",
            "files",
            "columns",
            "filters",
            "bucket",
            "broadcast",
            "aggregate",
        ];
        if let Some(unknown) = v
            .as_object()
            .ok_or("a fragment is a JSON object")?
            .keys()
            .find(|k| !KNOWN.contains(&k.as_str()))
        {
            return Err(format!(
                "unknown fragment field `{unknown}`: worker version mismatch?"
            ));
        }
        let strings = |key: &str| -> Result<Vec<String>, String> {
            v[key]
                .as_array()
                .ok_or(format!("`{key}` must be an array"))?
                .iter()
                .map(|s| {
                    s.as_str()
                        .map(str::to_owned)
                        .ok_or(format!("`{key}`: strings"))
                })
                .collect()
        };
        Ok(Self {
            metadata_location: v["metadata_location"]
                .as_str()
                .ok_or("`metadata_location` is required")?
                .to_owned(),
            snapshot_id: v["snapshot_id"].as_i64(),
            files: strings("files")?,
            columns: strings("columns")?,
            write: WriteTo::from_json(&v["write"])?,
            merge: MergeSpec::from_json(&v["merge"])?,
            computed: match &v["computed"] {
                Json::Null => Vec::new(),
                c => c
                    .as_array()
                    .ok_or("`computed` must be an array")?
                    .iter()
                    .map(|p| match (p[0].as_str(), p[1].as_str()) {
                        (Some(n), Some(e)) => Ok((n.to_owned(), e.to_owned())),
                        _ => Err("`computed[]` is [name, sql]".to_owned()),
                    })
                    .collect::<Result<_, _>>()?,
            },
            change: match &v["change"] {
                Json::Null => None,
                c => Some(RowChange {
                    predicate: c["predicate"]
                        .as_str()
                        .ok_or("`change.predicate` is required")?
                        .to_owned(),
                    update: match &c["update"] {
                        Json::Null => None,
                        u => Some(names(u, "change.update")?),
                    },
                }),
            },
            filters: match &v["filters"] {
                Json::Null => Vec::new(),
                _ => strings("filters")?,
            },
            broadcast: match &v["broadcast"] {
                Json::Null => None,
                b => Some(Broadcast {
                    join_type: b["join_type"]
                        .as_str()
                        .ok_or("`broadcast.join_type` is required")?
                        .to_owned(),
                    build_keys: names(&b["build_keys"], "broadcast.build_keys")?,
                    probe_keys: names(&b["probe_keys"], "broadcast.probe_keys")?,
                    projection: projection(&b["projection"], "broadcast.projection")?,
                    matched: b["matched"].as_bool().unwrap_or(false),
                    filter: JoinFilter::from_json(&b["filter"])?,
                }),
            },
            bucket: match &v["bucket"] {
                Json::Null => None,
                b => {
                    let n = |k: &str| {
                        b[k].as_u64()
                            .and_then(|n| usize::try_from(n).ok())
                            .ok_or(format!("`bucket.{k}` must be a count"))
                    };
                    let (partitions, index) = (n("partitions")?, n("index")?);
                    if index >= partitions {
                        return Err("`bucket.index` must be below `bucket.partitions`".into());
                    }
                    Some(Bucket {
                        shuffle: b["shuffle"]
                            .as_str()
                            .ok_or("`bucket.shuffle` is required")?
                            .to_owned(),
                        map: n("map")?,
                        partitions,
                        index,
                        keys: b["keys"]
                            .as_array()
                            .ok_or("`bucket.keys` must be an array")?
                            .iter()
                            .map(|k| count(k, "bucket.keys[]"))
                            .collect::<Result<_, _>>()?,
                        only: b["only"].as_bool().unwrap_or(false),
                        readers: b["readers"]
                            .as_u64()
                            .and_then(|n| usize::try_from(n).ok())
                            .unwrap_or(1)
                            .max(1),
                        sizes: b["sizes"].as_bool().unwrap_or(false),
                    })
                }
            },
            aggregate: match &v["aggregate"] {
                Json::Null => None,
                a => Some(AggregateSpec {
                    group_by: a["group_by"]
                        .as_array()
                        .ok_or("`aggregate.group_by` must be an array")?
                        .iter()
                        .map(|c| c.as_str().map(str::to_owned).ok_or("group_by: strings"))
                        .collect::<Result<_, _>>()?,
                    aggregates: a["aggregates"]
                        .as_array()
                        .ok_or("`aggregate.aggregates` must be an array")?
                        .iter()
                        .map(|e| {
                            Ok((
                                e["fn"].as_str().ok_or("aggregate fn")?.to_owned(),
                                e["column"].as_str().map(str::to_owned),
                            ))
                        })
                        .collect::<Result<_, String>>()?,
                }),
            },
        })
    }
}

/// One partition per fragment; each runs on its worker.
#[derive(Debug)]
pub struct RemoteScanExec {
    schema: SchemaRef,
    workers: Arc<Workers>,
    /// Healthy workers at planning time: owners and retry targets.
    urls: Vec<String>,
    /// In partition order.
    fragments: Vec<Assigned>,
    properties: Arc<PlanProperties>,
}

#[derive(Debug, Clone)]
struct Assigned {
    /// Index into `urls` of the worker that owns it.
    worker: usize,
    fragment: Fragment,
    bytes: u64,
}

impl RemoteScanExec {
    /// Assigns `files` (path, size in bytes) to `urls` by bytes (ADR
    /// 0060): largest first, each to the least-loaded worker (LPT), so
    /// one huge file does not land beside many others. Unknown sizes
    /// count as one byte. Empty fragments are not sent.
    pub fn new(
        schema: SchemaRef,
        workers: Arc<Workers>,
        urls: &[String],
        metadata_location: &str,
        snapshot_id: Option<i64>,
        mut files: Vec<(String, Option<u64>)>,
        columns: Vec<String>,
    ) -> Self {
        files.sort_by(|a, b| {
            b.1.unwrap_or(1)
                .cmp(&a.1.unwrap_or(1))
                .then_with(|| a.0.cmp(&b.0))
        });
        let mut load: Vec<(u64, Vec<String>)> = vec![(0, Vec::new()); urls.len()];
        for (path, size) in files {
            let Some((bytes, owned)) = load.iter_mut().min_by_key(|(b, _)| *b) else {
                break;
            };
            *bytes += size.unwrap_or(1);
            owned.push(path);
        }
        let fragments: Vec<Assigned> = load
            .into_iter()
            .enumerate()
            .filter(|(_, (_, files))| !files.is_empty())
            .map(|(worker, (bytes, files))| Assigned {
                worker,
                bytes,
                fragment: Fragment {
                    metadata_location: metadata_location.to_owned(),
                    snapshot_id,
                    files,
                    columns: columns.clone(),
                    aggregate: None,
                    filters: Vec::new(),
                    bucket: None,
                    broadcast: None,
                    write: None,
                    change: None,
                    merge: None,
                    computed: Vec::new(),
                },
            })
            .collect();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(fragments.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            schema,
            workers,
            urls: urls.to_vec(),
            fragments,
            properties,
        }
    }

    /// Every fragment applies `filters` (SQL, ADR 0062) on its worker.
    pub fn with_filters(mut self, filters: Vec<String>) -> Self {
        for a in &mut self.fragments {
            a.fragment.filters = filters.clone();
        }
        self
    }

    /// The same fragments, each aggregated on its worker into the partial
    /// states `schema` describes (ADR 0061).
    pub fn with_aggregate(&self, spec: AggregateSpec, schema: SchemaRef) -> Self {
        let mut out = self.with_schema(schema);
        for a in &mut out.fragments {
            a.fragment.aggregate = Some(spec.clone());
        }
        out
    }

    /// The same aggregated fragments, shuffled between their workers by
    /// group key and merged there (ADR 0064): each partition returns the
    /// final values, in `schema`, of the groups hashed to it.
    fn shuffled(&self, schema: SchemaRef) -> RemoteReduceExec {
        let shuffle = shuffle_id();
        let partitions = self.fragments.len();
        let maps = self.maps();
        let reduces = self
            .fragments
            .iter()
            .enumerate()
            .map(|(partition, a)| {
                (
                    self.urls[a.worker].clone(),
                    Reduce {
                        shuffle: shuffle.clone(),
                        partition,
                        partitions,
                        maps: maps.clone(),
                        join: None,
                        only: false,
                        write: None,
                        split: None,
                    },
                )
            })
            .collect();
        RemoteReduceExec::new(schema, self.workers.clone(), reduces, "aggregate".into())
    }

    /// (owner url, fragment) per fragment.
    fn maps(&self) -> Vec<(String, Fragment)> {
        self.fragments
            .iter()
            .map(|a| (self.urls[a.worker].clone(), a.fragment.clone()))
            .collect()
    }

    fn with_schema(&self, schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(self.fragments.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            schema,
            workers: self.workers.clone(),
            urls: self.urls.clone(),
            fragments: self.fragments.clone(),
            properties,
        }
    }

    /// The same fragments, each writing its rows as data files of the
    /// table at `target` instead of returning them (ADR 0091), with
    /// `columns` in the target's order. `None` for fragments that do not
    /// return plain rows (aggregated, joined or shuffled).
    pub(crate) fn writing(&self, target: WriteTo, columns: Vec<String>) -> Option<Self> {
        if self.fragments.iter().any(|a| {
            a.fragment.aggregate.is_some()
                || a.fragment.broadcast.is_some()
                || a.fragment.bucket.is_some()
        }) {
            return None;
        }
        let mut out = self.with_schema(written_schema());
        for a in &mut out.fragments {
            a.fragment.write = Some(target.clone());
            a.fragment.columns = columns.clone();
        }
        Some(out)
    }

    /// The same fragments, each applying `change` to its files and
    /// returning the files it wrote (ADR 0093).
    pub(crate) fn changing(&self, change: RowChange) -> Self {
        let mut out = self.with_schema(written_schema());
        for a in &mut out.fragments {
            a.fragment.change = Some(change.clone());
        }
        out
    }

    /// The same fragments, each running `spec` over its files with the
    /// source attached (ADR 0094).
    pub(crate) fn merging(&self, spec: MergeSpec) -> Self {
        let mut out = self.with_schema(written_schema());
        for a in &mut out.fragments {
            a.fragment.merge = Some(spec.clone());
        }
        out
    }

    /// Fragment `partition`'s output, with `attachment` (Arrow IPC) after
    /// its JSON (ADR 0066, 0094).
    pub(crate) fn attached_stream(
        &self,
        partition: usize,
        tag: Option<Arc<QueryTag>>,
        attachment: Bytes,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        self.fragment_stream(partition, tag, Some(attachment))
    }

    /// The columns each fragment returns, by name.
    pub(crate) fn columns(&self) -> Vec<String> {
        self.fragments
            .first()
            .map(|a| a.fragment.columns.clone())
            .unwrap_or_default()
    }

    /// Fragments, i.e. shuffle partitions when shuffled.
    pub fn fragment_count(&self) -> usize {
        self.fragments.len()
    }

    /// (worker url, files, bytes) per fragment, in partition order.
    pub fn assignments(&self) -> Vec<(String, usize, u64)> {
        self.fragments
            .iter()
            .map(|a| (self.urls[a.worker].clone(), a.fragment.files.len(), a.bytes))
            .collect()
    }
}

impl DisplayAs for RemoteScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let files: usize = self.fragments.iter().map(|a| a.fragment.files.len()).sum();
        let bytes: Vec<u64> = self.fragments.iter().map(|a| a.bytes).collect();
        let aggregate = self
            .fragments
            .first()
            .and_then(|a| a.fragment.aggregate.as_ref())
            .map_or(String::new(), |a| {
                let aggs: Vec<String> = a
                    .aggregates
                    .iter()
                    .map(|(f, c)| format!("{f}({})", c.as_deref().unwrap_or("*")))
                    .collect();
                format!(" aggregate=[{}] group_by={:?}", aggs.join(", "), a.group_by)
            });
        let filters = self
            .fragments
            .first()
            .filter(|a| !a.fragment.filters.is_empty())
            .map_or(String::new(), |a| {
                format!(" filters={:?}", a.fragment.filters)
            });
        write!(
            f,
            "RemoteScanExec: fragments={} files={files} bytes={bytes:?}{filters}{aggregate}",
            self.fragments.len(),
        )
    }
}

impl ExecutionPlan for RemoteScanExec {
    fn name(&self) -> &str {
        "RemoteScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let tag = ctx.session_config().get_extension::<QueryTag>();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            self.fragment_stream(partition, tag, None),
        )))
    }
}

impl RemoteScanExec {
    /// Fragment `partition` on its owner, then one other healthy worker
    /// (ADR 0060), with `attachment` after its JSON (ADR 0066).
    fn fragment_stream(
        &self,
        partition: usize,
        tag: Option<Arc<QueryTag>>,
        attachment: Option<Bytes>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        let Some(assigned) = self.fragments.get(partition) else {
            // No files at all: an empty partition.
            return stream::empty().boxed();
        };
        let n = self.urls.len();
        let mut body = assigned.fragment.to_json().to_string().into_bytes();
        if let Some(attachment) = attachment {
            body.push(b'\n');
            body.extend_from_slice(&attachment);
        }
        let body = Bytes::from(body);
        let targets = (0..n.min(2))
            .map(|i| (self.urls[(assigned.worker + i) % n].clone(), body.clone()))
            .collect();
        with_retry(self.workers.clone(), targets, tag, self.schema.clone())
    }

    /// The same fragments, each joined on its worker with a build side
    /// sent alongside (ADR 0066), returning `schema`.
    fn broadcast(&self, spec: Broadcast, schema: SchemaRef) -> Self {
        let mut out = self.with_schema(schema);
        for a in &mut out.fragments {
            a.fragment.broadcast = Some(spec.clone());
        }
        out
    }
}

/// A `CollectLeft` hash join whose probe side is a remote scan (ADR
/// 0066). The build side (the child) runs here once, as DataFusion would
/// collect it anyway; it is sent as Arrow IPC with every probe fragment,
/// and each worker joins its fragment against it. One partition per
/// fragment.
#[derive(Debug)]
pub struct RemoteBroadcastExec {
    build: Arc<dyn ExecutionPlan>,
    probe: Arc<RemoteScanExec>,
    join_type: String,
    /// The join as planned, run here when the build side is over the
    /// limit (ADR 0099).
    join: Arc<dyn ExecutionPlan>,
    /// The build side as IPC, and its memory, shared by all partitions;
    /// or the local join, when it was too large to send.
    shipped: Arc<tokio::sync::OnceCell<Result<Built, String>>>,
    /// The joined rows written as data files of this table, where they
    /// are joined (ADR 0122): the files written come back, one JSON each.
    write: Option<WriteTo>,
}

/// A collected build side: sent to the workers, or too large and joined
/// here (ADR 0099).
#[derive(Debug, Clone)]
enum Built {
    Shipped {
        bytes: Bytes,
        /// Held as long as the plan: the bytes' reservation.
        _memory: Arc<datafusion::execution::memory_pool::MemoryReservation>,
    },
    Local(Arc<dyn ExecutionPlan>),
}

/// `join` (a `CollectLeft` hash join as planned) over the build side
/// already collected, with its probe side as planned: the coordinator's
/// own join, for a build side over the broadcast limit (ADR 0099).
fn local_join(
    join: &Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let build =
        datafusion::datasource::memory::MemorySourceConfig::try_new_exec(&[batches], schema, None)?;
    let probe = join
        .children()
        .get(1)
        .map(|c| (*c).clone())
        .ok_or_else(|| DataFusionError::Internal("a join without its probe side".into()))?;
    use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
    let local = join.clone().replace_children(
        vec![build, probe],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )?;
    Ok(Arc::new(CoalescePartitionsExec::new(local)))
}

impl RemoteBroadcastExec {
    fn new(
        build: Arc<dyn ExecutionPlan>,
        probe: Arc<RemoteScanExec>,
        join_type: String,
        join: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            build,
            probe,
            join_type,
            join,
            shipped: Arc::default(),
            write: None,
        }
    }

    /// The same join, each probe fragment writing its joined rows as data
    /// files of `target` (columns picked in the table's order) and
    /// returning the files written (ADR 0122).
    pub(crate) fn writing(&self, target: WriteTo) -> Self {
        let mut probe = self.probe.with_schema(written_schema());
        for a in &mut probe.fragments {
            a.fragment.write = Some(target.clone());
        }
        Self {
            build: self.build.clone(),
            probe: Arc::new(probe),
            join_type: self.join_type.clone(),
            join: self.join.clone(),
            shipped: Arc::default(),
            write: Some(target),
        }
    }
}

/// `stream` with only the columns at `picks`, in that order (ADR 0092,
/// 0122); all of them without.
fn picked(
    stream: SendableRecordBatchStream,
    picks: Option<&[usize]>,
) -> Result<SendableRecordBatchStream> {
    let Some(picks) = picks.map(<[usize]>::to_vec) else {
        return Ok(stream);
    };
    let schema = Arc::new(stream.schema().project(&picks)?);
    let batches = stream.map(move |b| b.and_then(|b| Ok(b.project(&picks)?)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
}

impl DisplayAs for RemoteBroadcastExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RemoteBroadcastExec: {} join fragments={}",
            self.join_type,
            self.probe.fragments.len()
        )?;
        if let Some(a) = self
            .probe
            .fragments
            .first()
            .filter(|a| !a.fragment.filters.is_empty())
        {
            write!(f, " filters={:?}", a.fragment.filters)?;
        }
        Ok(())
    }
}

impl ExecutionPlan for RemoteBroadcastExec {
    fn name(&self) -> &str {
        "RemoteBroadcastExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.probe.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.build]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(build) = children.pop() else {
            return Err(DataFusionError::Internal(
                "broadcast join needs its build side".into(),
            ));
        };
        Ok(Arc::new(Self {
            write: self.write.clone(),
            ..Self::new(
                build,
                self.probe.clone(),
                self.join_type.clone(),
                self.join.clone(),
            )
        }))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let tag = ctx.session_config().get_extension::<QueryTag>();
        let (build, shipped, probe, join) = (
            self.build.clone(),
            self.shipped.clone(),
            self.probe.clone(),
            self.join.clone(),
        );
        let write = self.write.clone();
        let limit = self.probe.workers.broadcast_limit;
        // Cached build sides (ADR 0116), keyed by what they read.
        let cache = self.probe.workers.builds.clone();
        let key = (cache.max > 0).then(|| build_key(&self.build)).flatten();
        let task = ctx.clone();
        let body = stream::once(async move {
            let built = shipped
                .get_or_init(|| async move {
                    let fail = |e: DataFusionError| e.to_string();
                    if let Some((bytes, memory)) = key.as_deref().and_then(|k| cache.get(k)) {
                        if bytes.len() <= limit {
                            return Ok(Built::Shipped {
                                bytes,
                                _memory: memory,
                            });
                        }
                    }
                    let schema = build.schema();
                    let batches = datafusion::physical_plan::collect(build, ctx.clone())
                        .await
                        .map_err(fail)?;
                    let mut writer =
                        datafusion::arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &schema)
                            .map_err(|e| e.to_string())?;
                    for b in &batches {
                        writer.write(b).map_err(|e| e.to_string())?;
                    }
                    writer.finish().map_err(|e| e.to_string())?;
                    let bytes = writer.into_inner().map_err(|e| e.to_string())?;
                    // Over the limit (ADR 0099): not sent to every worker;
                    // the join runs here, on the build side in hand.
                    if bytes.len() > limit {
                        return local_join(&join, schema, batches)
                            .map(Built::Local)
                            .map_err(fail);
                    }
                    // Held as long as the plan: the build side is sent
                    // once per fragment.
                    let memory =
                        datafusion::execution::memory_pool::MemoryConsumer::new("broadcast-build")
                            .register(ctx.memory_pool());
                    memory.try_grow(bytes.len()).map_err(fail)?;
                    let (bytes, memory) = (Bytes::from(bytes), Arc::new(memory));
                    // The cache shares the reservation: counted once, freed
                    // when neither holds it.
                    if let Some(key) = key {
                        cache.put(key, bytes.clone(), memory.clone());
                    }
                    Ok(Built::Shipped {
                        bytes,
                        _memory: memory,
                    })
                })
                .await
                .clone()
                .map_err(|e| DataFusionError::External(e.into()))?;
            Ok::<_, DataFusionError>(match built {
                Built::Shipped { bytes, .. } => probe.fragment_stream(partition, tag, Some(bytes)),
                // The local join, whole, as the first partition; when the
                // rows were to be written where joined (ADR 0122), they are
                // written here, and the files written go back as the
                // workers' would.
                Built::Local(local) if partition == 0 => match &write {
                    None => local.execute(0, task)?.boxed(),
                    Some(target) => {
                        let rows =
                            picked(local.execute(0, task.clone())?, target.picks.as_deref())?;
                        let state = datafusion::execution::SessionStateBuilder::new()
                            .with_config(task.session_config().clone())
                            .with_runtime_env(task.runtime_env())
                            .build();
                        let files = crate::IcebergConnector::write_files(
                            &target.metadata_location,
                            target.metadata.as_ref(),
                            rows,
                            &state,
                        )
                        .await
                        .map_err(DataFusionError::from)?;
                        let batch = RecordBatch::try_new(
                            written_schema(),
                            vec![Arc::new(datafusion::arrow::array::StringArray::from(
                                files.iter().map(|f| f.to_string()).collect::<Vec<_>>(),
                            ))],
                        )?;
                        stream::iter([Ok(batch)]).boxed()
                    }
                },
                Built::Local(_) => stream::empty().boxed(),
            })
        })
        .flat_map(|r| match r {
            Ok(s) => s,
            Err(e) => stream::iter([Err(e)]).boxed(),
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.probe.schema.clone(),
            body,
        )))
    }
}

/// A left, full, left semi or anti broadcast join (ADR 0097): one output
/// partition. Every probe fragment runs at once on its worker against the
/// build side, returning its joined rows (left, full) and which build rows
/// matched; once all are done, the build rows the join type keeps on
/// their own follow: the unmatched ones (left, full: with NULLs for the
/// probe side; anti), or the matched ones (semi).
#[derive(Debug)]
pub struct RemoteGatheredJoinExec {
    build: Arc<dyn ExecutionPlan>,
    probe: Arc<RemoteScanExec>,
    join_type: datafusion::common::JoinType,
    schema: SchemaRef,
    /// The probe side's schema, for the NULLs of build-only rows.
    probe_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    /// The join as planned, run here over a build side above the
    /// broadcast limit (ADR 0099).
    join: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl RemoteGatheredJoinExec {
    fn new(
        build: Arc<dyn ExecutionPlan>,
        probe: Arc<RemoteScanExec>,
        join_type: datafusion::common::JoinType,
        schema: SchemaRef,
        probe_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        join: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            build,
            probe,
            join_type,
            schema,
            probe_schema,
            projection,
            join,
            properties,
        }
    }
}

impl DisplayAs for RemoteGatheredJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RemoteBroadcastExec: {} join fragments={} gathered",
            self.join_type,
            self.probe.fragments.len()
        )
    }
}

impl ExecutionPlan for RemoteGatheredJoinExec {
    fn name(&self) -> &str {
        "RemoteGatheredJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.build]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(build) = children.pop() else {
            return Err(DataFusionError::Internal(
                "broadcast join needs its build side".into(),
            ));
        };
        Ok(Arc::new(Self::new(
            build,
            self.probe.clone(),
            self.join_type,
            self.schema.clone(),
            self.probe_schema.clone(),
            self.projection.clone(),
            self.join.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::UInt64Type;
        use datafusion::common::JoinType;
        use futures::TryStreamExt;
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "a gathered broadcast join has one partition, not {partition}"
            )));
        }
        let tag = ctx.session_config().get_extension::<QueryTag>();
        let (build, probe, schema) = (self.build.clone(), self.probe.clone(), self.schema.clone());
        let (join_type, probe_schema, projection) = (
            self.join_type,
            self.probe_schema.clone(),
            self.projection.clone(),
        );
        let out = schema.clone();
        let (join, limit) = (self.join.clone(), self.probe.workers.broadcast_limit);
        let body = stream::once(async move {
            let build_schema = build.schema();
            let batches = datafusion::physical_plan::collect(build, ctx.clone()).await?;
            let rows = datafusion::arrow::compute::concat_batches(&build_schema, &batches)?;
            drop(batches);
            let mut writer =
                datafusion::arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &build_schema)?;
            writer.write(&rows)?;
            writer.finish()?;
            let bytes = Bytes::from(writer.into_inner()?);
            // Over the limit (ADR 0099): joined here, on the build side in
            // hand, the probe side's rows pulled as a plain scan.
            if bytes.len() > limit {
                let local = local_join(&join, build_schema, vec![rows])?;
                return Ok::<_, DataFusionError>(local.execute(0, ctx)?.boxed());
            }
            let memory = datafusion::execution::memory_pool::MemoryConsumer::new("broadcast-build")
                .register(ctx.memory_pool());
            memory.try_grow(bytes.len() + rows.get_array_memory_size())?;
            let marks = Arc::new(std::sync::Mutex::new(vec![false; rows.num_rows()]));
            let joined_rows = matches!(join_type, JoinType::Left | JoinType::Full);
            let fragments = (0..probe.fragments.len())
                .map(|p| probe.fragment_stream(p, tag.clone(), Some(bytes.clone())));
            let seen = marks.clone();
            let schema = out.clone();
            let joined = stream::select_all(fragments).try_filter_map(move |batch| {
                let last = batch.num_columns().saturating_sub(1);
                let indices = batch.column(last).as_primitive::<UInt64Type>().clone();
                {
                    let mut marks = seen.lock().unwrap_or_else(|e| e.into_inner());
                    for i in indices.iter().flatten() {
                        if let Some(m) = usize::try_from(i).ok().and_then(|i| marks.get_mut(i)) {
                            *m = true;
                        }
                    }
                }
                let kept = joined_rows
                    .then(|| {
                        RecordBatch::try_new_with_options(
                            schema.clone(),
                            batch.columns()[..last].to_vec(),
                            &datafusion::arrow::array::RecordBatchOptions::new()
                                .with_row_count(Some(batch.num_rows())),
                        )
                    })
                    .transpose()
                    .map_err(DataFusionError::from);
                futures::future::ready(kept)
            });
            let tail = stream::once(async move {
                let _memory = memory;
                let marks = marks.lock().unwrap_or_else(|e| e.into_inner()).clone();
                build_only(
                    &rows,
                    &marks,
                    join_type,
                    &probe_schema,
                    projection.as_deref(),
                    &out,
                )
            });
            Ok::<_, DataFusionError>(joined.chain(tail).boxed())
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            body,
        )))
    }
}

/// The rows a gathered join adds from its build side alone (ADR 0097):
/// matched rows for a semi join; unmatched ones otherwise, with NULLs for
/// the probe side's columns in a left or full join.
fn build_only(
    rows: &RecordBatch,
    marks: &[bool],
    join_type: datafusion::common::JoinType,
    probe: &SchemaRef,
    projection: Option<&[usize]>,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    use datafusion::arrow::array::{UInt32Array, new_null_array};
    use datafusion::common::JoinType;
    let keep = join_type == JoinType::LeftSemi;
    let picked: Vec<u32> = (0..marks.len())
        .filter(|i| marks[*i] == keep)
        .map(|i| i as u32)
        .collect();
    let indices = UInt32Array::from(picked);
    let mut columns = rows
        .columns()
        .iter()
        .map(|c| datafusion::arrow::compute::take(c.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if matches!(join_type, JoinType::Left | JoinType::Full) {
        columns.extend(
            probe
                .fields()
                .iter()
                .map(|f| new_null_array(f.data_type(), indices.len())),
        );
    }
    let columns = match projection {
        Some(p) => p
            .iter()
            .map(|&i| columns.get(i).cloned())
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| DataFusionError::Internal("join projection out of range".into()))?,
        None => columns,
    };
    let columns = columns
        .iter()
        .zip(schema.fields())
        .map(|(c, f)| datafusion::arrow::compute::cast(c, f.data_type()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &datafusion::arrow::array::RecordBatchOptions::new().with_row_count(Some(indices.len())),
    )?)
}

/// A unique enough shuffle id: workers key their maps by it.
fn shuffle_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    format!("{:016x}", h.finish())
}

/// One partition per shuffle reduce (ADR 0064, 0065), each posted to the
/// worker that runs it. Not retried elsewhere: a reduce's buckets are
/// consumed as they are fetched.
#[derive(Debug)]
pub struct RemoteReduceExec {
    schema: SchemaRef,
    workers: Arc<Workers>,
    reduces: Vec<(String, Reduce)>,
    /// What is reduced, for display: `aggregate` or the join type.
    kind: String,
    properties: Arc<PlanProperties>,
    /// Set once a partition runs: only then do workers hold maps to
    /// release when this plan is dropped (ADR 0073).
    executed: AtomicBool,
    /// Per bucket of a join, its split when hot (ADR 0117): decided once,
    /// from every map's bucket sizes, by the first partition to run.
    split_plan: Arc<tokio::sync::OnceCell<Vec<BucketSplit>>>,
}

impl RemoteReduceExec {
    /// The same reduces, each writing its rows as data files of `target`
    /// instead of returning them (ADR 0092).
    pub(crate) fn writing(&self, target: WriteTo) -> Self {
        let reduces = self
            .reduces
            .iter()
            .map(|(url, r)| {
                let mut r = r.clone();
                r.write = Some(target.clone());
                (url.clone(), r)
            })
            .collect();
        Self::new(
            written_schema(),
            self.workers.clone(),
            reduces,
            self.kind.clone(),
        )
    }

    fn new(
        schema: SchemaRef,
        workers: Arc<Workers>,
        reduces: Vec<(String, Reduce)>,
        kind: String,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(reduces.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            schema,
            workers,
            reduces,
            kind,
            properties,
            executed: AtomicBool::new(false),
            split_plan: Arc::default(),
        }
    }

    /// A reduce's targets: its worker, then, should that fail before its
    /// first batch, the next reducer's worker with a fresh shuffle for
    /// this one bucket (ADR 0072).
    fn targets(&self, partition: usize, reduce: &Reduce, url: &str) -> Vec<(String, Bytes)> {
        let mut targets = vec![(url.to_owned(), Bytes::from(reduce.to_json().to_string()))];
        let n = self.reduces.len();
        if let Some((other, _)) = self
            .reduces
            .get((partition + 1) % n)
            .filter(|(u, _)| u != url)
        {
            let retry = Reduce {
                shuffle: format!("{}~r{partition}", reduce.shuffle),
                only: true,
                ..reduce.clone()
            };
            targets.push((other.clone(), Bytes::from(retry.to_json().to_string())));
        }
        targets
    }
}

/// Every map's bucket sizes, per side of a join's shuffle (ADR 0117): each
/// map runs (as its reducers would have it run, the same keys hashed) and
/// reports its buckets' bytes, handing nothing out.
async fn bucket_sizes(
    workers: Arc<Workers>,
    reduce: &Reduce,
    tag: Option<Arc<QueryTag>>,
) -> Result<(Vec<Vec<u64>>, Vec<Vec<u64>>)> {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, UInt64Type};
    use futures::TryStreamExt;
    let Some(join) = &reduce.join else {
        return Ok((Vec::new(), Vec::new()));
    };
    let schema = Arc::new(Schema::new(vec![Field::new(
        "bytes",
        DataType::UInt64,
        false,
    )]));
    let side = |maps: &[(String, Fragment)], offset: usize, left: bool| {
        maps.iter()
            .enumerate()
            .map(|(i, (url, f))| {
                let mut f = f.clone();
                let keys = join.hash_keys(left, &f);
                let (workers, tag, schema) = (workers.clone(), tag.clone(), schema.clone());
                let url = url.clone();
                async move {
                    f.bucket = Some(Bucket {
                        shuffle: reduce.shuffle.clone(),
                        map: offset + i,
                        partitions: reduce.partitions,
                        index: 0,
                        keys: keys.map_err(DataFusionError::Execution)?,
                        only: false,
                        readers: 1,
                        sizes: true,
                    });
                    let body = Bytes::from(f.to_json().to_string());
                    let batches: Vec<RecordBatch> =
                        with_retry(workers, vec![(url, body)], tag, schema)
                            .try_collect()
                            .await?;
                    Ok::<_, DataFusionError>(
                        batches
                            .iter()
                            .flat_map(|b| {
                                b.column(0).as_primitive::<UInt64Type>().values().to_vec()
                            })
                            .collect::<Vec<u64>>(),
                    )
                }
            })
            .collect::<Vec<_>>()
    };
    futures::future::try_join(
        futures::future::try_join_all(side(&reduce.maps, 0, true)),
        futures::future::try_join_all(side(&join.right, reduce.maps.len(), false)),
    )
    .await
}

/// A join bucket's split (ADR 0117): the side split, and its maps in
/// groups; `None` to run the bucket whole.
type BucketSplit = Option<(bool, Vec<Vec<usize>>)>;

/// Which join buckets to split, and how (ADR 0117): per bucket, `None`, or
/// the side to split and its maps in groups, one reduce per group, the
/// other side's bucket read by each. A bucket is hot when its side's maps
/// hold over `min` bytes of it and four times that side's (lower) median
/// bucket. Only a side whose rows each appear at most once in the output
/// may be split: the left of an inner, left, left semi or anti join, the
/// right of an inner, right, right semi or anti one. The other side's
/// unmatched rows would come out once per part. Groups balance bytes,
/// largest maps first, over at most `reducers` parts.
fn plan_splits(
    left: &[Vec<u64>],
    right: &[Vec<u64>],
    join_type: datafusion::common::JoinType,
    min: u64,
    reducers: usize,
) -> Vec<BucketSplit> {
    use datafusion::common::JoinType;
    let buckets = left.iter().chain(right).map(Vec::len).max().unwrap_or(0);
    let at = |side: &[Vec<u64>], m: usize, j: usize| side[m].get(j).copied().unwrap_or(0);
    let totals = |side: &[Vec<u64>]| -> Vec<u64> {
        (0..buckets)
            .map(|j| (0..side.len()).map(|m| at(side, m, j)).sum())
            .collect()
    };
    let (lt, rt) = (totals(left), totals(right));
    let hot = |t: &[u64], j: usize| {
        let mut sorted = t.to_vec();
        sorted.sort_unstable();
        let median = sorted
            .get(sorted.len().saturating_sub(1) / 2)
            .copied()
            .unwrap_or(0);
        t[j] > min && t[j] > median.saturating_mul(4)
    };
    let left_ok = matches!(
        join_type,
        JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti
    );
    let right_ok = matches!(
        join_type,
        JoinType::Inner | JoinType::Right | JoinType::RightSemi | JoinType::RightAnti
    );
    (0..buckets)
        .map(|j| {
            let (is_left, side) = if left_ok && hot(&lt, j) {
                (true, left)
            } else if right_ok && hot(&rt, j) {
                (false, right)
            } else {
                return None;
            };
            let mut order: Vec<usize> = (0..side.len()).filter(|&m| at(side, m, j) > 0).collect();
            let parts = reducers.min(order.len());
            if parts < 2 {
                return None;
            }
            order.sort_by_key(|&m| std::cmp::Reverse(at(side, m, j)));
            let mut groups: Vec<(u64, Vec<usize>)> = vec![(0, Vec::new()); parts];
            for m in order {
                if let Some(g) = groups.iter_mut().min_by_key(|(load, _)| *load) {
                    g.0 += at(side, m, j);
                    g.1.push(m);
                }
            }
            Some((is_left, groups.into_iter().map(|(_, g)| g).collect()))
        })
        .collect()
}

/// The query is over (finished, failed or cancelled): every worker that
/// held a map or ran a reduce of this shuffle drops its maps now, instead
/// of at the TTL (ADR 0073). Best effort, off the dropping thread.
impl Drop for RemoteReduceExec {
    fn drop(&mut self) {
        let Some((_, first)) = self.reduces.first() else {
            return;
        };
        if !self.executed.load(Ordering::Relaxed) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut urls: Vec<String> = self
            .reduces
            .iter()
            .map(|(url, _)| url.clone())
            .chain(first.maps.iter().map(|(url, _)| url.clone()))
            .chain(
                first
                    .join
                    .iter()
                    .flat_map(|j| j.right.iter().map(|(u, _)| u.clone())),
            )
            .collect();
        urls.sort();
        urls.dedup();
        let body = Bytes::from(json!({"release": {"shuffle": first.shuffle}}).to_string());
        let workers = self.workers.clone();
        runtime.spawn(async move {
            let releases = urls.into_iter().map(|url| {
                let request = workers
                    .client
                    .post(format!("{url}/v1/fragment"))
                    .bearer_auth(&workers.token)
                    .header("content-type", "application/json")
                    .timeout(Duration::from_secs(5))
                    .body(body.clone())
                    .send();
                async move { request.await.map(|r| r.status().is_success()) }
            });
            futures::future::join_all(releases).await;
        });
    }
}

impl DisplayAs for RemoteReduceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let maps = self.reduces.first().map_or(0, |(_, r)| {
            r.maps.len() + r.join.as_ref().map_or(0, |j| j.right.len())
        });
        let filters = self
            .reduces
            .first()
            .and_then(|(_, r)| r.maps.first())
            .filter(|(_, m)| !m.filters.is_empty())
            .map_or(String::new(), |(_, m)| format!(" filters={:?}", m.filters));
        write!(
            f,
            "RemoteReduceExec: shuffled {} partitions={} maps={maps}{filters}",
            self.kind,
            self.reduces.len()
        )
    }
}

impl ExecutionPlan for RemoteReduceExec {
    fn name(&self) -> &str {
        "RemoteReduceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let tag = ctx.session_config().get_extension::<QueryTag>();
        let schema = self.schema.clone();
        self.executed.store(true, Ordering::Relaxed);
        let Some((url, reduce)) = self.reduces.get(partition) else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::empty(),
            )));
        };
        // A lost reducer took its buckets with it: the retry, on the next
        // reducer's worker, is a fresh shuffle for this one bucket (ADR
        // 0072), so it never asks for a bucket already handed out.
        let targets = self.targets(partition, reduce, url);
        let join_type = reduce
            .join
            .as_ref()
            .and_then(|j| j.join_type.parse::<datafusion::common::JoinType>().ok());
        let workers = self.workers.clone();
        let Some(join_type) = join_type.filter(|_| workers.skew_bytes > 0) else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                with_retry(workers, targets, tag, schema),
            )));
        };
        // Hot buckets (ADR 0117): sized once, split into parts that run on
        // successive reducers' workers and stream as this partition.
        let (cell, template) = (self.split_plan.clone(), self.reduces[0].1.clone());
        let urls: Vec<String> = self.reduces.iter().map(|(u, _)| u.clone()).collect();
        let reduce = reduce.clone();
        let out = schema.clone();
        let body = stream::once(async move {
            let plan = cell
                .get_or_init(|| async {
                    // Unsized (a map's worker failing, say), nothing splits:
                    // the reduces then run, and retry, as planned.
                    match bucket_sizes(workers.clone(), &template, tag.clone()).await {
                        Ok((left, right)) => {
                            plan_splits(&left, &right, join_type, workers.skew_bytes, urls.len())
                        }
                        Err(_) => Vec::new(),
                    }
                })
                .await;
            let Some((left, groups)) = plan.get(partition).cloned().flatten() else {
                return with_retry(workers, targets, tag, out);
            };
            workers.splits.fetch_add(1, Ordering::Relaxed);
            let readers = groups.len();
            let parts = groups.into_iter().enumerate().map(|(i, maps)| {
                let part = Reduce {
                    split: Some(Split {
                        left,
                        maps,
                        readers,
                    }),
                    ..reduce.clone()
                };
                // ponytail: a part is not retried elsewhere; its maps would
                // run again for it alone, which the split does not plan.
                let url = urls[(partition + i) % urls.len()].clone();
                let body = Bytes::from(part.to_json().to_string());
                with_retry(workers.clone(), vec![(url, body)], tag.clone(), out.clone())
            });
            stream::select_all(parts).boxed()
        })
        .flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, body)))
    }
}

/// Moves partial aggregations into worker fragments (ADR 0061).
///
/// Matches `AggregateExec(Partial)` whose input is, through pass-through
/// nodes only (repartition, coalesce), an `AccountedExec` over a
/// [`RemoteScanExec`], whose group keys are portable expressions (ADR
/// 0063) and whose aggregates are `count(*)` or `count`/`sum`/`min`/`max`
/// of one (no DISTINCT, FILTER, ORDER BY). Such an aggregate's partial state per
/// partition *is* the value computed over that partition's rows, so each
/// worker returns it for its fragment and the coordinator's `Final`
/// merges. Anything else is left as it is: rows travel, as before.
#[derive(Debug, Default)]
pub struct PartialAggregatePushdown;

impl PhysicalOptimizerRule for PartialAggregatePushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Shuffles first (they consume the partial aggregations they
        // replace), then partial pushdown where no shuffle applies.
        let plan = shuffle(plan, true, config)?;
        let plan = plan
            .transform_down(|node| {
                Ok(match push_partial(&node, config)? {
                    Some(pushed) => Transformed::yes(pushed),
                    None => Transformed::no(node),
                })
            })
            .data()?;
        // Writes of rows that come straight from workers are written
        // there (ADR 0091).
        plan.transform_down(|node| {
            Ok(match crate::iceberg::push_write(&node) {
                Some(pushed) => Transformed::yes(pushed),
                None => Transformed::no(node),
            })
        })
        .data()
    }

    fn name(&self) -> &str {
        "plimsoll_partial_aggregate_pushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn push_partial(
    node: &Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    Ok(partial_remote(node, config)?.map(|pushed| {
        Arc::new(AccountedExec::new("RemoteAggregate", Arc::new(pushed))) as Arc<dyn ExecutionPlan>
    }))
}

/// The remote scan under `node`, aggregating on workers, if `node` is a
/// pushable partial aggregation (see [`PartialAggregatePushdown`]).
fn partial_remote(
    node: &Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Option<RemoteScanExec>> {
    let Some(agg) = node.as_ref().downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };
    if *agg.mode() != AggregateMode::Partial
        || !agg.group_expr().is_single()
        || agg.filter_expr().iter().any(Option::is_some)
    {
        return Ok(None);
    }
    // Keys and arguments travel as SQL that plans back to the same
    // expression on the worker (ADR 0063); they read the scan's columns,
    // since only pass-through nodes sit in between (checked below).
    let schema = agg.input().schema();
    let sql = |e: &Arc<dyn datafusion::physical_expr::PhysicalExpr>| {
        crate::portable::physical_sql(e, &schema, config)
    };
    let mut group_by = Vec::new();
    for (e, _) in agg.group_expr().expr() {
        let Some(c) = sql(e) else { return Ok(None) };
        group_by.push(c);
    }
    let mut aggregates = Vec::new();
    for a in agg.aggr_expr() {
        let name = a.fun().name().to_lowercase();
        if a.is_distinct() || !a.order_bys().is_empty() || a.ignore_nulls() {
            return Ok(None);
        }
        let args = a.expressions();
        let arg = match (name.as_str(), args.as_slice()) {
            // count(*) is planned as count(<non-null literal>).
            ("count", [e])
                if e.as_ref()
                    .downcast_ref::<Literal>()
                    .is_some_and(|l| !l.value().is_null()) =>
            {
                None
            }
            ("count" | "sum" | "min" | "max", [e]) => match sql(e) {
                Some(c) => Some(c),
                None => return Ok(None),
            },
            _ => return Ok(None),
        };
        aggregates.push((name, arg));
    }
    let Some(below) = remote_under(agg.input()) else {
        return Ok(None);
    };
    let Some(remote) = below.as_ref().downcast_ref::<RemoteScanExec>() else {
        return Ok(None);
    };
    let spec = AggregateSpec {
        group_by,
        aggregates,
    };
    Ok(Some(remote.with_aggregate(spec, agg.schema())))
}

/// What a writing fragment returns (ADR 0091): one data file per row.
pub fn written_schema() -> SchemaRef {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![Field::new("file", DataType::Utf8, false)]))
}

/// The [`RemoteScanExec`] under `input`, reached through pass-through
/// nodes only (the optimizer may put a repartition on either side of the
/// scan's accounting, or, with few partitions, a cooperative-yield
/// wrapper), so its rows are `input`'s rows.
pub(crate) fn remote_under(input: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let pass = |mut p: Arc<dyn ExecutionPlan>| loop {
        if !matches!(
            p.name(),
            "RepartitionExec" | "CoalesceBatchesExec" | "CooperativeExec"
        ) {
            return Some(p);
        }
        p = p.children().first().map(|c| (*c).clone())?;
    };
    let accounted = pass(input.clone())?;
    accounted.as_ref().downcast_ref::<AccountedExec>()?;
    let remote = pass(accounted.children().first().map(|c| (*c).clone())?)?;
    remote.as_ref().downcast_ref::<RemoteScanExec>()?;
    Some(remote)
}

/// A join side the workers can produce: a plain remote scan, or one under
/// the projection DataFusion adds to compute expression join keys (every
/// scan column in order, then the keys), as that scan with the keys
/// computed on the workers (ADR 0111).
fn scan_side(input: &Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Option<RemoteScanExec> {
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::projection::ProjectionExec;
    if let Some(r) = remote_under(input) {
        let scan = r.as_ref().downcast_ref::<RemoteScanExec>()?;
        return Some(scan.with_schema(scan.schema.clone()));
    }
    let mut p = input.clone();
    while matches!(
        p.name(),
        "RepartitionExec" | "CoalesceBatchesExec" | "CooperativeExec"
    ) {
        p = p.children().first().map(|c| (*c).clone())?;
    }
    let projection = p.as_ref().downcast_ref::<ProjectionExec>()?;
    let below = projection.input();
    let r = remote_under(below)?;
    let scan = r.as_ref().downcast_ref::<RemoteScanExec>()?;
    let inner = below.schema();
    let width = inner.fields().len();
    let exprs = projection.expr();
    if exprs.len() <= width {
        return None;
    }
    for (i, e) in exprs.iter().take(width).enumerate() {
        let c = e.expr.as_ref().downcast_ref::<Column>()?;
        if c.index() != i || e.alias != *inner.field(i).name() {
            return None;
        }
    }
    let computed = exprs
        .iter()
        .skip(width)
        .map(|e| {
            let sql = crate::portable::physical_sql_equivalent(&e.expr, &inner, config)?;
            Some((e.alias.clone(), sql))
        })
        .collect::<Option<Vec<_>>>()?;
    let mut side = scan.with_schema(projection.schema());
    for a in &mut side.fragments {
        a.fragment.computed = computed.clone();
    }
    Some(side)
}

/// A join side the workers produce for a shuffled join: a remote scan
/// ([`scan_side`], `None`), or a grouped aggregation of one (ADR 0112):
/// its partial aggregates are the side's maps, and `Some(picks)` are the
/// side's columns by position in the aggregation's output, which reducers
/// merge before joining.
fn join_side(
    input: &Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Option<(RemoteScanExec, Option<Vec<usize>>)> {
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::projection::ProjectionExec;
    if let Some(scan) = scan_side(input, config) {
        return Some((scan, None));
    }
    let peel = |mut p: Arc<dyn ExecutionPlan>| loop {
        if !matches!(
            p.name(),
            "RepartitionExec"
                | "CoalesceBatchesExec"
                | "CooperativeExec"
                | "CoalescePartitionsExec"
        ) {
            return Some(p);
        }
        p = p.children().first().map(|c| (*c).clone())?;
    };
    let mut p = peel(input.clone())?;
    // Column picks and renames over the aggregation, nothing computed.
    let mut picks = None;
    if let Some(projection) = p.as_ref().downcast_ref::<ProjectionExec>() {
        picks = Some(
            projection
                .expr()
                .iter()
                .map(|e| e.expr.as_ref().downcast_ref::<Column>().map(Column::index))
                .collect::<Option<Vec<_>>>()?,
        );
        p = peel(projection.input().clone())?;
    }
    let agg = p.as_ref().downcast_ref::<AggregateExec>()?;
    if !matches!(
        agg.mode(),
        AggregateMode::Final | AggregateMode::FinalPartitioned
    ) || agg.group_expr().expr().is_empty()
    {
        return None;
    }
    let partial = peel(agg.input().clone())?;
    let remote = partial_remote(&partial, config).ok()??;
    // One state per aggregate (count/sum/min/max): merged, the state is
    // the final value (as for ADR 0064's shuffles).
    if partial.schema().fields().len() != agg.schema().fields().len() {
        return None;
    }
    let width = agg.schema().fields().len();
    Some((remote, Some(picks.unwrap_or_else(|| (0..width).collect()))))
}

/// Replaces a grouped final aggregation over a pushable partial one with
/// a shuffle between the workers (ADR 0064), when nothing above depends
/// on its partitioning (`free`). Recurses into the children otherwise.
fn shuffle(
    plan: Arc<dyn ExecutionPlan>,
    free: bool,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Remote results arrive in no particular order: a node whose output
    // ordering something above may rely on (a join keeping its sorted
    // probe side for a merge) is never replaced.
    if free && plan.properties().output_ordering().is_none() {
        if let Some(shuffled) = shuffle_final(&plan, config)? {
            return Ok(shuffled);
        }
        if let Some(shuffled) = shuffle_join(&plan, config) {
            return Ok(shuffled);
        }
        if let Some(broadcast) = broadcast_join(&plan, config)? {
            return Ok(broadcast);
        }
    }
    // Children stay free below nodes that take any partitioning (they
    // redistribute or gather it), and below nodes that keep their
    // input's partitioning when they are free themselves. Joins, unions,
    // windows and final aggregations depend on it.
    let free = matches!(
        plan.name(),
        "CoalescePartitionsExec" | "SortPreservingMergeExec" | "RepartitionExec"
    ) || (free
        && (matches!(
            plan.name(),
            "ProjectionExec"
                | "SortExec"
                | "SortExec(TopK)"
                | "FilterExec"
                | "CoalesceBatchesExec"
                | "LocalLimitExec"
                | "GlobalLimitExec"
        ) || plan
            .as_ref()
            .downcast_ref::<AggregateExec>()
            .is_some_and(|a| *a.mode() == AggregateMode::Partial)));
    let children = plan
        .children()
        .into_iter()
        .map(|c| shuffle(c.clone(), free, config))
        .collect::<Result<Vec<_>>>()?;
    datafusion::physical_plan::replace_children_if_necessary(plan, children)
}

/// A partitioned equi-join of two remote scans, on columns of equal
/// types, as a shuffle (ADR 0065): every scan fragment is a map hashed by
/// its join keys into one bucket per healthy worker, and worker `j` joins
/// bucket `j` of both sides. Equal key types hash equally, so matching
/// rows meet on one worker.
fn shuffle_join(
    node: &Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Option<Arc<dyn ExecutionPlan>> {
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    let join = node.as_ref().downcast_ref::<HashJoinExec>()?;
    if *join.partition_mode() != PartitionMode::Partitioned
        || join.null_equality() != NullEquality::NullEqualsNothing
        || matches!(join.join_type(), JoinType::LeftMark | JoinType::RightMark)
    {
        return None;
    }
    let ((left, left_picks), (right, right_picks)) = (
        join_side(join.left(), config)?,
        join_side(join.right(), config)?,
    );
    let (left, right) = (&left, &right);
    let usable = |s: &RemoteScanExec, picks: &Option<Vec<usize>>| {
        !s.fragments.is_empty()
            && s.fragments
                .iter()
                .all(|a| a.fragment.aggregate.is_some() == picks.is_some())
    };
    if !usable(left, &left_picks) || !usable(right, &right_picks) || left.urls.is_empty() {
        return None;
    }
    // The worker runs the same join: its filter as SQL (ADR 0098), no mark
    // join, no null-equal keys.
    let filter = join_filter(
        join.filter(),
        (join.left().schema(), join.right().schema()),
        config,
        (left_picks.is_some(), right_picks.is_some()),
    )?;
    let (ls, rs) = (join.left().schema(), join.right().schema());
    let (mut left_keys, mut right_keys) = (Vec::new(), Vec::new());
    for (a, b) in join.on() {
        let a = a.as_ref().downcast_ref::<Column>()?;
        let b = b.as_ref().downcast_ref::<Column>()?;
        if ls.field(a.index()).data_type() != rs.field(b.index()).data_type() {
            return None;
        }
        let name = |c: &Column, picks: &Option<Vec<usize>>| match picks {
            Some(_) => format!("c{}", c.index()),
            None => c.name().to_owned(),
        };
        left_keys.push(name(a, &left_picks));
        right_keys.push(name(b, &right_picks));
    }
    // An aggregated side's maps hash its group keys (ADR 0112): the join's
    // keys must be exactly those, or matching rows would not meet.
    let groups_are_keys = |s: &RemoteScanExec, picks: &Option<Vec<usize>>, left: bool| {
        let Some(picks) = picks else { return true };
        let groups = s
            .fragments
            .first()
            .and_then(|a| a.fragment.aggregate.as_ref())
            .map_or(0, |g| g.group_by.len());
        let mut keyed: Vec<usize> = join
            .on()
            .iter()
            .filter_map(|(a, b)| {
                let c = if left { a } else { b };
                let c = c.as_ref().downcast_ref::<Column>()?;
                picks.get(c.index()).copied()
            })
            .collect();
        keyed.sort_unstable();
        keyed == (0..groups).collect::<Vec<_>>()
    };
    if !groups_are_keys(left, &left_picks, true) || !groups_are_keys(right, &right_picks, false) {
        return None;
    }
    let spec = JoinSpec {
        join_type: join.join_type().to_string(),
        right: right.maps(),
        left_keys,
        right_keys,
        projection: join.projection.as_ref().map(|p| p.to_vec()),
        filter,
        left_picks,
        right_picks,
    };
    let (shuffle, partitions, maps) = (shuffle_id(), left.urls.len(), left.maps());
    let reduces = left
        .urls
        .iter()
        .enumerate()
        .map(|(partition, url)| {
            (
                url.clone(),
                Reduce {
                    shuffle: shuffle.clone(),
                    partition,
                    partitions,
                    maps: maps.clone(),
                    join: Some(spec.clone()),
                    only: false,
                    write: None,
                    split: None,
                },
            )
        })
        .collect();
    let kind = format!("{} join", join.join_type());
    Some(Arc::new(AccountedExec::new(
        "RemoteJoin",
        Arc::new(RemoteReduceExec::new(
            join.schema(),
            left.workers.clone(),
            reduces,
            kind,
        )),
    )))
}

/// A join that collects its left side whose probe side (right) is a plain
/// remote scan, as a broadcast (ADR 0066): a `CollectLeft` equi-join, or a
/// nested-loop join, which always collects its left side and joins on its
/// filter alone (ADR 0115). Join types driven by the probe side (inner,
/// right, right semi, right anti) run on the workers as planned; left,
/// full, left semi and anti ones gather the build rows' matches (ADR
/// 0097).
fn broadcast_join(
    node: &Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec, PartitionMode};
    // The join's pieces, whichever it is: an equi-join has keys.
    let (left, right, join_type, filter, projection, on) =
        if let Some(join) = node.as_ref().downcast_ref::<HashJoinExec>() {
            // NOT IN's null-aware anti join is not one the workers run.
            if *join.partition_mode() != PartitionMode::CollectLeft
                || join.null_equality() != NullEquality::NullEqualsNothing
                || join.null_aware
            {
                return Ok(None);
            }
            (
                join.left(),
                join.right(),
                *join.join_type(),
                join.filter(),
                join.projection.as_ref().map(|p| p.to_vec()),
                join.on(),
            )
        } else if let Some(join) = node.as_ref().downcast_ref::<NestedLoopJoinExec>() {
            (
                join.left(),
                join.right(),
                *join.join_type(),
                join.filter(),
                join.projection().as_ref().map(|p| p.to_vec()),
                &[][..],
            )
        } else {
            return Ok(None);
        };
    // Left, full, left semi and anti joins need every fragment's matches
    // to know a build row's fate: the coordinator gathers them (ADR 0097).
    let gathered = matches!(
        join_type,
        JoinType::Left | JoinType::Full | JoinType::LeftSemi | JoinType::LeftAnti
    );
    if !(gathered
        || matches!(
            join_type,
            JoinType::Inner | JoinType::Right | JoinType::RightSemi | JoinType::RightAnti
        ))
    {
        return Ok(None);
    }
    // A filter travels as SQL (ADR 0098), or the join stays.
    let Some(filter) = join_filter(
        filter,
        (left.schema(), right.schema()),
        config,
        (false, false),
    ) else {
        return Ok(None);
    };
    let Some(probe) = scan_side(right, config) else {
        return Ok(None);
    };
    let probe = &probe;
    if probe.workers.broadcast_limit == 0 {
        return Ok(None);
    }
    if probe.fragments.is_empty()
        || probe
            .fragments
            .iter()
            .any(|a| a.fragment.aggregate.is_some())
    {
        return Ok(None);
    }
    let (ls, rs) = (left.schema(), right.schema());
    let (mut build_keys, mut probe_keys) = (Vec::new(), Vec::new());
    for (a, b) in on {
        let (Some(a), Some(b)) = (
            a.as_ref().downcast_ref::<Column>(),
            b.as_ref().downcast_ref::<Column>(),
        ) else {
            return Ok(None);
        };
        if ls.field(a.index()).data_type() != rs.field(b.index()).data_type() {
            return Ok(None);
        }
        build_keys.push(a.name().to_owned());
        probe_keys.push(b.name().to_owned());
    }
    let spec = Broadcast {
        join_type: join_type.to_string(),
        build_keys,
        probe_keys,
        projection: projection.clone(),
        matched: gathered,
        filter,
    };
    // The build side is collected whole, so nothing in it depends on
    // its partitioning: it may shuffle too.
    let build = shuffle(left.clone(), true, config)?;
    if gathered {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let index = Field::new("__b_row", DataType::UInt64, true);
        let returned = if matches!(join_type, JoinType::Left | JoinType::Full) {
            let mut fields: Vec<_> = node.schema().fields().iter().cloned().collect();
            fields.push(Arc::new(index));
            Schema::new(fields)
        } else {
            Schema::new(vec![index])
        };
        let probe = Arc::new(probe.broadcast(spec, Arc::new(returned)));
        return Ok(Some(Arc::new(AccountedExec::new(
            "RemoteBroadcast",
            Arc::new(RemoteGatheredJoinExec::new(
                build,
                probe,
                join_type,
                node.schema(),
                right.schema(),
                projection,
                node.clone(),
            )),
        ))));
    }
    let probe = Arc::new(probe.broadcast(spec, node.schema()));
    Ok(Some(Arc::new(AccountedExec::new(
        "RemoteBroadcast",
        Arc::new(RemoteBroadcastExec::new(
            build,
            probe,
            join_type.to_string(),
            node.clone(),
        )),
    ))))
}

fn shuffle_final(
    node: &Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(agg) = node.as_ref().downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };
    if !matches!(
        agg.mode(),
        AggregateMode::Final | AggregateMode::FinalPartitioned
    ) || agg.group_expr().expr().is_empty()
    {
        return Ok(None);
    }
    let mut partial = agg.input().clone();
    while matches!(
        partial.name(),
        "RepartitionExec" | "CoalesceBatchesExec" | "CooperativeExec" | "CoalescePartitionsExec"
    ) {
        let Some(c) = partial.children().first().map(|c| (*c).clone()) else {
            return Ok(None);
        };
        partial = c;
    }
    let Some(remote) = partial_remote(&partial, config)? else {
        return Ok(None);
    };
    // One state per aggregate (count/sum/min/max): merged, the state is
    // the final value. Two fragments at least, or there is nothing to
    // shuffle.
    if partial.schema().fields().len() != agg.schema().fields().len() || remote.fragment_count() < 2
    {
        return Ok(None);
    }
    let shuffled: Arc<dyn ExecutionPlan> = Arc::new(AccountedExec::new(
        "RemoteShuffle",
        Arc::new(remote.shuffled(agg.schema())),
    ));
    // Same partition count as the node replaced when that was one.
    Ok(Some(
        if agg.properties().output_partitioning().partition_count() == 1 {
            Arc::new(CoalescePartitionsExec::new(shuffled))
        } else {
            shuffled
        },
    ))
}

/// A fragment request's failure, and whether another worker may be tried.
#[derive(Debug)]
struct WorkerError {
    message: String,
    /// Connection failures, 429 and 5xx: another worker may succeed. A
    /// 4xx other than 429 (bad token, bad request) would fail there too.
    retryable: bool,
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for WorkerError {}

/// Whether a failed worker request may succeed on another worker
/// (ADR 0060): connection failures, 429 and 5xx, and streams that broke
/// on the way; not a worker's refusal (4xx).
pub fn retryable(e: &DataFusionError) -> bool {
    match e {
        DataFusionError::External(inner) => inner
            .downcast_ref::<WorkerError>()
            .is_none_or(|w| w.retryable),
        _ => true,
    }
}

fn worker_error(message: String, retryable: bool) -> DataFusionError {
    DataFusionError::External(Box::new(WorkerError { message, retryable }))
}

/// Runs the task on `targets[0]`; if it fails before its first batch
/// with a retryable error, once more on `targets[1]` (ADR 0060), each with
/// its own body (a retried reduce is a different task, ADR 0072). Before
/// the first batch nothing has been emitted, so a retry cannot duplicate
/// rows; after it, any failure fails the query.
fn with_retry(
    workers: Arc<Workers>,
    targets: Vec<(String, Bytes)>,
    tag: Option<Arc<QueryTag>>,
    schema: SchemaRef,
) -> BoxStream<'static, Result<RecordBatch>> {
    struct Run {
        workers: Arc<Workers>,
        targets: VecDeque<(String, Bytes)>,
        tag: Option<Arc<QueryTag>>,
        schema: SchemaRef,
        current: Option<BoxStream<'static, Result<RecordBatch>>>,
        emitted: bool,
        first_failure: Option<String>,
        done: bool,
    }
    let run = Run {
        workers,
        targets: targets.into(),
        tag,
        schema,
        current: None,
        emitted: false,
        first_failure: None,
        done: false,
    };
    stream::unfold(run, |mut run| async move {
        loop {
            if run.done {
                return None;
            }
            let Some(current) = run.current.as_mut() else {
                let (url, body) = run.targets.pop_front()?;
                let body = stream::once(open(run.workers.clone(), url, body, run.tag.clone()))
                    .flat_map(|r| match r {
                        Ok(s) => s,
                        Err(e) => stream::iter([Err(e)]).boxed(),
                    })
                    .boxed();
                run.current = Some(decode(body, run.schema.clone()));
                continue;
            };
            match current.next().await {
                Some(Ok(batch)) => {
                    run.emitted = true;
                    return Some((Ok(batch), run));
                }
                Some(Err(e)) => {
                    let retryable = retryable(&e);
                    if !run.emitted && retryable && !run.targets.is_empty() {
                        run.first_failure = Some(e.to_string());
                        run.current = None;
                        continue;
                    }
                    run.done = true;
                    let e = match run.first_failure.take() {
                        Some(first) => DataFusionError::External(
                            format!("{e} (after a retry; first attempt: {first})").into(),
                        ),
                        None => e,
                    };
                    return Some((Err(e), run));
                }
                None => return None,
            }
        }
    })
    .boxed()
}

/// POSTs a task (fragment or reduce); errors before the stream keep the
/// worker's message.
async fn open(
    workers: Arc<Workers>,
    url: String,
    body: Bytes,
    tag: Option<Arc<QueryTag>>,
) -> Result<BoxStream<'static, Result<Bytes>>> {
    let mut request = workers
        .client
        .post(format!("{url}/v1/fragment"))
        .bearer_auth(&workers.token)
        .header("content-type", "application/json")
        // What the fragment spilled, after its stream (ADR 0121).
        .header(TRAILER_HEADER, "spill");
    let sink = tag.as_ref().and_then(|t| t.spill.clone());
    // The query's identity, trace and remaining time (ADR 0059).
    if let Some(tag) = tag {
        request = request
            .header("x-plimsoll-query-id", tag.query_id.to_string())
            .header("x-plimsoll-user", tag.user.as_str());
        if let Some(tp) = &tag.traceparent {
            request = request.header("traceparent", tp.as_str());
        }
        if let Some(deadline) = tag.deadline {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(worker_error(
                    "query deadline passed before a fragment was sent".into(),
                    false,
                ));
            }
            request = request.header("x-plimsoll-deadline-ms", left.as_millis().to_string());
        }
    }
    let response = request
        .body(body)
        .send()
        .await
        .map_err(|e| worker_error(format!("worker {url}: {e}"), true))?;
    if !response.status().is_success() {
        let status = response.status();
        let retryable = status.as_u16() == 429 || status.is_server_error();
        let text = response.text().await.unwrap_or_default();
        return Err(worker_error(
            format!("worker {url} answered {status}: {text}"),
            retryable,
        ));
    }
    let url = Arc::new(url);
    let body = response
        .bytes_stream()
        .map(move |b| b.map_err(|e| worker_error(format!("worker {url}: {e}"), true)))
        .boxed();
    Ok(split_trailer(body, sink))
}

/// Asks a worker to end a fragment's stream with [`SPILL_TRAILER`] (ADR
/// 0121). A worker that does not know it sends a plain IPC stream.
pub const TRAILER_HEADER: &str = "x-plimsoll-trailer";

/// After a fragment's end-of-stream marker, when asked for (ADR 0121):
/// these 8 bytes, then what the fragment spilled as a little-endian u64.
pub const SPILL_TRAILER: &[u8; 8] = b"PLMSPILL";

/// The trailer as a worker appends it.
pub fn spill_trailer(bytes: u64) -> Vec<u8> {
    let mut out = SPILL_TRAILER.to_vec();
    out.extend_from_slice(&bytes.to_le_bytes());
    out
}

/// `body` without its spill trailer (ADR 0121), which is added to `sink`.
/// The last 16 bytes are held back until the body ends: a trailer is
/// taken off, anything else (a worker that sends none) goes on as IPC.
fn split_trailer(
    body: BoxStream<'static, Result<Bytes>>,
    sink: Option<plimsoll_core::engine::SpillSink>,
) -> BoxStream<'static, Result<Bytes>> {
    const LEN: usize = 16;
    // (body, bytes held back, an error to send after them, ended)
    type State = (
        BoxStream<'static, Result<Bytes>>,
        Vec<u8>,
        Option<DataFusionError>,
        bool,
    );
    let start: State = (body, Vec::new(), None, false);
    stream::unfold(start, move |(mut body, mut held, error, ended)| {
        let sink = sink.clone();
        async move {
            if let Some(e) = error {
                return Some((Err(e), (body, held, None, true)));
            }
            if ended {
                return None;
            }
            loop {
                match body.next().await {
                    Some(Ok(chunk)) => {
                        held.extend_from_slice(&chunk);
                        if held.len() > LEN {
                            let out = held.split_off(held.len() - LEN);
                            let ready = std::mem::replace(&mut held, out);
                            return Some((Ok(Bytes::from(ready)), (body, held, None, false)));
                        }
                    }
                    // The bytes held back go first, as they would have
                    // without a trailer: a batch they complete is emitted
                    // before the error, so nothing is retried (ADR 0072).
                    Some(Err(e)) if !held.is_empty() => {
                        let ready = Bytes::from(std::mem::take(&mut held));
                        return Some((Ok(ready), (body, held, Some(e), false)));
                    }
                    Some(Err(e)) => return Some((Err(e), (body, held, None, true))),
                    None => {
                        if held.len() == LEN && held.starts_with(SPILL_TRAILER) {
                            let mut n = [0u8; 8];
                            n.copy_from_slice(&held[8..]);
                            if let Some(sink) = &sink {
                                sink.add(u64::from_le_bytes(n));
                            }
                            return None;
                        }
                        if held.is_empty() {
                            return None;
                        }
                        let rest = Bytes::from(std::mem::take(&mut held));
                        return Some((Ok(rest), (body, held, None, true)));
                    }
                }
            }
        }
    })
    .boxed()
}

/// A shuffle bucket from the worker at `url` (ADR 0064, 0068), as it
/// arrives: its schema (from the first IPC message) and a stream of its
/// batches, which fails without the end-of-stream marker. `token` is the
/// worker token the reducing worker was called with.
pub async fn open_bucket(
    url: String,
    token: &str,
    fragment: &Fragment,
    tag: Option<Arc<QueryTag>>,
) -> Result<(SchemaRef, BoxStream<'static, Result<RecordBatch>>)> {
    let workers = Arc::new(
        Workers::new(Vec::new(), token.to_owned())
            .map_err(|e| worker_error(e.to_string(), false))?,
    );
    let body = open(
        workers,
        url.clone(),
        Bytes::from(fragment.to_json().to_string()),
        tag,
    )
    .await?;
    with_schema(body)
        .await
        .map_err(|e| worker_error(format!("worker {url}: bucket: {e}"), false))
}

/// An Arrow IPC byte stream as its schema (read from the first message)
/// and its batches, decoded as they arrive; the bytes read for the schema
/// are decoded again. No schema, or no end-of-stream marker, is an error.
pub async fn with_schema(
    mut body: BoxStream<'static, Result<Bytes>>,
) -> Result<(SchemaRef, BoxStream<'static, Result<RecordBatch>>)> {
    let mut probe = StreamDecoder::new();
    let mut head = Vec::new();
    let schema = loop {
        let Some(chunk) = body.next().await else {
            return Err(DataFusionError::Execution("ended before its schema".into()));
        };
        let chunk = chunk?;
        head.push(Ok(chunk.clone()));
        let mut buf = Buffer::from(chunk);
        while !buf.is_empty() && probe.schema().is_none() {
            probe.decode(&mut buf)?;
        }
        if let Some(schema) = probe.schema() {
            break schema;
        }
    };
    let bytes = stream::iter(head).chain(body).boxed();
    Ok((schema.clone(), decode(bytes, schema)))
}

/// A whole Arrow IPC stream: its schema and batches.
pub fn read_ipc(bytes: impl AsRef<[u8]>) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let reader =
        datafusion::arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}

/// A task body: the JSON, then, after one newline, an optional Arrow IPC
/// attachment (a broadcast join's build side, ADR 0066). Serialized JSON
/// holds no raw newline, so the first one splits them.
pub fn split_body(body: &[u8]) -> (&[u8], &[u8]) {
    match body.iter().position(|b| *b == b'\n') {
        Some(i) => (&body[..i], &body[i + 1..]),
        None => (body, &[]),
    }
}

/// Arrow IPC's end-of-stream marker: continuation, then a zero length.
const END_OF_STREAM: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];

/// `batch` with `schema`'s names and metadata, columns cast to its types.
pub fn conform(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(c, f)| {
            if c.data_type() == f.data_type() {
                Ok(c.clone())
            } else {
                datafusion::arrow::compute::cast(c, f.data_type())
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .and_then(|cols| {
            // A join that needs no column (`count(*)` over `EXISTS`)
            // returns rows with no columns: only their count says how many.
            RecordBatch::try_new_with_options(
                schema.clone(),
                cols,
                &datafusion::arrow::array::RecordBatchOptions::new()
                    .with_row_count(Some(batch.num_rows())),
            )
        })
        .map_err(DataFusionError::from)
}

/// Arrow IPC bytes -> batches with `schema` (the coordinator's field names
/// and metadata). A stream that ends without its end-of-stream marker (a
/// worker that failed mid-scan, ADR 0037; a truncated spill file, ADR
/// 0067) is an error.
pub fn decode(
    bytes: BoxStream<'static, Result<Bytes>>,
    schema: SchemaRef,
) -> BoxStream<'static, Result<RecordBatch>> {
    struct State {
        bytes: BoxStream<'static, Result<Bytes>>,
        decoder: StreamDecoder,
        ready: VecDeque<RecordBatch>,
        done: bool,
        schema: SchemaRef,
        /// The last 8 bytes seen: must be the end-of-stream marker when
        /// the body ends.
        tail: Vec<u8>,
    }
    let state = State {
        bytes,
        decoder: StreamDecoder::new(),
        ready: VecDeque::new(),
        done: false,
        schema,
        tail: Vec::with_capacity(16),
    };
    stream::unfold(state, |mut st| async move {
        loop {
            if let Some(batch) = st.ready.pop_front() {
                // The coordinator's names and types: partial states come
                // back as the worker's SQL typed them (ADR 0061).
                let batch = conform(&batch, &st.schema);
                return Some((batch, st));
            }
            if st.done {
                return None;
            }
            match st.bytes.next().await {
                Some(Ok(chunk)) => {
                    st.tail
                        .extend_from_slice(&chunk[chunk.len().saturating_sub(8)..]);
                    let excess = st.tail.len().saturating_sub(8);
                    st.tail.drain(..excess);
                    let mut buf = Buffer::from(chunk);
                    while !buf.is_empty() {
                        match st.decoder.decode(&mut buf) {
                            Ok(Some(batch)) => st.ready.push_back(batch),
                            Ok(None) => {}
                            Err(e) => {
                                st.done = true;
                                return Some((Err(e.into()), st));
                            }
                        }
                    }
                }
                Some(Err(e)) => {
                    st.done = true;
                    return Some((Err(e), st));
                }
                None => {
                    st.done = true;
                    // `finish` accepts a stream cut at a message boundary;
                    // only the explicit end-of-stream marker proves the
                    // worker finished (ADR 0060). Without it, rows may be
                    // missing.
                    let finished = st.decoder.finish().is_ok() && st.tail == END_OF_STREAM;
                    if !finished {
                        return Some((
                            Err(worker_error(
                                "worker stream ended without its end-of-stream marker".into(),
                                true,
                            )),
                            st,
                        ));
                    }
                }
            }
        }
    })
    .boxed()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spill_trailers_are_split_off_wherever_chunks_end() {
        use futures::TryStreamExt;
        use plimsoll_core::engine::SpillSink;
        let ipc: Vec<u8> = (0..40u8).chain(END_OF_STREAM).collect();
        let mut with = ipc.clone();
        with.extend(spill_trailer(4096));
        for size in [1, 3, 16, 17, 100] {
            let chunked = |bytes: &[u8]| {
                stream::iter(
                    bytes
                        .chunks(size)
                        .map(|c| Ok(Bytes::copy_from_slice(c)))
                        .collect::<Vec<_>>(),
                )
                .boxed()
            };
            let sink = SpillSink::default();
            let out: Vec<Bytes> = split_trailer(chunked(&with), Some(sink.clone()))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(out.concat(), ipc, "chunks of {size}");
            assert_eq!(sink.bytes(), 4096);
            // No trailer (an older worker): the body passes whole.
            let out: Vec<Bytes> = split_trailer(chunked(&ipc), Some(sink.clone()))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(out.concat(), ipc);
            assert_eq!(sink.bytes(), 4096);
        }
    }

    #[test]
    fn hot_join_buckets_split_on_the_side_that_may() {
        use datafusion::common::JoinType;
        // Two buckets per map; bucket 0 of the left side is hot.
        let left = vec![vec![900, 10], vec![700, 10], vec![100, 10]];
        let right = vec![vec![50, 40], vec![50, 40]];
        let plan = plan_splits(&left, &right, JoinType::Inner, 100, 2);
        // Split in two parts, largest map first, balancing bytes.
        assert_eq!(plan, vec![Some((true, vec![vec![0], vec![1, 2]])), None]);
        // A left join may split its left side; a right join may not,
        // and its right side is not hot.
        assert_eq!(plan_splits(&left, &right, JoinType::Left, 100, 2), plan);
        assert_eq!(
            plan_splits(&left, &right, JoinType::Right, 100, 2),
            vec![None, None]
        );
        // Under the minimum, or with one reducer, nothing splits.
        assert_eq!(
            plan_splits(&left, &right, JoinType::Inner, 5000, 2),
            vec![None, None]
        );
        assert_eq!(
            plan_splits(&left, &right, JoinType::Inner, 100, 1),
            vec![None, None]
        );
        // A hot right side of a right join splits there.
        let plan = plan_splits(&right, &left, JoinType::Right, 100, 3);
        assert_eq!(
            plan,
            vec![Some((false, vec![vec![0], vec![1], vec![2]])), None]
        );
        // A full join splits neither side.
        assert_eq!(
            plan_splits(&left, &right, JoinType::Full, 100, 2),
            vec![None, None]
        );
    }

    #[test]
    fn fragments_round_trip_as_json() {
        let f = Fragment {
            metadata_location: "s3://b/t/metadata/v3.metadata.json".into(),
            snapshot_id: Some(42),
            files: vec!["s3://b/t/data/a.parquet".into()],
            columns: vec!["id".into()],
            aggregate: Some(AggregateSpec {
                group_by: vec!["re\"gion".into()],
                aggregates: vec![("count".into(), None), ("sum".into(), Some("id".into()))],
            }),
            filters: vec!["id > 3".into()],
            bucket: Some(Bucket {
                shuffle: "s1".into(),
                map: 1,
                partitions: 2,
                index: 0,
                keys: vec![0],
                only: true,
                readers: 3,
                sizes: true,
            }),
            broadcast: Some(Broadcast {
                join_type: "RightSemi".into(),
                build_keys: vec!["k".into()],
                probe_keys: vec!["id".into()],
                projection: None,
                matched: true,
                filter: Some(JoinFilter {
                    sql: "f0 < f1".into(),
                    columns: vec![(true, "a".into()), (false, "b".into())],
                }),
            }),
            write: Some(WriteTo {
                metadata_location: "s3://b/u/metadata/v1.metadata.json".into(),
                metadata: Some(json!({"format-version": 2})),
                picks: Some(vec![1, 0]),
            }),
            change: Some(RowChange {
                predicate: "id > 3".into(),
                update: Some(vec!["id".into(), "upper(name)".into()]),
            }),
            computed: vec![("lower(name)".into(), "lower(\"name\")".into())],
            merge: Some(MergeSpec {
                target_keys: vec!["t0".into()],
                source_keys: vec!["s0".into()],
                residual: Some("t1 > s1".into()),
                matched: vec![
                    ClauseSpec {
                        predicate: Some("s1 IS NULL".into()),
                        update: None,
                    },
                    ClauseSpec {
                        predicate: None,
                        update: Some(vec!["t0".into(), "s1".into()]),
                    },
                ],
                not_matched_by_source: vec![],
            }),
        };
        assert_eq!(Fragment::from_json(&f.to_json()).unwrap(), f);
        assert!(Fragment::from_json(&json!({"files": []})).is_err());
        // A newer coordinator's field is refused, never silently skipped.
        let mut newer = f.to_json();
        newer["sample"] = json!(0.1);
        let e = Fragment::from_json(&newer).unwrap_err();
        assert!(e.contains("unknown fragment field `sample`"), "{e}");
        let mut r = Reduce {
            shuffle: "s1".into(),
            partition: 1,
            partitions: 2,
            maps: vec![
                ("http://w1".into(), f.clone()),
                ("http://w2".into(), f.clone()),
            ],
            join: None,
            only: true,
            write: None,
            split: Some(Split {
                left: true,
                maps: vec![1],
                readers: 2,
            }),
        };
        assert_eq!(
            Task::from_json(&r.to_json()).unwrap(),
            Task::Reduce(r.clone())
        );
        let joined = Reduce {
            join: Some(JoinSpec {
                join_type: "LeftAnti".into(),
                right: vec![("http://w2".into(), f.clone())],
                left_keys: vec!["id".into()],
                right_keys: vec!["re\"gion".into()],
                projection: Some(vec![0]),
                filter: Some(JoinFilter {
                    sql: "f0 <> f1".into(),
                    columns: vec![(true, "id".into()), (false, "re\"gion".into())],
                }),
                left_picks: Some(vec![0, 2]),
                right_picks: None,
            }),
            ..r.clone()
        };
        assert_eq!(
            Task::from_json(&joined.to_json()).unwrap(),
            Task::Reduce(joined)
        );
        assert_eq!(split_body(b"{}\n\x01\x02"), (&b"{}"[..], &b"\x01\x02"[..]));
        assert_eq!(split_body(b"{}"), (&b"{}"[..], &b""[..]));
        assert_eq!(
            Task::from_json(&json!({"release": {"shuffle": "s1"}})).unwrap(),
            Task::Release("s1".into())
        );
        assert!(Task::from_json(&json!({"release": {"shuffle": ""}})).is_err());
        // Without a join, every map must aggregate.
        r.maps[0].1.aggregate = None;
        assert!(Task::from_json(&r.to_json()).is_err());
        assert_eq!(Task::from_json(&f.to_json()).unwrap(), Task::Scan(f));
        let mut bad = r.to_json();
        bad["reduce"]["partition"] = json!(2);
        assert!(Task::from_json(&bad).is_err());
        let w = Workers::new(vec!["http://w1/".into()], "secret".into()).unwrap();
        assert_eq!(w.urls(), ["http://w1"]);
        assert!(!format!("{w:?}").contains("secret"));
    }

    #[tokio::test]
    async fn bucket_streams_yield_their_schema_first() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::ipc::writer::StreamWriter;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let ipc = |batches: &[RecordBatch]| {
            let mut w = StreamWriter::try_new(Vec::new(), &schema).unwrap();
            for b in batches {
                w.write(b).unwrap();
            }
            w.into_inner().unwrap()
        };
        // Seven-byte chunks: the schema message spans several.
        let chunks = |bytes: Vec<u8>| {
            stream::iter(
                bytes
                    .chunks(7)
                    .map(|c| Ok(Bytes::copy_from_slice(c)))
                    .collect::<Vec<_>>(),
            )
            .boxed()
        };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let (got, batches) = with_schema(chunks(ipc(&[batch.clone(), batch.clone()])))
            .await
            .unwrap();
        assert_eq!(got, schema);
        let batches: Vec<_> = batches.map(|b| b.unwrap()).collect().await;
        assert_eq!(batches, [batch.clone(), batch.clone()]);
        // An empty bucket: a schema and no batches.
        let (_, none) = with_schema(chunks(ipc(&[]))).await.unwrap();
        assert_eq!(none.count().await, 0);
        // Truncated after its schema: the stream fails, never just ends.
        let mut cut = ipc(std::slice::from_ref(&batch));
        cut.truncate(cut.len() - 8);
        let (_, broken) = with_schema(chunks(cut)).await.unwrap();
        let results: Vec<_> = broken.collect().await;
        assert!(results.last().unwrap().is_err());
        assert!(with_schema(chunks(Vec::new())).await.is_err());
    }

    #[tokio::test]
    async fn dns_seeds_are_resolved_into_workers_each_round() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // A worker that is always ready, on 127.0.0.1.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                    .await;
            }
        });
        let w = Workers::new(
            vec![
                "http://static:1".into(),
                format!("dns+http://localhost:{port}"),
                "dns+http://no-such-worker.invalid:1".into(),
            ],
            "t".repeat(16),
        )
        .unwrap();
        // Before the first round: the static seed only, presumed healthy.
        assert_eq!(w.urls(), ["http://static:1"]);
        w.check_health(Duration::from_secs(2)).await;
        let found = format!("http://127.0.0.1:{port}");
        let urls = w.urls();
        assert!(
            urls.contains(&found) && urls.contains(&"http://static:1".to_owned()),
            "{urls:?}"
        );
        // Only the listening address answers /readyz.
        assert_eq!(w.healthy_urls(), std::slice::from_ref(&found));
        // Another round keeps it; an unresolvable name never adds any.
        w.check_health(Duration::from_secs(2)).await;
        assert!(w.healthy_urls().contains(&found));
        assert!(w.urls().iter().all(|u| !u.contains("invalid")));
    }

    #[test]
    fn fragments_balance_by_bytes() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let urls = vec!["http://w1".to_owned(), "http://w2".to_owned()];
        let w = Arc::new(Workers::new(urls.clone(), "t".repeat(16)).unwrap());
        let mb = |n: u64| Some(n << 20);
        // Round-robin would give w1 {100, 1, 1} and w2 {90, 1}: by bytes,
        // the 100 MiB file sits alone and the rest join the 90.
        let files = vec![
            ("a".into(), mb(100)),
            ("b".into(), mb(90)),
            ("c".into(), mb(1)),
            ("d".into(), mb(1)),
            ("e".into(), None),
        ];
        let plan = RemoteScanExec::new(schema, w, &urls, "m", None, files, vec!["id".into()]);
        let a = plan.assignments();
        assert_eq!(a[0], ("http://w1".into(), 1, 100 << 20));
        assert_eq!(a[1].1, 4);
        assert_eq!(a[1].2, (92 << 20) + 1);
    }

    /// Arrow's decoder accepts a stream cut cleanly at a message boundary;
    /// only the end-of-stream marker proves the worker finished (ADR 0060).
    #[tokio::test]
    async fn a_stream_without_its_end_marker_is_an_error() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::ipc::writer::StreamWriter;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        let mut w = StreamWriter::try_new(Vec::new(), &schema).unwrap();
        w.write(&batch).unwrap();
        let cut = std::mem::take(w.get_mut());
        w.finish().unwrap();
        let marker = std::mem::take(w.get_mut());
        assert_eq!(marker, END_OF_STREAM);
        let read = |chunks: Vec<Vec<u8>>| async {
            let bytes = stream::iter(chunks.into_iter().map(|c| Ok(Bytes::from(c)))).boxed();
            decode(bytes, schema.clone()).collect::<Vec<_>>().await
        };
        let out = read(vec![cut.clone()]).await;
        assert!(out[0].is_ok(), "the batch itself decodes");
        let err = out.last().unwrap().as_ref().unwrap_err().to_string();
        assert!(err.contains("end-of-stream marker"), "{err}");
        // Complete, even with the marker split across chunks.
        let (a, b) = marker.split_at(3);
        let out = read(vec![cut, a.to_vec(), b.to_vec()]).await;
        assert_eq!(out.len(), 1);
        assert!(out[0].is_ok());
    }
}
