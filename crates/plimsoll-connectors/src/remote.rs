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

/// Worker endpoints and the shared secret they accept, set as a session
/// config extension on coordinators.
#[derive(Clone)]
pub struct Workers {
    urls: Vec<String>,
    token: String,
    client: reqwest::Client,
    /// Per URL: last health check passed (ADR 0059). Starts healthy.
    healthy: Arc<Vec<AtomicBool>>,
}

impl fmt::Debug for Workers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never the token.
        f.debug_struct("Workers").field("urls", &self.urls).finish()
    }
}

impl Workers {
    pub fn new(urls: Vec<String>, token: String) -> Result<Self, reqwest::Error> {
        let healthy = Arc::new(urls.iter().map(|_| AtomicBool::new(true)).collect());
        Ok(Self {
            urls: urls
                .into_iter()
                .map(|u| u.trim_end_matches('/').to_owned())
                .collect(),
            token,
            client: reqwest::Client::builder().build()?,
            healthy,
        })
    }

    pub fn urls(&self) -> &[String] {
        &self.urls
    }

    /// Workers whose last `/readyz` answered 200: the ones fragments go
    /// to. A draining or dead worker drops out until it answers again.
    pub fn healthy_urls(&self) -> Vec<String> {
        self.urls
            .iter()
            .zip(self.healthy.iter())
            .filter(|(_, h)| h.load(Ordering::Relaxed))
            .map(|(u, _)| u.clone())
            .collect()
    }

    /// One round of health checks; returns how many workers are healthy.
    pub async fn check_health(&self, timeout: Duration) -> usize {
        let checks = self.urls.iter().map(|url| {
            let request = self
                .client
                .get(format!("{url}/readyz"))
                .timeout(timeout)
                .send();
            async move { request.await.is_ok_and(|r| r.status().is_success()) }
        });
        let results = futures::future::join_all(checks).await;
        for (flag, ok) in self.healthy.iter().zip(&results) {
            flag.store(*ok, Ordering::Relaxed);
        }
        results.iter().filter(|ok| **ok).count()
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
            "maps": maps_json(&self.maps),
            "join": self.join.as_ref().map(|j| json!({
                "join_type": j.join_type,
                "right": maps_json(&j.right),
                "left_keys": j.left_keys,
                "right_keys": j.right_keys,
                "projection": j.projection,
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
        })
    }
}

/// What `/v1/fragment` is asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Task {
    Scan(Fragment),
    Reduce(Reduce),
}

impl Task {
    pub fn from_json(v: &Json) -> Result<Self, String> {
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
        json!({
            "metadata_location": self.metadata_location,
            "snapshot_id": self.snapshot_id,
            "files": self.files,
            "columns": self.columns,
            "filters": self.filters,
            "bucket": self.bucket.as_ref().map(|b| json!({
                "shuffle": b.shuffle, "map": b.map, "partitions": b.partitions, "index": b.index, "keys": b.keys,
            })),
            "broadcast": self.broadcast.as_ref().map(|b| json!({
                "join_type": b.join_type,
                "build_keys": b.build_keys,
                "probe_keys": b.probe_keys,
                "projection": b.projection,
            })),
            "aggregate": self.aggregate.as_ref().map(|a| json!({
                "group_by": a.group_by,
                "aggregates": a.aggregates.iter().map(|(f, c)| json!({"fn": f, "column": c})).collect::<Vec<_>>(),
            })),
        })
    }

    pub fn from_json(v: &Json) -> Result<Self, String> {
        // A field this worker does not know could change the answer (a
        // filter it would skip): refuse it rather than ignore it.
        const KNOWN: [&str; 8] = [
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
        let targets = (0..n.min(2))
            .map(|i| self.urls[(assigned.worker + i) % n].clone())
            .collect();
        with_retry(
            self.workers.clone(),
            targets,
            Bytes::from(body),
            tag,
            self.schema.clone(),
        )
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
    /// The build side as IPC, and its memory, shared by all partitions.
    shipped: Arc<tokio::sync::OnceCell<Result<Shipped, String>>>,
}

type Shipped = (
    Bytes,
    Arc<datafusion::execution::memory_pool::MemoryReservation>,
);

impl RemoteBroadcastExec {
    fn new(build: Arc<dyn ExecutionPlan>, probe: Arc<RemoteScanExec>, join_type: String) -> Self {
        Self {
            build,
            probe,
            join_type,
            shipped: Arc::default(),
        }
    }
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
        Ok(Arc::new(Self::new(
            build,
            self.probe.clone(),
            self.join_type.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let tag = ctx.session_config().get_extension::<QueryTag>();
        let (build, shipped, probe) =
            (self.build.clone(), self.shipped.clone(), self.probe.clone());
        let body = stream::once(async move {
            let (bytes, _) = shipped
                .get_or_init(|| async move {
                    let fail = |e: DataFusionError| e.to_string();
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
                    // Held as long as the plan: the build side is sent
                    // once per fragment.
                    let memory =
                        datafusion::execution::memory_pool::MemoryConsumer::new("broadcast-build")
                            .register(ctx.memory_pool());
                    memory.try_grow(bytes.len()).map_err(fail)?;
                    Ok((Bytes::from(bytes), Arc::new(memory)))
                })
                .await
                .clone()
                .map_err(|e| DataFusionError::External(e.into()))?;
            Ok::<_, DataFusionError>(probe.fragment_stream(partition, tag, Some(bytes)))
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
}

impl RemoteReduceExec {
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
        }
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
        let Some((url, reduce)) = self.reduces.get(partition) else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::empty(),
            )));
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            with_retry(
                self.workers.clone(),
                vec![url.clone()],
                Bytes::from(reduce.to_json().to_string()),
                tag,
                schema,
            ),
        )))
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
        plan.transform_down(|node| {
            Ok(match push_partial(&node, config)? {
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

/// The [`RemoteScanExec`] under `input`, reached through pass-through
/// nodes only (the optimizer may put a repartition on either side of the
/// scan's accounting), so its rows are `input`'s rows.
fn remote_under(input: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let pass = |mut p: Arc<dyn ExecutionPlan>| loop {
        if !matches!(p.name(), "RepartitionExec" | "CoalesceBatchesExec") {
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
        if let Some(shuffled) = shuffle_join(&plan) {
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
fn shuffle_join(node: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    let join = node.as_ref().downcast_ref::<HashJoinExec>()?;
    // The worker runs the same join: no residual filter, no mark join,
    // no null-equal keys.
    if *join.partition_mode() != PartitionMode::Partitioned
        || join.filter().is_some()
        || join.null_equality() != NullEquality::NullEqualsNothing
        || matches!(join.join_type(), JoinType::LeftMark | JoinType::RightMark)
    {
        return None;
    }
    let (l, r) = (remote_under(join.left())?, remote_under(join.right())?);
    let left = l.as_ref().downcast_ref::<RemoteScanExec>()?;
    let right = r.as_ref().downcast_ref::<RemoteScanExec>()?;
    let plain = |s: &RemoteScanExec| {
        !s.fragments.is_empty() && s.fragments.iter().all(|a| a.fragment.aggregate.is_none())
    };
    if !plain(left) || !plain(right) || left.urls.is_empty() {
        return None;
    }
    let (ls, rs) = (join.left().schema(), join.right().schema());
    let (mut left_keys, mut right_keys) = (Vec::new(), Vec::new());
    for (a, b) in join.on() {
        let a = a.as_ref().downcast_ref::<Column>()?;
        let b = b.as_ref().downcast_ref::<Column>()?;
        if ls.field(a.index()).data_type() != rs.field(b.index()).data_type() {
            return None;
        }
        left_keys.push(a.name().to_owned());
        right_keys.push(b.name().to_owned());
    }
    let spec = JoinSpec {
        join_type: join.join_type().to_string(),
        right: right.maps(),
        left_keys,
        right_keys,
        projection: join.projection.as_ref().map(|p| p.to_vec()),
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

/// A `CollectLeft` equi-join whose probe side (right) is a plain remote
/// scan, as a broadcast (ADR 0066): only join types driven by the probe
/// side, whose output per probe row does not depend on other probe rows
/// (inner, right, right semi, right anti). A left or full join would emit
/// unmatched build rows once per worker.
fn broadcast_join(
    node: &Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    let Some(join) = node.as_ref().downcast_ref::<HashJoinExec>() else {
        return Ok(None);
    };
    if *join.partition_mode() != PartitionMode::CollectLeft
        || join.filter().is_some()
        || join.null_equality() != NullEquality::NullEqualsNothing
        || !matches!(
            join.join_type(),
            JoinType::Inner | JoinType::Right | JoinType::RightSemi | JoinType::RightAnti
        )
    {
        return Ok(None);
    }
    let Some(r) = remote_under(join.right()) else {
        return Ok(None);
    };
    let Some(probe) = r.as_ref().downcast_ref::<RemoteScanExec>() else {
        return Ok(None);
    };
    if probe.fragments.is_empty()
        || probe
            .fragments
            .iter()
            .any(|a| a.fragment.aggregate.is_some())
    {
        return Ok(None);
    }
    let (ls, rs) = (join.left().schema(), join.right().schema());
    let (mut build_keys, mut probe_keys) = (Vec::new(), Vec::new());
    for (a, b) in join.on() {
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
        join_type: join.join_type().to_string(),
        build_keys,
        probe_keys,
        projection: join.projection.as_ref().map(|p| p.to_vec()),
    };
    // The build side is collected whole, so nothing in it depends on
    // its partitioning: it may shuffle too.
    let build = shuffle(join.left().clone(), true, config)?;
    let probe = Arc::new(probe.broadcast(spec, join.schema()));
    Ok(Some(Arc::new(AccountedExec::new(
        "RemoteBroadcast",
        Arc::new(RemoteBroadcastExec::new(
            build,
            probe,
            join.join_type().to_string(),
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
        "RepartitionExec" | "CoalesceBatchesExec" | "CoalescePartitionsExec"
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

fn worker_error(message: String, retryable: bool) -> DataFusionError {
    DataFusionError::External(Box::new(WorkerError { message, retryable }))
}

/// Runs the fragment on `targets[0]`; if it fails before its first batch
/// with a retryable error, once more on `targets[1]` (ADR 0060). Before
/// the first batch nothing has been emitted, so a retry cannot duplicate
/// rows; after it, any failure fails the query.
fn with_retry(
    workers: Arc<Workers>,
    targets: Vec<String>,
    body: Bytes,
    tag: Option<Arc<QueryTag>>,
    schema: SchemaRef,
) -> BoxStream<'static, Result<RecordBatch>> {
    struct Run {
        workers: Arc<Workers>,
        targets: VecDeque<String>,
        body: Bytes,
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
        body,
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
                let url = run.targets.pop_front()?;
                let body = stream::once(open(
                    run.workers.clone(),
                    url,
                    run.body.clone(),
                    run.tag.clone(),
                ))
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
                    let retryable = match &e {
                        DataFusionError::External(inner) => inner
                            .downcast_ref::<WorkerError>()
                            .is_none_or(|w| w.retryable),
                        // A stream that broke before any batch.
                        _ => true,
                    };
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
        .header("content-type", "application/json");
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
    Ok(response
        .bytes_stream()
        .map(move |b| b.map_err(|e| worker_error(format!("worker {url}: {e}"), true)))
        .boxed())
}

/// A shuffle bucket from the worker at `url` (ADR 0064), whole: its
/// schema and batches as that worker typed them. `token` is the worker
/// token the reducing worker was called with.
pub async fn fetch_bucket(
    url: String,
    token: &str,
    fragment: &Fragment,
    tag: Option<Arc<QueryTag>>,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let workers = Arc::new(
        Workers::new(Vec::new(), token.to_owned())
            .map_err(|e| worker_error(e.to_string(), false))?,
    );
    let mut body = open(
        workers,
        url.clone(),
        Bytes::from(fragment.to_json().to_string()),
        tag,
    )
    .await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    if !bytes.ends_with(&END_OF_STREAM) {
        return Err(worker_error(
            format!("worker {url}: bucket ended without its end-of-stream marker"),
            false,
        ));
    }
    read_ipc(bytes)
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

/// Arrow IPC bytes -> batches with `schema` (the coordinator's field names
/// and metadata). A stream that ends without its end-of-stream marker (a
/// worker that failed mid-scan, ADR 0037) is an error.
fn decode(
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
                let batch = batch
                    .columns()
                    .iter()
                    .zip(st.schema.fields())
                    .map(|(c, f)| {
                        if c.data_type() == f.data_type() {
                            Ok(c.clone())
                        } else {
                            datafusion::arrow::compute::cast(c, f.data_type())
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .and_then(|cols| RecordBatch::try_new(st.schema.clone(), cols))
                    .map_err(DataFusionError::from);
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
            }),
            broadcast: Some(Broadcast {
                join_type: "RightSemi".into(),
                build_keys: vec!["k".into()],
                probe_keys: vec!["id".into()],
                projection: None,
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
            }),
            ..r.clone()
        };
        assert_eq!(
            Task::from_json(&joined.to_json()).unwrap(),
            Task::Reduce(joined)
        );
        assert_eq!(split_body(b"{}\n\x01\x02"), (&b"{}"[..], &b"\x01\x02"[..]));
        assert_eq!(split_body(b"{}"), (&b"{}"[..], &b""[..]));
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
