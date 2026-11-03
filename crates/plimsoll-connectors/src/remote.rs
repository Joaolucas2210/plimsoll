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
}

/// Which bucket of which shuffle map a reducing worker asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bucket {
    pub shuffle: String,
    /// This map's position among the shuffle's maps.
    pub map: usize,
    pub partitions: usize,
    pub index: usize,
}

/// A shuffle reduce (ADR 0064): fetch bucket `partition` of every map
/// (aggregated fragments, each kept by the worker at its URL), merge the
/// partial states and return final values for this bucket's groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reduce {
    pub shuffle: String,
    pub partition: usize,
    pub maps: Vec<(String, Fragment)>,
}

impl Reduce {
    pub fn to_json(&self) -> Json {
        json!({"reduce": {
            "shuffle": self.shuffle,
            "partition": self.partition,
            "maps": self.maps.iter().map(|(url, f)| json!({"url": url, "fragment": f.to_json()})).collect::<Vec<_>>(),
        }})
    }

    pub fn from_json(v: &Json) -> Result<Self, String> {
        let r = &v["reduce"];
        let maps = r["maps"]
            .as_array()
            .ok_or("`reduce.maps` must be an array")?
            .iter()
            .map(|m| {
                let url = m["url"].as_str().ok_or("`reduce.maps[].url`")?.to_owned();
                let fragment = Fragment::from_json(&m["fragment"])?;
                if fragment.aggregate.is_none() {
                    return Err("a shuffle map must aggregate".to_owned());
                }
                Ok((url, fragment))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let partition = r["partition"]
            .as_u64()
            .and_then(|p| usize::try_from(p).ok())
            .filter(|p| *p < maps.len())
            .ok_or("`reduce.partition` must index `maps`")?;
        Ok(Self {
            shuffle: r["shuffle"]
                .as_str()
                .ok_or("`reduce.shuffle` is required")?
                .to_owned(),
            partition,
            maps,
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
                "shuffle": b.shuffle, "map": b.map, "partitions": b.partitions, "index": b.index,
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
        const KNOWN: [&str; 7] = [
            "metadata_location",
            "snapshot_id",
            "files",
            "columns",
            "filters",
            "bucket",
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
    /// Shuffle id (ADR 0064): partition `i` is reduce `i`, run by the
    /// owner of fragment `i` over bucket `i` of every fragment.
    shuffle: Option<String>,
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
            shuffle: None,
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
    pub fn with_shuffle(&self, shuffle: String, schema: SchemaRef) -> Self {
        let mut out = self.with_schema(schema);
        out.shuffle = Some(shuffle);
        out
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
            shuffle: self.shuffle.clone(),
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
            "RemoteScanExec: fragments={} files={files} bytes={bytes:?}{filters}{aggregate}{}",
            self.fragments.len(),
            if self.shuffle.is_some() {
                " shuffled"
            } else {
                ""
            }
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
        let schema = self.schema.clone();
        let Some(assigned) = self.fragments.get(partition).cloned() else {
            // No files at all: an empty partition.
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::empty(),
            )));
        };
        let (body, targets) = match &self.shuffle {
            // A reduce is not retried elsewhere: its buckets are consumed
            // when fetched (ADR 0064).
            Some(shuffle) => (
                Reduce {
                    shuffle: shuffle.clone(),
                    partition,
                    maps: self
                        .fragments
                        .iter()
                        .map(|a| (self.urls[a.worker].clone(), a.fragment.clone()))
                        .collect(),
                }
                .to_json(),
                vec![self.urls[assigned.worker].clone()],
            ),
            // The owner first, then one other healthy worker (ADR 0060).
            None => {
                let n = self.urls.len();
                (
                    assigned.fragment.to_json(),
                    (0..n.min(2))
                        .map(|i| self.urls[(assigned.worker + i) % n].clone())
                        .collect(),
                )
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            with_retry(
                self.workers.clone(),
                targets,
                Arc::new(body.to_string()),
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
    // Down to the remote scan through pass-through nodes only (the
    // optimizer may put a repartition on either side of the accounting).
    let pass = |mut p: Arc<dyn ExecutionPlan>| loop {
        if !matches!(p.name(), "RepartitionExec" | "CoalesceBatchesExec") {
            return Some(p);
        }
        p = p.children().first().map(|c| (*c).clone())?;
    };
    let Some(input) = pass(agg.input().clone()) else {
        return Ok(None);
    };
    let Some(accounted) = input.as_ref().downcast_ref::<AccountedExec>() else {
        return Ok(None);
    };
    let Some(below) = accounted
        .children()
        .first()
        .map(|c| (*c).clone())
        .and_then(pass)
    else {
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

/// Replaces a grouped final aggregation over a pushable partial one with
/// a shuffle between the workers (ADR 0064), when nothing above depends
/// on its partitioning (`free`). Recurses into the children otherwise.
fn shuffle(
    plan: Arc<dyn ExecutionPlan>,
    free: bool,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    if free {
        if let Some(shuffled) = shuffle_final(&plan, config)? {
            return Ok(shuffled);
        }
    }
    // Children stay free below nodes that need no particular
    // partitioning from them; joins, unions, windows, repartitions and
    // aggregates are not among them.
    let free = matches!(
        plan.name(),
        "CoalescePartitionsExec" | "SortPreservingMergeExec"
    ) || (free
        && matches!(
            plan.name(),
            "ProjectionExec"
                | "SortExec"
                | "SortExec(TopK)"
                | "FilterExec"
                | "CoalesceBatchesExec"
                | "LocalLimitExec"
                | "GlobalLimitExec"
        ));
    let children = plan
        .children()
        .into_iter()
        .map(|c| shuffle(c.clone(), free, config))
        .collect::<Result<Vec<_>>>()?;
    datafusion::physical_plan::replace_children_if_necessary(plan, children)
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
    let id = {
        use std::hash::{BuildHasher, Hasher};
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
        );
        format!("{:016x}", h.finish())
    };
    let shuffled: Arc<dyn ExecutionPlan> = Arc::new(AccountedExec::new(
        "RemoteShuffle",
        Arc::new(remote.with_shuffle(id, agg.schema())),
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
    body: Arc<String>,
    tag: Option<Arc<QueryTag>>,
    schema: SchemaRef,
) -> BoxStream<'static, Result<RecordBatch>> {
    struct Run {
        workers: Arc<Workers>,
        targets: VecDeque<String>,
        body: Arc<String>,
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
    body: Arc<String>,
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
        .body(body.as_ref().clone())
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
        Arc::new(fragment.to_json().to_string()),
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
    let reader =
        datafusion::arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
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
            }),
        };
        assert_eq!(Fragment::from_json(&f.to_json()).unwrap(), f);
        assert!(Fragment::from_json(&json!({"files": []})).is_err());
        // A newer coordinator's field is refused, never silently skipped.
        let mut newer = f.to_json();
        newer["sample"] = json!(0.1);
        let e = Fragment::from_json(&newer).unwrap_err();
        assert!(e.contains("unknown fragment field `sample`"), "{e}");
        let r = Reduce {
            shuffle: "s1".into(),
            partition: 1,
            maps: vec![
                ("http://w1".into(), f.clone()),
                ("http://w2".into(), f.clone()),
            ],
        };
        assert_eq!(
            Task::from_json(&r.to_json()).unwrap(),
            Task::Reduce(r.clone())
        );
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
