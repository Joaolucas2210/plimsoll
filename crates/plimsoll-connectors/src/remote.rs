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
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::stream::{self, BoxStream, StreamExt};
use plimsoll_core::engine::QueryTag;
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
}

impl Fragment {
    pub fn to_json(&self) -> Json {
        json!({
            "metadata_location": self.metadata_location,
            "snapshot_id": self.snapshot_id,
            "files": self.files,
            "columns": self.columns,
        })
    }

    pub fn from_json(v: &Json) -> Result<Self, String> {
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
        })
    }
}

/// One partition per fragment; each runs on its worker.
#[derive(Debug)]
pub struct RemoteScanExec {
    schema: SchemaRef,
    workers: Arc<Workers>,
    /// (worker url, fragment), in partition order.
    fragments: Vec<(String, Fragment)>,
    properties: Arc<PlanProperties>,
}

impl RemoteScanExec {
    /// Splits `files` round-robin over the workers (empty fragments are
    /// not sent).
    pub fn new(
        schema: SchemaRef,
        workers: Arc<Workers>,
        urls: &[String],
        metadata_location: &str,
        snapshot_id: Option<i64>,
        files: Vec<String>,
        columns: Vec<String>,
    ) -> Self {
        let n = urls.len().max(1);
        let mut split: Vec<Vec<String>> = vec![Vec::new(); n];
        for (i, f) in files.into_iter().enumerate() {
            split[i % n].push(f);
        }
        let fragments: Vec<(String, Fragment)> = urls
            .iter()
            .zip(split)
            .filter(|(_, files)| !files.is_empty())
            .map(|(url, files)| {
                (
                    url.clone(),
                    Fragment {
                        metadata_location: metadata_location.to_owned(),
                        snapshot_id,
                        files,
                        columns: columns.clone(),
                    },
                )
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
            fragments,
            properties,
        }
    }
}

impl DisplayAs for RemoteScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let files: usize = self.fragments.iter().map(|(_, fr)| fr.files.len()).sum();
        write!(
            f,
            "RemoteScanExec: fragments={} files={files}",
            self.fragments.len()
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
        let Some((url, fragment)) = self.fragments.get(partition).cloned() else {
            // No files at all: an empty partition.
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::empty(),
            )));
        };
        let workers = self.workers.clone();
        let body = stream::once(open(workers, url, fragment, tag))
            .flat_map(|r| match r {
                Ok(s) => s,
                Err(e) => stream::iter([Err(e)]).boxed(),
            })
            .boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            decode(body, schema),
        )))
    }
}

/// POSTs the fragment; errors before the stream keep the worker's message.
async fn open(
    workers: Arc<Workers>,
    url: String,
    fragment: Fragment,
    tag: Option<Arc<QueryTag>>,
) -> Result<BoxStream<'static, Result<Bytes>>> {
    let external = |m: String| DataFusionError::External(m.into());
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
                return Err(external(
                    "query deadline passed before a fragment was sent".into(),
                ));
            }
            request = request.header("x-plimsoll-deadline-ms", left.as_millis().to_string());
        }
    }
    let response = request
        .body(fragment.to_json().to_string())
        .send()
        .await
        .map_err(|e| external(format!("worker {url}: {e}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(external(format!("worker {url} answered {status}: {text}")));
    }
    let url = Arc::new(url);
    Ok(response
        .bytes_stream()
        .map(move |b| b.map_err(|e| external(format!("worker {url}: {e}"))))
        .boxed())
}

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
    }
    let state = State {
        bytes,
        decoder: StreamDecoder::new(),
        ready: VecDeque::new(),
        done: false,
        schema,
    };
    stream::unfold(state, |mut st| async move {
        loop {
            if let Some(batch) = st.ready.pop_front() {
                let batch = RecordBatch::try_new(st.schema.clone(), batch.columns().to_vec())
                    .map_err(DataFusionError::from);
                return Some((batch, st));
            }
            if st.done {
                return None;
            }
            match st.bytes.next().await {
                Some(Ok(chunk)) => {
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
                    if let Err(e) = st.decoder.finish() {
                        return Some((
                            Err(DataFusionError::External(
                                format!("worker stream ended early: {e}").into(),
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
        };
        assert_eq!(Fragment::from_json(&f.to_json()).unwrap(), f);
        assert!(Fragment::from_json(&json!({"files": []})).is_err());
        let w = Workers::new(vec!["http://w1/".into()], "secret".into()).unwrap();
        assert_eq!(w.urls(), ["http://w1"]);
        assert!(!format!("{w:?}").contains("secret"));
    }
}
