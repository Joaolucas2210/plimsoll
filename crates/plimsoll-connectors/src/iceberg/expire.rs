//! Snapshot expiration (ADR 0079) and orphan file removal (ADR 0080).
//!
//! Old snapshots keep every file they ever referenced alive. Expiring
//! drops them from the table's metadata through the catalog
//! (`remove-snapshots`), then deletes the files only they reached:
//! manifest lists, manifests, and data and delete files no kept snapshot
//! reads. Files are deleted after the commit succeeds, never before.

use std::collections::{HashMap, HashSet};

use apache_avro::types::Value as Avro;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::object_store::ObjectStoreExt;
use plimsoll_core::storage::ScopedStore;
use serde_json::{Value as Json, json};

use super::write::{CommitError, Committer};
use super::{read_env, store_url};

/// Iceberg's defaults: `history.expire.max-snapshot-age-ms` (5 days) and
/// `history.expire.min-snapshots-to-keep` (1).
pub const DEFAULT_MAX_AGE_MS: i64 = 5 * 24 * 3_600_000;
pub const DEFAULT_RETAIN_LAST: usize = 1;

/// Which snapshots to keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpireOptions {
    /// Snapshots committed before this instant (ms since the epoch) may go.
    pub older_than_ms: i64,
    /// The most recent ancestors of every branch kept regardless of age.
    pub retain_last: usize,
}

/// What an expiration did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Expiration {
    pub snapshots_expired: usize,
    pub manifest_lists_deleted: usize,
    pub manifests_deleted: usize,
    pub data_files_deleted: usize,
    pub delete_files_deleted: usize,
}

/// The snapshots of `meta` to expire under `opts`. Kept: every branch
/// and tag head; per branch, its `retain_last` most recent ancestors and
/// every ancestor newer than the cutoff; any other snapshot newer than
/// the cutoff.
pub(super) fn expired(meta: &Json, opts: ExpireOptions) -> Vec<i64> {
    let snapshots: HashMap<i64, (i64, Option<i64>)> = meta["snapshots"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| {
            Some((
                s["snapshot-id"].as_i64()?,
                (
                    s["timestamp-ms"].as_i64().unwrap_or(i64::MAX),
                    s["parent-snapshot-id"].as_i64(),
                ),
            ))
        })
        .collect();
    let mut refs: Vec<(i64, bool)> = meta["refs"]
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(_, r)| Some((r["snapshot-id"].as_i64()?, r["type"] != "tag")))
        .collect();
    if refs.is_empty() {
        if let Some(current) = meta["current-snapshot-id"].as_i64().filter(|id| *id >= 0) {
            refs.push((current, true));
        }
    }
    let mut keep = HashSet::new();
    for (head, branch) in refs {
        keep.insert(head);
        if !branch {
            continue;
        }
        let (mut at, mut depth) = (Some(head), 0);
        while let Some(id) = at {
            let Some((ts, parent)) = snapshots.get(&id) else {
                break;
            };
            if depth < opts.retain_last || *ts >= opts.older_than_ms {
                keep.insert(id);
            }
            depth += 1;
            at = *parent;
        }
    }
    let mut out: Vec<i64> = snapshots
        .iter()
        .filter(|(id, (ts, _))| !keep.contains(id) && *ts < opts.older_than_ms)
        .map(|(id, _)| *id)
        .collect();
    out.sort_unstable();
    out
}

/// Everything a set of snapshots reaches: manifest lists, manifests, and
/// the data and delete files live (ADDED or EXISTING) in them.
#[derive(Debug, Default)]
struct Reach {
    lists: HashSet<String>,
    manifests: HashSet<String>,
    data: HashSet<String>,
    deletes: HashSet<String>,
}

async fn reach(
    meta: &Json,
    ids: &HashSet<i64>,
    env: &RuntimeEnv,
    scoped: Option<&ScopedStore>,
    deleted_too: bool,
) -> Result<Reach> {
    let mut out = Reach::default();
    for snap in meta["snapshots"].as_array().into_iter().flatten() {
        if !snap["snapshot-id"]
            .as_i64()
            .is_some_and(|id| ids.contains(&id))
        {
            continue;
        }
        let Some(list) = snap["manifest-list"].as_str() else {
            continue;
        };
        if !out.lists.insert(list.to_owned()) {
            continue;
        }
        let bytes = read_env(list, env, scoped).await?;
        for entry in avro(&bytes)? {
            let Some(path) = text(&entry, "manifest_path") else {
                continue;
            };
            if !out.manifests.insert(path.clone()) {
                continue;
            }
            let bytes = read_env(&path, env, scoped).await?;
            for entry in avro(&bytes)? {
                // DELETED entries name files their snapshot removed: not
                // read by it, but still named (orphan removal keeps them).
                if !deleted_too && field(&entry, "status").and_then(long) == Some(2) {
                    continue;
                }
                let Some(file) = field(&entry, "data_file") else {
                    continue;
                };
                let Some(path) = text(file, "file_path") else {
                    continue;
                };
                if field(file, "content").and_then(long).unwrap_or(0) == 0 {
                    out.data.insert(path);
                } else {
                    out.deletes.insert(path);
                }
            }
        }
    }
    Ok(out)
}

fn avro(bytes: &[u8]) -> Result<Vec<Avro>> {
    let reader =
        apache_avro::Reader::new(bytes).map_err(|e| DataFusionError::External(Box::new(e)))?;
    reader
        .map(|e| e.map_err(|e| DataFusionError::External(Box::new(e))))
        .collect()
}

fn field<'a>(v: &'a Avro, name: &str) -> Option<&'a Avro> {
    let Avro::Record(fields) = v else { return None };
    let v = fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)?;
    Some(match v {
        Avro::Union(_, inner) => inner.as_ref(),
        v => v,
    })
}

fn text(v: &Avro, name: &str) -> Option<String> {
    match field(v, name)? {
        Avro::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn long(v: &Avro) -> Option<i64> {
    match v {
        Avro::Long(n) => Some(*n),
        Avro::Int(n) => Some(i64::from(*n)),
        _ => None,
    }
}

/// Commits the expiration of every snapshot `opts` lets go, then deletes
/// the files only those snapshots reached. A commit that loses a race is
/// recomputed on the fresh metadata.
pub(super) async fn expire(
    committer: &dyn Committer,
    opts: ExpireOptions,
    env: &RuntimeEnv,
    scoped: Option<&ScopedStore>,
) -> Result<Expiration> {
    let failed = |m: String| DataFusionError::Execution(format!("expire snapshots: {m}"));
    for _ in 0..4 {
        let meta = committer.current().await.map_err(failed)?;
        let gone = expired(&meta, opts);
        if gone.is_empty() {
            return Ok(Expiration::default());
        }
        let main = meta["refs"]["main"]["snapshot-id"].as_i64();
        let requirements = vec![
            json!({"type": "assert-table-uuid", "uuid": meta["table-uuid"]}),
            json!({"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": main}),
        ];
        let updates = vec![json!({"action": "remove-snapshots", "snapshot-ids": gone})];
        match committer.commit(requirements, updates).await {
            Ok(()) => {}
            Err(CommitError::Conflict(_)) => continue,
            Err(CommitError::Failed(m)) => return Err(failed(m)),
        }
        // Committed: what only the expired snapshots reached can go.
        let gone: HashSet<i64> = gone.into_iter().collect();
        let kept: HashSet<i64> = meta["snapshots"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| s["snapshot-id"].as_i64())
            .filter(|id| !gone.contains(id))
            .collect();
        let (old, live) = (
            reach(&meta, &gone, env, scoped, false).await?,
            reach(&meta, &kept, env, scoped, false).await?,
        );

        let lists = only(&old.lists, &live.lists);
        let manifests = only(&old.manifests, &live.manifests);
        let data = only(&old.data, &live.data);
        let deletes = only(&old.deletes, &live.deletes);
        for paths in [&lists, &manifests, &data, &deletes] {
            remove(paths, env, scoped).await?;
        }
        return Ok(Expiration {
            snapshots_expired: gone.len(),
            manifest_lists_deleted: lists.len(),
            manifests_deleted: manifests.len(),
            data_files_deleted: data.len(),
            delete_files_deleted: deletes.len(),
        });
    }
    Err(failed(
        "the table kept changing; nothing was expired".into(),
    ))
}

/// Which files orphan removal may delete (ADR 0080).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrphanOptions {
    /// Only files last modified before this instant (ms since the epoch):
    /// a write in flight has files no commit references yet.
    pub older_than_ms: i64,
    /// List them; delete nothing.
    pub dry_run: bool,
}

/// What orphan removal found.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Orphans {
    /// Files listed under the table location.
    pub scanned: usize,
    /// Orphans found (full paths, sorted).
    pub orphans: Vec<String>,
    pub deleted: usize,
}

/// Files under `location` that no snapshot, manifest, statistics file or
/// metadata references, last modified before the cutoff; deleted unless
/// `dry_run`. The catalog's `*.metadata.json` and `version-hint.text`,
/// and hidden files, are never candidates.
pub(super) async fn orphans(
    meta: &Json,
    location: &str,
    opts: OrphanOptions,
    env: &RuntimeEnv,
    scoped: Option<&ScopedStore>,
) -> Result<Orphans> {
    use futures::TryStreamExt;
    let all: HashSet<i64> = meta["snapshots"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| s["snapshot-id"].as_i64())
        .collect();
    let reached = reach(meta, &all, env, scoped, true).await?;
    let statistics = ["statistics", "partition-statistics"]
        .into_iter()
        .flat_map(|key| meta[key].as_array().into_iter().flatten())
        .filter_map(|s| s["statistics-path"].as_str().map(str::to_owned));
    // Compared as object paths: `file:/x`, `file:///x` and `s3://b/x`
    // spellings of one file agree there.
    let mut referenced = HashSet::new();
    for path in reached
        .lists
        .into_iter()
        .chain(reached.manifests)
        .chain(reached.data)
        .chain(reached.deletes)
        .chain(statistics)
    {
        referenced.insert(ListingTableUrl::parse(&path)?.prefix().clone());
    }
    let root = ListingTableUrl::parse(format!("{}/", location.trim_end_matches('/')))?;
    let store = env.object_store(store_url(&root, scoped))?;
    let base = store_url(&root, scoped);
    let listed: Vec<_> = store
        .list(Some(root.prefix()))
        .try_collect()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut out = Orphans {
        scanned: listed.len(),
        ..Orphans::default()
    };
    for object in listed {
        let name = object.location.filename().unwrap_or_default();
        let catalogs = name.ends_with(".metadata.json") || name == "version-hint.text";
        if catalogs
            || name.starts_with('.')
            || object.last_modified.timestamp_millis() >= opts.older_than_ms
            || referenced.contains(&object.location)
        {
            continue;
        }
        out.orphans
            .push(format!("{}{}", base.as_str(), object.location));
    }
    out.orphans.sort();
    if !opts.dry_run {
        remove(&out.orphans, env, scoped).await?;
        out.deleted = out.orphans.len();
    }
    Ok(out)
}

/// Deletes one object; an already-missing one is fine.
async fn delete_one(
    (store, path): (
        std::sync::Arc<dyn datafusion::object_store::ObjectStore>,
        datafusion::object_store::path::Path,
    ),
) -> Result<()> {
    match store.delete(&path).await {
        Ok(()) | Err(datafusion::object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(DataFusionError::External(Box::new(e))),
    }
}

/// What `a` reaches that `b` does not.
fn only(a: &HashSet<String>, b: &HashSet<String>) -> Vec<String> {
    a.difference(b).cloned().collect()
}

/// Deletes `paths`, a few at a time. Already-missing files are fine.
async fn remove(paths: &[String], env: &RuntimeEnv, scoped: Option<&ScopedStore>) -> Result<()> {
    let mut jobs = Vec::with_capacity(paths.len());
    for path in paths {
        let url = ListingTableUrl::parse(path)?;
        jobs.push((
            env.object_store(store_url(&url, scoped))?,
            url.prefix().clone(),
        ));
    }
    // Sixteen at a time; futures collected before awaiting (a lazy
    // `map` over the stream would make this future not `Send`).
    for chunk in jobs.chunks(16) {
        let deletes: Vec<_> = chunk.iter().cloned().map(delete_one).collect();
        for done in futures::future::join_all(deletes).await {
            done?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn heads_recent_and_last_ancestors_are_kept() {
        // main: 1 <- 2 <- 3 <- 4 (head); tag `audit` on 1; 9 is a stray
        // snapshot (a rolled-back write), 10 a recent stray.
        let snap = |id: i64, ts: i64, parent: Option<i64>| json!({"snapshot-id": id, "timestamp-ms": ts, "parent-snapshot-id": parent});
        let meta = json!({
            "snapshots": [
                snap(1, 100, None), snap(2, 200, Some(1)), snap(3, 300, Some(2)),
                snap(4, 400, Some(3)), snap(9, 150, Some(1)), snap(10, 500, Some(1)),
            ],
            "refs": {
                "main": {"snapshot-id": 4, "type": "branch"},
                "audit": {"snapshot-id": 1, "type": "tag"},
            },
        });
        let at = |older_than_ms: i64, retain_last: usize| {
            expired(
                &meta,
                ExpireOptions {
                    older_than_ms,
                    retain_last,
                },
            )
        };
        // Everything older than 350 except the head, the tag and the
        // recent stray.
        assert_eq!(at(350, 1), [2, 3, 9]);
        // Keep the last three of main whatever their age.
        assert_eq!(at(350, 3), [9]);
        // Nothing is older than 50.
        assert!(at(50, 1).is_empty());
        // A table without refs: the current snapshot is main.
        let bare = json!({
            "snapshots": [snap(1, 100, None), snap(2, 200, Some(1))],
            "current-snapshot-id": 2,
        });
        assert_eq!(
            expired(
                &bare,
                ExpireOptions {
                    older_than_ms: 1000,
                    retain_last: 1
                }
            ),
            [1]
        );
    }
}
