//! Scheduled maintenance (ADR 0086): every `PLIMSOLL_MAINTENANCE_INTERVAL_MS`,
//! each catalog table whose properties opt in is compacted, has its old
//! snapshots expired and, if asked, its orphan files removed, the same
//! operations as `/v1/maintenance/*`, one table at a time.
//!
//! The policy is the table's own, in its properties, so it travels with
//! the table and any engine can read or change it:
//!
//! | property | default |
//! |---|---|
//! | `plimsoll.maintenance.enabled` | `false`: the table is skipped |
//! | `plimsoll.maintenance.compact` | `true` |
//! | `plimsoll.maintenance.expire-snapshots` | `true` |
//! | `history.expire.max-snapshot-age-ms` (Iceberg's) | 5 days |
//! | `history.expire.min-snapshots-to-keep` (Iceberg's) | 1 |
//! | `plimsoll.maintenance.remove-orphans` | `false` |
//! | `plimsoll.maintenance.orphan-age-ms` | 3 days, never under 1 hour |

use std::sync::Arc;
use std::time::{Duration, Instant};

use plimsoll_connectors::iceberg::expire::{
    DEFAULT_MAX_AGE_MS, DEFAULT_RETAIN_LAST, ExpireOptions, OrphanOptions,
};
use plimsoll_control::catalog::{Catalog, TableIdent};
use plimsoll_control::governance::{Admission, DEFAULT_GROUP};
use plimsoll_core::datafusion::prelude::SessionContext;
use serde_json::{Value, json};

use crate::app::{ORPHAN_DEFAULT_AGE_MS, ORPHAN_MIN_AGE_MS};
use crate::log::{self, Level};

/// Who scheduled maintenance runs as, for admission and logs.
pub const MAINTENANCE_USER: &str = "plimsoll-maintenance";

/// What to do to one table, from its properties.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    pub compact: bool,
    pub expire: Option<ExpireOptions>,
    pub orphans: Option<OrphanOptions>,
}

impl Policy {
    /// `None` when the table has not opted in; `Err` names a property
    /// whose value is not understood (the table is then left alone).
    pub fn of(properties: &Value, now_ms: i64) -> Result<Option<Self>, String> {
        let get = |k: &str| properties.get(k).and_then(Value::as_str);
        let flag = |k: &str, default: bool| match get(k) {
            None => Ok(default),
            Some(v) if v.eq_ignore_ascii_case("true") => Ok(true),
            Some(v) if v.eq_ignore_ascii_case("false") => Ok(false),
            Some(v) => Err(format!("{k}: expected true or false, got {v:?}")),
        };
        let number = |k: &str, default: i64| match get(k) {
            None => Ok(default),
            Some(v) => v
                .trim()
                .parse::<i64>()
                .ok()
                .filter(|n| *n >= 0)
                .ok_or_else(|| format!("{k}: expected a non-negative integer, got {v:?}")),
        };
        if !flag("plimsoll.maintenance.enabled", false)? {
            return Ok(None);
        }
        let expire = flag("plimsoll.maintenance.expire-snapshots", true)?
            .then(|| -> Result<_, String> {
                Ok(ExpireOptions {
                    older_than_ms: now_ms
                        - number("history.expire.max-snapshot-age-ms", DEFAULT_MAX_AGE_MS)?,
                    retain_last: usize::try_from(number(
                        "history.expire.min-snapshots-to-keep",
                        DEFAULT_RETAIN_LAST as i64,
                    )?)
                    .unwrap_or(DEFAULT_RETAIN_LAST)
                    .max(1),
                })
            })
            .transpose()?;
        let orphans = flag("plimsoll.maintenance.remove-orphans", false)?
            .then(|| -> Result<_, String> {
                let age = number("plimsoll.maintenance.orphan-age-ms", ORPHAN_DEFAULT_AGE_MS)?;
                Ok(OrphanOptions {
                    older_than_ms: now_ms - age.max(ORPHAN_MIN_AGE_MS),
                    dry_run: false,
                })
            })
            .transpose()?;
        Ok(Some(Self {
            compact: flag("plimsoll.maintenance.compact", true)?,
            expire,
            orphans,
        }))
    }
}

/// One pass over every table of every single-level namespace of
/// `catalog` (the ones served as schemas). Each step of each table logs a
/// `scheduled_maintenance` line; a failure moves on to the next step's
/// table. With `admission`, each table holds a slot while it runs, so
/// maintenance queues behind queries like any other work. With `lease`
/// (holder, how long), a table is skipped while another coordinator's
/// claim on it holds, and claimed for that long otherwise (ADR 0101).
/// Returns how many tables were maintained.
pub async fn pass(
    ctx: &SessionContext,
    catalog: &dyn Catalog,
    admission: Option<&Admission>,
    lease: Option<(&str, Duration)>,
) -> usize {
    let warn = |what: &str, table: &str, e: String| {
        log::emit(
            Level::Warn,
            "scheduled_maintenance",
            json!({"table": table, "step": what, "error": e}),
        );
    };
    let namespaces = match catalog.list_namespaces().await {
        Ok(n) => n,
        Err(e) => {
            warn("list", "*", e.to_string());
            return 0;
        }
    };
    let mut maintained = 0;
    for ns in namespaces.into_iter().filter(|n| n.0.len() == 1) {
        let tables = match catalog.list_tables(&ns).await {
            Ok(t) => t,
            Err(e) => {
                warn("list", &ns.to_string(), e.to_string());
                continue;
            }
        };
        for ident in tables {
            if run_table(ctx, catalog, admission, lease, &ident).await {
                maintained += 1;
            }
        }
    }
    maintained
}

async fn run_table(
    ctx: &SessionContext,
    catalog: &dyn Catalog,
    admission: Option<&Admission>,
    lease: Option<(&str, Duration)>,
    ident: &TableIdent,
) -> bool {
    let table = ident.to_string();
    let meta = match catalog.load_table(ident).await {
        Ok(t) => t.metadata,
        Err(e) => {
            log::emit(
                Level::Warn,
                "scheduled_maintenance",
                json!({"table": table, "step": "load", "error": e.to_string()}),
            );
            return false;
        }
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);
    let policy = match Policy::of(&meta["properties"], now) {
        Ok(Some(p)) => p,
        Ok(None) => return false,
        Err(e) => {
            log::emit(
                Level::Warn,
                "scheduled_maintenance",
                json!({"table": table, "step": "policy", "error": e}),
            );
            return false;
        }
    };
    let _slot = match admission {
        Some(a) => match a.acquire(MAINTENANCE_USER, DEFAULT_GROUP).await {
            Ok(slot) => Some(slot),
            Err(e) => {
                log::emit(
                    Level::Warn,
                    "scheduled_maintenance",
                    json!({"table": table, "step": "admission", "error": e.to_string()}),
                );
                return false;
            }
        },
        None => None,
    };
    // Claimed once a slot is held, so the claim covers the work, not the
    // wait for it.
    if let Some((holder, ttl)) = lease {
        let ttl_ms = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
        match crate::lakehouse::claim_maintenance(ctx, &table, holder, ttl_ms).await {
            Ok(true) => {}
            Ok(false) => {
                log::emit(
                    Level::Info,
                    "scheduled_maintenance",
                    json!({"table": table, "step": "claim", "skipped": "held by another coordinator"}),
                );
                return false;
            }
            Err(e) => {
                log::emit(
                    Level::Warn,
                    "scheduled_maintenance",
                    json!({"table": table, "step": "claim", "error": e.to_string()}),
                );
                return false;
            }
        }
    }
    let step = |name: &'static str, started: Instant, outcome: Result<Value, String>| {
        let ms = (started.elapsed().as_micros() as f64) / 1e3;
        match outcome {
            Ok(mut fields) => {
                fields["table"] = json!(table);
                fields["step"] = json!(name);
                fields["total_ms"] = json!(ms);
                log::emit(Level::Info, "scheduled_maintenance", fields);
            }
            Err(e) => log::emit(
                Level::Warn,
                "scheduled_maintenance",
                json!({"table": table, "step": name, "error": e, "total_ms": ms}),
            ),
        }
    };
    // Compaction first: expiration then reclaims the files it replaced.
    if policy.compact {
        let started = Instant::now();
        let r = crate::lakehouse::compact(ctx, &table).await;
        step(
            "compact",
            started,
            r.map(|r| {
                json!({"partitions": r.partitions, "files_rewritten": r.files_rewritten,
                       "files_written": r.files_written, "rows": r.rows})
            })
            .map_err(|e| e.to_string()),
        );
    }
    if let Some(opts) = policy.expire {
        let started = Instant::now();
        let r = crate::lakehouse::expire_snapshots(ctx, &table, opts).await;
        step(
            "expire_snapshots",
            started,
            r.map(|r| {
                json!({"snapshots_expired": r.snapshots_expired,
                       "data_files_deleted": r.data_files_deleted,
                       "delete_files_deleted": r.delete_files_deleted,
                       "manifests_deleted": r.manifests_deleted})
            })
            .map_err(|e| e.to_string()),
        );
    }
    if let Some(opts) = policy.orphans {
        let started = Instant::now();
        let r = crate::lakehouse::remove_orphans(ctx, &table, opts).await;
        step(
            "remove_orphans",
            started,
            r.map(|r| json!({"scanned": r.scanned, "deleted": r.deleted}))
                .map_err(|e| e.to_string()),
        );
    }
    true
}

/// Runs [`pass`] every `every`, the next one starting `every` after the
/// last ended (passes never overlap). Each table is claimed for `every`
/// as `holder`: of several coordinators, one maintains it per interval.
pub async fn every(
    ctx: SessionContext,
    catalog: Arc<dyn Catalog>,
    admission: Admission,
    every: Duration,
    holder: String,
) {
    loop {
        tokio::time::sleep(every).await;
        let started = Instant::now();
        let tables = pass(
            &ctx,
            catalog.as_ref(),
            Some(&admission),
            Some((&holder, every)),
        )
        .await;
        log::emit(
            Level::Info,
            "scheduled_maintenance_pass",
            json!({"tables": tables, "total_ms": started.elapsed().as_millis() as u64}),
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn policies_come_from_table_properties() {
        let now = 10 * ORPHAN_DEFAULT_AGE_MS;
        assert_eq!(Policy::of(&json!({}), now), Ok(None));
        assert_eq!(
            Policy::of(&json!({"plimsoll.maintenance.enabled": "false"}), now),
            Ok(None)
        );
        assert_eq!(
            Policy::of(&json!({"plimsoll.maintenance.enabled": "true"}), now),
            Ok(Some(Policy {
                compact: true,
                expire: Some(ExpireOptions {
                    older_than_ms: now - DEFAULT_MAX_AGE_MS,
                    retain_last: 1
                }),
                orphans: None,
            }))
        );
        let p = Policy::of(
            &json!({
                "plimsoll.maintenance.enabled": "TRUE",
                "plimsoll.maintenance.compact": "false",
                "history.expire.max-snapshot-age-ms": "0",
                "history.expire.min-snapshots-to-keep": "0",
                "plimsoll.maintenance.remove-orphans": "true",
                "plimsoll.maintenance.orphan-age-ms": "1",
            }),
            now,
        )
        .unwrap()
        .unwrap();
        assert!(!p.compact);
        assert_eq!(
            p.expire,
            Some(ExpireOptions {
                older_than_ms: now,
                retain_last: 1
            })
        );
        // Never under the in-flight window.
        assert_eq!(p.orphans.unwrap().older_than_ms, now - ORPHAN_MIN_AGE_MS);
        assert!(!p.orphans.unwrap().dry_run);
        for bad in [
            json!({"plimsoll.maintenance.enabled": "yes"}),
            json!({"plimsoll.maintenance.enabled": "true", "history.expire.max-snapshot-age-ms": "-1"}),
            json!({"plimsoll.maintenance.enabled": "true", "plimsoll.maintenance.remove-orphans": "1"}),
        ] {
            assert!(Policy::of(&bad, now).is_err(), "{bad}");
        }
    }
}
