//! Queries across coordinators (ADR 0102).
//!
//! Coordinators behind one load balancer (ADR 0101) each know only their
//! own queries. With `PLIMSOLL_COORDINATORS`, `system.runtime.queries`
//! lists every coordinator's, and a cancel that finds nothing here is
//! passed to the others. They answer on `/v1/peer/queries`, with the
//! worker token, and never forward further, so the peer list may
//! include this node itself (a `dns+` name resolving to every replica).

use std::collections::BTreeMap;
use std::time::Duration;

use axum::http::StatusCode;
use plimsoll_connectors::remote::Workers;
use plimsoll_control::governance::{QueryId, QueryInfo};
use serde_json::{Value, json};

use crate::log::{self, Level};

/// How long a peer may take to answer: a listing or cancel waits on the
/// slowest one.
const TIMEOUT: Duration = Duration::from_secs(2);

/// The other coordinators: the same seeds, discovery, health checks and
/// shared token as workers (ADR 0058, 0070).
#[derive(Debug)]
pub struct Peers(pub Workers);

impl Peers {
    /// Every peer's queries, unscoped: the caller applies its viewer. A
    /// peer that does not answer is left out and logged, so losing a
    /// coordinator never fails the listing.
    pub async fn queries(&self) -> Vec<QueryInfo> {
        let urls = self.0.healthy_urls();
        let calls = urls.iter().map(|url| async move {
            let r = self
                .0
                .request(reqwest::Method::GET, &format!("{url}/v1/peer/queries"))
                .timeout(TIMEOUT)
                .send()
                .await
                .and_then(reqwest::Response::error_for_status);
            match r {
                Ok(r) => match r.bytes().await {
                    Ok(body) => {
                        serde_json::from_slice::<Vec<QueryInfo>>(&body).map_err(|e| e.to_string())
                    }
                    Err(e) => Err(e.to_string()),
                },
                Err(e) => Err(e.to_string()),
            }
            .inspect_err(|e| {
                log::emit(
                    Level::Warn,
                    "peer_unreachable",
                    json!({ "peer": url, "error": e }),
                );
            })
            .unwrap_or_default()
        });
        futures::future::join_all(calls)
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    /// The ids of the peers that answer: with this node's, every live
    /// coordinator once (ADR 0103), whether or not the list names this one.
    pub async fn nodes(&self) -> Vec<QueryId> {
        let urls = self.0.healthy_urls();
        let calls = urls.iter().map(|url| async move {
            let r = self
                .0
                .request(reqwest::Method::GET, &format!("{url}/v1/peer/node"))
                .timeout(TIMEOUT)
                .send()
                .await
                .ok()?
                .error_for_status()
                .ok()?;
            let body: Value = serde_json::from_slice(&r.bytes().await.ok()?).ok()?;
            body["node"].as_u64()
        });
        futures::future::join_all(calls)
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    /// Asks every peer to cancel `id` (only if `only` owns it, when set).
    /// The first answer other than 404, or `None` when no peer has it.
    pub async fn cancel(&self, id: QueryId, only: Option<&str>) -> Option<(StatusCode, Value)> {
        let urls = self.0.healthy_urls();
        let calls = urls.iter().map(|url| async move {
            let mut req = self
                .0
                .request(
                    reqwest::Method::DELETE,
                    &format!("{url}/v1/peer/queries/{id}"),
                )
                .timeout(TIMEOUT);
            if let Some(user) = only {
                req = req.header("x-plimsoll-owner", user);
            }
            let r = req.send().await.ok()?;
            let status = StatusCode::from_u16(r.status().as_u16()).ok()?;
            if status == StatusCode::NOT_FOUND {
                return None;
            }
            let body = r.bytes().await.unwrap_or_default();
            Some((status, serde_json::from_slice(&body).unwrap_or(Value::Null)))
        });
        futures::future::join_all(calls)
            .await
            .into_iter()
            .flatten()
            .next()
    }
}

/// `local` and the peers' queries as one list: by id, which no two
/// coordinators share (ADR 0101), so this node answering as its own peer
/// counts once.
pub fn merge(local: Vec<QueryInfo>, peers: Vec<QueryInfo>) -> Vec<QueryInfo> {
    let mut by_id: BTreeMap<QueryId, QueryInfo> = BTreeMap::new();
    for q in local.into_iter().chain(peers) {
        by_id.entry(q.id).or_insert(q);
    }
    by_id.into_values().collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use plimsoll_control::governance::QueryState;

    #[test]
    fn queries_round_trip_and_merge_by_id() {
        let q = |id, user: &str| QueryInfo {
            id,
            user: user.into(),
            resource_group: "default".into(),
            sql: "SELECT 1".into(),
            state: QueryState::Running,
            created_at_ms: 1,
            started_at_ms: Some(2),
            finished_at_ms: None,
            error: None,
        };
        let json = serde_json::to_value(vec![q(5, "a")]).unwrap();
        assert_eq!(json[0]["state"], "running");
        let back: Vec<QueryInfo> = serde_json::from_value(json).unwrap();
        assert_eq!(back, vec![q(5, "a")]);
        // 5 came back from this node answering as a peer: counted once.
        let all = merge(vec![q(5, "a"), q(9, "b")], vec![q(5, "a"), q(7, "c")]);
        let ids: Vec<_> = all.iter().map(|q| q.id).collect();
        assert_eq!(ids, [5, 7, 9]);
    }
}
