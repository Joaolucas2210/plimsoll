//! Queries across coordinators (ADR 0102).
//!
//! Coordinators behind one load balancer (ADR 0101) each know only their
//! own queries. With `PLIMSOLL_COORDINATORS`, `system.runtime.queries`
//! lists every coordinator's, and a cancel that finds nothing here is
//! passed to the others. They answer on `/v1/peer/queries`, with the
//! worker token, and never forward further, so the peer list may
//! include this node itself (a `dns+` name resolving to every replica).
//!
//! They also count each other to share admission limits (ADR 0103), and
//! with exact limits elect the one that admits for all (ADR 0104).

use std::collections::BTreeMap;
use std::sync::RwLock;
use std::time::Duration;

use axum::http::StatusCode;
use plimsoll_connectors::remote::Workers;
use plimsoll_control::governance::{AdmissionError, Gate, Permit, QueryId, QueryInfo};
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

    /// The ids and URLs of the peers that answer: with this node's, every
    /// live coordinator once (ADR 0103), whether or not the list names
    /// this one.
    pub async fn nodes(&self) -> Vec<(QueryId, String)> {
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
            Some((body["node"].as_u64()?, url.clone()))
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

/// The live coordinators, `me` and `peers` each counted once, and the
/// one that admits for all under exact limits (ADR 0104): the lowest id,
/// as a URL, or `None` when that is `me`.
pub fn elect(me: QueryId, peers: &[(QueryId, String)]) -> (usize, Option<String>) {
    let mut ids: Vec<QueryId> = peers.iter().map(|(id, _)| *id).chain([me]).collect();
    ids.sort_unstable();
    ids.dedup();
    let leader = peers
        .iter()
        .filter(|(id, _)| *id < me)
        .min_by_key(|(id, _)| *id)
        .map(|(_, url)| url.clone());
    (ids.len(), leader)
}

/// Exact cluster limits (ADR 0104): admission asked of the leader, whose
/// own counters then cover every coordinator. The slot is held there for
/// as long as the leader's response streams, so it is given back when
/// the query ends, or when this node dies.
#[derive(Debug)]
pub struct LeaderGate {
    peers: std::sync::Arc<Peers>,
    /// The leader's URL; `None` while this node leads.
    leader: RwLock<Option<String>>,
    /// How long a slot may be held: the query timeout.
    hold: Duration,
}

impl LeaderGate {
    pub fn new(peers: std::sync::Arc<Peers>, hold: Duration) -> Self {
        Self {
            peers,
            leader: RwLock::new(None),
            hold,
        }
    }

    /// The leader from now on, as [`elect`] found it.
    pub fn follow(&self, leader: Option<String>) {
        *self.leader.write().unwrap_or_else(|e| e.into_inner()) = leader;
    }
}

#[async_trait::async_trait]
impl Gate for LeaderGate {
    async fn admit(&self, user: &str, group: &str) -> Result<Option<Permit>, AdmissionError> {
        let leader = self
            .leader
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let Some(url) = leader else {
            return Ok(None);
        };
        // No overall timeout: the wait is the leader's queue, bounded by
        // the caller's deadline, and the body lasts as long as the query.
        let sent = self
            .peers
            .0
            .request(reqwest::Method::POST, &format!("{url}/v1/peer/admit"))
            .header("content-type", "application/json")
            .header("x-plimsoll-deadline-ms", self.hold.as_millis().to_string())
            .body(json!({ "user": user, "group": group }).to_string())
            .send()
            .await;
        let failure = match sent {
            Ok(r) if r.status() == reqwest::StatusCode::OK => return Ok(Some(Box::new(r))),
            Ok(r) if r.status() == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                let body = r.bytes().await.unwrap_or_default();
                return Err(serde_json::from_slice(&body).unwrap_or(AdmissionError::Closed));
            }
            Ok(r) => format!("HTTP {}", r.status()),
            Err(e) => e.to_string(),
        };
        // The leader is gone or not one: this node's part decides
        // (ADR 0103) until the next round elects another.
        log::emit(
            Level::Warn,
            "admission_leader_unreachable",
            json!({ "leader": url, "error": failure }),
        );
        Ok(None)
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

    #[test]
    fn the_lowest_live_node_leads() {
        let peers = |ids: &[u64]| -> Vec<(u64, String)> {
            ids.iter()
                .map(|id| (*id, format!("http://n{id}")))
                .collect()
        };
        // Alone, or listed among its own peers: it leads, counted once.
        assert_eq!(elect(5, &[]), (1, None));
        assert_eq!(elect(5, &peers(&[5, 9])), (2, None));
        assert_eq!(
            elect(5, &peers(&[9, 3, 5, 7])),
            (4, Some("http://n3".into()))
        );
    }
}
