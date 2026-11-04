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
//! with exact limits elect the one that admits for all (ADR 0104), which
//! asks the others to report before it admits anything (ADR 0107).

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::http::StatusCode;
use plimsoll_connectors::remote::Workers;
use plimsoll_control::governance::{Admission, AdmissionError, Gate, Permit, QueryId, QueryInfo};
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
pub fn elect(me: QueryId, peers: &[(QueryId, String)]) -> Elected {
    let mut ids: Vec<QueryId> = peers.iter().map(|(id, _)| *id).chain([me]).collect();
    ids.sort_unstable();
    ids.dedup();
    let leader = peers
        .iter()
        .filter(|(id, _)| *id < me)
        .min_by_key(|(id, _)| *id)
        .cloned();
    (ids.len(), leader)
}

/// Between peer rounds: health checks, membership, election (ADR 0102).
pub const PEER_ROUND: Duration = Duration::from_secs(5);

/// The longest a new leader starts nothing new (ADR 0106): every
/// follower's next round, its checks and its re-homing fit in two rounds.
/// Ended as soon as every peer has reported (ADR 0107).
pub const TAKEOVER_HOLD: Duration = Duration::from_secs(10);

/// What [`elect`] found: live coordinators and the leader, `None` when
/// that is this node.
type Elected = (usize, Option<(QueryId, String)>);

/// Shared admission among coordinators (ADR 0103–0107): one round elects,
/// sets this node's part, and under exact limits follows the leader.
pub struct Coordination {
    pub peers: Arc<Peers>,
    admission: Admission,
    /// `Some` under exact limits (ADR 0104).
    gate: Option<Arc<LeaderGate>>,
    /// The last round's outcome; `None` before the first. In it, a `None`
    /// leader is this node.
    last: tokio::sync::Mutex<Option<Elected>>,
}

impl Coordination {
    /// With `gate`, the limits are exact: this node holds new admissions
    /// until its first round says whether it leads (ADR 0106).
    pub fn new(peers: Arc<Peers>, admission: Admission, gate: Option<Arc<LeaderGate>>) -> Self {
        if let Some(gate) = &gate {
            admission.set_gate(gate.clone());
            admission.hold(TAKEOVER_HOLD);
        }
        Self {
            peers,
            admission,
            gate,
            last: tokio::sync::Mutex::new(None),
        }
    }

    /// One round: who is alive, who leads, and what follows from it. When
    /// this node takes over and `announce` is set, it asks every live peer
    /// to run its round now, and ends its hold once all of them follow it
    /// (ADR 0107). Returns the leader's node id as this node sees it.
    pub async fn round(self: &Arc<Self>, announce: bool) -> QueryId {
        let me = self.admission.registry().first_id();
        let alive = self.peers.nodes().await;
        let (coordinators, leader) = elect(me, &alive);
        let mut last = self.last.lock().await;
        if last.as_ref() != Some(&(coordinators, leader.clone())) {
            let took_over = leader.is_none() && !matches!(*last, Some((_, None)));
            match &self.gate {
                Some(gate) => {
                    // A leader holds the whole limits.
                    self.admission.share_with(match leader {
                        Some(_) => coordinators,
                        None => 1,
                    });
                    if took_over {
                        self.admission.hold(TAKEOVER_HOLD);
                    }
                    gate.follow(leader.as_ref().map(|(_, url)| url.clone()), &self.admission)
                        .await;
                    // Followers ask the leader; nothing to wait for here.
                    if leader.is_some() {
                        self.admission.end_hold();
                    }
                    if took_over && announce {
                        tokio::spawn(self.clone().announce(me, alive));
                    }
                }
                None => self.admission.share_with(coordinators),
            }
            log::emit(
                Level::Info,
                "admission_shared",
                json!({
                    "coordinators": coordinators,
                    "leader": self.gate.as_ref().map(|_| leader.as_ref().map_or("self", |(_, url)| url.as_str())),
                }),
            );
            *last = Some((coordinators, leader.clone()));
        }
        leader.map_or(me, |(id, _)| id)
    }

    /// Asks every peer that answered this round to run its own (`POST
    /// /v1/peer/round`), which re-homes its running queries here before it
    /// answers. The hold ends when all of them name this node.
    async fn announce(self: Arc<Self>, me: QueryId, alive: Vec<(QueryId, String)>) {
        let peers = &self.peers;
        let calls = alive.iter().map(|(_, url)| async move {
            let r = peers
                .0
                .request(reqwest::Method::POST, &format!("{url}/v1/peer/round"))
                .timeout(TAKEOVER_HOLD)
                .send()
                .await
                .ok()?
                .error_for_status()
                .ok()?;
            let body: Value = serde_json::from_slice(&r.bytes().await.ok()?).ok()?;
            body["leader"].as_u64()
        });
        let answers = futures::future::join_all(calls).await;
        let following = answers.iter().filter(|a| **a == Some(me)).count();
        if following == answers.len() {
            self.admission.end_hold();
        }
        log::emit(
            Level::Info,
            "admission_takeover",
            json!({ "peers": answers.len(), "following": following,
                    "hold_ended": following == answers.len() }),
        );
    }
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

    /// The leader from now on, as [`elect`] found it. When it changes,
    /// this node's running queries are re-homed (ADR 0105): adopted by the
    /// new leader, whose counts then include them, and released at the
    /// old one; or, when this node leads, counted here only.
    pub async fn follow(&self, leader: Option<String>, admission: &Admission) {
        {
            let mut current = self.leader.write().unwrap_or_else(|e| e.into_inner());
            if *current == leader {
                return;
            }
            current.clone_from(&leader);
        }
        let held = admission.held();
        if held.is_empty() {
            return;
        }
        let rehome = held.iter().map(|h| {
            let leader = leader.as_deref();
            async move {
                let Some(url) = leader else {
                    h.replace(None);
                    return false;
                };
                let sent = self.post(url, &h.user, &h.group, true).await;
                match sent
                    .map_err(|e| e.to_string())
                    .and_then(|r| match r.status() {
                        reqwest::StatusCode::OK => Ok(r),
                        status => Err(format!("HTTP {}", status.as_u16())),
                    }) {
                    Ok(permit) => {
                        h.replace(Some(Box::new(permit)));
                        true
                    }
                    // Kept where it was: the old leader, if it is alive,
                    // still counts it.
                    Err(e) => {
                        log::emit(
                            Level::Warn,
                            "admission_rehome_failed",
                            json!({ "leader": url, "error": e }),
                        );
                        false
                    }
                }
            }
        });
        let adopted = futures::future::join_all(rehome)
            .await
            .into_iter()
            .filter(|a| *a)
            .count();
        log::emit(
            Level::Info,
            "admission_rehomed",
            json!({ "leader": leader.as_deref().unwrap_or("self"),
                    "queries": held.len(), "adopted": adopted }),
        );
    }

    /// Asks `url` to admit, or with `adopt` to count a running query. A
    /// 200's body holds the slot for as long as it is kept.
    async fn post(
        &self,
        url: &str,
        user: &str,
        group: &str,
        adopt: bool,
    ) -> Result<reqwest::Response, reqwest::Error> {
        // No overall timeout: the wait is the leader's queue, bounded by
        // the caller's deadline, and the body lasts as long as the query.
        self.peers
            .0
            .request(reqwest::Method::POST, &format!("{url}/v1/peer/admit"))
            .header("content-type", "application/json")
            .header("x-plimsoll-deadline-ms", self.hold.as_millis().to_string())
            .body(json!({ "user": user, "group": group, "adopt": adopt }).to_string())
            .send()
            .await
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
        let failure = match self.post(&url, user, group, false).await {
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
            (4, Some((3, "http://n3".into())))
        );
    }
}
