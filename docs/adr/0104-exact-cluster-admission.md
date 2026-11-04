# ADR 0104 — Exact cluster admission through an elected leader

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0023 (admission), ADR 0101/0102/0103 (several coordinators, peers, shared parts)
- Scope: `Gate`, `Permit`, `Admission::{set_gate, acquire_here}`, `peers::{elect, LeaderGate}`, `POST /v1/peer/admit`, `AdmissionScope::Exact`

## Context

Shared parts (ADR 0103) keep admission local, but they are not exact:
- each node rounds its part up, so a scope can overshoot by up to N−1;
- a user whose queries reach one coordinator gets that node's part, not
  the cluster's limit.

Hard quotas, such as a per-user cap of 1, or a global cap that protects
the shared workers, need one place that counts every coordinator's
queries.

## Decision drivers

1. **Exact while the cluster is healthy.** One counter per scope, and
   one FIFO queue.
2. **No new infrastructure.** Coordinators already find, health-check
   and authenticate each other (ADR 0102).
3. **Losing any coordinator, the counting one included, must not stop
   admission.** Degrading to ADR 0103's parts is acceptable. Refusing
   queries is not.
4. **Slots are never leaked** by a coordinator that crashes while
   holding them.

## Decision

- **`PLIMSOLL_ADMISSION_SCOPE=exact`** (needs `PLIMSOLL_COORDINATORS`).
- **The leader is the live coordinator with the lowest node id.** Node
  ids are the random first query ids (ADR 0101), collected each 5 s peer
  round (`peers::elect`).
  - Every coordinator computes the leader from the same membership, with
    no votes and no store.
  - The leader enforces the whole limits with its own `Admission`.
- **Followers ask the leader.** A `Gate` set on the follower's
  `Admission` sends `POST /v1/peer/admit {"user", "group"}` to the
  leader, under the worker token.
  - The leader runs `acquire_here`: its own counters and queue, never
    its gate, so nothing loops if two nodes disagree about the leader.
  - It answers once a slot is granted, or 429 with the serialized
    `AdmissionError` (queue full in some scope).
  - **The slot lives as long as the response body.** The leader streams
    one byte a second while it holds the slot.
    - When the follower's query ends, the follower drops the response
      and the connection closes. The leader's next write fails, the
      stream is dropped and the slot is released. The test sees this
      within milliseconds.
    - A follower that dies closes the connection the same way.
    - The stream also ends at the query timeout, so a lost connection
      that is never noticed holds a slot for at most that long.
  - On the follower, the query counts as queued while it waits and as
    running once admitted, so gauges and a later fallback see it.
- **Fallback.** If the leader cannot be reached (connection refused,
  reset, or not an admitting node), the follower logs
  `admission_leader_unreachable` and admits by its own part
  (`ceil(limit / N)`, ADR 0103) until a round elects someone else. The
  leader itself runs with the whole limits (`share_with(1)`).

## Consequences

**Positive**
- Exact cluster-wide limits, and one FIFO queue across coordinators,
  while the leader lives.
- No database, lock service or object-store traffic on the query path.
  The cost is one LAN round trip per admission and one held connection
  per running query on a follower.

**Negative (accepted debt)**
- **A new leader starts at zero.** Queries the old one admitted keep
  running, unknown to the new one, so the cluster can briefly exceed
  its limits by those queries. This is bounded by the query timeout.
- **Membership lag.** At boot, each node leads until its first round
  sees its peers (observed by hand). During a partition, each side
  elects its own leader. Either way, every leader's limits apply.
- **A draining leader** (SIGTERM, ADR 0026) still holds its grants and
  serves queued requests until they finish or time out. Peers mark it
  unhealthy within a round and elect another, which counts from zero:
  the same bounded overshoot as above. Graceful shutdown waits for
  those streams, up to the query timeout.
- **One held connection per running follower query** on the leader:
  thousands of concurrent queries mean thousands of idle streams.
- **The leader's gauges** count every coordinator's running queries.
  Followers count their own.

## Validation

- `governance::tests::a_gate_decides_before_local_limits` checks that:
  - slots the gate grants bypass local limits but are counted, and are
    released when dropped;
  - a gate refusal is the caller's error;
  - "no leader" falls back to local limits, which see the gate's grants;
  - `AdmissionError` round-trips through JSON.
- `peers::tests::the_lowest_live_node_leads`: the lowest id leads, and a
  node listed among its own peers is counted once.
- `app::tests::queries_are_listed_and_cancelled_across_coordinators`
  (real socket):
  - a follower's slot is counted on the leader (`(1, 0)` on both);
  - dropping the follower's slot frees it on the leader.
- `config::tests::coordinators_are_given_as_workers_are`: `exact`, and
  that it needs peers.
- By hand, three coordinators with `exact` and `PLIMSOLL_MAX_RUNNING=2`,
  with one slow query sent to each:
  - the elected leader showed 2 running, its own query and one
    follower's, and 1 queued; the third query waited in the leader's
    queue;
  - after the leader was killed with SIGKILL, the waiting follower
    logged `admission_leader_unreachable` and admitted by its part;
  - the survivors elected a new leader and kept answering.
- Also observed by hand, and recorded above as debt: with SIGTERM, the
  draining leader kept the queued request until its drain ended.

## Revisit when

- Limits must hold across leader changes: the leader persists or
  rebuilds its counts (followers report their running queries to a new
  leader in their first round).
- Leader churn or follower connections become a problem: lease-based
  leadership in the object store (ADR 0101's mechanism) and batched
  slot heartbeats.
