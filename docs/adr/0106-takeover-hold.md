# ADR 0106 — A new leader waits before admitting

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0104 (exact cluster admission), ADR 0105 (re-homing on leader changes)
- Scope: `Admission::hold`, `State::hold_until`, `TAKEOVER_HOLD`, `PEER_ROUND`

## Context

After a leader change, followers re-home their running queries to the
new leader within their next peer round (ADR 0105). Until those
adoptions arrive, the new leader does not know about the queries, and
may admit new ones over the limits. The same applies at boot: every
coordinator leads until its first round has seen its peers (ADR 0104).

## Decision drivers

1. **Close the window instead of shortening it.** Exact limits should
   hold across a leader change, not just mostly.
2. **Running work is never affected.** Only new admissions may wait.
3. **No new protocol.** Followers do not acknowledge anything. The
   leader waits as long as re-homing can take.

## Decision

- **`Admission::hold(d)`**: for `d`, no new query starts on this node.
  - That covers its own queries and admissions for followers
    (`/v1/peer/admit`).
  - Held queries queue, within the queue limits, and start when the
    hold ends.
  - Adoptions (`adopt`, ADR 0105) are counted during the hold. That is
    what it waits for.
- **When a node holds**, with `exact` admission:
  - at boot, as soon as the gate is set, since a node leads until its
    first round says otherwise;
  - when it starts leading after following another (`leader` was
    `Some`, now `None`).
- **For how long**: `TAKEOVER_HOLD` = 10 s, two peer rounds
  (`PEER_ROUND` = 5 s). A follower notices the change in its next round
  (up to 5 s away) after its health checks and membership requests
  (2 s timeouts each), then re-homes. That fits within two rounds.

## Consequences

**Positive**
- A new leader admits nothing until the running queries have been
  reported, so exact limits hold across leader changes and at boot.
- Followers are not changed, and no protocol message is added.

**Negative (accepted debt)**
- **Queries wait up to 10 s after every leader change and at boot.** A
  deployment with `PLIMSOLL_MAX_QUEUED=0` refuses queries during the
  hold (queue full, retryable), as it would for any full limit.
- **The duration is a bound, not a confirmation.** A follower slower
  than two rounds (overloaded, or with very slow DNS) re-homes late,
  and its queries are not counted for that long. Waiting for every
  follower's report would need to know who is still alive, which is
  what the rounds estimate.
- Followers falling back to their parts (ADR 0103) while the leader is
  unreachable are not held. They are not leaders.

## Validation

- `governance::tests::a_hold_queues_new_queries_but_not_adoptions`
  checks that:
  - a query sent during a 200 ms hold queues and starts only after it;
  - an adoption is counted during the hold;
  - with no queue, a held query is refused.
- By hand, three coordinators, `exact`, `PLIMSOLL_MAX_RUNNING=2`:
  1. a follower and the leader each ran a slow query;
  2. the leader was killed with SIGKILL;
  3. the other follower took over, and two queries were sent to it as
     soon as it logged the takeover.
- One second later, the new leader had 1 running (the adopted query)
  and the 2 new ones queued.
- After the hold, it had 2 running and 1 queued: exactly the limit.

## Revisit when

- Leader changes are frequent enough that 10 s pauses hurt: followers
  could report on demand (the new leader asks each peer for its held
  queries), and the hold would end when all have answered. Resolved by
  ADR 0107 (`/v1/peer/round`).
