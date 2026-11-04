# ADR 0107 — The takeover hold ends when every peer follows

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0104 (exact admission), ADR 0105 (re-homing), ADR 0106 (takeover hold)
- Scope: `peers::Coordination::{round, announce}`, `POST /v1/peer/round`, `Admission::end_hold`, `PEER_ROUND`, `TAKEOVER_HOLD`

## Context

A new leader holds new admissions for 10 s (ADR 0106), the longest
re-homing can take with rounds 5 s apart. Usually the followers are done
much sooner, or have nothing to re-home. Every leader change, and every
boot, still cost 10 s of queued queries.

## Decision drivers

1. **Wait for the reports, not for the clock.** The hold exists only
   until the followers have re-homed their queries.
2. **Keep the bound.** A follower that does not answer must not hold
   admissions longer than ADR 0106 did.
3. **No new state machine.** The follower's response is the same round
   it runs every 5 s, run on demand.

## Decision

- **The round moves into `peers::Coordination`**: membership, election,
  parts, following the leader and re-homing. The background loop calls
  it every `PEER_ROUND`. `LeaderGate::follow` now finishes re-homing,
  with adoptions in parallel, before the round returns.
- **`POST /v1/peer/round`** (worker token): the receiving node runs its
  round now, so its running queries are re-homed before it answers, and
  it answers with the node id of the leader it follows. A round run this
  way never announces in turn, so there are no cascades.
- **Announcing.**
  - When a node takes over (leads after following, or after its first
    round at boot), it holds as in ADR 0106 and calls
    `/v1/peer/round` on every peer that answered the round that elected
    it, including itself when it is among its peers.
  - If every one of them names it, `Admission::end_hold` ends the hold
    and queued queries start.
  - A peer that does not answer, or follows someone else, leaves the
    hold to its 10 s bound.
  - Each takeover logs `admission_takeover` with the number of peers,
    how many follow it, and whether the hold ended.
- **Followers end their own hold** in the first round that elects
  someone else. A node that boots into a running cluster stops waiting
  as soon as it knows it follows.

## Consequences

**Positive**
- Leader changes and boots cost one round trip to each peer, plus its
  re-homing, instead of a fixed 10 s. That is milliseconds on a LAN
  (0.4 s for the whole test).
- The bound is unchanged: a missing peer costs exactly what it cost
  before.

**Negative (accepted debt)**
- **"Every peer that answered the electing round"** misses a peer that
  was briefly unreachable during that round but holds running queries.
  It re-homes in its next round, after the hold may already have ended,
  so a query can be admitted over the limit in between. The window is
  now confined to peers the new leader could not see.
- **A follower's round runs on demand** and may overlap its background
  round. Rounds share a lock, so the second one sees the first's outcome
  and does nothing.

## Validation

- `governance::tests::a_hold_queues_new_queries_but_not_adoptions`:
  `end_hold` starts a queued query at once.
- `peers::tests::the_lowest_live_node_leads`: `elect` returns the
  leader's id and URL.
- `app::tests::a_new_leader_admits_once_every_peer_follows_it`, two
  coordinators on real sockets, both holding at boot:
  - nothing is admitted while they hold;
  - the lower id runs its round, takes over and announces;
  - it admits well within the 10 s hold;
  - the follower admits through it, and the leader counts both.
- By hand, three coordinators, `exact`, `PLIMSOLL_MAX_RUNNING=2`, in the
  same scenario as ADR 0106 (a follower's query running, the leader
  killed, two queries sent to the new leader as soon as it took over):
  - one second later, it had 2 running (the adopted query and one new)
    and 1 queued, where ADR 0106 alone had 1 running and 2 queued;
  - `admission_takeover` logged `peers: 2, following: 2,
    hold_ended: true`.

## Revisit when

- Peers that miss the electing round matter: the leader could keep
  announcing to newly seen peers, and end the hold only after the next
  full round with no unknown peers. Resolved by ADR 0108.
