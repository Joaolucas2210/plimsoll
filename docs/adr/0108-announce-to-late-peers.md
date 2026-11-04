# ADR 0108 — Announcing to peers that missed the election

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0104–0107 (exact admission, re-homing, takeover hold and announcement)
- Scope: `Coordination::{announce_to_new, seen, announcements}`

## Context

A new leader announces itself only to the peers that answered the round
that elected it (ADR 0107). A peer missing from that round could still
be running queries the leader does not count:
- a peer that was briefly unreachable;
- one on the other side of a partition that has just healed, which led
  its own side and admitted queries there;
- one that booted while the leader was already leading.

Such a peer re-homes them only in its own next round, up to 5 s later.
Meanwhile the leader, whose hold had already ended, could admit over
the limit.

## Decision drivers

1. **Treat a late peer like a takeover.** Re-homing and the hold already
   handle this.
2. **Disagreement must not stall admissions.** A peer that keeps
   following someone else must not hold the leader at every round.
3. **No new endpoints.** `/v1/peer/round` already re-homes and answers.

## Decision

- **While a node leads, every round compares the live peers with the
  ones it has announced itself to** (`seen`).
  - A takeover clears `seen`, so every peer is new to a new leader.
  - Peers that leave are forgotten, so they are announced to again when
    they return.
  - A follower keeps no `seen`.
- **Each new peer is announced to as at a takeover.** The leader holds
  new admissions (`TAKEOVER_HOLD`, 10 s at most) and calls
  `/v1/peer/round` on the new peers. The peer re-homes its running
  queries here before answering, and the hold ends when every new peer
  names this node.
- **Once per appearance.** A peer that does not answer, or answers with
  another leader, is in `seen` after that round. It holds the leader
  once, up to the 10 s bound, and not again until it leaves and comes
  back. Its queries come in when its own round follows this leader.
- **Overlapping announcements**, for example a takeover and then a late
  peer, are numbered. Only the latest one may end the hold, since an
  earlier batch's answers do not cover peers it never asked.
- A leader alone, at boot or after everyone else left, ends its hold at
  once: there is nobody to wait for.
- Rounds run on demand (`/v1/peer/round`) do not announce, as before.
- The leader no longer announces to itself when it is among its peers
  (ADR 0107 did): it always follows itself.

## Consequences

**Positive**
- A peer the leader could not see during the election is counted as
  soon as the leader sees it, plus one round trip, instead of after the
  peer's own next round.
- Booting a coordinator into a running cluster costs the leader a hold
  of one round trip, and the new node has nothing to re-home.

**Negative (accepted debt)**
- **Between the peer becoming reachable and the first round on either
  side** (up to 5 s), the leader still does not know about the peer's
  queries. Whichever round runs first closes it: the leader's announces
  (this ADR), and the peer's own round follows the leader and re-homes
  (ADR 0105), which already is the joining peer's push. The window is
  detection latency, now confined to that.
- **A newer announcement ends the hold** even if an older one's peers
  never answered. Those peers re-home in their own rounds.
- Every new peer causes a hold, usually of milliseconds. A flapping peer
  (alive, gone, alive) causes one per return.

## Validation

- `app::tests::a_peer_seen_late_is_announced_to_and_counted`, two
  coordinators on real sockets:
  1. `x` leads alone (`y` not served yet);
  2. `y`, unable to see `x`, leads alone and admits a query;
  3. `x` comes up and runs its round with the same leader, so it is not
     a takeover. It sees `y` as new, holds and announces;
  4. `y` follows and re-homes, and `x` counts `y`'s query;
  5. `x` admits its own within the 10 s bound, with 2 counted.

  Without this ADR, `x` never counts `y`'s query in the test, because
  `y`'s background round never runs.
- `app::tests::a_new_leader_admits_once_every_peer_follows_it`: the
  takeover path through the same code.

## Revisit when

- The detection window matters: rounds could run more often while a
  configured peer is unreachable, since that is when a partition can
  heal. A push from the joining node would not help, because it only
  learns it can reach the leader in a round too.
