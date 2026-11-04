# ADR 0110 — Backing off the fast peer rounds

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0109 (faster rounds while a peer is down), ADR 0102 (peers)
- Scope: `peers::Pace`, the peer loop in `main` (replaces `Peers::next_round`)

## Context

Rounds run every second while any coordinator is down (ADR 0109), so a
returning one is counted within a second. A coordinator that stays down
(decommissioned but still listed, a long outage) then costs every peer a
health check per second indefinitely, or a 2 s timeout per round if its
host drops packets.

## Decision drivers

1. **Short outages stay fast.** Restarts and brief partitions, the
   common case, are seen within a second.
2. **Long outages cost what a healthy cluster costs**: one round every
   5 s.
3. **A new failure is watched closely again**, even during a long one.

## Decision

- **`peers::Pace`**, kept by the peer loop, sets each pause from the
  members that failed their last health check:
  - none down: `PEER_ROUND` (5 s), and the pace resets;
  - the same members down as at the last round: double the last pause,
    starting at `FAST_ROUND` (1 s) and capped at `PEER_ROUND`. That
    gives 1, 2, 4, 5, 5… s;
  - a different set down (another member failed, or some came back):
    `FAST_ROUND` again.
- The set is compared as sorted URLs: static seeds and the addresses
  `dns+` seeds resolve to (ADR 0070).

## Consequences

**Positive**
- A peer that comes back within seconds is seen within a second, as in
  ADR 0109.
- One that stays down settles at the healthy pace after about 7 s
  (1 + 2 + 4).

**Negative (accepted debt)**
- **A peer down for longer is seen up to 5 s after it returns**, as
  before ADR 0109. The bound is the takeover hold's assumption, so exact
  admission's guarantees are unchanged.
- **Any change to the set resets the pace**, including a peer that
  flaps. A flapping peer keeps the rounds fast, which is also when its
  re-homing matters most.

## Validation

- `peers::tests::fast_rounds_back_off_while_the_same_peers_stay_down`
  checks that:
  - one peer down gives pauses of 1, 2, 4, 5, 5, 5 s;
  - a second peer going down restarts at 1 s and backs off again;
  - one coming back while the other stays down restarts at 1 s;
  - with all back, the pause is 5 s, and the next outage starts at 1 s.
- `peers::tests::rounds_are_faster_while_a_peer_is_down`: with all
  members healthy the pause is 5 s, and with one refusing connections it
  is 1 s.
- By hand: a coordinator whose second peer answered `/readyz` with 503.
  That peer got 7 checks in 24 s, spaced 1.0, 2.0, 4.0, 5.8, 5.0 and
  5.0 s. ADR 0109 alone would have sent about 24.

## Revisit when

- Peers come and go often enough that the reset on every change keeps
  rounds fast most of the time: reset only for newly failed members.
