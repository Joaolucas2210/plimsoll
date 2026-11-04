# ADR 0109 — Faster peer rounds while a peer is down

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0102 (peers), ADR 0105 (re-homing), ADR 0108 (announcing to late peers)
- Scope: `peers::{FAST_ROUND, Peers::next_round}`, the peer loop in `main`

## Context

When a coordinator comes back (a partition heals, a node restarts or
boots), its running queries reach the leader's counts in the first peer
round on either side (ADR 0105, 0108). Rounds run every 5 s, so for up
to 5 s the leader can admit over the exact limits. A push from the
returning node would not help: it also learns that it can reach the
leader only in a round.

## Decision drivers

1. **Spend the checks when they buy something.** A peer can only come
   back while one is missing.
2. **A healthy cluster pays nothing more.**
3. **Bounded cost while a peer is really gone.**

## Decision

- **`Peers::next_round`** sets the pause before the next round:
  - `FAST_ROUND` (1 s) while any member failed its last health check;
  - `PEER_ROUND` (5 s) otherwise.
- **Members** are the configured seeds and the addresses `dns+` seeds
  resolve to (ADR 0070). A static seed that is down keeps the rounds
  fast until it answers again or is removed from the configuration.
  This node, when listed, is always healthy.
- **Nothing else changes.**
  - The takeover hold's bound (`TAKEOVER_HOLD`, 10 s) still assumes
    5 s rounds, the slow case, so it remains an upper bound.
  - Rounds still never overlap: the loop pauses after each one.

## Consequences

**Positive**
- A returning coordinator is seen, and its queries re-homed, within
  about a second instead of up to five.
- A healthy cluster behaves exactly as before.

**Negative (accepted debt)**
- **While a peer is down, every coordinator checks it each second.** A
  host that drops packets costs a 2 s health-check timeout per round,
  so a round lasts about 3 s. A peer that refuses connections costs
  nothing noticeable. Either way, the cost is one request per
  coordinator per round.
- **A peer that is gone for good**, listed statically and never
  removed, keeps the rounds fast forever. `dns+` seeds drop it once DNS
  does.
- The pace is not configurable. A knob can come when a deployment needs
  one.

## Validation

- `peers::tests::rounds_are_faster_while_a_peer_is_down`: with every
  member healthy the pause is 5 s, and with one refusing connections it
  is 1 s.
- By hand, two `exact` coordinators. A started alone and B followed
  3.0–4.6 s later, in 0.4 s steps so B's start fell at different points
  of A's rounds. A logged two coordinators 0.21, 0.43, 0.61, 0.83 and
  1.01 s after B started listening. With 5 s rounds this could take up
  to 5 s.

## Revisit when

- Large clusters, or peers behind slow links, make per-second checks of
  a missing peer costly: back off (1, 2, 4 s, capped at 5 s) while the
  same peer stays down.
