# ADR 0105 — Admission counts that survive a leader change

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0104 (exact cluster admission), ADR 0103 (shared parts)
- Scope: `Held`, `Admission::{held, adopt}`, `LeaderGate::follow` re-homing, `/v1/peer/admit` `adopt`

## Context

Under exact admission (ADR 0104), the leader's counters are the
cluster's. When the leader changes (it died, drained, or a partition
healed), the new leader started at zero. Every query still running
elsewhere was invisible to it, so the cluster could run its full limit
again on top of them, until they ended. For a per-user cap of 1, a
leader change meant 2.

## Decision drivers

1. **The counts follow the running queries.** Whoever leads must count
   every query running in the cluster.
2. **No persistent state.** Running queries are the truth, and their
   coordinators know them. A stored counter would only drift from them.
3. **Re-homing must not refuse running work.** A query that is already
   running is never stopped to satisfy a limit.

## Decision

- **Each coordinator tracks its own running queries** while a gate is
  set (`Held`: user, group and the current permit), whether the leader
  or the node itself admitted them. A query's entry goes away with its
  slot.
- **On a leader change** (`LeaderGate::follow` with a different
  leader), every held query is re-homed:
  - to a new remote leader: `POST /v1/peer/admit` with
    `"adopt": true`. The leader counts the query with `Admission::adopt`,
    always granted, regardless of limits, and holds it for as long as
    the response streams (ADR 0104). The new permit replaces the old
    one, whose connection closes, so the old leader, if alive, releases
    it;
  - to this node, when it becomes the leader: the permit is dropped. Its
    own counters already include its queries, because the follower
    counted them as running when they were granted (ADR 0104).
  - If adoption fails, the old permit is kept, and the old leader, if it
    lives, still counts the query. The failure is logged as
    `admission_rehome_failed`.
  - Each change logs `admission_rehomed` with the number of queries and
    how many were adopted.
- **Over-limit counts drain naturally.** Adopted queries can put the
  new leader over its limits. New queries then wait in its queue until
  the counts fall back under them, exactly as for any full limit.

## Consequences

**Positive**
- The counts survive a leader change with no shared storage. The
  running queries rebuild them within one peer round (5 s).
- The old leader stops counting re-homed queries when their
  connections move, so a leader that steps down without dying does not
  keep phantom slots.

**Negative (accepted debt)**
- **One round of blindness.** Between the change and the re-homing
  (up to a round plus the adoption requests), the new leader may admit
  queries it does not yet know are over the limit. That window is
  bounded, unlike the query-timeout window of ADR 0104.
- **A query granted by the old leader after the re-homing snapshot**
  keeps its old permit until the next change. If the old leader is
  alive, it is counted there, not at the new leader.
- **Queries that died with a dead leader** are gone, along with their
  slots, and need no re-homing. A follower whose query was admitted by
  the dead leader re-homes it.
- One adoption request per running query per change: fine at tens of
  coordinators and hundreds of queries, and it happens only when the
  leader changes.

## Validation

- `governance::tests::own_queries_are_tracked_for_a_new_leader` checks
  that:
  - a query admitted under a gate is tracked, whether or not a leader
    granted it;
  - replacing its permit releases the old one;
  - it is untracked when its slot drops;
  - `adopt` counts beyond the limits, after which new queries are
    refused.
- `app::tests::queries_are_listed_and_cancelled_across_coordinators`
  (real socket) checks that:
  - a follower's query held at leader `b` is released at `b` when the
    follower leads (`follow(None)`), yet still counted locally;
  - `b` adopts it when it leads again;
  - dropping the slot frees it at `b`.
- By hand, three coordinators, `exact`, `PLIMSOLL_MAX_RUNNING=2`, run
  twice with these steps:
  1. a follower and the leader each started a slow query;
  2. the leader was killed with SIGKILL;
  3. after the next round, two more queries were sent.
- In both runs the survivors counted 2 running and 1 queued, exactly
  the limit, against 3 running without re-homing:
  - in one run the follower holding the query became the leader and
    counted it locally (`admission_rehomed`, `"leader": "self"`);
  - in the other, the other follower became the leader, and the query
    was adopted over HTTP.

## Revisit when

- The one-round window matters: a new leader could delay new
  admissions until its peers have reported, by waiting for one round
  after it is elected.
