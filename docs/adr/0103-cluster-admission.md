# ADR 0103 — Admission limits shared by the coordinators

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0023 (resource groups), ADR 0101 (several coordinators), ADR 0102 (peers)
- Scope: `Admission::share_with`, `State::part`, `QueryRegistry::first_id`, `GET /v1/peer/node`, `Peers::nodes`, `PLIMSOLL_ADMISSION_SCOPE`

## Context

Admission limits (running and queued, global, per resource group and per
user; ADR 0023) were per coordinator. With N coordinators (ADR 0101),
`PLIMSOLL_MAX_RUNNING=8` meant 8N queries on the shared workers, a user
limited to 2 could run 2N, and both numbers changed whenever a replica
was added or lost. Operators had to divide by hand and redeploy when N
changed.

## Decision drivers

1. **No per-query coordination.** Admitting a query must not wait on
   another node or on a shared store. A coordination dependency on the
   hot path would also be a new way to fail.
2. **Survive losing coordinators.** The survivors must take over the
   lost capacity, without a restart.
3. **Never refuse everything.** No node may end up with a limit of 0
   for a scope its peers can serve.

## Decision

- **`PLIMSOLL_ADMISSION_SCOPE=cluster`** (default `node`, the previous
  behavior) makes every admission limit the cluster's. It needs
  `PLIMSOLL_COORDINATORS` (ADR 0102).
- **Each coordinator enforces its part**: `ceil(limit / N)` for every
  running and queued limit (global, group, user), where N is the number
  of live coordinators.
  - The part is rounded up, so a limit smaller than N is 1 per node, not
    0.
  - A limit of 0 stays 0: "no queue" means no queue anywhere.
- **N counts live coordinators once.** Every peer round (5 s, after the
  health checks), each healthy peer is asked for its node id
  (`GET /v1/peer/node`, worker token). The id is its first query id,
  random per node (ADR 0101). N is the number of distinct ids, this
  node's included, so a peer list that names the node itself (a `dns+`
  name) does not count it twice.
- **N changes take effect at once.** A lost coordinator raises the
  survivors' parts within one round, and queued queries that now fit
  start immediately (`share_with` promotes). A new one lowers them, and
  running queries are never preempted, so the cluster drains down to
  the new parts. Each change logs `admission_shared` with the count.

## Consequences

**Positive**
- One set of limits for the cluster, configured once and kept as
  replicas come and go.
- Admission stays local and instant, with no dependency added to the
  query path.

**Negative (accepted debt)**
- **Rounding up overshoots** by at most N−1 per scope: a per-user limit
  of 1 across 3 coordinators admits up to 3. Exact limits would need a
  shared counter, which is a coordination dependency (revisit).
- **Parts assume a balanced load balancer.** A user whose queries all
  reach one coordinator gets that node's part, not the cluster limit.
  Queries queue instead of running, so the result is under-use, never
  overload.
- **Membership lags by one round** (5 s plus timeouts). A node that just
  started enforces the full limits until its first round finishes. A
  partition makes both sides see fewer peers and raise their parts, so
  the overshoot is bounded by the full limits on each side.
- Memory limits, the result cache and resource-group memory shares
  (ADR 0036) stay per node: they protect the node's own memory.

## Validation

- `governance::tests::cluster_limits_are_shared_by_the_coordinators`:
  - with 5 running and 3 queued shared by 3, a node runs 2 and queues
    1, and the next query gets `QueueFull { limit: 1 }`;
  - sharing with 2 instead starts the queued query;
  - a limit of 1 shared by 3 is still 1.
- `config::tests::coordinators_are_given_as_workers_are`: `node`,
  `cluster`, `cluster` without peers, and invalid values.
- `app::tests::queries_are_listed_and_cancelled_across_coordinators`:
  `Peers::nodes` returns the peer's id.
- By hand, two coordinators with `PLIMSOLL_MAX_RUNNING=2`:
  - a coordinator ran one slow query and queued the second;
  - stopping the other coordinator started the queued query within
    the next round, so 2 were running;
  - `admission_shared` was logged at 2 coordinators, then at 1.

## Revisit when

- Exact cluster limits matter (billing, hard quotas): a shared counter
  (lease or token bucket in the object store or a database), off the
  hot path where possible.
- Load is unbalanced by design (sticky sessions): weight each part by
  each node's recent demand, exchanged in the same round.
