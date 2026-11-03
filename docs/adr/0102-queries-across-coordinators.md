# ADR 0102 — Queries across coordinators

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0022 (scoped system tables), ADR 0034 (cancellation), ADR 0058/0070 (worker token, discovery), ADR 0101 (several coordinators)
- Scope: `peers::{Peers, merge}`, `PLIMSOLL_COORDINATORS`, `GET /v1/peer/queries`, `DELETE /v1/peer/queries/{id}`, `system::QueriesTable` peers, `cancel_query` forwarding, `Workers::request`, `config::nodes`

## Context

With several coordinators behind a load balancer (ADR 0101), each one
knew only its own queries:

- `system.runtime.queries` showed whatever the balancer's pick happened
  to run;
- `DELETE /v1/queries/{id}` answered 404 for a query running on any
  other coordinator.

An operator looking for a runaway query, or a client cancelling its
own, had to find the right node first.

## Decision drivers

1. **Same answers from any coordinator.** Same rows and same cancel
   outcome, under the same principal scoping (ADR 0022, 0034).
2. **Losing a coordinator never fails a listing.** HA is the reason
   there are several.
3. **No new trust path.** Coordinators already share the worker token
   and talk to nodes found by seeds or DNS (ADR 0058, 0070).

## Decision

- **`PLIMSOLL_COORDINATORS`**, in the same shapes as
  `PLIMSOLL_WORKERS`: base URLs or `dns+http(s)://name:port`, discovered
  and health-checked by the same code every 5 s.
  - It needs `PLIMSOLL_WORKER_TOKEN`, as workers do.
  - The list may include the node itself, so every replica can share
    one headless-service name.
- **Node-to-node endpoints**, under the worker token. Without a token
  they do not exist (404); with a wrong one they answer 401.
  - `GET /v1/peer/queries` returns the node's own queries, unscoped, as
    JSON.
  - `DELETE /v1/peer/queries/{id}` cancels one of the node's own queries.
    With `x-plimsoll-owner`, it only cancels a query of that user.
  - They never forward, so there are no loops.
- **Listing.** `system.runtime.queries` merges its own rows with every
  healthy peer's, deduplicated by id (unique per ADR 0101, which also
  covers the node answering as its own peer). It then scopes them to
  the viewer as before. A peer that does not answer within 2 s is left
  out and logged (`peer_unreachable`). Its queries died with it, or will
  be listed once it answers again.
- **Cancel.** The visibility check runs locally first. If the query is
  not visible here, the cancel goes to every healthy peer with the
  caller's scoping as `x-plimsoll-owner` (none for operators or with
  auth disabled). The first answer other than 404 (202 or 409) is
  returned as is, and with none the answer is 404. A user still cannot
  tell another user's query from a missing one.

## Consequences

**Positive**
- Any coordinator lists and cancels any query, with the same scoping.
- There is no new secret, client, discovery mechanism or crate.

**Negative (accepted debt)**
- **Fan-out per listing and per remote cancel.** That is N requests,
  bounded by the 2 s timeout. Fine for an admin table at tens of
  coordinators, not a hot path.
- **The worker token now also lets a holder list and cancel every
  query.** It already let its holder run any fragment as any user, so
  the trust boundary does not move. A cluster that wants them apart
  needs separate tokens (revisit).
- **Rows do not say which coordinator runs the query.** The id range
  identifies it, since each node starts at its own 2^32 block.

## Validation

- `peers::tests::queries_round_trip_and_merge_by_id`: `QueryInfo`
  round-trips through JSON, and merging counts a node seen as its own
  peer once.
- `config::tests::coordinators_are_given_as_workers_are`: shapes, and
  the token required.
- `app::tests::queries_are_listed_and_cancelled_across_coordinators`
  (two routers, the peer on a real socket):
  - the peer's running query is listed with its user;
  - another user's cancel through the first coordinator is 404;
  - the owner's is 202, and the query's own request ends 409
    `query_cancelled`;
  - `/v1/peer/queries` without the token is 401.
- By hand, two coordinators each listing both (itself included):
  - `system.runtime.queries` on one returned both nodes' queries,
    each once;
  - with the second coordinator down, the listing answered in 26 ms
    with the first node's rows: the startup health check had already
    dropped the dead peer.

## Revisit when

- Clusters want coordinator and worker credentials apart: a
  `PLIMSOLL_PEER_TOKEN`.
- Listings get frequent enough that fan-out cost matters: a gossip of
  running queries instead.
