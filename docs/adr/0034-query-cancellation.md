# ADR 0034 — Query cancellation

- Status: Accepted
- Date: 2026-11-01
- Owner: JoaoLucas2210
- Related: ADR 0013 (error codes), ADR 0021/0022 (principals, operators, scoped visibility), ADR 0023 (admission slots), ADR 0026 (drain), ADR 0027 (query log)
- Scope: `plimsoll_control::governance::{QueryRegistry::cancel, CancelOutcome, GovernedError::Cancelled, Admission::execute}`, `plimsoll-server::app::cancel_query`

## Context

Two things could stop a query: its own deadline (`PLIMSOLL_QUERY_TIMEOUT_MS`)
or the client closing the connection. Neither helps an operator watching
`system.runtime.queries` with a runaway query in front of them:

- a 25-minute cross join;
- a dashboard hammering a hot table;
- a query holding most of the pool and a running slot, while others queue
  or fail with `memory_limit_exceeded`.

The client connection is often a BI server's pooled connection, so
"close the connection" is not available. The deadline is global and long
by design. Every engine in this space (Trino `DELETE /v1/query/{id}`,
Spark's job cancel, Postgres `pg_cancel_backend`) gives the owner and
the admin a kill switch.

## Decision drivers

1. Cancel must release everything, exactly like a timeout: the admission
   slot, pool reservations and spill files. Ideally the same mechanism, so
   there is no new cleanup path.
2. It must work in both states: **queued** (leave the queue, never run) and
   **running**.
3. Authorization is consistent with ADR 0022:
   - owners cancel their own queries;
   - operators cancel any query;
   - a query id that belongs to someone else reveals nothing.
4. The request whose query was cancelled gets a clear, non-retryable
   answer, and the cancel is visible in the logs and metrics.

## Considered options

**A. A per-query `tokio::sync::Notify` in the registry, raced against the
governed future (chosen).**
- `Admission::execute` `select!`s the deadline-wrapped run against the
  query's cancel signal.
- On cancel, it drops the governed future. That is the same drop the
  timeout performs, so DataFusion streams stop, and the slot and
  reservations are released by their `Drop` impls (driver 1).
- `notify_one` stores a permit, so a cancel that arrives before the future
  first awaits the signal is not lost.

**B. A `CancellationToken` passed into the engine.** It is more granular
(the engine could check it between batches), but it is a new cleanup path
next to the drop path. It would also bring `tokio-util` for one type. The
drop path already stops DataFusion at its next await point.

**C. Abort the request's task.** Axum handlers are not addressable by id,
and aborting would skip the registry's terminal transition and the query
log line. Rejected.

## Decision

- **Registry.**
  - `QueryRegistry::cancel(id)` returns `Cancelling`,
    `NotRunning(state)` or `NotFound`.
  - Signals live in a map from `register` until `finish`, so the map only
    holds cancellable queries.
- **Admission.**
  - A cancel ends the query with `GovernedError::Cancelled` and records
    `Cancelled` with the error `cancelled by request`.
  - It counts in `plimsoll_queries_total{state="cancelled"}`.
- **HTTP: `DELETE /v1/queries/{id}`.**
  - Authenticated like `/v1/sql`; with auth disabled, anyone may cancel
    (development).

  | Case | Response |
  |---|---|
  | Owner or operator | `202 {"query_id", "state": "cancelling"}` |
  | Someone else's query, unknown id, non-numeric id | `404 query_not_found`; existence is not revealed |
  | Already terminal | `409 query_not_running`, naming the state |

- **The cancelled request** answers `409 query_cancelled`
  (`retryable=false`). The response is a conflict with another request, not
  a server fault. It produces the usual `query` log line with that code.

## Consequences

**Positive**
- A runaway query is one HTTP call away from gone. Its memory and slot
  return immediately, which unblocks queued queries.
- Cancelling a queued query removes it before it ever runs, with no
  `started_at`.
- No new cleanup code: cancel and timeout share the drop path that the
  existing tests already prove releases everything.

**Negative (accepted)**
- **Cancellation is cooperative.** It takes effect at the query's next
  await point. DataFusion operators yield regularly (tested below with a
  CPU-bound `generate_series` count), but a single long synchronous
  kernel call finishes before the drop.
- **No batch cancel.** There is no "cancel all of user X"; scripts can
  loop over `system.runtime.queries`.
- **Node-local ids.** With a future coordinator/worker split, cancel must
  be routed to the node that owns the query.
- **409 is a judgment call.** It was chosen over the non-standard 499 that
  proxies use for client disconnects.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Users cancel others' queries | — | High | Only owner or operator; others get 404 (tested). |
| Probing query ids for activity | Low | Low | 404 for both missing and foreign ids. |
| Cancel races with completion | Expected | None | `finish` is idempotent; the first terminal state wins; a late cancel gets 409 `query_not_running`. |

## Validation

- `governance::tests::cancel_stops_running_and_queued_queries_and_frees_the_slot`:
  - one query running and one queued;
  - cancelling the queued one leaves the queue without ever starting;
  - cancelling the running one frees the slot (`load() == (0, 0)`);
  - `cancelled` total is 2;
  - a repeat cancel gets `NotRunning`, and an unknown id gets `NotFound`;
  - no signals are left behind.
- `app::tests::owners_and_operators_cancel_queries_others_see_nothing`:
  - for another user, an unknown id and a non-numeric id, the answer is
    404 `query_not_found`;
  - with no token, the answer is 401, and the query is still running after
    all of these;
  - the owner gets 202, the query's own request answers
    409 `query_cancelled`, and the slot is released;
  - a repeat cancel gets 409 `query_not_running`;
  - an operator cancels another user's query.
- Manual smoke on the binary:
  - `SELECT count(*) FROM generate_series(1, 20000000000)` was running;
  - `DELETE /v1/queries/1` returned 202;
  - the query answered `query_cancelled` 1.5 s after it started;
  - `/healthz` then showed running 0 and pool used 0, and the process sat
    idle;
  - `plimsoll_queries_total{state="cancelled"} 1` and
    `plimsoll_query_errors_total{code="query_cancelled"} 1`.

## Revisit when

- A coordinator/worker split needs cancel routing.
- Operators need bulk cancel, or "cancel on drain" for long queries
  (ADR 0026's revisit item).
- A tight synchronous kernel is found to ignore cancellation for long.
