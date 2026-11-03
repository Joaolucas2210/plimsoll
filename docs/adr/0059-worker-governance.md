# ADR 0059 — Coordinator/worker step 2: governance on workers

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0058 (scan fragments; resolves its first three debts), ADR 0023/0036 (resource groups and memory shares), ADR 0031 (traces), ADR 0026 (readiness)
- Scope: `plimsoll_core::engine::{QueryTag, TableAccess::tag}`, `plimsoll_connectors::remote::{Workers::{healthy_urls, check_health}, open}`, `IcebergTable::scan`, `plimsoll-server::{app::{fragment, FragmentReport}, auth::Viewer::tag, trace::server_span, main}`

## Context

Step 1 (ADR 0058) moved scans to workers, but left workers ungoverned:

- fragments bypassed the worker's admission, so any number could run at
  once;
- a worker could not tell whose query a fragment belonged to;
- the trace stopped at the coordinator;
- the coordinator's deadline did not reach the worker;
- the coordinator kept sending work to a worker that was draining or
  dead.

## Decision drivers

1. **A worker protects itself** with the same tools as any node:
   - admission with limits and memory shares;
   - deadlines;
   - fail-fast rejections that are retryable.
2. **One query, one identity, one trace** across nodes.
3. **Losing workers degrades to single-node,** never to errors when the
   coordinator could still answer.

## Decision

- **Identity.** `QueryTag { query_id, user, traceparent, deadline }`
  lives in core.
  - The server's `Viewer` carries it via `TableAccess::tag`. The query id
    is set when admission assigns it, the deadline is arrival plus the
    query timeout, and the `traceparent` is the request span's.
  - The engine puts it in the query's session config.
  - `RemoteScanExec` reads it from the `TaskContext` and sends
    `x-plimsoll-query-id`, `x-plimsoll-user`, `traceparent` and
    `x-plimsoll-deadline-ms` (the remaining budget).
  - A budget already spent fails the fragment before any request.
- **Worker admission.** A fragment acquires an admission slot as the
  coordinator's user, in group `fragments` if configured, otherwise
  `default`.
  - That group's limits and memory share (ADR 0023, 0036) bound fragment
    work on the worker.
  - A full queue gives `429 queue_full`, and the coordinator's query
    fails with that, which is retryable.
  - The slot is held by the response stream's state, so it lives exactly
    as long as the fragment streams, and is released on completion,
    error or disconnect.
- **Deadline.** The worker enforces `min(remaining budget, own query
  timeout)`, both on the admission wait and on every batch. Past it, the
  body aborts and the coordinator sees a truncated stream.
- **Observability.**
  - Every fragment logs one `fragment` line when its stream ends or is
    dropped. It carries the coordinator's `query_id`, `user`,
    `trace_id`, `files`, `rows`, `bytes`, `error` and `total_ms`.
  - With OTLP on, the worker exports a `fragment` SERVER span that
    continues the coordinator's trace (`server_span`).
- **Health.** The coordinator checks every worker's `/readyz` every 5 s,
  with a 2 s timeout, and logs `workers_health` on changes.
  - Scans fan out only to healthy workers.
  - With none healthy, the scan runs locally.
  - A draining worker, whose readiness is off during drain (ADR 0026),
    stops receiving fragments within one interval while still finishing
    the ones it has.

## Consequences

**Positive**
- Workers are governed like any node. Many coordinators cannot overrun a
  worker; they get 429s.
- One trace shows the coordinator's query and each worker's fragment
  together. Worker logs join coordinator logs on `query_id` and
  `trace_id`.
- Rolling restarts of workers degrade to local scans instead of failing
  queries.

**Negative (accepted)**
- **No retry.** A fragment rejected with 429, or failing on a worker,
  fails the query. Step 3 retries before the first batch.
- **Health is polled, not pushed.** A worker can die between checks; its
  fragments then fail as in step 1.
- **The user is the coordinator's assertion,** trusted because of the
  worker token. Workers do not re-check grants. The coordinator already
  did (ADR 0025, 0044, 0045), and fragments are file lists of tables it
  may read.

## Validation

- `tests/distributed.rs::fragments_carry_the_query_identity`, with a fake
  worker that captures headers:
  - it receives query id 7, user `ana`, the `traceparent`, a remaining
    deadline between 25 and 30 s, and the bearer token;
  - with a spent deadline, no request is sent and the error says so.
- `tests/distributed.rs::workers_govern_fragments_and_report_health`
  (interop), with a real worker whose `fragments` group is limited to
  `1/0`:
  - with the slot held, the coordinator's query fails with 429;
  - released, it answers 32 rows summing to 768 again;
  - with the worker not ready, the health check reports 0 and the same
    query runs locally (no `RemoteScanExec`) with the same answer;
  - ready again, it fans out again.
- `tests/distributed.rs::distributed_scans_match_local_ones` still
  matches local answers on the MOR, EQ and DV tables.
- Manual check with a worker and a coordinator, both exporting to one
  OTLP collector:
  - the worker's `fragment` span had the coordinator's `query` span as
    parent, in the same trace;
  - the worker's `fragment` log line had `query_id: 1`, `user: ana`, the
    trace id, `files: 3` and `rows: 32`.
