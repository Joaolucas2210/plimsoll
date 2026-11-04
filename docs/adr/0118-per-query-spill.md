# ADR 0118 — Per-query spill attribution

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0003/0012 (memory ceiling and spill), ADR 0069 (shuffle spill visibility), ADR 0071 (operator spans), ADR 0022 (system.runtime.queries), ADR 0102 (peer listings)
- Scope: `SpillSink`, `TableAccess::spill_sink`, `QueryStream::spilled_bytes`, `QueryResult`/`QueryOutput`/`Streamed::spilled_bytes`, `QueryRegistry::record_spill`, `QueryInfo::spilled_bytes`, the `query` and `fragment` log lines, `system.runtime.queries.spilled_bytes`

## Context

Spill was visible only as a node-wide counter
(`plimsoll_spill_bytes_total`) and, for sampled queries, on operator
spans in traces (ADR 0071). Nothing said which query wrote the disk, so
an operator could not answer the questions a full disk or a slow node
raises: who spilled, how much, and was it the query that then hit the
memory limit.

## Decision drivers

1. **Exact, from the plan.** DataFusion's operators already count their
   spilled bytes. The node-wide counter is their sum, gathered when a
   query's stream is dropped.
2. **Failed queries included.** A query that spills and then fails
   (`memory_limit_exceeded`) is the one most worth explaining.
3. **Where operators already look**: the query log line, and
   `system.runtime.queries`.

## Decision

- **Per query on the coordinator.**
  - `QueryStream::spilled_bytes` sums every node of the executed plan,
    as the engine's total does.
  - Successful queries carry it out (`QueryResult`, `QueryOutput`,
    `Streamed`).
  - Failed ones are covered by a **`SpillSink`**: a counter the caller
    hands the engine through `TableAccess::spill_sink`, which the
    query's stream adds to when it is dropped, whether it finished or
    failed. The server's `Viewer` carries one per request.
- **Reported** as `spilled_bytes`:
  - in the `query` log line, for every query that ran (`null` for those
    rejected or denied before running);
  - in the query's registry entry, so `system.runtime.queries` has a
    `spilled_bytes` column, also listed by peers (ADR 0102; older peers
    omit it).
  - When a query reported nothing (it failed), the sink's count is used
    (`settle_spill`), on the plain path and on the streamed path alike.
- **Per fragment on workers.** Each `fragment` log line (scans, partial
  aggregates, maps, reduces) adds what its own plan spilled. These
  lines already carry the query's id and trace id, so a query's spill
  on its workers is their sum in any log tool.

## Consequences

**Positive**
- A full disk or a memory failure can be traced to the queries that
  spilled, from the log or from SQL, failed queries included.
- The per-query counts add up to the node's total. Checked by hand:
  three failing queries reported 4,458,168, 5,572,496 and 11,144,984
  bytes, and `plimsoll_spill_bytes_total` read 21,175,648.

**Negative (accepted debt)**
- **Workers' spill does not travel back to the coordinator.** The
  fragment stream has no trailer, so the coordinator's line counts only
  its own spill. Workers' lines must be summed by query id.
- **Shuffle bucket spill (ADR 0069)** is logged per bucket by shuffle
  id (`shuffle_spill`), not per query. A map serves many reducers and
  its spill belongs to no single fragment request.
- Maintenance and catalog work do not run through the query path and
  are not attributed.

## Validation

- `engine::tests::a_query_reports_what_it_spilled` (core): a sort over
  more than an 8 MiB ceiling spills. The query's count is above zero
  and equals the engine's total, and a query that spills nothing
  reports 0.
- `system::tests::snapshot_reflects_registry_states` (control): the new
  column, and `record_spill` reaching the snapshot.
- By hand, a server at 256 MiB with a spill directory, running large
  window sorts:
  - two failing plain queries and one failing NDJSON stream (failed
    after its header) logged and listed their spill;
  - the three counts added up to the node's total;
  - a query that spilled nothing logged 0.
- `tests/distributed.rs`: every `fragment` line carries
  `spilled_bytes`.

## Revisit when

- The coordinator should show a query's whole spill: workers send their
  fragment's count in a trailer or a final control message, and the
  coordinator adds them up.
- Shuffle spill should be attributed: pass the query id with map
  requests and log it on `shuffle_spill`.
