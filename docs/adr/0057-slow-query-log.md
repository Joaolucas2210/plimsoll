# ADR 0057 — Slow-query log

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0027 (query log line), ADR 0046 (`reads`), ADR 0056 (planning events)
- Scope: `plimsoll-server::{log::QueryLog::slow_fields, app::slow_query, config::slow_query}`

## Context

Planning spans (ADR 0056) explain a slow query, but only for teams that
run a trace backend and sample the request. Many deployments have logs
only. The `query` line (ADR 0027) shows *that* a query was slow, through
its phase timings. It does not show *why*: how many files and manifests
were read, whether deletes were applied, whether the manifest cache hit.

## Decision

- **`PLIMSOLL_SLOW_QUERY_MS`** is a non-negative integer. Unset or `0`
  means off.
- A query whose total time is at least the threshold emits a second line,
  `slow_query` (warn), after its `query` line. It has the query line's
  fields:
  - identity, code, `query_id`, `trace_id`, `sql_sha256`;
  - phase timings and `reads`.

  It adds `planning_events`: every planning event (ADR 0056), with its
  duration in ms and its attributes flattened. For example,
  `iceberg.plan` with `files`, `manifests`, `delete_files`,
  `equality_deletes` and `manifest_cache_hits`, or `catalog.load_table`
  with `table`.
- It is emitted on every result path: `json`, `ndjson` and `arrow` (when
  the stream ends), and for failures too.
- The ADR 0027 rule holds: no SQL text and no parameter values. The
  error message is omitted, since the `query` line has it.

## Consequences

**Positive**
- A slow query is explainable from logs alone. For example,
  `files=48/50, delete_files=120, manifest_cache_hits=0` says "compact
  and warm the cache" without a trace.
- Off by default, so quiet deployments do not change.

**Negative (accepted)**
- **Two lines per slow query.** They join on `query_id` or `trace_id`.
- **No execution breakdown,** only planning (ADR 0056's limit).

## Validation

- `log::tests::query_lines_carry_identity_outcome_and_timings_but_no_sql`:
  `slow_fields` has the planning event with its attributes and duration
  (1.5 ms) and the user, and no SQL literal.
- `config::tests::slow_query_threshold_is_off_by_default`: unset and `0`
  mean off, `2500` means 2.5 s, and a non-number is rejected.
- Manual check on the binary with a 1 ms threshold, Spark's MOR table and
  a parameterized query:
  - the `slow_query` line had `iceberg.plan` with `files=1/3`,
    `manifests=4/4`, `delete_files=2`, `manifest_cache_hits=0` and
    6.97 ms;
  - it had `reads: [{"table": "datafusion.bronze.mor", "columns":
    ["name"]}]` and the `trace_id`;
  - the parameter's value appeared nowhere in the log.
