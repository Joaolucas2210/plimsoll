# ADR 0046 — Audit trail of data reads

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0027 (query log line), ADR 0043 (parameters never logged), ADR 0044 (column deny; same scan walk), ADR 0045 (row filters)
- Scope: `plimsoll_core::engine::{TableRead, QueryResult::reads, QueryStream::reads}`, `plimsoll-server::{engine::{QueryOutput, Streamed}::reads, log::QueryLog::reads}`

## Context

With column deny (ADR 0044) and row filters (ADR 0045), Plimsoll holds
regulated data. Regulated data brings audit questions:

- who read `gold.users.email` last week?
- which principals queried `orders` without a row filter?
- did anyone touch `pii.*` after the incident?

The `query` log line (ADR 0027) has the user, outcome and a fingerprint
of the SQL text. It does not say what the query read: the SQL
fingerprint cannot be searched by table, and the text itself is never
logged.

## Decision drivers

1. **What was read, not what was written.** Resolved tables (catalog,
   schema, name) and the columns actually read. `SELECT *`, aliases and
   views must not hide it.
2. **The same source of truth as enforcement.** The audit must list
   exactly what the grant checks saw, so that the two cannot disagree.
3. **No values in logs.** SQL literals and parameters stay out (ADR 0027,
   ADR 0043).
4. **One line per query** in the existing pipeline, with no new sink or
   dependency.

## Decision

- **Recording.** The scan walk that enforces column deny (ADR 0044) now
  always runs, with or without an access policy. For every `TableScan` of
  the optimized plan, subqueries included, it records a `TableRead`:

  | Field | Meaning |
  |---|---|
  | `table` | `catalog.schema.table` as resolved |
  | `columns` | Projected columns plus pushed-filter columns, sorted and unique |
  | `row_filter` | Whether ADR 0045 injected a filter on this scan |

- **Plumbing.**
  - `QueryResult.reads` and `QueryStream.reads` carry the records.
  - In the server they reach `QueryOutput.reads` (JSON) and
    `Streamed.reads` (NDJSON and Arrow, logged when the stream ends).
  - `QueryLog.reads` emits them as the field `reads`, for example
    `[{"table": "datafusion.bronze.mor", "columns": ["id", "name"],
    "row_filter": true}]`.
- **Reads only exist for queries that planned.** Denied attempts
  (`forbidden`) already log the denied table or column in `message`.
  Rejected requests (401, 429) read nothing.
- **Never logged:** SQL text, parameter values, and the row-filter
  predicates themselves. `row_filter` is a boolean.

## Consequences

**Positive**
- Access questions become log queries. For example, in Loki:
  `{app="plimsoll"} | json | reads =~ ".*users.*email.*"` shows `user`,
  `ts_ms` and `trace_id`.
- The audit and the enforcement come from one walk, so the audit cannot
  claim a column was not read when the grant check saw it.
- No cost beyond formatting a small array on each log line.

**Negative (accepted)**
- **Reads are what the plan scans, not what the client received.**
  `SELECT count(*) WHERE email LIKE …` lists `email` even though no email
  value was returned. That is the conservative and correct reading for
  audit.
- **The log line grows** with the number of tables and columns. Wide
  `SELECT *` queries produce long arrays.
- **Retention and tamper-evidence** are the log pipeline's job.

## Validation

- `engine::tests::records_what_each_scan_read`: a join of `gold.orders`
  (row-filtered) and `gold.users`, with a `LIKE` on `email` and a
  `GROUP BY`, records:
  - `orders` with `[amount, email, id, region]`, where the filter columns
    count, and `row_filter: true`;
  - `users` with `[id, name]` and `row_filter: false`.
- `log::tests::query_lines_carry_identity_outcome_and_timings_but_no_sql`:
  `reads` is serialized as specified.
- Manual check on the binary, with Spark's MOR table as `bronze.mor`, ana
  filtered to `id < 10`, and a query with a parameter:
  - the answer was `[[6, 8]]`, the ids below 10 left after Spark's MOR
    deletes;
  - the log line had `user: ana` and `reads: [{"table":
    "datafusion.bronze.mor", "columns": ["id", "name"], "row_filter":
    true}]`;
  - the parameter value appeared nowhere in the log.
