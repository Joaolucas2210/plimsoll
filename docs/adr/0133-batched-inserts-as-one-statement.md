# ADR 0133 — Batched inserts as one statement

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0043 (bound parameters), ADR 0076 (appends), ADR 0127 (Flight SQL parameters), ADR 0129 (batched updates)
- Scope: `flight.rs` (`batched_insert`, `do_put_prepared_statement_update`)

## Context

A Flight SQL batch of parameter rows ran as one statement per row
(ADR 0129). Each was its own query and, on an Iceberg table, its own
commit, since there are no transactions. That had three costs:
- **Failure.** A failure mid-batch left the earlier rows committed.
- **Small files.** Every row wrote its own data file and snapshot.
  A thousand-row JDBC `executeBatch` made a thousand of each, for
  compaction to clean up.
- **Overhead.** Every row went through admission, planning and the
  catalog commit on its own.

The batch that matters is JDBC's `INSERT INTO t VALUES (?, ?)` with
`addBatch()`/`executeBatch()`: the way applications load rows.

## Decision

- **A batched `INSERT … VALUES` of one row becomes one statement.**
  The bound statement is parsed (DataFusion's SQL parser).
  - When it is a single `INSERT` whose source is a `VALUES` of exactly
    one row, that row is repeated once per parameter row.
  - Placeholders `$1..$k` of copy *i* become `$(i·k+1)..$(i·k+k)`, and
    the parameters are flattened in the same order.
  - Literals and expressions in the row (`'k'`, `$2 || '!'`) are kept
    in every copy.
  - The result is one query: one admission slot, one plan, one write,
    one commit. It returns the total count.
- **Anything else runs row by row, as before** (ADR 0129):
  - `UPDATE`, `DELETE` and `MERGE` with parameters;
  - `INSERT … SELECT`;
  - a `VALUES` that already has several rows;
  - placeholders that are not `$1..$k` for a row of `k` values.
  Each row is still its own statement there: no general rewrite makes
  them one without changing what they mean.

## Consequences

**Positive**
- `executeBatch` inserts are atomic: all rows commit or none do. They
  write files sized by the batch, not by the row.
- One query per batch in the log and in `system.runtime.queries`.

**Negative (accepted debt)**
- **Very large batches make a very large statement.** Its text grows
  with the rows (the values stay parameters, never spliced). JDBC
  drivers batch in hundreds or thousands. An Arrow `DoPut` ingestion
  (`CommandStatementIngest`) is the path for bulk loads.
- **Other batched statements remain row by row**, each its own commit.

## Validation

- `flight::tests::a_batched_insert_is_one_statement`:
  - the rewrite of `INSERT INTO t (a, b, c) VALUES ($1, 'k', $2 ||
    '!')` for two rows, with its flattened parameters;
  - `UPDATE`, multi-row `VALUES`, `INSERT … SELECT`, out-of-range
    placeholders, unparsable SQL and ragged rows are left alone.
- `flight::tests::prepared_statements_bind_parameters`:
  - a three-row JDBC-style batch on `INSERT INTO trips VALUES (?, ?)`
    returns 3 and adds 3 rows;
  - it adds exactly one `INSERT` entry to `system.runtime.queries`.
    With the rewrite disabled, the test fails with 3 entries instead
    of 1.
