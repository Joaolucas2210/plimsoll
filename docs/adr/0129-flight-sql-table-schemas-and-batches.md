# ADR 0129 — Flight SQL table schemas and batched updates

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0124 (Flight SQL), ADR 0127 (bound parameters)
- Scope: `flight.rs` (`do_get_tables`, `columns`, `fields`, `bound`, `Prepared::rows`)

## Context

Two gaps were left in the Flight SQL front door:
- **`GetTables` with `include_schema`** returned empty schemas (ADR
  0124). It is how JDBC's `DatabaseMetaData.getColumns`, DBeaver's
  navigator and BI tools' field lists learn a table's columns.
- **Several parameter rows** were refused (ADR 0127). JDBC's
  `addBatch`/`executeBatch` sends them as one batch: it is the usual
  way to insert many rows from an application.

## Decision

- **Table schemas come from `information_schema.columns`.**
  - With `include_schema`, `GetTables` reads it once, through
    `/v1/sql` as the caller, so the same grants apply. Columns are
    grouped per table in `ordinal_position` order.
  - Each column's `data_type`, DataFusion's display of its Arrow type,
    is read back as an Arrow `DataType`. `is_nullable` sets
    nullability.
  - A table with a type that does not read back is planned (`SELECT *
    … LIMIT 0`) for its schema instead.
  - Without `include_schema`, nothing changes.
- **A parameter batch of several rows is kept whole in the handle**
  (`rows`, next to the one-row `params`).
  - **An update runs once per row**, in order. The counts are summed,
    which is JDBC's `executeBatch` total.
  - **A query with several rows is `INVALID_ARGUMENT`**: a result set
    has one parameter row.
  - A one-row batch binds the statement as before.

## Consequences

**Positive**
- Column listings work in JDBC/DBeaver/BI tools, in one catalog read
  instead of one plan per table.
- Batched inserts and updates from applications work.

**Negative (accepted debt)**
- **A batch is one statement per row**, each its own commit (there are
  no transactions). A failure mid-batch leaves the earlier rows
  committed, and its error names the failing row's statement. Turning a
  batch into one `INSERT … VALUES` would make it a single commit.
- **Handles carry every row**, so a large batch makes a large handle.
  JDBC sends batches of hundreds, not millions.

## Validation

`flight::tests` (tonic client, real engine):
- `GetTables` with `include_schema` on `trips` returns `column1:Int64,
  column2:Utf8`, decoded from the IPC schema in `table_schema`.
- A three-row batch on `SELECT ? + 1` as an update returns `2 + 3 + 4`.
  The same batch as a query is `INVALID_ARGUMENT`.
- The single-row binding tests of ADR 0127 still pass.
