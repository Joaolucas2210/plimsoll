# ADR 0127 — Bound parameters in Flight SQL prepared statements

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0043 (bound parameters), ADR 0124 (Flight SQL)
- Scope: `flight.rs` (`Prepared`, `numbered`, `bound`, `do_put_prepared_statement_query`)

## Context

Flight SQL prepared statements ran only without parameters (ADR 0124).
JDBC's `PreparedStatement` with `?`, ADBC's `cursor.execute(sql,
params)` and every BI filter built on them bind a parameter batch
first. Those failed with `UNIMPLEMENTED`. The HTTP API has bound
parameters since ADR 0043; Flight did not pass them on.

Two details made it more than a pass-through:
- DataFusion parses every bare `?` as the same placeholder, so `a = ?
  AND b = ?` with values of two types fails to plan.
- The service is stateless (ADR 0124): there is nowhere to keep a
  binding between the `DoPut` that sends it and the `GetFlightInfo`
  that runs it.

## Decision

- **The prepared statement's handle is a small JSON document** of its
  SQL and, once bound, its parameters (`{"sql", "params"}`).
  - `CreatePreparedStatement` returns it with no parameters.
  - **`DoPut` of a parameter batch** (`CommandPreparedStatementQuery`)
    returns a new handle carrying the batch's row. The protocol allows
    this: an updated handle replaces the previous one, which is how a
    stateless server takes parameters.
  - Every later call uses the parameters from its handle.
- **Placeholders are numbered.** On `CreatePreparedStatement`, each `?`
  becomes `$1`, `$2`, ..., in order. Literals, quoted identifiers and
  `--` / `/* */` comments are left alone. `$n` written by hand is kept.
- **Values become what `/v1/sql` takes** (ADR 0043): each column of the
  bound row becomes a JSON scalar (null, boolean, number, else its text).
  The API casts each to the type its placeholder needs. A count that
  does not match is the API's `INVALID_ARGUMENT`.
- **One row per execution.** A batch of several rows (executemany) is
  `UNIMPLEMENTED`.
- **Unbound, the dataset schema is empty**: placeholders without values
  do not plan. Once bound, `GetFlightInfo` announces the schema.

## Consequences

**Positive**
- JDBC `?` statements, ADBC parameterized queries and BI filters work.
  Values never enter the SQL text, the logs or `system.runtime.queries`
  (ADR 0043).
- Still stateless: any coordinator serves any call.

**Negative (accepted debt)**
- **No `parameter_schema`** is announced. Clients bind by position,
  which the drivers do.
- **No batched executes.**
- **Handles grow with their parameters.** They travel with every call,
  so large values cost per call.

## Validation

- `flight::tests::question_marks_are_numbered_outside_literals_and_comments`.
- `flight::tests::prepared_statements_bind_parameters` (tonic client):
  - an `Int64` and a `Utf8` parameter filter the rows, and the bound
    statement announces its schema;
  - a bound update returns its count;
  - one value for two placeholders is `INVALID_ARGUMENT`.
- By hand, with the ADBC Flight SQL driver from Python:
  - `cursor.execute(sql, (10, "porto"))` with two `?` of two types;
  - a second execute with another value.

## Revisit when

- A client needs `parameter_schema`: plan the statement's parameter
  types (DataFusion's `get_parameter_types`) behind the API.
- Batched executes: one request per row, or a `VALUES` join.
