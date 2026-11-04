# ADR 0124 — Arrow Flight SQL front door

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0021 (tokens), ADR 0035 (streamed results), ADR 0037 (Arrow results), ADR 0099 (TLS)
- Scope: `flight.rs`, `PLIMSOLL_FLIGHT_BIND`, `main.rs` wiring

## Context

The only way in was the HTTP API (`/v1/sql`, JSON, NDJSON or Arrow).
Every SQL client, BI tool and notebook driver needs its own adapter for
that. Arrow Flight SQL is the open, columnar protocol the ecosystem
already speaks:
- ADBC drivers (Python, Go, C, R, Java);
- the Flight SQL JDBC driver, and through it DBeaver, Superset, Tableau
  and others.

Results stay Arrow end to end. Not having it was the main gap before an
MVP.

## Decision drivers

1. **One query path.** Grants, row filters, admission, deadlines,
   cancellation, the query log and traces must be exactly the HTTP
   API's, not a second copy that drifts.
2. **Stateless across coordinators.** Behind a load balancer (ADR 0101),
   any call may land on any node.
3. **Dependencies.** `arrow-flight` is the arrow line DataFusion already
   builds (59.x). Its gRPC stack (tonic, prost) adds a handful of
   crates over hyper, h2, tower and rustls, which are already in the
   tree. Flight SQL is the standard; nothing lighter speaks it.

## Decision

- **`PLIMSOLL_FLIGHT_BIND`** (unset = off) opens a gRPC listener for a
  `FlightSqlService`, the **gateway**.
- **The gateway posts every statement to the node's own `/v1/sql`
  router, in process** (no socket).
  - It passes the caller's `authorization` and `traceparent`, plus
    `x-plimsoll-resource-group` (and `x-plimsoll-user` with
    `PLIMSOLL_AUTH=disabled`).
  - Results are requested as Arrow IPC. They are decoded as they
    arrive and re-framed as Flight data: memory stays bounded, and a
    client that goes away drops the body, which cancels the query (ADR
    0035).
  - Non-200 answers become gRPC codes: 401 → `UNAUTHENTICATED`, 403 →
    `PERMISSION_DENIED`, 400 → `INVALID_ARGUMENT`, 422/429 →
    `RESOURCE_EXHAUSTED`, 503 → `UNAVAILABLE`, 504 →
    `DEADLINE_EXCEEDED`. The API's message is carried over.
  - A failure after the first batch is `ABORTED` (the HTTP body is cut,
    ADR 0037).
- **Handles are the SQL text.**
  - Tickets carry the SQL (`TicketStatementQuery`, the prepared-
    statement command).
  - Prepared statements are only their SQL; closing one frees nothing.
  - No state is kept between calls.
- **Schemas before execution.** `GetFlightInfo` and
  `CreatePreparedStatement` announce the result schema. They plan
  `SELECT * FROM (<sql>) LIMIT 0` through the same API, so the same
  checks apply. A statement that is not a query gets an empty schema.
- **Authentication.**
  - Bearer tokens as on HTTP.
  - A Basic `user:token` handshake (JDBC's username and password)
    returns `Bearer <token>`, accepted only if the token is that
    user's.
- **Metadata.**
  - `GetSqlInfo`: server name, version, not read-only.
  - `GetCatalogs`, `GetDbSchemas` and `GetTables`: from
    `information_schema.tables`, run as the caller. The protocol's
    filters are applied by arrow-flight's builders.
  - `GetTableTypes`.
  - Updates (`CommandStatementUpdate`, prepared updates) return the
    statement's `count`, and 0 for DDL.
- **TLS** with the node's `PLIMSOLL_TLS_CERT`/`KEY` (ADR 0099), read at
  boot.
- **Shutdown:** the listener closes when the HTTP listener has drained,
  with a 5 s bound.
- The image exposes 32010, and the quickstart publishes it.

## Consequences

**Positive**
- ADBC, JDBC and the tools built on them connect with a username and a
  token, and get Arrow.
- Every Flight call is a `/v1/sql` query: the same log line,
  `system.runtime.queries` entry, grants and limits.

**Negative (accepted debt)**
- **Each statement is planned twice** (the `LIMIT 0` probe, then the
  run), and the probe is its own logged query. Cache by SQL and
  snapshots if it shows up in latencies.
- **No bound parameters** on prepared statements (`DoPut` of a
  parameter batch), and no `DoPut` ingestion or Substrait.
- **`GetTables` with `include_schema`** returns empty table schemas.
  Primary/foreign keys and `GetXdbcTypeInfo` are unimplemented.
- **Flight's certificate is read at boot.** HTTP's reloads (ADR 0099).

## Validation

- `flight::tests` (real engine, tonic client on an ephemeral port):
  - anonymous calls, a wrong token and another user's token are
    refused;
  - the schema is announced before the run, also with no rows;
  - rows come back, and invalid SQL is `INVALID_ARGUMENT`;
  - an update returns its count;
  - prepared statements expose their dataset schema and run;
  - `GetTables` filters by name; catalogs and SQL info are listed.
- By hand, with the ADBC Flight SQL driver (Go, from Python) against
  the binary, over a Parquet table:
  - handshake, grouped query, empty result with schema;
  - `adbc_get_objects` lists `bronze`;
  - an unknown table is `INVALID_ARGUMENT`; a wrong password is
    refused;
  - a 64-character token, whose Basic credentials grpc-go sends
    without base64 padding (accepted padded or not; a unit test covers
    both);
  - over `grpc+tls` with the node's certificate; plaintext against it
    fails.
- `config::tests`: an invalid `PLIMSOLL_FLIGHT_BIND` fails the boot.

## Revisit when

- Parameterized prepared statements are needed (JDBC `?`): bind the
  parameter batch into the handle as `params` (ADR 0043).
- BI tools need column metadata from `GetTables`.
