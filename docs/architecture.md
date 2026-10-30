# Architecture

Plimsoll is a single-node lakehouse query service split into a **control
plane** (decides whether and where a query runs) and a **compute plane**
(decides how it runs, inside a hard memory ceiling). See
[ADR 0002](adr/0002-platform-planes.md).

## Stack

```text
 ┌─────────────────────────────────────────────────────────────────────────┐
 │ plimsoll-server  (axum 0.8, ADR 0004)                                   │
 │   /livez  /readyz  /healthz  /metrics          POST /v1/sql             │
 │   config from env (fail fast) · graceful shutdown (SIGTERM / ctrl-c)    │
 └──────────────┬───────────────────────────────────────────┬──────────────┘
                │ Admission::execute(req, deadline, run)    │ QueryEngine::execute(sql)
 ┌──────────────▼──────────────────────┐   ┌────────────────▼──────────────┐
 │ CONTROL PLANE: plimsoll-control     │   │ COMPUTE PLANE: plimsoll-core  │
 │                                     │   │                               │
 │ governance                          │   │ engine façade (DataFusion)    │
 │   QueryRegistry  (state, timings)   │   │ CeilingPool  (ADR 0003)       │
 │   Admission      (global/group/user │   │   reservations ≤ load line    │
 │                   running + queued) │   │   spill to disk               │
 │   AdmissionSlot  (RAII)             │   │ federation (ADR 0005)         │
 │   deadline       (tokio timeout)    │   │   PushdownPolicy (ADR 0006)   │
 │ catalog                             │   │   FilterGuard                 │
 │   Catalog trait  (Iceberg REST)     │   └────────────────┬──────────────┘
 │   InMemoryCatalog / REST (Polaris)  │                    │
 │ system                              │   ┌────────────────▼──────────────┐
 │   system.runtime.queries schema     │   │ plimsoll-connectors           │
 │   + registry snapshot               │   │   Parquet / Iceberg, Postgres │
 └──────────────┬──────────────────────┘   └────────────────┬──────────────┘
                │ metadata location                         │ data
        ┌───────▼─────────┐                       ┌─────────▼──────────┐
        │ Iceberg REST    │                       │ object store / FS, │
        │ catalog         │                       │ Postgres           │
        │ (Polaris)       │                       └────────────────────┘
        └─────────────────┘
```

## Request lifecycle (`POST /v1/sql`)

1. **Readiness**: if the node is starting or draining → `503 not_ready`.
2. **Register**: `QueryRegistry` records the query as `queued`.
3. **Admit**: `Admission::acquire` returns an `AdmissionSlot` immediately if
   global, group and user running counts are under their limits; otherwise
   the query waits in a FIFO queue, or is rejected (`429 queue_full`) if the
   global, group or user queue is full.
4. **Run**: state → `running`; the engine executes under the `CeilingPool`.
5. **Deadline**: one `tokio::time::timeout` covers steps 3–4. On expiry the
   execution future is dropped (DataFusion streams stop on drop) →
   `504 query_timeout`, state `failed`.
6. **Release**: the slot is dropped on every exit path, which promotes the next
   runnable queued query. A dropped client connection marks the query
   `cancelled`.

Every failure in this list is a response code, never a process exit.

## Observability

- `/metrics` is rendered at scrape time from the owners of each value
  (admission counts, registry totals, pool provider); no duplicated counters.
- `system.runtime.queries` exposes the registry as a table (schema in
  `plimsoll_control::system::QUERIES_COLUMNS`).
- Next: OTLP export (see [ROADMAP](../ROADMAP.md)).

## Invariants (enforced by `scripts/gates.sh`)

- `plimsoll-core` does not depend on `plimsoll-connectors`.
- `plimsoll-control` depends on neither `plimsoll-core` nor `datafusion`.
- No `unwrap`/`expect` outside tests (workspace clippy lints).

## Decision records

See the [ADR index](adr/README.md) (0001–0010: status, date and one-line
decision for each).
