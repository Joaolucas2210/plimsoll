# Architecture

Plimsoll is a lakehouse query service: SQL over Apache Iceberg tables in
object storage (plus Parquet and federated Postgres), on Apache
DataFusion. One binary runs every role: a single node, a coordinator
with workers, several coordinators behind a load balancer. It is split
into a **control plane**, which decides *whether and where* a query
runs, and a **compute plane**, which decides *how* it runs inside a hard
memory ceiling (ADR 0002).

The rule that shapes everything: **the query fails, the node doesn't.**
Overload, a runaway query or a bad table ends one query with a typed
error. It never ends the process.

## Crates

```text
 ┌──────────────────────────────────────────────────────────────────────────┐
 │ plimsoll-server  (the binary; edge)                                      │
 │   HTTP (axum): /v1/sql /v1/queries /v1/maintenance/* /metrics /*z       │
 │   Arrow Flight SQL (tonic) → the same /v1/sql, in process (ADR 0124)     │
 │   worker endpoints: /v1/fragment · peer endpoints: /v1/peer/*            │
 │   auth · TLS · config from env · logs/traces/metrics · lakehouse wiring  │
 └───────────┬───────────────────────────────────────────┬──────────────────┘
             │ admit, register, deadline                 │ plan + execute
 ┌───────────▼──────────────────────┐   ┌────────────────▼──────────────────┐
 │ CONTROL PLANE: plimsoll-control  │   │ COMPUTE PLANE: plimsoll-core      │
 │  governance: registry, admission │   │  engine façade (DataFusion)       │
 │   (global/group/user), deadlines │   │  CeilingPool: the memory ceiling, │
 │  catalog: Iceberg REST client    │   │   spill, caches under it          │
 │   (Polaris, vended credentials)  │   │  federation policy + FilterGuard  │
 │  system.runtime.queries schema   │   │  DDL, result cache, plan trace    │
 │  (no DataFusion here)            │   ├───────────────────────────────────┤
 └──────────────────────────────────┘   │ plimsoll-connectors               │
                                        │  Iceberg read/write/maintenance   │
                                        │  Parquet · Postgres               │
                                        │  remote.rs: distribution          │
                                        └───────────────────────────────────┘
```

| Crate | Plane | Main modules |
|---|---|---|
| `plimsoll-core` | compute | `engine` (façade, `QueryTag`, `SpillSink`), `memory` (`CeilingPool`, accounted consumers), `federation` (policy, guard, subqueries), `ddl`, `result_cache`, `plan_trace`, `storage` |
| `plimsoll-connectors` | compute | `iceberg` (+ `prune`, `deletes`, `write`, `merge`, `alter`, `create`, `expire`, `remote_write`, `meta`), `parquet`, `postgres`, `portable` (SQL that travels), `remote` (fragments, shuffles, broadcasts, physical rules) |
| `plimsoll-control` | control | `governance` (`QueryRegistry`, `Admission`, `Gate`), `catalog` + `rest` (Iceberg REST), `system` |
| `plimsoll-server` | edge | `app` (routes), `engine` (`CoreEngine`, fragments, shuffle maps), `flight`, `auth`, `config`, `lakehouse` (catalog schemas), `peers` (coordinators), `schedule` (maintenance), `tables` (env tables), `tls`, `log`, `trace`, `metrics` |

Invariants, checked by `scripts/gates.sh`:
- `plimsoll-core` does not depend on `plimsoll-connectors`.
- `plimsoll-control` depends on neither `plimsoll-core` nor DataFusion.
- No `unwrap`/`expect` outside tests (workspace lints).

## A query's life (`POST /v1/sql`, or Flight SQL)

1. **Authenticate.** A bearer token's SHA-256 digest maps to a user
   (ADR 0021). Flight SQL also accepts a Basic `user:token` handshake
   (ADR 0124).
2. **Readiness.** Starting or draining → `503`.
3. **Register and admit.** The registry records the query `queued`.
   Admission grants a slot if global, resource-group and per-user
   running counts are under their limits; otherwise it queues (FIFO) or
   rejects (`429`). Across coordinators the limits can be the cluster's:
   per-coordinator parts or one elected leader (ADR 0103–0110).
4. **Plan.**
   - Tables resolve live against the catalog (fresh snapshots), or from
     env tables.
   - Grants, column denies and row filters are applied to every scan
     after optimization (ADR 0025, 0044, 0045).
   - Iceberg scans prune manifests and files by partition and column
     statistics.
   - Physical rules may distribute the plan (below).
5. **Execute** under the `CeilingPool`: every operator reserves memory
   from one process-wide pool, spilling to disk first. Crossing the line
   fails that query (`422 memory_limit_exceeded`).
6. **Deadline.** One timeout covers queue and execution (`504`). A
   dropped client connection cancels the query.
7. **Release and record.** The slot frees on every path. One structured
   log line, a trace, `system.runtime.queries` and `/metrics` record
   the outcome, the tables read and the bytes spilled.

Results stream as JSON, NDJSON or Arrow IPC, with memory bounded by the
chunks in flight (ADR 0035, 0037).

## Distribution

A node with `PLIMSOLL_WORKERS` coordinates; one with
`PLIMSOLL_WORKER_TOKEN` serves fragments. Workers are found by URL list or
DNS (`dns+http://…`, re-resolved every 5 s), and health-checked.

```text
            coordinator                                  workers
  SQL → plan → physical rules ─ fragments (JSON + optional Arrow) ─► /v1/fragment
                     ▲                                              │
                     └──────── Arrow IPC results (+ spill trailer) ─┘
                                worker ⇄ worker: shuffle buckets
```

- **Remote scans.** Iceberg data files are split into fragments by size
  and sent to workers, with portable filters (ADR 0058–0063).
- **Partial aggregation** on workers: only states travel. Grouped
  aggregations **shuffle** by key between workers (ADR 0061, 0064).
- **Joins.**
  - **Broadcast** of a small side, up to `PLIMSOLL_BROADCAST_MAX_BYTES`,
    cached per pinned snapshot (ADR 0066, 0099, 0116).
  - **Shuffled hash joins** of two large sides, with hot buckets split
    across reducers (ADR 0117).
  - **Non-equi joins** against a small side (ADR 0115).
- **Writes on workers.** `INSERT … SELECT`, CTAS, `UPDATE`, `DELETE` and
  `MERGE` write data files where the rows are. The coordinator commits
  once (ADR 0091–0094, 0122). A `MERGE` source over the broadcast limit
  merges on the coordinator (ADR 0130).
- **Recovery.**
  - A fragment is retried once before its first batch.
  - A lost reducer's bucket reruns elsewhere (ADR 0072).
  - Shuffle maps are released when the query and every stream reading
    them are done (ADR 0073, 0128), else at a TTL.
- **Governance on workers.** A `fragments` resource group bounds
  concurrency and memory. Workers report spill back to the query (ADR
  0118–0121).

## Lakehouse

- **Reads.**
  - Iceberg v1–v3: merge-on-read position and equality deletes,
    deletion vectors.
  - Time travel by snapshot, timestamp, branch or tag; metadata tables.
  - Schema evolution.
- **Writes.**
  - DDL (`CREATE [AS SELECT]`, `ALTER`, `DROP`) and DML (`INSERT`,
    `UPDATE`, `DELETE`, `MERGE`).
  - Each statement is one snapshot commit, with conflict checks.
- **Maintenance.** Compaction, snapshot expiration and orphan removal,
  on demand or scheduled per table. Several coordinators claim tables
  by lease.
- **Statistics.** Column bounds from manifests, distinct counts from
  Puffin, and live row counts feed DataFusion's join ordering (ADR
  0113, 0114).

## Interfaces and operations

- **HTTP API** (`/v1/sql` with bound parameters, cancel, maintenance)
  and **Arrow Flight SQL**:
  - statements and prepared statements with `?` parameters;
  - batches, a batched `INSERT` in one commit;
  - catalog listings with column schemas (ADR 0124, 0127, 0129, 0133).
- **TLS** in process, with certificate reload (ADR 0099).
- **Observability.**
  - Structured JSON logs and a slow-query log.
  - OTLP traces and metrics.
  - Prometheus `/metrics`, including fragments by kind.
  - The `system.runtime.queries` table.
- **Several coordinators** share workers, the query list,
  cancellation and admission (ADR 0101–0110).
- **Packaging.** One image for every role, and a Compose quickstart.
  Signed amd64/arm64 releases from a tag (ADR 0123, 0125, 0131, 0132).

## Configuration

Environment only. Invalid values abort startup, and secrets are never
logged. The full table is in the [reference](reference.md#configuration-and-running).

## Decisions

Every choice above has an ADR with its context, alternatives and accepted
debt: [docs/adr](adr/README.md). What is done and what is next:
[ROADMAP.md](../ROADMAP.md).
