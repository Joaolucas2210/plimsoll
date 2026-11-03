# Plimsoll

> **The query fails. The node doesn't.**

Plimsoll is an open lakehouse query platform written in Rust on top of
[Apache DataFusion](https://datafusion.apache.org/). It is a build-to-learn
project that takes the core ideas of commercial lakehouse platforms (open
table formats, a catalog-centric control plane, federated queries, governed
multi-tenant execution) and reimplements them from first principles, with
resource safety as the non-negotiable property.

## The load line

The Plimsoll line is the mark on a ship's hull showing how deep it may sit in
the water. Load past the line and the ship is not allowed to sail; the cargo is
refused, the ship survives.

Plimsoll applies the same rule to queries:

- A process-wide **memory ceiling** (`CeilingPool`, ADR 0003) caps every byte
  operators reserve. A query that crosses the line gets a
  `memory_limit_exceeded` error; every other query keeps running.
- **Admission control** caps how many queries run and wait, globally, per
  resource group and per user. Excess load is rejected with `429 queue_full`
  instead of thrashing the node.
- **Time limits** bound queue wait + execution. A runaway query is cancelled
  and marked `failed`; its slot and memory are released.

Overload is a per-query outcome, never a crashed process.

## Architecture

```text
              ┌──────────────── plimsoll-server (HTTP, axum) ────────────────┐
  client ───► │ /v1/sql  /healthz /livez /readyz /metrics                     │
              └───────┬──────────────────────────────────────┬────────────────┘
                      │ admit / register / time-limit         │ execute
           ┌──────────▼──────────┐                 ┌──────────▼──────────────┐
           │  CONTROL PLANE      │                 │  COMPUTE PLANE          │
           │  plimsoll-control   │                 │  plimsoll-core          │
           │  - Iceberg REST     │  table metadata │  - DataFusion engine    │
           │    catalog client   ├────────────────►│  - CeilingPool          │
           │  - admission/queues │                 │  - federation policy    │
           │  - query registry   │  system tables  │  plimsoll-connectors    │
           │  - system.runtime.* ├────────────────►│  - Parquet/Iceberg, PG  │
           └─────────────────────┘                 └─────────────────────────┘
```

- **Control plane** decides *whether and where* a query runs. The catalog
  (Iceberg REST, Polaris-compatible) is control plane: it knows table identity
  and metadata locations, never data.
- **Compute plane** decides *how* a query runs, inside the memory ceiling.

Details: [docs/architecture.md](docs/architecture.md), [ADR 0002](docs/adr/0002-platform-planes.md).

## Medallion namespace convention

Iceberg namespaces follow the medallion layering; the first namespace level
names the layer:

| Namespace | Contents | Typical writer |
|---|---|---|
| `bronze` | Raw ingested data, source schema, append-only | ingestion jobs |
| `silver` | Cleaned, conformed, deduplicated entities | transformation jobs |
| `gold`   | Business-level aggregates and marts | analytics models |

Example: `gold.finance.revenue_daily` is namespace `["gold", "finance"]`,
table `revenue_daily`. The convention is documentation today; governance rules
per layer (e.g. read-only `bronze` for analysts) build on it later.

## Crate map

| Crate | Plane | Role |
|---|---|---|
| `plimsoll-core` | compute | Engine façade, `CeilingPool` memory ceiling, federation policy. Never depends on connectors. |
| `plimsoll-connectors` | compute | Concrete sources (Parquet/Iceberg, Postgres) implementing core's contracts. |
| `plimsoll-control` | control | Iceberg REST catalog client, governance (registry, admission, time limits), `system.runtime.queries` schema. No DataFusion. |
| `plimsoll-server` | edge | HTTP binary: health, metrics, `POST /v1/sql`. |

## Running

Requires a stable Rust toolchain (edition 2024, rust ≥ 1.85).

```bash
# Tokens are configured as SHA-256 digests; the server never sees them raw.
TOKEN=$(openssl rand -hex 32)
export PLIMSOLL_AUTH_TOKENS="ana:sha256:$(printf %s "$TOKEN" | sha256sum | cut -d' ' -f1)"
cargo run -p plimsoll-server
curl -s localhost:8080/healthz
curl -s -XPOST localhost:8080/v1/sql -H 'content-type: application/json' \
     -H "authorization: Bearer $TOKEN" -d '{"sql": "select 1"}'
curl -s localhost:8080/metrics
```

Time travel on catalog tables: `SELECT * FROM bronze."trips@<snapshot-id>"`
or `bronze."trips@2024-05-01T00:00:00Z"` (ADR 0028), or a branch/tag:
`bronze."trips@audit"` (ADR 0029). To find ids, instants and refs:
`bronze."trips$snapshots"`, `"trips$history"`, `"trips$refs"` (ADR 0030). Files and
manifests of the current snapshot: `"trips$files"`, `"trips$manifests"` (ADR 0032),
and per-partition totals: `"trips$partitions"` (ADR 0047).

Local experiments without tokens: `PLIMSOLL_AUTH=disabled` (then `user` is
read from the request body and trusted as sent).

Configuration is environment-only; invalid values abort startup.

| Variable | Default | Meaning |
|---|---|---|
| `PLIMSOLL_BIND` | `127.0.0.1:8080` | Listen address |
| `PLIMSOLL_AUTH_TOKENS` | required | `ana:sha256:<hex>,etl:sha256:<hex>`: SHA-256 of each bearer token and its user (ADR 0021) |
| `PLIMSOLL_AUTH_OPERATORS` | unset | Users (from the tokens) who see every query in `system.runtime.queries`; others see their own (ADR 0022) |
| `PLIMSOLL_RESOURCE_GROUPS` | unset | `interactive=4/16,batch=2/64/40%`: per group, max running / max queued (ADR 0023), and optionally a share of the memory ceiling that the group's queries together may hold (ADR 0036); `default` always exists with the global limits |
| `PLIMSOLL_AUTH_GROUPS` | unset | `etl=batch,ana=interactive\|batch`: groups each user may use, first = their default; unlisted users get `default` (ADR 0023) |
| `PLIMSOLL_AUTH_GRANTS` | unset | `ana=gold\|silver,etl=*`: schemas each user may read; once set, unlisted users read no data (system tables stay scoped per user) (ADR 0025) |
| `PLIMSOLL_AUTH_WRITE_GRANTS` | unset | `etl=bronze\|silver`: schemas each user may `CREATE TABLE`, `ALTER TABLE`, `INSERT INTO`, `UPDATE`, `DELETE FROM` and `MERGE INTO` (ADR 0077, 0081–0084; `MERGE` also needs a read grant on the target and no row filter on it) (appends to v2 Iceberg tables served by the REST catalog, partitioned or not, with column statistics; ADR 0076); unset = no writes; reads of the source still need read grants (ADR 0075) |
| `PLIMSOLL_AUTH_DROP_GRANTS` | unset | `etl=bronze`: schemas whose tables each user may `DROP TABLE [PURGE]`; a write grant does not include it; unset = no drops (ADR 0089) |
| `PLIMSOLL_AUTH_COLUMN_DENY` | unset | `ana=gold.users.email\|gold.users.ssn`: columns a user may not read, checked on what each scan reads after optimization, filters and subqueries included; `403 forbidden` (ADR 0044) |
| `PLIMSOLL_AUTH_ROW_FILTERS` | unset | `{"ana": {"gold.orders": "region = 'eu'"}}`: a SQL predicate every row a user reads from a table must satisfy, applied to every scan (joins, subqueries, time travel); the table's `$` metadata tables are denied to that user (ADR 0045) |
| `PLIMSOLL_AUTH` | `required` | `disabled` = no authentication, `user` trusted from the body (development only) |
| `PLIMSOLL_MEMORY_LIMIT_BYTES` | `1073741824` | Memory ceiling (the load line) |
| `PLIMSOLL_MAX_RUNNING` | `8` | Max concurrently running queries |
| `PLIMSOLL_MAX_QUEUED` | `64` | Max queued queries (0 = reject when busy) |
| `PLIMSOLL_MAX_RUNNING_PER_USER` | `PLIMSOLL_MAX_RUNNING` | Per-user running limit |
| `PLIMSOLL_QUERY_TIMEOUT_MS` | `30000` | Deadline for queue wait + execution |
| `PLIMSOLL_DRAIN_DELAY_MS` | `5000` | After SIGTERM: `/readyz` is 503 but queries are still served for this long, then the listener closes (ctrl-c skips it) (ADR 0026) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | OTLP/HTTP collector base URL (e.g. `http://collector:4318`); spans go to `/v1/traces` as JSON. Unset = trace context is still propagated, nothing exported (ADR 0031) |
| `OTEL_EXPORTER_OTLP_HEADERS` | unset | `x-api-key=...,x-tenant=...` sent with every export (secret, env only, never logged) |
| `OTEL_SERVICE_NAME` | `plimsoll` | `service.name` of exported spans |
| `PLIMSOLL_RESULT_CACHE_BYTES` | `0` (off) | Result cache for repeated queries over Iceberg snapshots, reserved in the memory pool, at most half the ceiling; keyed by SQL + every table's snapshot id, so a new commit is a miss (ADR 0038) |
| `PLIMSOLL_SLOW_QUERY_MS` | unset (off) | Queries at least this long also log a `slow_query` warn line: timings, `reads`, and each planning event (catalog loads; Iceberg files/manifests/deletes/cache hits). Never SQL text (ADR 0057) |
| `PLIMSOLL_WORKER_TOKEN` | unset | Shared secret (≥ 16 chars, env only) between coordinator and workers; on a worker it enables `POST /v1/fragment` (ADR 0058) |
| `PLIMSOLL_WORKERS` | unset | `http://w1:8080,http://w2:8080`: this node coordinates; Iceberg scans fan out to the workers whose `/readyz` answers (checked every 5 s; none healthy = local scan); tables with vended credentials stay local (ADR 0058, 0059). An entry `dns+http://workers.ns.svc:8080` makes every address that name resolves to (e.g. a Kubernetes headless service) a worker, re-resolved every 5 s so workers join and leave without a restart (ADR 0070). On workers, a `fragments` resource group (`PLIMSOLL_RESOURCE_GROUPS=fragments=8/16/50%`) bounds fragment concurrency and memory |
| `PLIMSOLL_SPILL_DIR` | unset | Spill directory; unset = no spill, pressure fails the query |
| `PLIMSOLL_PARQUET_TABLES` | unset | `bronze.trips=/data/trips/,gold.daily=/data/d.parquet` |
| `PLIMSOLL_ICEBERG_TABLES` | unset | `bronze.trips=/wh/bronze/trips` (table dir or `*.metadata.json`) |
| `PLIMSOLL_PG_TABLES` | unset | `silver.users=public.users` (needs `PLIMSOLL_PG_URL`) |
| `PLIMSOLL_PG_URL` | unset | Postgres connection string (secret, env only) |
| `AWS_ENDPOINT`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, ... | unset | S3 / S3-compatible storage for `s3://` tables (standard AWS env; `AWS_ALLOW_HTTP=true` for plain HTTP) |
| `PLIMSOLL_CATALOG_URI` | unset | Iceberg REST base URI (e.g. Polaris `http://host:8181/api/catalog`) |
| `PLIMSOLL_CATALOG_WAREHOUSE` | unset | Sent to `/v1/config` (Polaris: catalog name) |
| `PLIMSOLL_CATALOG_TOKEN` | unset | Bearer token. Secrets come from env only and are never logged. |
| `PLIMSOLL_CATALOG_CREDENTIAL` | unset | OAuth2 `client_id:client_secret` (Polaris); exclusive with the token |
| `PLIMSOLL_CATALOG_SCOPE` | `PRINCIPAL_ROLE:ALL` | OAuth2 scope |
| `PLIMSOLL_CATALOG_VENDED_CREDENTIALS` | `false` | `true` asks the catalog for per-table storage credentials (Polaris credential vending) |
| `PLIMSOLL_CATALOG_REFRESH_MS` | `60000` | How often `SHOW TABLES` / `information_schema` re-list the catalog (new tables and namespaces); `0` = boot only. Queries always resolve tables live (ADR 0033) |
| `PLIMSOLL_MAINTENANCE_INTERVAL_MS` | `0` (off) | Between scheduled maintenance passes: tables with `plimsoll.maintenance.enabled = 'true'` are compacted, have snapshots expired (Iceberg's `history.expire.*` properties) and, with `plimsoll.maintenance.remove-orphans`, orphan files removed; queued under admission as `plimsoll-maintenance`; run it on one coordinator (ADR 0086) |

### SQL API

`POST /v1/sql` with `{"sql": "...", "user": "...", "resource_group": "..."?, "format": "json"|"ndjson"|"arrow"?, "params": [...]?}`.

- `"params"` binds `$1`, `$2`, ... as values (ADR 0043): JSON scalars,
  cast to the type each placeholder needs (`"2024-05-01"` for a `DATE`),
  never spliced into the SQL, so client values cannot inject SQL and do
  not appear in logs or `system.runtime.queries`. Counts must match.

- `200` → `{"query_id": 1, "columns": [...], "rows": [[...]]}`
- `"format": "ndjson"` streams `application/x-ndjson` (ADR 0035): a header
  line `{"query_id", "columns"}`, one JSON array per row, then
  `{"done": true, "rows": N}`, or an `{"error": {...}}` line if the query
  fails mid-stream. Results of any size stream under the memory ceiling;
  errors before the header keep their HTTP status. Closing the connection
  cancels the query.
- `"format": "arrow"` streams `application/vnd.apache.arrow.stream` (ADR
  0037): typed, columnar, read directly by pyarrow (`pa.ipc.open_stream`),
  polars and DuckDB. The query id is in `x-plimsoll-query-id`. A failure
  after the first batch aborts the body, so the stream lacks its
  end-of-stream marker and readers raise instead of returning short data.
- error → `{"error": {"code": "...", "message": "...", "query_id": 1|null}}`

| Code | HTTP | Meaning |
|---|---|---|
| `invalid_request` | 400 | Malformed body, empty `sql`/`user` |
| `invalid_query` | 400 | Parse/plan error |
| `memory_limit_exceeded` | 422 | Query crossed the load line; node unaffected |
| `queue_full` | 429 | Admission queue full (global, group or user) |
| `engine_not_wired` | 501 | Compute plane not connected yet |
| `not_ready` | 503 | Starting or draining |
| `query_timeout` | 504 | Deadline exceeded; query cancelled |
| `query_cancelled` | 409 | Stopped by `DELETE /v1/queries/{id}` (ADR 0034) |

`DELETE /v1/queries/{id}` stops a queued or running query and releases its
memory and admission slot. Owners cancel their own queries,
`PLIMSOLL_AUTH_OPERATORS` any. `202` → `{"query_id": 1, "state": "cancelling"}`;
`404 query_not_found` for unknown ids and for other users' queries;
`409 query_not_running` once it has finished. Ids are in every response,
log line and `system.runtime.queries`.

### Compaction

`POST /v1/maintenance/compact` with `{"table": "bronze.events"}` rewrites
a catalog-served Iceberg table's small files and files carrying deletes
into fresh ones, partition by partition, as one `replace` snapshot; the
rows do not change and time travel to earlier snapshots still works.
Needs read and write grants on the table, holds an admission slot, and
answers `{"partitions", "files_rewritten", "files_written", "rows"}`
(ADR 0078).

`POST /v1/maintenance/expire-snapshots` with `{"table": "bronze.events",
"older_than_ms": 1730000000000, "retain_last": 1}` (defaults: 5 days ago,
1) drops snapshots committed before the cutoff, keeping every branch and
tag head and the last `retain_last` of each branch, then deletes the
files only they reached (ADR 0079).

`POST /v1/maintenance/remove-orphans` with `{"table": "bronze.events",
"dry_run": true}` lists (or, without `dry_run`, deletes) files under the
table location that no snapshot or metadata references and that were
last modified before `older_than_ms` (default 3 days ago; anything later
than 1 hour ago is refused, as a write in flight may still commit them)
(ADR 0080).

### Probes

- `/livez` — process alive (never checks dependencies).
- `/readyz` — `503` while starting or draining after SIGTERM.
- `/healthz` — JSON summary (readiness, uptime, running/queued, pool usage).
- `/metrics` — Prometheus text: `plimsoll_pool_used_bytes`,
  `plimsoll_pool_limit_bytes`, `plimsoll_spill_bytes_total`,
  `plimsoll_queries_running`, `plimsoll_queries_queued`,
  `plimsoll_queries_total{state}`.

### Tests and gates

```bash
cargo test --workspace
scripts/gates.sh   # fmt, clippy -D warnings, tests, architecture invariants
```

## Decision records

Index with status, date and the one-line decision of every ADR:
[docs/adr/README.md](docs/adr/README.md) (0001–0094; template in
[docs/adr/0000-template.md](docs/adr/0000-template.md)).

Roadmap: [ROADMAP.md](ROADMAP.md).

## Non-goals

- Not a Databricks/Spark clone: no notebooks, no Spark API, no proprietary
  formats. Open formats (Parquet, Iceberg) and open catalogs only.
- No in-process TLS yet; deploy the query API behind a TLS-terminating
  proxy. Authentication is bearer tokens (ADR 0021).
- Writes are `CREATE` (with `AS SELECT`, ADR 0090), `ALTER` (columns,
  nested fields, properties and partition fields) and `DROP TABLE`, `INSERT INTO`, `UPDATE`, `DELETE
  FROM`, `MERGE INTO` and table maintenance (compaction, snapshot
  expiration, orphan removal; ADR 0075–0089). See
  [ROADMAP.md](ROADMAP.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
