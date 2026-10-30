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
cargo run -p plimsoll-server
curl -s localhost:8080/healthz
curl -s -XPOST localhost:8080/v1/sql -H 'content-type: application/json' \
     -d '{"sql": "select 1", "user": "ana"}'
curl -s localhost:8080/metrics
```

Configuration is environment-only; invalid values abort startup.

| Variable | Default | Meaning |
|---|---|---|
| `PLIMSOLL_BIND` | `127.0.0.1:8080` | Listen address |
| `PLIMSOLL_MEMORY_LIMIT_BYTES` | `1073741824` | Memory ceiling (the load line) |
| `PLIMSOLL_MAX_RUNNING` | `8` | Max concurrently running queries |
| `PLIMSOLL_MAX_QUEUED` | `64` | Max queued queries (0 = reject when busy) |
| `PLIMSOLL_MAX_RUNNING_PER_USER` | `PLIMSOLL_MAX_RUNNING` | Per-user running limit |
| `PLIMSOLL_QUERY_TIMEOUT_MS` | `30000` | Deadline for queue wait + execution |
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

### SQL API

`POST /v1/sql` with `{"sql": "...", "user": "...", "resource_group": "..."?}`.

- `200` → `{"query_id": 1, "columns": [...], "rows": [[...]]}`
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
[docs/adr/README.md](docs/adr/README.md) (0001–0010; template in
[docs/adr/0000-template.md](docs/adr/0000-template.md)).

Roadmap: [ROADMAP.md](ROADMAP.md).

## Non-goals

- Not a Databricks/Spark clone: no notebooks, no Spark API, no proprietary
  formats. Open formats (Parquet, Iceberg) and open catalogs only.
- Not a distributed engine yet: single node first, with plane boundaries that
  allow splitting later.
- No in-process TLS or authentication yet; deploy behind a proxy. The `user`
  field is trusted until authentication lands.
- No write path (DML/compaction) in the early phases; the read path and its
  resource safety come first.

## License

Apache-2.0. See [LICENSE](LICENSE).
