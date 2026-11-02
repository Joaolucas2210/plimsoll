# Roadmap

Each phase ends with `scripts/gates.sh` green and the invariant intact:
*the query fails, the node doesn't.*

## Phase 0 — Foundations

- [x] Cargo workspace, crate boundaries (core / connectors / control / server)
- [x] `CeilingPool`: process-wide memory ceiling (ADR 0003)
- [x] Server with `/livez`, `/readyz`, `/healthz`, `/metrics`, graceful shutdown
- [x] ADRs 0001–0004, quality gates script with architecture checks

## Phase 1 — Local read path under the pool

- [x] Engine façade in `plimsoll-core`: `SessionContext` built on `CeilingPool`, result buffers in the Output slice
- [x] Wire `QueryEngine` in the server to the façade
- [x] Parquet scans from local/object storage under the ceiling, spill enabled (S3 via ADR 0009)
- [x] Iceberg table reads (metadata → manifests → Parquet) under the ceiling (ADR 0007)
- [x] Test: a query exceeding the ceiling fails with `memory_limit_exceeded`
      and the next query on the same node succeeds; concurrent variant: 8 joins
      racing past a ceiling that fits one, each exact or failing alone

## Phase 2 — Postgres federation

- [x] Postgres connector as a `TableProvider` in `plimsoll-connectors`
- [x] `PushdownPolicy`: which predicates/projections/limits may be pushed (ADR 0006)
- [x] `FilterGuard`: re-apply filters locally when pushdown semantics differ
      (collation, NULLs, types) — correctness over speed (ADR 0005)
- [x] Connector buffers accounted against the ceiling (ADR 0012)

## Phase 3 — Control plane

- [x] Iceberg REST catalog contract + in-memory implementation (`plimsoll-control::catalog`)
- [x] HTTP implementation of the catalog client (Polaris), `reqwest` + rustls (ADR 0008)
- [x] One HTTP/TLS stack: catalog on object_store's reqwest 0.12 + ring (ADR 0015)
- [x] Catalog namespaces as schemas; tables resolved per query (fresh snapshots)
- [x] Catalog-vended storage credentials, scoped per table (ADR 0009)
- [x] Scan pruning: manifests by partition summaries, files by column bounds (ADR 0010)
- [x] Memory at the edges: Postgres streamed + accounted, response JSON accounted, manifests decoded lazily (ADR 0012)
- [x] Error taxonomy (`code`, `retryable`, `dependency`), `plimsoll_query_errors_total{code}`, dependency failure budgets (ADR 0013)
- [x] Merge-on-read position deletes, validated against Spark (ADR 0014)
- [x] Equality deletes (upserts/CDC), validated against Iceberg Java writers (ADR 0016)
- [x] Caches under the ceiling (cache reserve), stale directory listings fixed, decoded delete files cached (ADR 0018)
- [x] Format v3: deletion vectors (Puffin + Roaring), `timestamp_ns`; defaults/encryption refused (ADR 0017)
- [x] Admission: running/queued limits globally, per resource group, per user (RAII slots)
- [x] Query registry + per-query deadline (queue wait + execution)
- [x] `system.runtime.queries` schema + snapshot source
- [x] Expose `system.runtime.queries` as a DataFusion table (server)
- [x] Metrics: pool used/limit, spill bytes, running/queued, totals by state
- [x] Concurrent manifest / delete-file / DV fetches during planning (`meta_fetch_concurrency`), 4.3× faster planning at 50 ms/GET (ADR 0020)
- [x] Name mapping: files without field ids (add_files imports) resolve through `schema.name-mapping.default`, aliases included (ADR 0042)
- [x] Nested types: struct, list, map; struct fields added later read as NULL; nested renames refused; validated on Spark (ADR 0041)
- [x] Schema evolution by field id: renames, type promotion and re-added columns, equality-delete keys by id, validated on a Spark-evolved table (ADR 0040)
- [x] Manifest cache: manifest lists and manifests cached by path in the cache reserve; replanning a snapshot reads nothing (ADR 0039)
- [x] Result cache: SQL + pinned Iceberg snapshots as key, deterministic plans only, reserved in the pool, opt-in (ADR 0038)
- [x] Arrow IPC results: `"format": "arrow"`, same bounded chunk path, mid-stream errors abort the stream (ADR 0037)
- [x] Memory shares per resource group: `batch=2/64/40%` caps the group's queries together, below the ceiling; per-group used/share gauges (ADR 0036)
- [x] Streamed results: `"format": "ndjson"`, bounded by the chunks in flight, back-pressure to the scan, disconnect cancels (ADR 0035)
- [x] Query cancellation: `DELETE /v1/queries/{id}` for owners and operators; queued or running; slot and memory released (ADR 0034)
- [x] Live catalog listings: tables and namespaces created after boot appear in `SHOW TABLES` / `information_schema` (`PLIMSOLL_CATALOG_REFRESH_MS`) (ADR 0033)
- [x] Iceberg file metadata tables: `"table$files"`, `"table$manifests"`, streamed per manifest under the ceiling (ADR 0032)
- [x] Query traces: W3C `traceparent` in/out, `trace_id` in logs, OTLP/HTTP JSON export of a span per query and phase (ADR 0031)
- [x] Iceberg metadata tables: `"table$snapshots"`, `"table$history"`, `"table$refs"` (ADR 0030)
- [x] Iceberg branches and tags: `"table@<ref>"` (ADR 0029)
- [x] Iceberg time travel: `"table@<snapshot-id>"` / `"table@<RFC 3339 instant>"`, snapshot's own schema, grants unchanged (ADR 0028)
- [x] Structured JSON logs: one `query` line per request (identity, outcome, phase timings, SQL fingerprint, never SQL text) (ADR 0027)
- [x] SIGTERM drain: readiness off, keep serving for `PLIMSOLL_DRAIN_DELAY_MS`, then close (ADR 0026)
- [x] Per-schema data grants per principal, checked on every table scan before execution (ADR 0025)
- [x] Read-only query API: no DDL/DML/COPY/SET through `/v1/sql` (closed arbitrary file read/write, ADR 0024)
- [x] Configurable resource groups with their own limits, bound to principals; unknown group 400, unbound 403 (ADR 0023)
- [x] `system.runtime.queries` scoped to the principal; operators see all; fail closed without a viewer (ADR 0022)
- [x] Bearer-token authentication, `user` = principal, SHA-256 digests in env, secure by default (ADR 0021)
- [x] Query latency histograms by phase: queue, planning, execution, serialization, total (ADR 0019)
- [x] Wire pool/spill metrics to the live `CeilingPool`

## Next obvious PR

**Iceberg name mapping** (`schema.name-mapping.default`): files imported
without Parquet field ids (`add_files`, migrations from Hive tables) are
refused in renamed tables today (ADR 0040). Resolve them through the
table's name mapping so imported data reads correctly.

## Later

- Spans below the request (per-operator, per-fetch) via `tracing` +
  `opentelemetry-otlp`, and OTLP metrics (ADR 0031's upgrade path).

- Coordinator/worker split along the existing plane boundary.
