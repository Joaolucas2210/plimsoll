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
      and the next query on the same node succeeds (concurrent variant: TODO)

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
- [x] SIGTERM drain: readiness off, keep serving for `PLIMSOLL_DRAIN_DELAY_MS`, then close (ADR 0026)
- [x] Per-schema data grants per principal, checked on every table scan before execution (ADR 0025)
- [x] Read-only query API: no DDL/DML/COPY/SET through `/v1/sql` (closed arbitrary file read/write, ADR 0024)
- [x] Configurable resource groups with their own limits, bound to principals; unknown group 400, unbound 403 (ADR 0023)
- [x] `system.runtime.queries` scoped to the principal; operators see all; fail closed without a viewer (ADR 0022)
- [x] Bearer-token authentication, `user` = principal, SHA-256 digests in env, secure by default (ADR 0021)
- [x] Query latency histograms by phase: queue, planning, execution, serialization, total (ADR 0019)
- [x] Wire pool/spill metrics to the live `CeilingPool`

## Next obvious PR

**Structured logs**: one JSON line per finished query (`query_id`, `user`,
`resource_group`, `code`, phase timings) instead of `eprintln!`, so failures
can be found by user or code without the registry (ADRs 0013, 0019).

## Later

- OTLP traces and metrics export (OpenTelemetry), query-level spans
  (admission wait, planning, execution). No OpenTelemetry dependency until then.
- Coordinator/worker split along the existing plane boundary.
