# Architecture Decision Records

Decisions that shape Plimsoll, one per file, in the format of
[0000-template.md](0000-template.md). Accepted decisions are not edited to
change them: a new ADR supersedes the old one, which is then marked
`Superseded by ADR XXXX`. Factual corrections (stale debt lists, wrong names)
are allowed in place.

| # | Title | Status | Date | Decision (one line) | Related |
|---|---|---|---|---|---|
| [0001](0001-rust-datafusion.md) | Rust + Apache DataFusion as the engine foundation | Accepted | 2026-10-29 | Rust 2024 + DataFusion 55 as a library; its `MemoryPool` is the hook for the ceiling; `unwrap`/`expect` denied | 0002, 0003, 0004, 0007 |
| [0002](0002-platform-planes.md) | Control plane vs compute plane | Accepted | 2026-10-29 | `plimsoll-control` (catalog, admission, registry) never depends on `datafusion` or `plimsoll-core`; core never on connectors; checked by `scripts/gates.sh` | 0001, 0003, 0005, 0008 |
| [0003](0003-memory-ceiling.md) | Process-wide memory ceiling (`CeilingPool`) | Accepted | 2026-10-29 | One fallible pool per process with an output slice and top-5 consumer errors; breach fails the query (422), never the node | 0001, 0007, 0009 |
| [0004](0004-http-axum.md) | HTTP surface with axum 0.8 | Accepted | 2026-10-29 | axum (`http1`, `json`, `tokio`), in-process handler tests, typed error codes, hand-rolled Prometheus exposition | 0001, 0002, 0003, 0008 |
| [0005](0005-federation-correctness.md) | Federation correctness: a filter can never disappear | Accepted | 2026-10-29 | `FederationFilterGuard` re-applies every untrusted filter on the scan; trust = weaker of policy and source; scalar subqueries pre-evaluated | 0006, 0007, 0003 |
| [0006](0006-pushdown-policy.md) | Per-source pushdown policy | Accepted | 2026-10-29 | Typed per-class `PushdownPolicy`; Postgres trusts only integer/bool/null checks; parameters, never inlined literals | 0005, 0007, 0003 |
| [0007](0007-iceberg-read-path.md) | Iceberg read path under the ceiling | Accepted | 2026-10-29 | Own metadata walk + DataFusion `ParquetSource` wrapped in `AccountedExec`; refuse deletes/renames/v3 with typed errors | 0003, 0005, 0006, 0008, 0009, 0010 |
| [0008](0008-rest-catalog-client.md) | Iceberg REST catalog client and per-query table resolution | Accepted | 2026-10-29 | Own `reqwest` client (config handshake, OAuth2, paging); `loadTable` on every query, no stale cache | 0002, 0007, 0009, 0004 |
| [0009](0009-object-storage.md) | Object storage and catalog-vended credentials | Accepted | 2026-10-30 | Lazy `s3://` stores from AWS env; vended keys in per-credential `ScopedStore`s under synthetic URLs | 0003, 0007, 0008 |
| [0010](0010-iceberg-scan-pruning.md) | Iceberg scan planning: manifest and file pruning | Accepted | 2026-10-30 | Prune manifests and data files before the scan instead of decoding and `HEAD`-ing every file | 0003, 0005, 0007, 0009 |
| [0011](0011-ci-cost-and-build-reuse.md) | CI cost: one build, one job, one cache | Accepted | 2026-10-30 | Single job building once with `--workspace`; live tests reuse the build; shared main-only cache; docs-only pushes skip CI | 0001, 0007, 0010 |
| [0012](0012-memory-at-the-edges.md) | Memory at the edges: connectors, planning and the response | Accepted | 2026-10-30 | Postgres streams in accounted batches; results keep their reservation through JSON serialization; manifests decode lazily under reservation | 0003, 0004, 0006, 0007, 0010 |
| [0013](0013-error-taxonomy-and-failure-budgets.md) | Error taxonomy and dependency failure budgets | Accepted | 2026-10-30 | Errors classified at the source into `invalid_query`/`memory_limit_exceeded`/`unsupported_feature`/`invalid_table`/`dependency_failed`/`internal` with `retryable`; dependencies fail in seconds, not at the query timeout | 0004, 0005, 0007, 0008, 0009, 0012 |
| [0014](0014-merge-on-read-position-deletes.md) | Merge-on-read: position deletes as Parquet row selections | Accepted | 2026-10-30 | Position delete files become `ParquetRowSelection`s so deleted rows are never decoded; seq-number rule, reserved positions; equality deletes refused; Spark interop workflow | 0007, 0010, 0011, 0012, 0013 |
| [0015](0015-one-http-tls-stack.md) | One HTTP/TLS stack | Accepted | 2026-10-30 | Catalog client on the same `reqwest 0.12` + `ring` as `object_store`; `aws-lc-rs` and 14 other crates leave the build | 0008, 0009, 0011 |
| [0016](0016-equality-deletes.md) | Merge-on-read: equality deletes as grouped hash anti-joins | Accepted | 2026-10-30 | Files grouped by the equality delete sets that apply (strict seq, same partition or global); each group anti-joins the keys with null = null; reference-writer interop | 0007, 0010, 0011, 0012, 0013, 0014 |
| [0017](0017-format-v3-deletion-vectors.md) | Format v3: deletion vectors, and what v3 must not silently change | Accepted | 2026-10-30 | Accept v3; DV blobs read by range, length/magic/CRC verified, Roaring decoded with `roaring`, applied as row selections; refuse column defaults and encryption | 0007, 0013, 0014, 0016 |
| [0018](0018-caches-under-the-ceiling.md) | Caches under the ceiling, and a delete-file cache | Accepted | 2026-10-30 | DataFusion caches sized to a `CeilingPool` cache reserve queries never get; listing cache off (stale directories); decoded delete files cached by immutable path | 0003, 0012, 0014, 0016, 0017 |
| [0019](0019-query-latency-histograms.md) | Query latency histograms by phase | Accepted | 2026-10-30 | `plimsoll_query_duration_seconds{phase}` (queue, planning, execution, serialization, total) as hand-rolled fixed-bucket atomic histograms; no metrics crate | 0004, 0013, 0018 |
| [0020](0020-concurrent-planning-fetches.md) | Concurrent metadata fetches during Iceberg planning | Accepted | 2026-10-30 | Manifests, delete files and DV blobs fetched `meta_fetch_concurrency` at a time, each reserved; decoded and merged in manifest order | 0007, 0010, 0012, 0014, 0016, 0017, 0018, 0019 |
| [0021](0021-bearer-token-authentication.md) | Bearer-token authentication for the SQL endpoint | Accepted | 2026-10-30 | `Authorization: Bearer` mapped to a principal via SHA-256 digests in `PLIMSOLL_AUTH_TOKENS`; `user` = principal, impersonation 403; boot fails without tokens unless `PLIMSOLL_AUTH=disabled` | 0004, 0013, 0015 |
| [0022](0022-principal-scoped-system-tables.md) | Principal-scoped system tables | Accepted | 2026-10-30 | The caller's `Viewer` rides in a per-query session config extension; `system.runtime.queries` shows own rows, all for `PLIMSOLL_AUTH_OPERATORS`, and fails closed without a viewer | 0002, 0021 |
| [0023](0023-resource-groups.md) | Configurable resource groups bound to principals | Accepted | 2026-10-30 | Closed set of groups (`default` + `PLIMSOLL_RESOURCE_GROUPS`) with own running/queued limits; `PLIMSOLL_AUTH_GROUPS` binds principals, first = default; unknown 400, unbound 403 | 0004, 0013, 0021, 0022 |
| [0024](0024-read-only-query-api.md) | Read-only query API | Accepted | 2026-10-30 | User SQL runs with DDL, DML (incl. `COPY`) and statements (`SET`) disabled: closes arbitrary server file read/write and shared-catalog mutation | 0002, 0004, 0013, 0021 |
| [0025](0025-schema-grants.md) | Per-schema data grants | Accepted | 2026-10-30 | `PLIMSOLL_AUTH_GRANTS` lists readable schemas per principal; the engine checks every `TableScan` (subqueries, CTEs, joins) through a `TableAccess` policy before anything executes; denial is 403 | 0002, 0013, 0021, 0022, 0024 |
| [0026](0026-shutdown-drain.md) | Shutdown drain: unadvertise, keep serving, then close | Accepted | 2026-10-30 | Readiness and serving are separate flags; SIGTERM turns `/readyz` 503 but serves queries for `PLIMSOLL_DRAIN_DELAY_MS` (5 s) before closing; ctrl-c closes at once | 0004, 0013 |
| [0027](0027-structured-logs.md) | Structured JSON logs | Accepted | 2026-10-30 | One JSON object per line on stderr; a `query` event for every `/v1/sql` outcome with identity, code, phase timings and a SQL fingerprint (never the text); no logging crate | 0004, 0013, 0019, 0021 |
| [0028](0028-iceberg-time-travel.md) | Iceberg time travel by table-name suffix | Accepted | 2026-10-30 | `bronze."trips@<snapshot-id>"` or `@<RFC 3339>` pins the snapshot (and its schema) by rewriting metadata before planning; instants resolve via `snapshot-log`; the reference stays in its schema so grants apply | 0007, 0010, 0022, 0025 |
| [0029](0029-iceberg-refs.md) | Iceberg branches and tags in time travel | Accepted | 2026-10-30 | `"table@<ref>"` resolves through metadata `refs` after snapshot ids and instants; unknown specs fall back to a whole-name lookup | 0028, 0025 |
| [0030](0030-iceberg-metadata-tables.md) | Iceberg metadata tables | Accepted | 2026-10-30 | `"table$snapshots"`, `"table$history"`, `"table$refs"` built in memory from the per-query `loadTable` metadata; Spark column names; grants apply via the schema | 0028, 0029, 0025 |
| [0031](0031-query-traces.md) | Query traces over OTLP/HTTP JSON | Accepted | 2026-11-01 | W3C `traceparent` continued or started per `/v1/sql` request and echoed; `trace_id` in the query log; with `OTEL_EXPORTER_OTLP_ENDPOINT`, a `query` span plus one per phase, batched to `/v1/traces` without an OTel SDK | 0019, 0027, 0015, 0004 |
| [0032](0032-iceberg-file-metadata-tables.md) | Iceberg file metadata tables: `$files`, `$manifests` | Accepted | 2026-11-01 | Manifests read at execution through a streaming partition, one batch per manifest, fetches reserved in the pool; Spark column names; partition as JSON text | 0030, 0012, 0020, 0025 |
| [0033](0033-live-catalog-listings.md) | Live catalog listings | Accepted | 2026-11-01 | A periodic re-list (`PLIMSOLL_CATALOG_REFRESH_MS`, 60 s) updates table names and registers new namespaces; list-then-swap so a catalog error changes nothing; never shadows env tables | 0008, 0030 |
| [0034](0034-query-cancellation.md) | Query cancellation | Accepted | 2026-11-01 | `DELETE /v1/queries/{id}`: a per-query `Notify` in the registry races the deadline; cancelling drops the governed future (queue wait or execution); owners and operators only, others get 404; the query answers `409 query_cancelled` | 0013, 0021, 0022, 0026 |

## Reading order

- Invariant first: 0003 (memory, with 0012 and 0018), 0005 (correctness).
- Shape of the system: 0001, 0002, 0004.
- Lakehouse path: 0007 → 0008 → 0009 → 0010 → 0014 → 0016 → 0017 → 0028 → 0029 → 0030 → 0032.
- Federation: 0005 → 0006.
- Observability: 0013 → 0019 → 0027 → 0031.

## Conventions

- File name `NNNN-short-slug.md`; numbers are never reused.
- `Owner` is a plain GitHub handle (no `@`).
- Every claim must be checkable in the repo: type, env var, test or CI job
  names, not paraphrases.
- Shortcuts in code carry a `ponytail:` comment; the owning ADR lists them
  under Consequences → Negative.
