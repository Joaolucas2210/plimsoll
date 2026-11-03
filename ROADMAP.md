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
- [x] Joins with filters on workers: the residual filter shipped as SQL over `f0…` columns mapped to each side, checked by equivalence after simplification; shuffled, broadcast and gathered joins (ADR 0098)
- [x] Left, full, semi and anti broadcast joins on workers: workers join with numbered build rows and report the matches; the coordinator adds the build-only rows (`EXISTS`, `NOT EXISTS`, `IN`, left and full joins; `NOT IN` stays local) (ADR 0097)
- [x] Row lineage in rewrites: first row ids inherited on read, `_row_id` / `_last_updated_sequence_number` read where materialized and written into rewritten files; UPDATE, merge-update and compaction work on v3 (ADR 0096)
- [x] Writes to v3 tables: row lineage (`first-row-id`, `added-rows`, manifest `first_row_id`), deletes as deletion vectors in Puffin (one per data file, earlier deletes merged and replaced), `CREATE TABLE … 'format-version' = '3'`; rewrites (UPDATE, merge-update, compaction) refused until lineage is carried (ADR 0095)
- [x] `MERGE` on workers: the source attached to each fragment of target files; workers run `WHEN MATCHED` / `NOT MATCHED BY SOURCE` with clauses as SQL over a renamed merged row and report the source rows they paired; the coordinator inserts the unpaired ones and commits one overwrite (ADR 0094)
- [x] `DELETE` and `UPDATE` on workers: target files split across workers, each rewriting its own (position deletes, rewritten rows) from the predicate and new values shipped as SQL; the coordinator commits one delete/overwrite (ADR 0093)
- [x] CTAS and post-shuffle writes on workers: CTAS hands its plan to the create transaction, workers write into the staged table (metadata sent inline); shuffled aggregates and joins are written by their reducers (ADR 0092)
- [x] Data files written on workers: `INSERT … SELECT` over a remote scan writes its files where the rows are; workers report the files and the coordinator commits one append (ADR 0091)
- [x] `CREATE TABLE … AS SELECT` in one commit: staged create, rows written, `assert-create` commit carrying table and first snapshot; the query is governed like any read (ADR 0090)
- [x] `DROP TABLE [IF EXISTS] … [PURGE]` under its own grant (`PLIMSOLL_AUTH_DROP_GRANTS`), through the catalog's `dropTable` (ADR 0089)
- [x] Nested field evolution: nested fields resolve by id in the scan (renamed, moved and re-added fields read right); `ALTER TABLE … ADD | RENAME | DROP COLUMN a.b` in Spark's syntax (ADR 0088)
- [x] Writes to nested columns: field ids at every level (struct fields, list elements, map keys and values), no statistics under a nested column's id; `CREATE TABLE` / `ADD COLUMN` with `STRUCT` and `ARRAY`; read by Spark and pyiceberg (ADR 0087)
- [x] Scheduled maintenance: `PLIMSOLL_MAINTENANCE_INTERVAL_MS` passes over catalog tables that opt in through properties (`plimsoll.maintenance.*`, Iceberg's `history.expire.*`): compaction, expiration and optional orphan removal under admission (ADR 0086)
- [x] Partition evolution: `ALTER TABLE … ADD | DROP PARTITION FIELD` (field ids and specs reused); position deletes written in their data file's spec, one manifest per content and spec; compaction moves old-spec files to the current spec (ADR 0085)
- [x] `ALTER TABLE`: add, drop and rename columns, `DROP NOT NULL`, Iceberg type promotions and `SET TBLPROPERTIES`, one schema per statement with retry; renaming a denied column is denied; result cache keyed by schema too (ADR 0084)
- [x] `CREATE TABLE [IF NOT EXISTS] … PARTITION BY (…) OPTIONS (…)` through the REST catalog under the write grant; the rest of DDL still refused (ADR 0083)
- [x] `MERGE INTO … USING … ON …`: equi-join on the source, every WHEN form (matched update/delete, not matched insert, not matched by source); one `overwrite` snapshot; target needs read and write grants, row-filtered writers refused (ADR 0082)
- [x] `UPDATE … SET … WHERE`: position deletes plus rewritten rows (partition moves included) in one `overwrite` snapshot; subqueries in SET refused (ADR 0081)
- [x] Orphan file removal: `POST /v1/maintenance/remove-orphans` (dry run, ≥1 hour window) deletes unreferenced files under the table location (ADR 0080)
- [x] Snapshot expiration: `POST /v1/maintenance/expire-snapshots` drops old snapshots (branch and tag heads and the last N kept) through the catalog, then deletes files only they reached (ADR 0079)
- [x] Compaction: `POST /v1/maintenance/compact` rewrites small files and files carrying deletes into a `replace` snapshot; manifests rewritten with explicit sequence numbers; read by Spark and pyiceberg, time travel intact (ADR 0078)
- [x] `DELETE FROM … WHERE` as position delete files, skipping rows already deleted; subquery predicates refused (DataFusion would drop them); read by Spark and pyiceberg (ADR 0077)
- [x] Partitioned appends (identity, bucket, truncate, year/month/day/hour) and per-file column statistics, pruned by Plimsoll, Spark and pyiceberg (ADR 0076)
- [x] `INSERT INTO` Iceberg tables (append): v2, unpartitioned, through the REST catalog commit; write grants per schema; read by Spark and pyiceberg (ADR 0075)
- [x] Shuffle maps expire with their query's deadline, swept every 5 s; bucket requests for released shuffles refused (ADR 0074)
- [x] Shuffles released when queries end: workers drop a finished, failed or cancelled query's maps (and its retries') at once; the TTL is only a backstop (ADR 0073)
- [x] Shuffles survive a lost worker: maps re-run elsewhere for one bucket, reduces retried under a fresh shuffle id, 3 s connect timeout for every worker call (ADR 0072)
- [x] Execution spans per operator, from DataFusion's own metrics, on the coordinator and on workers, nested in one distributed trace (ADR 0071)
- [x] Worker discovery by DNS: `dns+http://name:port` entries re-resolved every health round; workers join and leave without restarts (ADR 0070)
- [x] Shuffle visibility: spilled buckets in `spilled_bytes` and a `shuffle_spill` line; worker requests classified by kind in the `fragment` log and span; temp-dir limit verified (ADR 0069)
- [x] Streaming shuffle reducers: buckets are opened as streams and read by the merge or join as a streaming table, no buffering outside operators (ADR 0068)
- [x] Shuffle buckets spill to disk: maps account every batch before keeping it and spill buckets the pool refuses to the spill directory (ADR 0067)
- [x] Broadcast joins on workers: small build sides (now sized by manifest row counts) are collected once and sent with each probe fragment; inner/right/right semi/anti only; shuffles never replace ordered nodes (ADR 0066)
- [x] Join shuffle between workers: partitioned equi-joins of two remote scans hash both sides into buckets and join on workers (ADR 0065)
- [x] Aggregation shuffle between workers: grouped partial states hash-partitioned, merged on workers, final rows to the coordinator (ADR 0064)
- [x] Expression keys and arguments in worker aggregations (`GROUP BY id % 16`, `sum(price * qty)`), shipped as round-tripped SQL (ADR 0063)
- [x] Row filters on workers: portable filters (SQL round trip) are exact, enforced by the scan locally and on workers; filtered aggregations pushed (ADR 0062)
- [x] Partial aggregation on workers: count/sum/min/max and column GROUP BY return partial states, not rows (ADR 0061)
- [x] Coordinator/worker step 3: fragments balanced by bytes (LPT), one retry on another worker before the first batch, end-of-stream marker required (ADR 0060)
- [x] Coordinator/worker step 2: fragments under worker admission (`fragments` group), deadline, query id/user/trace propagated, worker health with local fallback (ADR 0059)
- [x] Coordinator/worker step 1: Iceberg scans fan out to workers as file fragments over HTTP + Arrow IPC, deletes applied on workers, same answers (ADR 0058)
- [x] Slow-query log: `PLIMSOLL_SLOW_QUERY_MS`, a `slow_query` line with timings, reads and planning events (ADR 0057)
- [x] Planning spans in traces: `catalog.load_table` and `iceberg.plan` (files/manifests/deletes) under `planning` (ADR 0056)
- [x] Exact identity-partition filters: decided per file, no `FilterExec`, partition-filtered `count(*)` from metadata (ADR 0055)
- [x] `count(*)` from manifest record counts when exact (no deletes, filters or limit) (ADR 0054)
- [x] Decimal pruning: unscaled `i128` bounds, exact literal rescaling, decimal bucket/truncate partitions (ADR 0053)
- [x] `LIKE 'prefix%'` / `starts_with` prune as string ranges (ADR 0052)
- [x] Bucket and truncate partition pruning, per file on partition tuples (Iceberg Murmur3, spec vectors) (ADR 0051)
- [x] Pruner as a predicate tree: OR, IN lists, exact NOT, at file and manifest level (ADR 0050)
- [x] Null-count pruning: all-NULL columns match no comparison; `IS NULL` / `IS NOT NULL` prune on counts (ADR 0049)
- [x] Pruning on nested struct fields (`s['a']` uses `s.a` bounds) (ADR 0048)
- [x] Iceberg `$partitions`: per (spec, partition) data rows/files/bytes and delete rows/files, folded from `$files` under the ceiling (ADR 0047)
- [x] Audit trail: `reads` (table, columns read, row filter) on every query log line, from the optimized plan (ADR 0046)
- [x] Row filters per principal: a predicate under every scan of the table, cache-keyed, metadata tables denied (ADR 0045)
- [x] Column-level deny: per principal, on the columns each optimized scan reads (filters included), time travel and `$files` partitions covered (ADR 0044)
- [x] Query parameters: `"params"` bind `$n` as typed values, cast to the inferred type, part of the result-cache key (ADR 0043)
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

**Expression join keys on workers** — hash expression keys
(`ON lower(a.k) = lower(b.k)`) on the maps, shipped as SQL like
aggregation keys, so shuffled and broadcast joins take them too.

## Missing

**Lakehouse writes** (the largest gap)
- Several deletion vectors per Puffin file; v3 compaction of files with
  equality deletes; decimal partition values; data
  a shuffled `MERGE` for sources larger than a node, broadcast-join
  writes; vectorized partition transforms.
- `MERGE` beyond the pool (spilled source); deletion vectors for v3
  tables.
- Maintenance: a per-table lease for several coordinators, per-step
  schedules, metrics.
- DDL: nested promotions, leaf statistics for nested fields, `CREATE OR
  REPLACE TABLE … AS SELECT`.

**Distribution**
- Expression join keys on workers (above); non-equi (nested loop)
  joins against a small side;
  an explicit cap on broadcast build sides, or caching them per snapshot
  (ADR 0066).
- Expression join keys and residual filters in shuffled joins; joins
  between a remote scan and an aggregate or subquery (ADR 0065).
- Cheaper recovery: one re-run of a lost map serving every reducer;
  more than one retry per step (ADR 0072).
- Postgres (federated) scans stay on the coordinator.
- Coordinator high availability (several coordinators sharing
  workers); SRV-based discovery; TLS to workers discovered by IP
  (ADR 0070).

**Planning and performance**
- Column statistics from manifests (bounds, null counts) and filter
  selectivity for estimates; skip the shuffle for few-group
  aggregations (ADR 0064/0066).
- Skew: spill the largest bucket first, split hot buckets (ADR 0067).
- Spilling hash-join build sides (DataFusion upstream, ADR 0068).

**Security and operations**
- In-process TLS for the query API (today: behind a proxy).
- OTLP metrics; shuffle-map operator spans; per-kind fragment
  counters; per-query spill attribution (ADR 0069/0071).
