# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [SemVer](https://semver.org/). Before 1.0, a minor
version may change the HTTP API, Flight SQL behaviour or environment
variables; each such change is listed here. Decisions are in
[docs/adr](docs/adr/README.md).

## [Unreleased]

## [0.1.0]

First release: an open lakehouse query engine on DataFusion and Iceberg
that fails the query, never the node.

### Engine and governance
- One process-wide memory ceiling (`CeilingPool`) with spill; a query
  over it fails alone with `memory_limit_exceeded` (ADR 0003, 0012, 0018).
- Admission by running and queued limits, globally, per resource group
  and per user, with deadlines, cancellation and per-group memory shares
  (ADR 0022–0026, 0034, 0036).
- Bearer tokens (SHA-256 digests), read/write/drop grants per schema,
  column denies and row filters (ADR 0021–0025, 0044, 0045, 0077, 0089).

### Lakehouse
- Iceberg reads through REST catalogs (Polaris, vended credentials):
  merge-on-read position and equality deletes, v3 deletion vectors,
  time travel, branches and tags, metadata tables, partition and column
  pruning (ADR 0007–0017, 0028–0032, 0047).
- Writes: `CREATE TABLE [AS SELECT]`, `ALTER`, `DROP`, `INSERT`,
  `UPDATE`, `DELETE`, `MERGE INTO`; compaction, snapshot expiration,
  orphan removal, scheduled maintenance (ADR 0075–0090).
- Parquet tables and Postgres federation with safe pushdown (ADR 0005,
  0006).

### Distribution
- Coordinator and workers: remote scans, partial aggregations, shuffled
  and broadcast joins (equi and non-equi), skew splitting, writes on
  workers, retries, DNS discovery and TLS (ADR 0058–0074, 0091–0099,
  0111–0122).
- Several coordinators behind a load balancer: shared query lists,
  cancellation and cluster-wide or exact admission (ADR 0101–0110).

### Interfaces and operations
- HTTP API: JSON, streamed NDJSON and Arrow results, bound parameters
  (ADR 0035, 0037, 0043).
- Arrow Flight SQL for ADBC, JDBC and BI tools (ADR 0124).
- Structured query logs, OTLP traces and metrics, `/metrics`,
  `system.runtime.queries` (ADR 0027, 0031, 0046, 0057, 0100, 0118–0121).
- Container image and a Compose quickstart (ADR 0123); releases from a
  tag (ADR 0125).

[Unreleased]: https://github.com/Joaolucas2210/plimsoll/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Joaolucas2210/plimsoll/releases/tag/v0.1.0
