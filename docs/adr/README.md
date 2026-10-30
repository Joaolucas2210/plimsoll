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

## Reading order

- Invariant first: 0003 (memory), 0005 (correctness).
- Shape of the system: 0001, 0002, 0004.
- Lakehouse path: 0007 → 0008 → 0009 → 0010.
- Federation: 0005 → 0006.

## Conventions

- File name `NNNN-short-slug.md`; numbers are never reused.
- `Owner` is a plain GitHub handle (no `@`).
- Every claim must be checkable in the repo: type, env var, test or CI job
  names, not paraphrases.
- Shortcuts in code carry a `ponytail:` comment; the owning ADR lists them
  under Consequences → Negative.
