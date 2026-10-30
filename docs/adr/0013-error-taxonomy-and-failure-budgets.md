# ADR 0013 — Error taxonomy and dependency failure budgets

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0004 (HTTP contract, refined), ADR 0005, ADR 0007, ADR 0008, ADR 0009, ADR 0012
- Scope: `plimsoll-connectors::ConnectorError`, `plimsoll-server` (`engine::classify`, `app::ApiError`, `metrics`), `plimsoll_core::storage`, `plimsoll-control::rest`

## Context

Until now every failure that was not a parse error or the memory ceiling
became `500 internal`: an Iceberg table with delete files, a catalog outage,
S3 refusing credentials, Postgres down. Three audiences need to tell these
apart and could not:

- **Clients**: should I retry? fix my query? give up on this table?
- **On-call**: is the node broken (page Plimsoll) or is an upstream down
  (page someone else)? A 5xx storm caused by S3 looks like a Plimsoll bug.
- **Admission**: a dependency that hangs holds an admission slot until the
  query timeout. Measured before this ADR, with storage unreachable:
  `object_store` retried for up to **3 min** (its default), the REST
  catalog client waited **30 s** per request. During an outage every query
  sat at the timeout, saturating `PLIMSOLL_MAX_RUNNING` and turning one
  failed dependency into a node-wide queue.

## Decision drivers

1. On-call can distinguish *node fault* from *upstream fault* from *data /
   unsupported* from a single field, without reading logs.
2. Clients get a stable `code` and an explicit `retryable` flag.
3. A failing dependency costs a query seconds, not the query timeout.
4. Classification must survive DataFusion's error wrapping (context, Parquet
   external errors, connector nesting).
5. No new dependencies; the control plane stays DataFusion-free (ADR 0002).

## Considered options

**A. Classify at the source, map at the edge (chosen).** Producers emit typed
errors whose variant *is* the class (`ConnectorError::Unsupported`,
`::InvalidMetadata`, `::PostgresUnavailable` vs `::Postgres`; `CatalogError`;
`object_store::Error`). The server walks `DataFusionError` (root, `External`,
`ObjectStore`, `ParquetError::External`, nested `ConnectorError::DataFusion`)
and downcasts known types. Pros: each producer decides what it knows (only the
Postgres connector understands SQLSTATE); the server is the single mapping
table. Cons: the server must know the error types of the crates it composes
(it already depends on them).

**B. Parse error messages.** Zero types. Brittle across DataFusion upgrades
and driver versions; rejected.

**C. A shared error-class trait in `plimsoll-core`.** Producers implement
`fn class()`. Cleaner in theory, but `CatalogError` lives in the
DataFusion-free control plane and `object_store::Error` is foreign; both
would need wrappers. More code than A for the same result.

## Decision

**Classes and HTTP contract** (`ApiError::of`, the single mapping table):

| Class (`EngineError`) | HTTP | `code` | `retryable` | Source examples |
|---|---|---|---|---|
| `InvalidQuery` | 400 | `invalid_query` | no | parse/plan errors, unknown table/namespace |
| `MemoryLimitExceeded` | 422 | `memory_limit_exceeded` | no | `ResourcesExhausted` (ADR 0003/0012) |
| `Unsupported` | 422 | `unsupported_feature` | no | Iceberg v3 column defaults / encryption, nested types, equality keys on dropped fields; Postgres type |
| `InvalidTable` | 422 | `invalid_table` | no | malformed metadata, data file `NotFound`, undecodable Parquet |
| `Dependency{dependency}` | 502 | `dependency_failed` + `dependency` | yes | catalog (`Http`, `Auth`, `Status`, `Protocol`), `object_store` (anything but `NotFound`), `postgres` (SQLSTATE 08/28/53/57 or no response) |
| (admission) | 429 / 503 / 504 | `queue_full` / `not_ready` / `query_timeout` | yes | governance |
| `Internal` | 500 | `internal` | no | everything unclassified, Postgres rejecting *our* SQL |

Body: `{"error": {"code", "message", "retryable", "query_id", "dependency"?}}`.
Only `internal` means "Plimsoll is wrong"; 4xx never indicate an unhealthy
node; 502 names the upstream.

**Metric**: `plimsoll_query_errors_total{code}` counts every `/v1/sql` error by
code. Alert on `code="internal"` (page Plimsoll) separately from
`code="dependency_failed"` (page the dependency owner).

**Failure budgets** (driver 3):
- `object_store` (env and vended stores): 3 retries within 10 s, connect
  timeout 3 s, request timeout 30 s, set key-by-key so `from_env` settings such
  as `AWS_ALLOW_HTTP` survive.
- REST catalog client: request timeout 10 s, connect timeout 3 s.

## Consequences

**Positive**
- Measured (local smoke, 2026-10-30): S3 refusing connections →
  `502 dependency_failed/object_store` in **0.38 s**; catalog down → `502
  dependency_failed/catalog` in **5 ms**; S3 hung behind a catalog that needs
  it → `502 …/catalog` in **10 s** (was ≥30 s); node healthy throughout.
- Clients can implement retry policies from `retryable` alone.

**Negative (accepted debt)**
- A storage server that accepts connections but never answers still costs up
  to the 30 s request timeout per request (large ranged Parquet reads need
  that headroom). The query timeout remains the outer bound.
- Postgres errors outside the "unavailable" SQLSTATE classes are `internal`
  even when caused by data (e.g. a cast failure in the source); refine when
  seen.
- Fixed budgets (`ponytail:`); not configurable yet.

**Operational**
- Dashboards: split error rate by `code`; `dependency_failed` spikes point at
  upstreams, `internal` at Plimsoll.
- The JSON body includes `dependency` for 502s; logs stay `eprintln` for now.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A new error path is unclassified and shows as `internal` | Medium | Medium (false pages) | `internal` is the explicit default; tests pin each class; review new producers against this table. |
| A wrapping change in DataFusion hides the typed source | Low | Medium | Unit test covers context, Parquet external and connector nesting; live tests assert real shapes (S3 auth). |
| Short budgets fail queries during brief storage blips | Medium | Low (retryable 502) | `retryable: true`; budgets documented; make configurable when needed. |
| Replacing client options again drops env settings | Low | High (all S3 reads fail) | Regression test `failure_budget_keeps_env_settings`. |

## Validation

- `engine::tests::errors_are_classified_at_their_source`: every class,
  including context-wrapped, Parquet-external and connector-nested errors.
- `app::tests::error_contract_carries_class_retryability_and_metrics`: HTTP
  status, `code`, `retryable`, `dependency`, and the metric lines.
- `lakehouse::tests::reads_with_catalog_vended_credentials` (live, CI): wrong
  S3 credentials classify as `Dependency { dependency: "object_store" }`.
- `storage::tests::failure_budget_keeps_env_settings`: budgets applied,
  `AWS_ALLOW_HTTP` preserved.
- Manual outage smoke (numbers above): SeaweedFS stopped and paused, REST
  catalog stopped, with the server running.

## Revisit when

- An alert policy needs finer codes (e.g. `dependency_auth` vs
  `dependency_unavailable`).
- Deployments need different budgets (make them config).
- Structured logging lands: log `code`/`dependency` per failed query. (Done: ADR 0027.)
- A connector adds a new failure mode (extend the table first).
