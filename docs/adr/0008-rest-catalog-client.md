# ADR 0008 — Iceberg REST catalog client and per-query table resolution

- Status: Accepted
- Date: 2026-10-29
- Owner: JoaoLucas2210
- Related: ADR 0002, ADR 0007, ADR 0009, ADR 0004
- Scope: `plimsoll_control::{catalog, rest}`, `plimsoll_server::lakehouse`, `plimsoll-server/src/main.rs`

## Context

The catalog is the control plane's source of truth for table identity and
the current `metadata-location` (ADR 0002). Targets: Apache Polaris and any
spec-compliant Iceberg REST server. Details differ between servers and must
not be hard-coded: Polaris serves `/v1/{prefix}/...` and requires an OAuth2
client-credentials token; the Apache reference server
(`apache/iceberg-rest-fixture`) has no prefix and no auth. Tables change
underneath a running engine (commits, drops); a stale snapshot is a wrong
answer.

## Decision drivers

1. Freshness: a query sees the catalog's current snapshot.
2. Spec compliance over server-specific code paths.
3. No second Arrow/DataFusion in the tree; control plane stays DataFusion-free.
4. Secrets only from env, never logged.
5. Fail fast at boot on misconfiguration; at query time fail only the query.

## Considered options

### Option A — Own client on `reqwest` + per-query `loadTable` (chosen)

Pros: drivers 1–5; small surface (config, oauth, list namespaces/tables,
load table). Cons: one catalog round trip per table per query; we own spec
details (paging, separators, error bodies).

### Option B — `iceberg-catalog-rest`

Pulls iceberg-rust and Arrow 58 (ADR 0007).

### Option C — Hand-rolled HTTP on hyper

hyper is already present via axum, but TLS, redirects, proxies and timeouts
would be ours to get right.

### Option D — Load all tables at boot

Zero per-query latency. Cons: stale snapshots until restart; boot time grows
with the catalog.

## Decision

- **Configuration** (`CatalogConfig::from_lookup`, env only):
  `PLIMSOLL_CATALOG_URI` (absent → no catalog; must start with `http://` or
  `https://`), `PLIMSOLL_CATALOG_WAREHOUSE`, `PLIMSOLL_CATALOG_TOKEN` (static
  bearer) **or** `PLIMSOLL_CATALOG_CREDENTIAL=client_id:secret` (+
  `PLIMSOLL_CATALOG_SCOPE`, default `PRINCIPAL_ROLE:ALL`) — both set is a
  config error — and `PLIMSOLL_CATALOG_VENDED_CREDENTIALS=true|false` (ADR
  0009). Secrets are redacted in `Debug` of `CatalogConfig` and `LoadedTable`.
- **Handshake** (`RestCatalog::connect`): client-credentials token from
  `{uri}/v1/oauth/tokens` if configured, then `GET /v1/config?warehouse=`;
  `overrides` win over `defaults` for `prefix` and `namespace-separator`
  (default `%1F`). HTTP timeout 30 s.
- **Auth renewal.** A 401 triggers one token refresh and retry, for client
  credentials only. 401/403 otherwise → `CatalogError::Auth`.
- **Paging.** `next-page-token` is followed for namespaces and tables.
- **Errors.** Typed `CatalogError` (`Config`, `NamespaceNotFound`,
  `TableNotFound`, `Http`, `Auth`, `Status`, `Protocol`); 404 is split into
  namespace vs table by the spec error `type`.
- **Per-query resolution** (`plimsoll_server::lakehouse::register`): each
  single-level namespace becomes a DataFusion `SchemaProvider` in the default
  catalog, replacing the medallion placeholder of the same name.
  `SchemaProvider::table` calls `loadTable` on every resolution and builds the
  guarded provider from the returned metadata
  (`IcebergConnector::from_metadata(...).with_storage_properties(...)`,
  no extra `metadata.json` read). `TableNotFound` → table absent; any other
  catalog error fails that query.
- **Boot order.** Catalog first, then env tables (`PLIMSOLL_*_TABLES`), so an
  env table aimed at a catalog-backed schema fails at boot instead of being
  shadowed.
- **Dependency.** `reqwest 0.13`, `default-features = false`, feature
  `rustls` (aws-lc-rs provider in `Cargo.lock`, no OpenSSL).

Why over Option D: per-query resolution makes freshness a property of the
design, not of restart discipline; the cost is one round trip with a known
upgrade path (ETag).

## Consequences

**Positive.** New commits and drops are visible on the next query. The same
code path serves Polaris and the reference server. Vended credentials come
for free with each `loadTable` (ADR 0009).

**Negative (accepted debt).**
- One `loadTable` round trip per table reference per query; catalog latency
  is query latency.
- Table *listings* (`SHOW TABLES`, `information_schema`) are cached at boot;
  lookups are live.
- Multi-level namespaces are skipped at boot (logged).
- Only the spec's `/v1/oauth/tokens`; no `oauth2-server-uri` for external IdPs.
- `LoadedTable::metadata` is untyped JSON.

**Operational.** Boot fails (non-zero exit, message on stderr) on invalid
config, unreachable catalog or rejected credentials at handshake. At query
time a catalog outage fails each query that touches catalog tables with HTTP
500 `internal` (`catalog unreachable: ...`); env-registered tables keep
working. Unknown tables are 400 `invalid_query`.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Catalog latency/outage degrades every lakehouse query | Medium | High | 30 s timeout; upgrade: ETag / `If-None-Match` cache (spec supports it) |
| Polaris-specific behaviour regresses unnoticed | Medium | Medium | Spec stub test covers prefix + OAuth + renewal; Polaris checked manually only |
| Token expiry mid-flight | Medium | Low | One refresh-and-retry on 401 |
| Secret leakage in logs | Low | High | Redacting `Debug`; secrets only from env |

## Validation

- `plimsoll_control::rest::tests` (in-process axum stub, exact wire format):
  `handshake_pagination_auth_renewal_and_not_found`,
  `bad_static_token_is_an_auth_error`,
  `vended_credentials_are_requested_and_best_prefix_wins`.
- `plimsoll_control::catalog::tests`: `config_absent_is_none_and_invalid_fails_fast`,
  `auth_modes_and_redaction`, `in_memory_catalog_lists_and_loads`.
- `plimsoll_server::lakehouse::tests::tables_resolve_per_query_through_the_catalog`
  (a new commit is seen by the next query; multi-level namespace skipped).
- CI job `iceberg-interop`: `live_rest_catalog` against
  `apache/iceberg-rest-fixture` with a pyiceberg-written table on S3.
- Apache Polaris (OAuth, prefix, namespaces): verified manually when the
  client landed (commit 5dfbf91); not in CI.

## Revisit when

- p99 `loadTable` latency is a significant share of query latency, or the
  catalog rate-limits us (add ETag caching).
- A deployment uses multi-level namespaces or an external IdP.
- Polaris compatibility breaks or is required as a release gate (add a CI job).
