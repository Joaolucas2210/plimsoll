# ADR 0008 — Iceberg REST catalog client

- Status: Accepted
- Scope: `plimsoll-control::rest`, `plimsoll-server::lakehouse`

## Context

The catalog is the control plane's source of truth for table identity and
the current `metadata-location` (ADR 0002). Targets: Apache Polaris and any
spec-compliant Iceberg REST server. Two details differ between servers and
must not be hard-coded: Polaris serves `/v1/{catalog}/...` and requires an
OAuth2 client-credentials token; the Apache reference server has no prefix
and no auth.

## Decision

- **Spec handshake.** `GET /v1/config?warehouse=` at boot; `overrides` win
  over `defaults` for `prefix` and `namespace-separator` (default `%1F`).
- **Auth.** `PLIMSOLL_CATALOG_TOKEN` (static bearer) *or*
  `PLIMSOLL_CATALOG_CREDENTIAL=client_id:secret` (+ `PLIMSOLL_CATALOG_SCOPE`,
  default `PRINCIPAL_ROLE:ALL`) exchanged at `/v1/oauth/tokens`. A 401 renews
  the token once. Secrets come from env only and are redacted in `Debug`.
- **Paging.** `next-page-token` is followed for namespaces and tables.
- **Per-query resolution.** Each single-level namespace becomes a DataFusion
  `SchemaProvider`; `table()` calls `loadTable` on every query and builds the
  provider from the returned metadata (`IcebergConnector::from_metadata`, no
  extra read). New commits are visible immediately; there is no stale cache.
- **Failure.** Boot fails on unreachable catalog or bad credentials. At query
  time a catalog error fails that query only.
- **Dependency.** `reqwest` 0.13 with `rustls` (aws-lc-rs provider, no
  OpenSSL). `plimsoll-control` still has no DataFusion dependency.

## Alternatives

- **`iceberg-catalog-rest`:** pulls iceberg-rust and Arrow 58 (see ADR 0007).
- **Hand-rolled HTTP on hyper:** we already have hyper via axum, but TLS,
  redirects, proxies and timeouts would be ours to get right.
- **Load all tables at boot:** stale snapshots until restart, and boot time
  grows with the catalog.

## Consequences

- Verified against `apache/iceberg-rest-fixture` (CI, with a pyiceberg-written
  table and a live commit) and against Apache Polaris (OAuth, prefix,
  namespaces).
- One `loadTable` round trip per table per query. Upgrade path if it shows in
  latency: an ETag / `If-None-Match` cache (spec supports it).
- Debt: multi-level namespaces are skipped; table listings are cached at boot
  (lookups are live); no `oauth2-server-uri` for external IdPs; object stores
  other than the local filesystem (S3/GCS/ADLS, vended credentials) are not
  registered yet, so Polaris tables on S3 list but cannot be scanned.
