# ADR 0009 — Object storage and catalog-vended credentials

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0003, ADR 0007, ADR 0008
- Scope: `plimsoll_core::storage` (`LazyObjectStores`, `ScopedStore`, `scoped_s3_store`), `plimsoll_core::memory::runtime_env`, `plimsoll_connectors::iceberg`, `plimsoll_control::rest`

## Context

Lakehouse tables live in object storage. Iceberg paths are full URIs
(`s3://bucket/...`) and a table's metadata, manifests and data files may all
be remote, in buckets the operator does not enumerate up front: the catalog
decides. DataFusion resolves stores through an `ObjectStoreRegistry`; by
default only `file://` is registered. Catalogs such as Polaris can *vend*
short-lived, path-scoped credentials per table on `loadTable`
(`X-Iceberg-Access-Delegation: vended-credentials`), which must not leak to
other tables in the same bucket.

## Decision drivers

1. Any bucket the catalog points to is readable without pre-registration.
2. No new secrets surface: standard AWS env/role, catalog-vended keys.
3. Vended credentials are isolated per credential set, never bucket-wide.
4. The read path stays unchanged and under the `CeilingPool`.
5. Minimal dependency weight.

## Considered options

### Option A — Lazy registry + scoped stores under synthetic URLs (chosen)

Resolve `s3://` on first use from env; register vended-credential stores under
their own URL. Pros: drivers 1–5. Cons: synthetic scheme is an indirection;
scoped entries accumulate.

### Option B — Register stores from config at boot

Explicit. Cons: needs the bucket list up front; catalogs can point anywhere.

### Option C — Replace the bucket's store with vended credentials

Simple. Cons: one table's short-lived, path-scoped keys would replace the
bucket's store for every other table and break them.

## Decision

- **`LazyObjectStores`** wraps DataFusion's `DefaultObjectStoreRegistry`. A
  miss on `s3://` or `s3a://` builds `AmazonS3Builder::from_env()` for that
  bucket and caches it (a race builds twice; last wins, harmless). Other
  unregistered schemes (`gs://`, `abfs://`) fail. Installed in every session
  by `memory::runtime_env`, so metadata, Avro manifests and Parquet all
  resolve through it under the ceiling (ADR 0003, ADR 0007).
- **Configuration** is the standard AWS env read by `object_store`
  (`AWS_ENDPOINT`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_ALLOW_HTTP`, ...) with
  fallback to the instance/pod role. No Plimsoll-specific storage variables.
- **Table storage properties.** `RestCatalog::load_table` returns
  `LoadedTable::storage` = response `config` overlaid with the
  `storage-credentials` entry whose `prefix` is the longest match for the
  table `location`. The delegation header is sent only when
  `PLIMSOLL_CATALOG_VENDED_CREDENTIALS=true`.
- **Scoped stores.** `IcebergConnector::with_storage_properties` calls
  `scoped_s3_store`: when the properties carry `s3.access-key-id` and the
  table location is on S3, it builds a store from `s3.*` / `client.region`
  properties (unset ones fall back to `AWS_*` env) and registers it under
  `s3-<hash>://<bucket>`, the hash covering access key id, session token and
  endpoint. Metadata, manifest and Parquet reads in that bucket go through
  it; other buckets use the lazy env store. This applies whenever the catalog
  returns keys in `config`, not only with vending enabled.
- **Refresh.** Credentials are re-fetched on every query because every query
  calls `loadTable` (ADR 0008).
- **Dependency:** `object_store 0.13` (the version DataFusion 55 uses), feature
  `aws`, plus `url 2`.

Why over Option C: per-credential registry keys make isolation structural;
the cost is one synthetic URL and a bounded number of stale entries.

## Consequences

**Positive.** Any S3-compatible store works (CI uses SeaweedFS). No storage
secrets in files. A catalog can narrow access per table without operator
action.

**Negative (accepted debt, `ponytail:` in code).**
- GCS/ADLS not enabled (`gcp` / `azure` features; same few lines).
- Rotated credentials leave old `ScopedStore` entries in the registry until
  restart (one small client each). Upgrade: evict on
  `s3.session-token-expires-at-ms`.
- One object store per table: files spanning stores fail with
  `NotImplemented` (ADR 0007).
- Two HTTP/TLS stacks in the binary: `object_store` brings `reqwest 0.12`
  with `ring`; the catalog client uses `reqwest 0.13` with `aws-lc-rs`
  (ADR 0008).

**Operational.** Wrong or missing credentials surface as a failed query
(object-store error, HTTP 500 `internal`), not at boot: stores are built
lazily. A query that outlives a vended session token fails mid-scan.
`Debug` of `ScopedStore` prints only bucket and synthetic URL.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Misconfigured AWS env discovered only at first query | Medium | Medium | Readiness does not cover storage; add a boot probe if it bites |
| Vended token expires during a long scan | Low | Medium | Fresh `loadTable` per query; long queries need token refresh (not implemented) |
| Unbounded scoped-store growth with rapid rotation | Low | Low | Evict on expiry (upgrade path) |
| Hash collision between credential sets | Very low | High | 64-bit `DefaultHasher` over key id, token, endpoint |

## Validation

- `plimsoll_core::storage::tests`: `s3_is_built_on_demand_and_cached_unknown_schemes_fail`,
  `scoped_store_from_vended_properties` (same credentials → same key; secret
  absent from `Debug`).
- `plimsoll_control::rest::tests::vended_credentials_are_requested_and_best_prefix_wins`
  (header sent only when enabled; longest prefix wins; `config` is the base).
- CI job `iceberg-interop`: REST fixture with an `s3://warehouse/` on
  SeaweedFS; `reads_a_pyiceberg_table` and `live_rest_catalog` read over
  `s3://` with env credentials; step "catalog-vended credentials (env keys
  are wrong on purpose)" runs
  `plimsoll_server::lakehouse::tests::reads_with_catalog_vended_credentials`:
  with `AWS_ACCESS_KEY_ID=wrong` the plain read must fail and the read with
  injected per-table keys (`PLIMSOLL_TEST_VENDED`, a read-only SeaweedFS
  identity) must succeed. The reference fixture does not vend, so the test
  wraps `RestCatalog` to inject the keys; the header path itself is covered
  by the stub test above.

## Revisit when

- A deployment needs GCS or ADLS.
- A real vending catalog (Polaris) becomes a supported target: add it to CI.
- Credential rotation frequency makes registry growth visible in memory.
- Queries routinely outlive vended token lifetimes.
