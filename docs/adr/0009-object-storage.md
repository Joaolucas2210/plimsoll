# ADR 0009 — Object storage

- Status: Accepted
- Scope: `plimsoll-core::storage`, the engine runtime

## Context

Lakehouse tables live in object storage. Iceberg paths are full URIs
(`s3://bucket/...`) and a table's metadata, manifests and data may all be
remote. DataFusion resolves stores through an `ObjectStoreRegistry`; by
default only `file://` is registered.

## Decision

- **Resolve stores on first use.** `LazyObjectStores` wraps DataFusion's
  registry: a miss on `s3://` / `s3a://` builds an `AmazonS3Builder::from_env()`
  store for that bucket and caches it. Every engine session gets it through
  `memory::runtime_env`, so the read path (metadata, Avro manifests, Parquet)
  is unchanged and stays under the `CeilingPool` (ADR 0003, 0007).
- **Configuration is the standard AWS env** (`AWS_ENDPOINT`, `AWS_REGION`,
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
  `AWS_ALLOW_HTTP`), falling back to the instance/pod role. No new Plimsoll
  variables, no secrets in files.
- **Dependency:** `object_store` 0.13 (the version DataFusion already uses) with
  the `aws` feature.

## Alternatives

- **Register stores from config at boot:** needs the bucket list up front;
  catalogs can point anywhere.
- **Catalog-vended credentials** (`X-Iceberg-Access-Delegation`): the right
  end state for Polaris (per-table, short-lived), but credentials are scoped
  per table while DataFusion keys stores by bucket. Deferred; see below.

## Consequences

- Any S3-compatible store works; CI runs the REST catalog on SeaweedFS
  (the MinIO community images are no longer published).
- Debt: GCS/ADLS (`gcp` / `azure` features, same few lines); vended
  credentials, which need per-table store keys (e.g. a synthetic
  `ObjectStoreUrl` per table) and refresh before expiry.
