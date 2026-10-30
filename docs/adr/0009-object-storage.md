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

- **Catalog-vended credentials.** With `PLIMSOLL_CATALOG_VENDED_CREDENTIALS=true`
  the REST client sends `X-Iceberg-Access-Delegation: vended-credentials`;
  the response `config` plus the longest-prefix `storage-credentials` entry
  become the table's storage properties. `scoped_s3_store` turns them into a
  `ScopedStore` registered under a synthetic URL (`s3-<hash>://bucket`) keyed
  by those credentials; the table's metadata, manifests and Parquet reads in
  that bucket go through it. Credentials are refreshed for free: every query
  calls `loadTable` (ADR 0008).

## Alternatives

- **Register stores from config at boot:** needs the bucket list up front;
  catalogs can point anywhere.
- **Replace the bucket's store with vended credentials:** one table's
  short-lived, path-scoped keys would leak into every other table in the
  bucket and break them.

## Consequences

- Any S3-compatible store works; CI runs the REST catalog on SeaweedFS
  (the MinIO community images are no longer published).
- CI enforces credentials on S3: env keys are wrong on purpose and the read
  only succeeds through the vended ones.
- Debt: GCS/ADLS (`gcp` / `azure` features, same few lines); expired
  scoped stores stay in the registry until restart (small, bounded by
  credential rotations); a single query outliving its session token fails.
