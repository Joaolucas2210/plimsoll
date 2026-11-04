# ADR 0123 — Container image and quickstart

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0021 (tokens), ADR 0058 (workers), ADR 0077 (write grants), ADR 0015 (REST catalog)
- Scope: `Dockerfile`, `.dockerignore`, `compose.yaml`, README "Quickstart"

## Context

Running Plimsoll meant building it with Cargo, then standing up an
Iceberg REST catalog and storage by hand. A cluster also needed workers
pointed at the same warehouse, a shared worker token, and namespaces
created in the catalog. That knowledge lived only in the test
fixtures. Nobody could try the engine in minutes, and nothing could be
deployed without writing an image first.

## Decision drivers

1. **One artifact for every role.** Coordinator and worker are the same
   binary with different environment (ADR 0058), so they get the same
   image.
2. **Secrets stay in the environment** and are never baked into the
   image or the compose file.
3. **Small and boring.** No new tooling, no extra services beyond what a
   lakehouse needs.

## Decision

- **`Dockerfile`**, in two stages:
  - The build stage runs `cargo build --release --locked -p
    plimsoll-server` on the official Rust image. BuildKit cache mounts
    keep the registry and `target/` across builds.
  - The runtime stage is `debian:bookworm-slim`, with `ca-certificates`
    for https catalogs, object stores and collectors. It runs as an
    unprivileged `plimsoll` user (uid 1000).
  - `PLIMSOLL_BIND=0.0.0.0:8080` is set, since the binary's default is
    loopback.
- **`compose.yaml`**, the quickstart:
  - the reference Iceberg REST catalog (`apache/iceberg-rest-fixture`,
    the one interop tests use), with its JDBC (SQLite) catalog and a
    `file://` warehouse on a named volume;
  - two one-shot services: one gives the volume to uid 1000, shared by
    the catalog and the nodes; the other creates the `bronze`, `silver`
    and `gold` namespaces;
  - two workers and a coordinator from the same image. The coordinator
    lists them in `PLIMSOLL_WORKERS`, is published on `127.0.0.1:8080`
    only, and starts once both workers' `/readyz` answer;
  - `PLIMSOLL_AUTH_TOKENS` and `PLIMSOLL_WORKER_TOKEN` are required from
    the caller's environment (`${VAR:?}`): compose refuses to start
    without them, as the binary would;
  - the quickstart user `ana` may write and drop tables in the three
    namespaces.
- **README "Quickstart"** generates the tokens, starts the stack,
  creates a table, inserts rows, queries them, and tears down.

## Consequences

**Positive**
- `docker compose up` gives a distributed, writable lakehouse in
  minutes, on the same catalog as the interop tests.
- The image is what deployments (Kubernetes, ECS, Nomad) run, with
  configuration unchanged: environment only.

**Negative (accepted debt)**
- **No published image or release yet.** Users build locally. A
  registry push belongs with the release job.
- **The quickstart is not production.** Plain HTTP, a file warehouse on
  one host, and the catalog's SQLite. A production stack uses S3 (the
  `AWS_*` variables) and a real catalog (Polaris, `PLIMSOLL_CATALOG_*`).
- **No Helm chart or manifests.** The image and `dns+http://` worker
  discovery (ADR 0070) are what one needs.

## Validation

- `docker compose up -d --build` from a clean volume: every service
  healthy or completed.
- The README's steps run as written: `CREATE TABLE`, `INSERT`, an
  aggregate `SELECT`, and a coordinator `/metrics` scrape.
- A second `up` on the same volume keeps tables and namespaces.

## Revisit when

- Releases exist: publish the image (multi-arch) from the release job.
- Someone deploys on Kubernetes: a minimal manifest or chart, with a
  headless service for workers.
