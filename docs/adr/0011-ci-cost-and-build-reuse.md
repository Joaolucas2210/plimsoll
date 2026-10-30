# ADR 0011 — CI cost: one build, one job, one cache

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0001 (DataFusion build weight), ADR 0007–0010 (live integration tests)
- Scope: `.github/workflows/ci.yml`, `scripts/gates.sh`

## Context

The repository is private, so GitHub Actions minutes are billed, per job,
rounded up to the whole minute. Measured on runs `41053809773` and
`41056075914` (2026-10-30):

| Job | Warm cache | Cold cache |
|---|---|---|
| gates (fmt, clippy, test, arch) | 2.1 min | 10.0 min |
| postgres federation (live) | 1.3 min | 6.2 min |
| iceberg interop (REST, S3) | 5.1 min | 5.2 min |
| **Billed per push** | **~10 min** | **~23 min** |

Root causes:

1. **Three jobs, three builds.** Each job compiled DataFusion (~400 crates)
   separately and kept its own `rust-cache` entry per `Cargo.lock` hash: 11
   entries, 5.8 GB of the 10 GB cache quota, mostly dead keys.
2. **Feature-resolution recompiles.** `cargo test -p plimsoll-server` after
   `cargo test -p plimsoll-connectors` resolves a different feature set, so
   the vended-credentials step spent **180 s** compiling.
3. **Docs pushes ran everything.** An ADR-only commit cost a full run.
4. **Redundant interop.** pyiceberg ran against a local warehouse *and* an S3
   one; both exercise the same reader.

## Decision drivers

1. Billed minutes per push (the user-facing cost).
2. Keep the same coverage: gates + every live integration test.
3. Wall-clock feedback under ~5 min warm.
4. No new tools or services to maintain.

## Considered options

**A. One job; build once with `--workspace`; live tests by name filter (chosen).**
Postgres is a job service; SeaweedFS + the REST fixture start in the first
step and warm up while Rust compiles. `scripts/gates.sh` builds every test
binary; live tests run as `cargo test --workspace -- --ignored <names>`, which
reuses the same artifacts (verified: 0 crates recompiled). Pros: one billed
job, one cache. Cons: jobs no longer fail independently; wall clock is the sum
of steps (mitigated by overlapping container start-up with compilation).

**B. Build job + `cargo nextest archive` consumed by parallel jobs.** Keeps
parallelism, but every extra job still pays checkout, toolchain, cache restore
and the per-job minute rounding. Cheaper wall clock, more billed minutes.

**C. sccache with a remote backend.** Shares compilation across jobs, but adds
a service, credentials and its own failure modes for a single-repo project.

**D. Make the repository public.** Actions are free for public repositories.
Outside this ADR's scope (a product decision, not CI design); noted for the
owner.

## Decision

- Single job `gates + live integration`, `timeout-minutes: 25` as a cost cap.
- `Swatinem/rust-cache` with `shared-key: plimsoll` and
  `save-if: refs/heads/main` (PRs restore main's cache, never write one).
- CI-only `CARGO_PROFILE_{DEV,TEST}_DEBUG=0` and `CARGO_INCREMENTAL=0`:
  smaller artifacts, faster links, smaller cache.
- `paths-ignore` for `**/*.md`, `docs/**`, `LICENSE` on push and PR;
  `workflow_dispatch` for manual runs.
- Live tests always invoked with `--workspace` (never `-p`) so they reuse the
  gates build; the step asserts the fixture env exists so ignored tests cannot
  pass by returning early.
- The local-warehouse pyiceberg run is dropped; the S3 run (metadata written
  by the Java REST fixture, data by pyiceberg) covers more.
- Container logs printed only on failure.
- Stale per-job caches deleted once.

## Consequences

**Positive**: expected ~3–4 billed minutes per code push warm (one job,
build once), zero for docs-only pushes; one cache entry per lockfile instead
of three.

**Negative**: one failing live test fails the whole run; less parallelism, so
cold-cache wall clock is close to a single full build (~10 min).

**Operational**: a cold cache (new `Cargo.lock`) costs one full build once;
the next push is warm. If a run needs re-running for docs, use
`workflow_dispatch`.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A docs-only change breaks something CI would catch (e.g. a doc-test) | Low | Low | No doc-tests exist; revisit if added. |
| A future `-p` invocation reintroduces recompiles | Medium | Medium (minutes) | Rule written in the workflow comment and here; review checks it. |
| Cache churn from frequent lockfile changes | Low | Medium | One shared key; only `main` saves. |
| Services slow to start make the job flaky | Low | Low | Explicit readiness loops (bucket creation, `/v1/config`), logs on failure. |

## Validation

- Locally: after `cargo test --workspace`, the live-test command compiles 0
  crates and selects exactly `pg_live_roundtrip`, `live_rest_catalog`,
  `reads_a_pyiceberg_table`, `prunes_a_partitioned_pyiceberg_table`,
  `reads_with_catalog_vended_credentials`.
- In CI: job duration and billed minutes compared against the table above.

## Revisit when

- Billed minutes per push exceed ~6 warm, or cold builds become frequent.
- The test suite needs parallelism to stay under ~10 min (then option B).
- The repository becomes public (minutes stop being the constraint).
- Doc-tests or docs tooling are added (narrow `paths-ignore`).
