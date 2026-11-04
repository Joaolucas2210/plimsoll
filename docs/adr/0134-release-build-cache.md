# ADR 0134 — A build cache for releases

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0011 (CI budget), ADR 0125 (releases), ADR 0132 (arm64)
- Scope: `.github/workflows/release.yml`

## Context

The release job compiled the whole workspace in release mode from
scratch. The first successful arm64 dry run (ADR 0132) spent 33 of its
37 minutes in `cargo build`, for one target. A tag builds two, so it
would take about an hour of paid minutes (ADR 0011). The `ci` workflow
has used `Swatinem/rust-cache` since ADR 0011; the release job had no
cache.

## Decision

- **`Swatinem/rust-cache@v2`** in the release job, after the targets
  are installed, with its own key (`shared-key: release`). Release
  artifacts never mix with `ci`'s debug ones.
- **Saved only from `main`**, that is, by the `workflow_dispatch` dry
  run. GitHub scopes caches by ref: a tag's run can restore the default
  branch's caches, but what a tag saves is visible to that tag alone.
  Saving from tags would only spend storage.
- What the cache holds is what the dry run builds: the aarch64 target's
  dependencies, plus the host's build scripts and proc-macros.

## Consequences

**Positive**
- A dry run after the first one rebuilds only the workspace's own
  crates: minutes instead of half an hour.
- A tag restores the aarch64 dependencies. Only x86_64 compiles cold,
  roughly halving the release.

**Negative (accepted debt)**
- **x86_64 release dependencies are never cached**: the dry run does not
  build them (ADR 0132). Building both in the dry run would warm them,
  at the cost of every dry run. Worth it only if releases become
  frequent.
- **The cache counts against the repository's 10 GB**, next to `ci`'s.
  rust-cache keeps dependencies only, not the workspace's crates.
- **A new `Cargo.lock` or toolchain is a new key**: the next dry run
  after a dependency bump is cold again.

## Validation

- `release.yml` parses as YAML.
- The step matches the `ci` workflow's use of the same action, which
  has run since ADR 0011.
- The next dry run saves the cache; the one after it restores it.
