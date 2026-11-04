# ADR 0125 — Releases from a tag

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0011 (CI budget), ADR 0123 (image), ADR 0124 (Flight SQL)
- Scope: `.github/workflows/release.yml`, `CHANGELOG.md`

## Context

The image (ADR 0123) existed only where someone built it. There was no
versioned artifact to deploy or pin, and no record of what changed
between versions. An MVP needs both. CI minutes are paid (ADR 0011), so
releasing must cost nothing between releases.

## Decision

- **A `v*` tag runs `release.yml`.** Nothing else triggers it.
- **The tag is checked before anything is built:**
  - it must equal the workspace `version` in `Cargo.toml`;
  - `CHANGELOG.md` must have a non-empty `## [x.y.z]` section, which
    becomes the release notes.
- **One build.** The image is built from the repository's `Dockerfile`
  and pushed to `ghcr.io/<owner>/<repo>`, as the tag and as `latest`.
- **The binary is copied out of that image**, so the release asset and
  the image hold the same bytes. It is packed with `LICENSE` and
  `README.md` as `plimsoll-server-<tag>-x86_64-linux-gnu.tar.gz`, with
  a `.sha256`.
- **A GitHub release** for the tag carries the archive, its checksum
  and the changelog section.
- **`CHANGELOG.md`** follows Keep a Changelog and SemVer. Before 1.0, a
  minor version may break the HTTP API, Flight SQL or environment
  variables, and says so.

## Consequences

**Positive**
- `docker pull ghcr.io/…:v0.1.0` or a checksummed binary: a pinnable,
  reproducible deployment unit.
- A release fails fast on a forgotten version bump or changelog entry,
  before spending a build.
- Zero cost between releases.

**Negative (accepted debt)**
- **x86_64 only.** An arm64 image built under QEMU would take most of
  an hour of paid minutes; add a native arm64 runner when someone needs
  it.
- **No signing or SBOM** (cosign, provenance). Add when distributing
  beyond the team.
- **Gates are not re-run on the tag.** A tag is cut from a `main`
  commit whose CI was green; a check of that commit's status is the
  upgrade path.

## Validation

- The workflow parses as YAML. Its version and changelog checks were
  run locally against this repository: `0.1.0` matches, and the
  `[0.1.0]` section is extracted.
- The image build is the quickstart's (ADR 0123), already built and run
  locally.
- The first `v0.1.0` tag exercises the publishing steps.

## Revisit when

- arm64 users appear, or releases need signatures.
