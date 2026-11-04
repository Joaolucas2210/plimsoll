# ADR 0132 — arm64 releases, cross-compiled

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0011 (CI budget), ADR 0123 (image), ADR 0125 (releases), ADR 0131 (signing)
- Scope: `.github/workflows/release.yml`, `Dockerfile.release`, `.gitignore`

## Context

Releases were x86_64 only (ADR 0125, 0131). arm64 matters for
deployments: Graviton, Ampere and Apple-silicon development machines.
The obvious way, a multi-platform `docker buildx` of the source
`Dockerfile`, compiles the whole workspace under QEMU. That takes most
of an hour of paid minutes per release in a private repository (ADR
0011). Native arm runners are paid in private repositories too.

## Decision

- **Cross-compile on the x86_64 runner.**
  - `gcc-aarch64-linux-gnu` is the linker and the C compiler ring
    needs (`CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER`,
    `CC_aarch64_unknown_linux_gnu`).
  - `libc6-dev-arm64-cross` holds arm64's libc headers. It is only a
    "recommends" of the cross gcc, so `--no-install-recommends` drops
    it. Without it, ring's C picked up the host's `/usr/include` and
    failed (`bits/libc-header-start.h: No such file or directory`), as
    the first dry run showed.
  - One `cargo build --release --locked` per target, at native speed.
- **Pack, don't build, the image.** `Dockerfile.release` is the
  runtime stage of `Dockerfile` (Debian slim, CA roots, uid 1000, the
  same environment and ports). It copies `dist/${TARGETARCH}/
  plimsoll-server`.
  - `buildx` builds it for `linux/amd64,linux/arm64`. QEMU only runs the
    arm64 stage's `apt-get`: seconds, not a compile.
  - The image is a multi-platform manifest, signed by digest with its
    SBOM and provenance (ADR 0131).
- **Two archives per release** (`…-x86_64-linux-gnu.tar.gz`,
  `…-aarch64-linux-gnu.tar.gz`). Each is checksummed and signed, and
  holds the same binary as the image for its platform.
- **A dry run before the first tag.** `workflow_dispatch` builds
  without logging in, pushing, signing or releasing (`PUBLISH` is true
  only for `v*` tags). Its tag is `dry-run`.
  - It compiles **only aarch64** and packs only `linux/arm64`. The
    x86_64 build is what the normal CI already proves, so the dry run
    spends minutes only on what nothing else checks.
  - A tag builds both (`TARGETS`, `PLATFORMS`, set by the first step).
- The source `Dockerfile` stays as it was, for the quickstart (ADR
  0123): it builds from source on whatever machine runs it.

## Consequences

**Positive**
- `docker pull` works on arm64 hosts, and arm64 archives come with
  every release, for roughly the cost of one more native compile.
- The release job can be tried end to end, on demand, before it
  publishes anything.

**Negative (accepted debt)**
- **The arm64 binary is never executed in CI**: it is built, not
  tested. The workspace tests run on x86_64 only (ADR 0011). A smoke
  run under QEMU (`--version`, `/readyz`) is the cheap next step if a
  platform-specific bug ever shows up.
- **Two Dockerfiles share a runtime stage**, kept the same by hand (a
  dozen lines).

## Validation

- `release.yml` parses as YAML.
- Locally, with an x86_64 binary in `dist/amd64` and `dist/arm64`:
  - the archive step's loop, as written, produces both archives, each
    holding `plimsoll-server`, `LICENSE` and `README.md`;
  - `Dockerfile.release` builds for `linux/amd64`; the container runs
    as uid 1000 and answers `200` on `/readyz`.
- Not run locally: the aarch64 cross-compile (no cross toolchain or
  root on the development machine). The `workflow_dispatch` dry run is
  its check, and it runs before any tag.
  - The first dry run failed on ring's C (missing arm64 libc headers);
    `libc6-dev-arm64-cross` fixes it.
  - The build and archive loops were run locally, with stand-in
    binaries, for both target lists: the dry run makes only the
    aarch64 archive, a tag makes both.
