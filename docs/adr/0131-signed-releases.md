# ADR 0131 — Signed releases, with SBOM and provenance

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0125 (releases from a tag), ADR 0011 (CI budget)
- Scope: `.github/workflows/release.yml`

## Context

Releases (ADR 0125) published an image and a checksummed archive. A
checksum shows that a download is intact. It does not show that this
repository's workflow built it: anyone who can replace the archive can
replace its `.sha256` too. A lakehouse engine handles credentials and
data, so whoever deploys it should be able to verify where it came
from.

Reviewing the workflow before its first run also showed that its image
name could never have been pushed. `ghcr.io/${{ github.repository }}`
is `ghcr.io/Joaolucas2210/plimsoll`, and registries refuse uppercase
names.

## Decision

- **Keyless signatures (Sigstore cosign).** The job's GitHub OIDC
  identity (`id-token: write`) signs:
  - the pushed image, by digest;
  - each release archive, as a `.sigstore.json` bundle attached to the
    release.
  There is no key to keep or rotate. The certificate names this
  repository's `release` workflow and the tag, and Sigstore's
  transparency log records it.
- **SBOM and build provenance** are attached to the image by BuildKit
  (`sbom: true`, `provenance: mode=max`).
- **The image name is lowercased once** (`IMAGE`, from
  `GITHUB_REPOSITORY`) and used for the tags, the binary extraction and
  the signature.

Verification, for whoever deploys:

```bash
cosign verify ghcr.io/joaolucas2210/plimsoll:v0.1.0 \
  --certificate-identity-regexp 'https://github.com/Joaolucas2210/plimsoll/.github/workflows/release.yml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
cosign verify-blob plimsoll-server-v0.1.0-x86_64-linux-gnu.tar.gz \
  --bundle plimsoll-server-v0.1.0-x86_64-linux-gnu.tar.gz.sigstore.json \
  --certificate-identity-regexp 'https://github.com/Joaolucas2210/plimsoll/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Consequences

**Positive**
- Images and archives can be verified as built by this workflow from a
  tag. The SBOM lists what is inside.
- The first release can push its image at all.
- Releases still cost nothing between tags.

**Negative (accepted debt)**
- **Still x86_64 only.** In a private repository, arm64 means QEMU
  emulation (most of an hour per release) or paid arm runners. Add a
  native arm64 job when someone runs on arm.
- **Not exercised until the first tag.** The workflow parses, the
  lowercase name was checked in a shell, and the steps are the actions'
  documented ones. The `v0.1.0` tag is their first run.

## Validation

- `release.yml` parses as YAML.
- `${GITHUB_REPOSITORY,,}` for `Joaolucas2210/plimsoll` gives
  `ghcr.io/joaolucas2210/plimsoll` (checked in bash).
- No `github.repository` remains in an image reference.
