# ADR 0015 — One HTTP/TLS stack

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0008 (refines its dependency line), ADR 0009, ADR 0011, ADR 0013
- Scope: `crates/plimsoll-control/Cargo.toml`, `Cargo.lock`

## Context

The server binary carried two HTTP clients with two TLS crypto providers:

| Caller | Client | rustls provider |
|---|---|---|
| `object_store` 0.13 (`aws`), all S3 I/O | `reqwest 0.12` (`rustls-tls-native-roots`) | `ring` |
| `plimsoll-control::rest` (ADR 0008) | `reqwest 0.13` (`rustls`) | `aws-lc-rs` |

That costs three things:

- **Build time.** `aws-lc-sys` compiles the AWS-LC C library through `cmake`;
  it is the slowest crate in a cold build and needs a C toolchain + cmake on
  every builder (CI and contributors). ADR 0011 pays cold builds on `main`.
- **Two TLS behaviours to reason about.** Different root stores
  (`rustls-native-certs` vs `rustls-platform-verifier`), different cipher
  providers, different connection pools. A TLS incident (corporate proxy CA,
  certificate rotation) could break the catalog but not storage, or the
  reverse, for reasons invisible from the config.
- **Supply-chain surface.** 15 extra crates, including the JNI shim pulled by
  the platform verifier.

rustls with both `ring` and `aws-lc-rs` features also has no unambiguous
process default provider; both reqwest versions pass providers explicitly, so
this was not a live bug, but it is a trap for any future direct rustls use.

## Decision drivers

1. One TLS configuration surface for every outbound connection.
2. Cheaper cold builds without a C/cmake toolchain requirement.
3. No behaviour change for the catalog client (timeouts, auth, API).
4. Follow `object_store`, which we cannot choose, rather than fight it.

## Considered options

**A. Catalog on reqwest 0.12 with object_store's features (chosen).** One
`reqwest`, one `ring` provider, native root certificates for both. The
catalog uses only the base API (`Client::builder`, timeouts, headers,
`bearer_auth`, `body`, `bytes`), identical across 0.12 and 0.13.

**B. Push object_store to reqwest 0.13 / aws-lc-rs.** Not ours to choose:
object_store 0.13 pins reqwest 0.12. Would need a fork or a custom
`HttpConnector`. Rejected.

**C. Accept the split.** Zero work; keeps all costs above. Rejected: the fix
is a one-line manifest change.

**D. Catalog on hyper directly.** Removes a dependency edge but re-implements
timeouts, redirects and TLS wiring that reqwest 0.12 already brings in
transitively. Rejected (ADR 0008 option analysis still holds).

## Decision

Option A: `reqwest = { version = "0.12", default-features = false, features =
["rustls-tls-native-roots"] }` in `plimsoll-control`, the same feature set
`object_store` enables. `cargo tree -d` shows a single `reqwest`; `aws-lc-rs`
is absent from `Cargo.lock`.

## Consequences

**Positive**
- Removed from `Cargo.lock`: `aws-lc-rs`, `aws-lc-sys`, `cmake`, `fs_extra`,
  `dunce`, `jni*` (4), `combine`, `simd_cesu8`, `rustls-platform-verifier*`
  (2), `webpki-root-certs`, the second `reqwest` (15 packages).
- No C toolchain/cmake needed for the TLS stack.
- Storage and catalog trust the same roots (OS store via
  `rustls-native-certs`): one place to add a corporate CA.

**Negative (accepted debt)**
- The catalog moves when object_store moves: a reqwest major bump in
  object_store means bumping the catalog in the same PR (cheap; the API used
  is minimal).
- `ring` instead of `aws-lc-rs`: no FIPS-validated provider. Acceptable until
  a FIPS requirement exists.
- Native roots only: an image without CA certificates fails TLS for both
  storage and catalog (previously the platform verifier had the same
  requirement on Linux).

**Operational**
- Container images must ship `ca-certificates` (one requirement for both).

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A future dependency re-introduces a second reqwest/provider | Medium | Low (build cost) | `cargo tree -d` review on dependency PRs; revisit trigger below. |
| reqwest 0.12 behaviour differs for the catalog | Low | Medium | Same base API; `rest::tests` (axum stub: handshake, OAuth refresh, paging, errors) and live `live_rest_catalog` in CI. |
| FIPS requirement appears | Low | Medium | Switch both via object_store's provider choice in one place. |

## Validation

- `cargo tree -i aws-lc-rs` → no match; `cargo tree -d` lists one `reqwest`.
- `scripts/gates.sh` green; `plimsoll-control` REST tests and the live REST
  catalog test in CI unchanged.

## Revisit when

- object_store moves to reqwest 0.13+ (move the catalog with it).
- A FIPS or HSM-backed TLS requirement appears.
- A dependency adds a second HTTP client (check `cargo tree -d`).
