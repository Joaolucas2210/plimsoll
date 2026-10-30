# ADR 0021 — Bearer-token authentication for the SQL endpoint

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0004 (resolves its `user`-trusted debt), ADR 0013 (error contract), ADR 0015 (`ring` already in the build)
- Scope: `plimsoll-server` (`auth`, `config`, `app::sql`, `main`), `README.md`

## Context

`POST /v1/sql` took `user` from the request body and trusted it. Everything
keyed on the user depended on that: per-user admission limits,
`system.runtime.queries`, any future authorization. Any client could claim
any user, which turned per-user limits into a suggestion and made the
registry's `user` column fiction. ADR 0004 recorded this as a `ponytail:`
with "authenticate before multi-tenant use" as the upgrade path.

Constraints from the project brief: secrets come only from env, and config
fails fast.

## Decision drivers

1. Identity is derived from a credential, never asserted by the client.
2. Secure by default: a misconfigured deployment refuses to start rather
   than running open.
3. Secrets at rest are not usable credentials. Env is visible through
   `docker inspect`, `/proc/<pid>/environ`, CI logs and crash reports.
4. No new dependency, and nothing heavy (no OIDC stack) until there is an
   identity provider to talk to.
5. Typed errors in the ADR 0013 contract, counted in metrics.

## Considered options

**A. Static bearer tokens, stored as SHA-256 digests (chosen).**
`PLIMSOLL_AUTH_TOKENS=ana:sha256:<hex>,…`. The server hashes the presented
token and looks the digest up.

Why the lookup is safe without a constant-time comparison: the attacker
controls the input, but what gets compared is the SHA-256 of that input.
Learning byte-by-byte timing about a digest does not help forge a preimage.

Why plain SHA-256 and not a password hash (Argon2/bcrypt): tokens are
generated with high entropy (`openssl rand -hex 32`), so brute-forcing a
digest is infeasible. A slow hash would only add latency per request.

**B. Raw tokens in env.** Simpler, but an env dump hands out working
credentials. Rejected (driver 3).

**C. OIDC/JWT validation (JWKS fetch, `exp`/`aud` checks).** This is the
right model with an identity provider. It needs a JWT crate, key rotation
and clock-skew handling. It is the upgrade path, and the `Auth` enum is
where it goes.

**D. mTLS.** This means certificate distribution and a TLS terminator in
process. It is better done at a proxy or mesh, which can forward a verified
identity. Revisit with a deployment that has a mesh.

**E. Keep trusting `user`, rely on a gateway.** This makes every deployment
depend on correct gateway configuration, with no defence in depth. Rejected.

## Decision

- `PLIMSOLL_AUTH` is `required` (the default) or `disabled`. When required,
  `PLIMSOLL_AUTH_TOKENS` must hold at least one `user:sha256:<64 hex>` entry,
  otherwise boot fails with
  `invalid PLIMSOLL_AUTH_TOKENS="": at least one token is required (or set
  PLIMSOLL_AUTH=disabled)`.
- Malformed entries, an empty user, or two users sharing a token fail boot.
  The error value is `<redacted>`, so a raw token pasted by mistake never
  reaches the logs.
- `/v1/sql` authenticates **first**, before readiness or body validation, so
  an anonymous caller learns nothing.
  - Missing token or non-Bearer scheme: `401 unauthorized` with
    `WWW-Authenticate: Bearer`.
  - Unknown token: `401` as well.
- `user` = principal. A body `user` that differs is refused with
  `403 forbidden`; omitting it is allowed.
- With `disabled`, behaviour is the previous one, plus a boot warning.
- Digests use `ring::digest` (SHA-256). `ring` is already compiled as the
  rustls provider (ADR 0015), so no crate is added.
- `Auth`'s `Debug` lists users only. The boot log shows
  `auth: Tokens(["ana", …])` or the warning.
- `/livez`, `/readyz`, `/healthz` and `/metrics` stay unauthenticated. They
  are probe and scrape endpoints, and they expose no data or SQL.

## Consequences

**Positive**
- Per-user admission limits and the registry's `user` are now
  trustworthy.
- `plimsoll_query_errors_total{code="unauthorized"|"forbidden"}` shows
  credential problems separately from query errors.
- Rotation is "add the new digest, deploy, remove the old one". Two tokens
  per user are allowed because only digests must be unique.

**Negative (accepted debt)**
- Static tokens. There is no expiry, and revocation needs a restart. That
  is acceptable for service accounts; human users need OIDC (option C).
- **Authorization is not done yet.** Any authenticated principal can:
  - read every user's SQL through `system.runtime.queries`;
  - pick any `resource_group`.

  These are the next ROADMAP item.
- Tokens travel in clear unless TLS is terminated in front of Plimsoll (the
  server speaks plain HTTP, ADR 0004). Deployments must terminate TLS at the
  ingress.
- Breaking change for local runs: `cargo run -p plimsoll-server` without
  tokens now needs `PLIMSOLL_AUTH=disabled`. The README is updated.

**Operational**
- Generate:
  `TOKEN=$(openssl rand -hex 32); printf %s "$TOKEN" | sha256sum`.
- A spike in `unauthorized` errors after a deploy usually means a rotated
  token was not distributed.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Deployment runs with auth disabled in production | Low | High | Off only by explicit `PLIMSOLL_AUTH=disabled`; loud boot warning; the auth mode appears in the boot line. |
| Raw token placed in `PLIMSOLL_AUTH_TOKENS` | Medium | Medium (secret in env and logs) | Rejected at boot (not a digest) with a redacted error. |
| Low-entropy tokens brute-forced from leaked digests | Low | High | Docs generate 256-bit tokens; revisit with a slow hash if humans choose tokens. |
| Token sniffed over plain HTTP | Medium without TLS | High | TLS at the ingress (documented); the bind default is `127.0.0.1`. |

## Validation

- `auth::tests`:
  - the digest matches `sha256sum`;
  - secure by default;
  - `disabled` is honoured, unknown modes are rejected;
  - a raw token is not echoed;
  - duplicate tokens are rejected;
  - `Debug` shows users only;
  - Bearer parsing (case, spacing, other schemes, empty token, unknown
    token).
- `config::tests::auth_is_required_by_default`.
- `app::tests::sql_requires_a_bearer_token_and_runs_as_its_principal`:
  - 401 with challenge for a missing or unknown token;
  - 403 on impersonation;
  - 200 with `user` omitted or matching;
  - the registry records the principal;
  - error metrics are counted.
- Manual smoke on the binary: boot without tokens exits 1 with the message
  above. Then no token gives 401, a valid token gives 200, and `user: "bob"`
  with Ana's token gives 403.

## Revisit when

- An identity provider exists: OIDC/JWT behind the same `Auth` enum.
- A service mesh forwards verified identities: trust its header, but only
  from the mesh.
- Multi-tenant use: authorization for system tables and resource groups
  (next ROADMAP item).
