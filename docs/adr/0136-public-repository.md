# ADR 0136 — Opening the repository: branch rules, required checks, pinned actions

- Status: Accepted
- Date: 2026-11-05
- Owner: JoaoLucas2210
- Related: ADR 0011 (amended for pull requests), ADR 0125, ADR 0131
- Scope: `.github/workflows/*.yml`, `.github/` (CODEOWNERS, dependabot, templates), `.gitleaks.toml`,
  `SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, repository rulesets and settings

## Context

The repository was private, with one maintainer pushing to `main` directly and CI sized for private minutes
(ADR 0011). Opening it changes three things. Anyone can open a pull request from a fork, and `pull_request`
workflows then run untrusted code. Every past commit becomes readable. A release (ADR 0125) is cut by pushing a
`v*` tag, so whoever can push tags can publish a signed image (ADR 0131).

Before opening, the full history (142 commits) was scanned. gitleaks found one match, a 64-hex test literal in
`plimsoll_server::flight::tests::basic_credentials_with_or_without_padding`. A grep over every revision found no
home paths, personal e-mail addresses, private hostnames or IPs, cloud keys or tokens. Every author and committer
uses the GitHub noreply address, and no blob is larger than 1 MB.

## Decision drivers

1. Untrusted pull requests run with read-only credentials and no secrets.
2. `main` and `v*` tags change only through reviewed, checked paths.
3. A docs-only pull request is never blocked by a check that cannot run.
4. Keep ADR 0011's budget: no new heavy job.

## Considered options

### Option A — Rulesets, required checks, a `changes` job, SHA-pinned actions (chosen)

Pros: drivers 1–4; rulesets cover branches and tags in one place. Cons: one extra five-minute job per pull
request, and SHA pins need Dependabot to stay current.

### Option B — Classic branch protection and tag-pinned actions

No code. Cons: classic protection does not cover tag creation. A required check with `paths-ignore` stays
pending forever on a docs-only PR. A tag such as `@v4` can be re-pointed upstream, which is how the 2025
tj-actions compromise spread.

### Option C — Stay private, publish only releases

Cons: it defeats the point of opening, and contributors could not read the code they run.

## Decision

1. **Workflows.**
   - `ci.yml` and `interop.yml` declare `permissions: contents: read`, and every `actions/checkout` sets
     `persist-credentials: false`.
   - `release.yml` keeps its job-wide write scopes. It runs only on `v*` tags and `workflow_dispatch`, never on
     a pull request.
   - Every `uses:` is pinned to a full commit SHA with the version as a comment.
   - `dtolnay/rust-toolchain` gets an explicit `toolchain: stable`, because a SHA ref no longer names it.
2. **Required checks** on `main` are `gates + live integration` and `secret scan`.
   - On `pull_request` the workflow has no `paths-ignore`. A `changes` job diffs the PR against its base and sets
     `code=false` when only `*.md`, `docs/**` or `LICENSE` changed. `gates + live integration` then is skipped
     by its `if:`, and GitHub reports a skipped required job as passing.
   - Pushes to `main` keep `paths-ignore` (ADR 0011).
   - `secret scan` runs gitleaks 8.18.4, pinned by digest, over the full history with `.gitleaks.toml`. The only
     allowlist entry is the exact test literal above.
3. **Branch ruleset `main`:**
   - no deletion, no force push, linear history;
   - pull request required, merged by **squash only**, conversations resolved, stale approvals dismissed;
   - zero required approvals, because there is a single maintainer;
   - repository admins may bypass only through a pull request.
4. **Tag ruleset `v*`:** only admins create, update or delete release tags.
5. **Repository settings:** squash merge only, with the PR title and body as the commit message; head branches
   are deleted after merge; wiki and projects off; private vulnerability reporting, secret scanning with push
   protection, Dependabot alerts and security updates on.
6. **Community files:** `SECURITY.md` (the node dying, governance bypass and injection are in scope),
   `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md` (Contributor Covenant 2.1), CODEOWNERS, issue and PR templates,
   weekly grouped Dependabot for cargo, actions and Docker.

Option A beats B because only rulesets protect tags, and the release is a tag.

## Consequences

**Positive.**
- Fork PRs cannot write, publish or read secrets.
- Every change on `main` is one squashed commit that passed the gates and the demo.
- A re-pointed upstream action tag no longer changes what CI runs.

**Negative (accepted debt).**
- The `changes` job adds about one minute to every pull request.
- Zero required approvals means review is a habit, not a gate, until a second maintainer exists.
- Dependabot PRs are needed to move SHA pins.
- The container package `ghcr.io/joaolucas2210/plimsoll` has its own visibility, which the API cannot change
  for user-owned packages. It is made public in the package settings by hand.

**Operational.**
- A blocked merge shows the missing check by name.
- A docs-only PR shows `gates + live integration` as *skipped* (passing).
- A pushed secret fails `secret scan` with a redacted finding.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A required check is renamed and silently stops gating | Low | High | The job names are cited here and in CONTRIBUTING; a rename needs a ruleset change in the same PR |
| `changes` misclassifies a code change as docs-only | Low | Medium | The pattern is narrow (`*.md`, `docs/**`, `LICENSE`); pushes to `main` still run the full CI |
| A malicious fork PR edits the workflow | Medium | Low | `pull_request` runs the PR's workflow with a read-only token and no secrets; `release.yml` never runs on PRs |

## Validation

- `.github/workflows/ci.yml` jobs `changes`, `secret-scan` and `ci`.
- `gitleaks detect --log-opts=--all` with `.gitleaks.toml`: no leaks.
- actionlint 1.7.7 over `.github/workflows/`.
- The rulesets and settings are read back with `gh api repos/Joaolucas2210/plimsoll/rulesets`.

## Revisit when

- A second maintainer joins: require one approval and code-owner review.
- GitHub allows changing a user-owned package's visibility through the API.
- Release tags need signed commits or signed tags.
