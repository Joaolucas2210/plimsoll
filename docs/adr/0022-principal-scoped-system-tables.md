# ADR 0022 — Principal-scoped system tables

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0021 (authentication; this resolves its first authorization debt), ADR 0002 (planes), ADR 0004
- Scope: `plimsoll_core::engine::Engine::sql_with`, `plimsoll-server` (`auth::{Auth, Viewer}`, `system::QueriesTable`, `engine::QueryEngine`, `app::sql`)

## Context

ADR 0021 made `user` trustworthy. Authorization did not follow it.
`system.runtime.queries` returned every query to every authenticated caller,
including SQL text and error messages. SQL text routinely carries business
data: literal customer ids, emails in `WHERE` clauses, sometimes secrets
pasted into queries. Anyone with any token could read everyone's.

The system table is a DataFusion `TableProvider` registered once in the
process-wide `SessionContext`. Its `scan` had no idea who was asking: the
engine ran SQL with no caller context at all.

## Decision drivers

1. Least privilege: a user sees their own queries. Seeing others' is an
   explicit grant.
2. Fail closed: a code path that forgets to pass the caller must return an
   error, never everything.
3. The core engine stays ignorant of auth semantics (ADR 0002 layering).
   It may carry opaque context, but it must not interpret it.
4. No per-user `SessionContext` or catalog duplication; catalogs, runtime,
   pool and caches stay shared.
5. Misconfiguration (a typo in an operator name) must fail boot, not silently
   demote someone.

## Considered options

**A. Per-query session config extension (chosen).**
`Engine::sql_with(sql, Arc<T>)` clones the `SessionState`, sets the
extension and runs the query. The clone is cheap: the catalog list, runtime
and pool are `Arc`s. Providers read `state.config().get_extension::<Viewer>()`.
The core passes an opaque type. Only the server and its provider know
`Viewer`.

**B. A view per user (`CREATE VIEW … WHERE user = '…'`).** This mixes
identity into SQL and the catalog, means one view per principal, and the
base table stays readable.

**C. Filter in the HTTP layer (rewrite SQL or post-filter results).**
Rewriting SQL is fragile. Post-filtering breaks aggregates
(`count(*)` would count rows the caller cannot see).

**D. One `SessionContext` per principal.** It duplicates catalog
registration and state per user, and the lakehouse catalog would need
registering N times.

## Decision

- `Viewer { only: Option<String> }`.
  - `Some(user)`: rows are restricted to that user.
  - `None`: all rows. This applies to operators, and to everyone when
    `PLIMSOLL_AUTH=disabled` (development).
- `PLIMSOLL_AUTH_OPERATORS=ops,…` lists users who see all queries.
  - Every name must be a user in `PLIMSOLL_AUTH_TOKENS`, otherwise boot
    fails (typo protection).
  - Operators with `PLIMSOLL_AUTH=disabled` also fail boot, since the
    setting would be meaningless.
  - The boot line logs users and operators, never digests.
- `QueryEngine::execute(sql, viewer)`. The handler derives the viewer from
  the authenticated principal. `CoreEngine` calls
  `Engine::sql_with(sql, Arc<Viewer>)`.
- `QueriesTable::scan` requires a viewer. When it is missing, the scan fails
  with `Plan` error "system.runtime.queries needs the caller's identity",
  which reaches the client as `invalid_query`. It never falls back to all
  rows.
- Filtering happens in the provider, before any operator sees the rows. So
  `count(*)`, `GROUP BY` and joins only ever see permitted rows.

## Consequences

**Positive**
- Query text is no longer visible across users.
- The mechanism is general. Any future provider that depends on the caller
  (per-user row filters, column masking, audit) reads the same extension.
- The core API addition is generic (`sql_with<T>`). The core still knows
  nothing about authentication.

**Negative (accepted debt)**
- One `SessionState` clone per query: config and registry maps, a few
  microseconds; negligible next to planning. If it ever shows in profiles,
  share an `Arc<SessionState>` and override only the config.
- Visibility is binary (own vs all). There are no team scopes or
  per-namespace grants.
- `Engine::sql` (no viewer) remains for internal callers. Any of them that
  touches the system table gets the fail-closed error, by design.
- Data tables (bronze/silver/gold) have no authorization yet. Any principal
  can read any table. That belongs with resource groups and namespace grants
  (ROADMAP: per-layer governance).

**Operational**
- Grant operator rights to on-call or service accounts through
  `PLIMSOLL_AUTH_OPERATORS`. Everyone else self-serves their own query
  history.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A new code path runs SQL without a viewer | Medium | Low (error) | Fail closed in the provider; test asserts the error. |
| A new system table forgets to filter | Medium | High (leak) | Convention: providers that expose per-user data must read `Viewer`; review checklist; this ADR. |
| Operator typo silently demotes on-call | Low | Medium | Boot fails on unknown operator names. |
| Aggregates leak counts of hidden rows | Low | Medium | Filtering in `scan`, below all operators. |

## Validation

- `auth::tests::operators_see_all_queries_others_only_their_own`:
  - viewers for an operator, a user and disabled auth;
  - unknown operator fails boot;
  - operators without auth fail boot.
- `app::tests::system_queries_are_scoped_to_the_principal`, with the real
  engine, three principals and HTTP with bearer tokens:
  - Ana sees `[ana]`;
  - Bob sees `[bob]`;
  - the operator sees `[ana, bob, ops]`;
  - `Engine::sql` without a viewer fails with the identity error.
- Existing tests: the system table test (OOM visible to its own user) and
  the engine tests pass unchanged.

## Revisit when

- Teams or tenants need shared visibility (group-scoped viewers).
- Data-level authorization (namespace or table grants, row filters) is
  needed: reuse the same extension.
- Profiles show the per-query state clone.
