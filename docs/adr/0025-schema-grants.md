# ADR 0025 — Per-schema data grants

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0022 (the `Viewer` extension), ADR 0021 (principals), ADR 0024 (read-only API, a prerequisite), ADR 0013 (error classes), ADR 0002 (layering)
- Scope: `plimsoll_core::engine` (`TableAccess`, `AccessDenied`, `Engine::sql_with`), `plimsoll-server` (`auth::{Auth, Viewer}`, `engine::classify`, `app::ApiError`, `main`)

## Context

After ADRs 0021–0024, Plimsoll knew who was asking and stopped callers from
changing the catalog. It still let every principal read every table. The
medallion layout makes this matter:

- `bronze` holds raw, often personal data;
- `gold` holds curated aggregates;
- the typical policy is "analysts read gold and silver, pipelines read
  everything".

Lakehouse catalog namespaces are registered as schemas in the default
catalog (`lakehouse::register`). So "which schemas may this principal read"
covers medallion schemas, env tables and catalog namespaces with one
concept.

## Decision drivers

1. Enforce on what the query **reads**, not on what the SQL text looks
   like. CTEs, aliases, joins, `IN (subquery)` and scalar subqueries must
   not bypass it.
2. Deny before anything executes. Scalar subqueries are pre-evaluated, that
   is executed, during planning (ADR 0005), so a late check would already
   have read the data.
3. The core stays free of identity concepts (ADR 0002). It enforces a
   policy it is handed.
4. Configuration is explicit and fails fast; adoption is gradual.
5. A distinct error class: this is not a malformed query.

## Considered options

**A. Policy check on the logical plan's `TableScan` nodes (chosen).** After
DataFusion resolves the SQL into a logical plan, and before subquery
pre-evaluation and physical planning, the engine walks the plan with
`apply_with_subqueries`. Every `TableScan`'s reference is resolved against
the default catalog and schema and checked through a `TableAccess` trait.
The server implements it on `Viewer`.

This works because the plan is what will be read:

- CTEs are inlined into it;
- views do not exist (DDL is off, ADR 0024);
- every subquery is visited.

**B. Parse the SQL and check table references
(`resolve_table_references`).** This must handle CTE shadowing correctly,
and it runs a second resolution path that can diverge from the planner's.
Rejected for driver 1.

**C. Wrap every `SchemaProvider` / `TableProvider` and check at `scan`.**
`SchemaProvider::table` has no session access, so the check could only run
at `scan` time. That is after scalar subqueries ran (driver 2), and it would
require wrapping dynamic catalogs. Rejected.

**D. Row- and column-level policies.** These are finer-grained and belong
on top of A, using the same hook. They are not needed yet.

## Decision

- **Config.** `PLIMSOLL_AUTH_GRANTS=ana=gold|silver,etl=*`.
  - Unset means grants are not enforced: everyone reads everything, and a
    boot warning says so. This allows gradual adoption, the same as before
    this ADR.
  - Set means enforced, and unlisted principals read **no** data.
  - Unknown users, malformed entries and duplicate users fail boot.
  - With `PLIMSOLL_AUTH=disabled`, setting grants fails boot.
- **Core.**
  - `pub trait TableAccess: Send + Sync + 'static { fn check(&self,
    &ResolvedTableReference) -> Result<()> }`.
  - `Engine::sql_with<T: TableAccess>(sql, Arc<T>)` checks every scan, then
    puts the same object in the session config (ADR 0022).
  - `AccessDenied { table }` is the typed error, wrapped in
    `DataFusionError::External`.
- **Server.**
  - `Viewer.schemas: Option<HashSet<String>>`, where `*` means all.
  - Always readable:
    - `system.*`, which scopes its own rows (ADR 0022);
    - `information_schema`, which exposes names only (see Consequences).
  - `classify` maps `AccessDenied` to `EngineError::Forbidden`, which
    answers `403 forbidden`, non-retryable. The message names the
    fully-qualified table.

## Consequences

**Positive**
- Medallion policy is enforceable: grants per principal on `bronze`,
  `silver`, `gold` and catalog namespaces.
- No query shape bypasses it. The test covers direct reads, CTE, join,
  `IN` subquery and scalar subquery, and nothing executes before the
  denial.
- `forbidden` errors are counted in `plimsoll_query_errors_total`, apart
  from bad queries.
- The `TableAccess` hook is the place for row filters and column masks
  later.

**Negative (accepted debt)**
- Grants are per schema. There are no per-table or per-column grants.
- `information_schema` stays readable. Principals can see table and column
  **names** in schemas they cannot read, but no data. Restrict it if names
  are sensitive.
- Static config; changes need a restart. Grant changes do not affect
  queries already running.
- Grants on schemas that do not exist are accepted silently (catalog
  namespaces are discovered at boot and can change). A typo in a schema
  name grants nothing; that fails closed, but quietly.

**Operational**
- Rollout:
  1. set `PLIMSOLL_AUTH_GRANTS` with `etl=*` for pipelines and explicit
     schemas for people;
  2. watch `plimsoll_query_errors_total{code="forbidden"}` for denied
     legitimate use.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A plan shape hides a scan from the walker | Low | High | `apply_with_subqueries` visits subquery expressions; test covers CTE, join, `IN`, scalar subquery. Re-run on DataFusion upgrades. |
| A front door runs SQL without a policy | Medium | High | Server only calls `sql_with` with a `Viewer`; `Engine::sql` is for internal and test use. Review rule. |
| A table provider resolves to other data than its reference (e.g. a future federated table pointing at another schema's data) | Low | Medium | Grants protect references, not storage. Document; connector-level ACLs if needed. |
| Grants unset by mistake in production | Medium | High | Boot warning; operational checklist. |

## Validation

- `engine::tests::table_access_is_checked_on_every_scan` (core, with a
  policy denying `gold`):
  - denies a direct read, a CTE, a join and a scalar subquery;
  - the error is `AccessDenied` with the full table name;
  - the pool returns to 0.
- `auth::tests::grants_limit_readable_schemas`:
  - not configured means open;
  - granted schemas and `*` are readable;
  - an unlisted principal reads no data but still reads `system` and
    `information_schema`;
  - malformed, unknown-user and duplicate entries fail boot.
- `app::tests::grants_limit_readable_schemas_over_http`, with the real
  engine:
  - `gold` gives 200;
  - `bronze` gives 403, also when hidden in an `IN (subquery)`;
  - `system.runtime.queries` gives 200.

## Revisit when

- Per-table, row or column policies are needed (extend `TableAccess`).
- Grants must change without a restart (external policy store).
- `information_schema` names are sensitive (filter it per grants).
