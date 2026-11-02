# ADR 0044 — Column-level deny

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0025 (per-schema grants), ADR 0024 (read-only), ADR 0028 (time travel names), ADR 0032 (`$files`), ADR 0038 (result cache)
- Scope: `plimsoll_core::engine::{TableAccess::check_columns, plan_stream}`, `plimsoll-server::auth::{Auth::Tokens::column_deny, Viewer::{denied_columns, check_columns}}`

## Context

Grants are per schema (ADR 0025): a principal reads all of `gold` or
none of it. Real tables mix sensitive and ordinary columns. A `users`
table has `id`, `name` and `country`, and also `email` and `ssn`. Today
the only way to hide `email` from an analyst is a separate table without
it, which means duplicated pipelines and two sources of truth.

## Decision drivers

1. **Deny what is read, nothing more.** `SELECT id FROM users` must work
   for a principal denied `users.email`. Only queries that actually read
   `email` fail.
2. **No side channels.** A filter on a denied column reveals it, because
   `WHERE email = 'x'` is an existence oracle. The same goes for
   `length(email)` and subqueries.
3. **Before anything executes or is served from cache.** No partial
   result, no cached hit (ADR 0038) for a denied query.
4. **Fail-fast configuration.** A typo must not leave a column readable.

## Considered options

**A. Check the columns each `TableScan` reads in the optimized logical
plan (chosen).**
- After optimization, projection pushdown has trimmed every scan to the
  columns the query needs: select list, joins, aggregates, filters.
  Filters pushed *into* the scan are added explicitly, because the
  projection can drop a column only a pushed filter uses.
- One check per scan, before physical planning.

**B. Check column references in the unoptimized plan.** Every expression
would need resolving through aliases, subquery aliases and `*`
expansion. The optimizer already does that work. Rejected.

**C. Masking (return `NULL` or `***`).** It is friendlier for `SELECT *`,
but it changes answers silently: a `count(DISTINCT email)` becomes 1. A
403 is honest. Masking can be layered on later as a view-like rewrite.

## Decision

- **Configuration.**
  `PLIMSOLL_AUTH_COLUMN_DENY=ana=gold.users.email|gold.users.ssn,bob=…`
  takes entries of exactly `schema.table.column`. A wrong shape, an
  unknown user or a duplicate user fails at boot. It needs
  `PLIMSOLL_AUTH=required`.
- **Engine.** The new hook `TableAccess::check_columns(table, columns)`
  defaults to allowing everything.
  - `plan_stream` now optimizes the plan itself, once.
  - For every `TableScan` (subqueries included) it collects the projected
    schema's fields plus every column referenced by the scan's pushed
    filters, and calls the hook.
  - Then it plans physically from the optimized plan.
  - This all happens before the result-cache lookup and before any
    execution.
- **Viewer.**
  - It denies when any read column is in the principal's set.
  - The table name is mapped through ADR 0028: `users@123` and
    `users@audit` are `users`.
  - `users$files` is denied its `partition` column when the principal has
    any denied column on `users`, because partition values can be a
    denied column's values. File paths and sizes stay readable.
  - Errors are `AccessDenied`, so `403 forbidden` names
    `gold.users.email (column)`.
- **Count-only queries** (`SELECT count(*)`) read no columns and are
  allowed.

## Consequences

**Positive**
- Sensitive columns can be withheld per principal without duplicating
  tables.
- Side channels through filters, expressions and subqueries are closed by
  construction, because they all make the scan read the column.
- Time travel and metadata tables follow the same rule.

**Negative (accepted)**
- **`SELECT *` fails** for a principal with denied columns on the table.
  The principal must list columns. There is no masking.
- **Deny lists only, no column allow lists.** Allow-style grants remain
  per schema.
- **Planning optimizes in the engine now,** rather than inside
  `create_physical_plan`. The work is the same, done once.
- **Statistics-only answers** could in theory be computed from a denied
  column's metadata. This does not arise in DataFusion today, because
  `min(email)` reads the column.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| An optimizer rule drops a column from the scan while still using it | Very low | High | The scan only produces what it reads; anything computed from a column must come out of a scan that read it. Pushed filters are added explicitly. |
| A denied column leaks through partition values in `$files` | — | Medium | `partition` is withheld (tested). |
| A config typo leaves a column readable | Low | High | Strict `schema.table.column` shape, known users; fail at boot (tested). |

## Validation

- `auth::tests::column_deny_is_parsed_and_checked_per_principal`:
  - ana is denied `email` and `ssn`, but other columns of the table and
    other tables are fine;
  - `users@123` is denied `ssn`;
  - `users$files` allows `file_path` and denies `partition`;
  - bob is unrestricted;
  - four malformed entries are refused.
- `app::tests::denied_columns_are_withheld_wherever_they_are_read`, on the
  real engine:
  - `SELECT id, name`, `count(*)` and expressions over other columns
    answer 200;
  - each of the following answers 403 naming the column:
    - `SELECT *`;
    - `WHERE email = 'a@x'`;
    - `length(email)`;
    - an `IN (subquery … WHERE email LIKE …)`;
  - an unrestricted principal reads everything.
