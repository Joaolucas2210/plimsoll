# ADR 0045 — Row filters per principal

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0025 (schema grants), ADR 0044 (column deny), ADR 0028 (time travel), ADR 0032 (metadata tables), ADR 0038 (result cache)
- Scope: `plimsoll_core::engine::{TableAccess::row_filter, plan_stream}`, `plimsoll-server::auth::{Auth::Tokens::row_filters, Viewer::{row_filters, row_filter, check}}`, the result-cache statement key

## Context

Multi-tenant tables are the norm in a lakehouse: one `orders` table,
partitioned by region or customer, read by many teams that may each see
only their slice. Schema grants (ADR 0025) and column deny (ADR 0044)
cannot express "ana sees EU orders only". Per-tenant tables or views
would duplicate data and pipelines.

## Decision drivers

1. **Every scan.** The filter must hold wherever the table is read:
   - top-level selects and joins, self-joins included;
   - `EXISTS`, `IN` and scalar subqueries;
   - time travel (`orders@123`).
2. **Not widenable.** No user predicate (`OR true`, `region = 'us'`) can
   bring other rows back.
3. **No side doors:**
   - the result cache must never serve one principal's filtered answer to
     another;
   - metadata tables must not reveal other tenants' files, partitions or
     counts.
4. **Fail closed on operator mistakes.** A broken predicate fails the
   query without revealing the predicate to the caller.

## Considered options

**A. Inject a `Filter` directly above every matching `TableScan`, before
optimization (chosen).**
- The filter is a plan node between the scan and everything else.
  Nothing above it, user predicates included, ever sees other rows.
- The optimizer may push it into the scan (pruning, Parquet predicates),
  which only makes it cheaper.

**B. Rewrite the SQL text** (wrap tables in subqueries). It is fragile
across aliases, CTEs and quoting, and it is injection-adjacent. Rejected.

**C. Rely on views per tenant.** DDL is disabled (ADR 0024), and views
would be N copies of the policy. Rejected.

## Decision

- **Configuration.**
  `PLIMSOLL_AUTH_ROW_FILTERS={"ana": {"gold.orders": "region = 'eu'"}}` is
  JSON, because predicates contain the separators of the other variables.
  At boot:
  - users must exist;
  - tables must be `schema.table`;
  - every predicate must parse as a SQL expression.

  It needs `PLIMSOLL_AUTH=required`.
- **Engine.** `TableAccess::row_filter(table)` returns the predicate, or
  `None`.
  - In `plan_stream`, after the schema grant check and before
    optimization, `transform_up_with_subqueries` wraps every matching
    `TableScan` in `Filter(predicate)`. The predicate is planned against
    that scan's schema.
  - A predicate that does not plan (a wrong column) is an `Execution`
    error, so a 500 whose message names the table but not the predicate.
- **Viewer.** The table name is mapped through ADR 0028, so `orders@123`
  uses the `orders` filter. `orders$<anything>` (files, manifests,
  snapshots, history, refs) is denied to a filtered principal: those
  tables describe every tenant's data.
- **Result cache** (ADR 0038). The statement key includes every applied
  `table:predicate`. Principals with different filters, or none, never
  share an entry; principals with the same filter on the same snapshot
  do.
- **Column deny interplay** (ADR 0044). The column check runs on the
  optimized plan, which includes the injected filter. A row filter that
  uses a column denied to the same principal therefore denies all of
  that principal's queries on the table. This fails closed, and is
  documented.

## Consequences

**Positive**
- Tenants share tables without seeing each other's rows, whatever SQL
  they write. In the test, `OR true`, self-joins and `EXISTS (… region =
  'us')` all stay inside the filter.
- Filters prune: on Iceberg, `region = 'eu'` prunes partitions and files
  like any predicate (ADR 0010).

**Negative (accepted)**
- **Error side channels** (the classic leaky-function problem). The
  optimizer may evaluate a user predicate and the row filter in the same
  conjunction, so a deliberately failing expression (`1 / (amount - 400)`)
  could raise on a hidden row and reveal that it exists. Postgres solves
  this with `LEAKPROOF` function marking; DataFusion has no equivalent.
  This matters for adversarial tenants. Revisit with an optimizer barrier
  if needed.
- **Only one predicate per user and table,** expressed in SQL. There are
  no group-based or attribute-based policies yet.
- **Metadata tables are all-or-nothing** for filtered principals.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A cached answer crosses principals | — | High | Applied filters are in the key (tested: three filters, three entries, hits stay separate). |
| A typo in a predicate exposes rows | Low | High | Syntax checked at boot; a predicate that does not plan fails the query (500), never skips the filter. |
| A user widens the filter | — | High | The filter is a separate plan node below user predicates (tested with `OR true`). |
| Error-based inference of hidden rows | Low | Medium | Documented; requires a deliberately failing expression; revisit with an optimizer barrier. |

## Validation

- `app::tests::row_filters_confine_principals_to_their_rows`, on the real
  engine, with ana limited to `region = 'eu'` and bob unrestricted:

  | Query | ana | bob |
  |---|---|---|
  | `count(*)` | 2 | 3 |
  | `sum(amount)` | 30 | 430 |
  | `WHERE region = 'us' OR true` | 2 | 3 |
  | self-join `a.id <> b.id` | 2 | 6 |
  | `EXISTS (… region = 'us')` | 0 | 1 |

  A principal whose filter names a missing column gets a 500 that does
  not contain the predicate.
- `auth::tests::row_filters_are_validated_and_cover_time_travel_and_metadata`:
  - `orders@123` gets the filter, and other tables do not;
  - `orders$files` and `$snapshots` are denied, while `users$files` is
    allowed;
  - four malformed configurations are refused.
- `result_cache::tests::row_filtered_answers_are_cached_per_filter`: no
  filter gives 6, `v > 1` gives 5 and `v > 2` gives 3; there are 3
  entries, and repeats hit their own entry.
