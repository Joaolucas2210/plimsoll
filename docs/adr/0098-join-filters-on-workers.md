# ADR 0098 — Joins with filters on workers

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0065 (shuffled joins), ADR 0066/0097 (broadcast joins), ADR 0062/0063 (portable SQL)
- Scope: `remote::{JoinFilter, join_filter}` (`Broadcast::filter`, `JoinSpec::filter`), `portable::physical_sql_equivalent`, the worker's `join_filter`

## Context

Equi-joins with an extra condition are common:

- `a.id = b.id AND a.ts > b.ts`;
- `EXISTS (… WHERE b.k = a.k AND b.v > a.v)`;
- `LEFT JOIN … ON a.k = b.k AND b.active`.

DataFusion keeps the equalities as hash keys and the rest as the
join's `JoinFilter`: an expression over an intermediate schema, each of
whose columns comes from one side. Shuffled and broadcast joins
refused any join with a filter, so these ran on the coordinator, with
both sides pulled there.

## Decision drivers

1. **The same filter on the worker.** No silent reinterpretation.
2. **One mechanism** for shuffled (ADR 0065) and broadcast (ADR 0066,
   0097) joins.

## Decision

- **Shipping the filter.**
  - The intermediate columns are renamed `f0…` and the expression is
    rendered as SQL (ADR 0063). Each `f{i}` is recorded as a side
    (left, which is the build side for a broadcast, or right) and that
    side's column name.
  - On the worker, the SQL is parsed against the `f{i}` columns, typed
    from the aliased sides, and each column is rewritten to its side's
    qualified column (`b.` / `p.`, or `l.` / `r.`). The filter then joins
    the equalities in the join's `ON`, and DataFusion again plans it as
    the join's filter.
- **Equivalence, not identity.**
  - Scan filters must round-trip to the *identical* physical
    expression, since the scan claims them as exact (ADR 0062).
  - A join filter is only evaluated, so `physical_sql_equivalent`
    accepts SQL that plans back to the original *once both are
    simplified alike*. DataFusion's canonicalization turns `f0 < f1`
    into `f1 > f0`, which identity would reject.
  - Anything that does not survive (UDFs, lossy renderings) keeps the
    join local.
- **Join types.** Every type already distributed takes a filter:
  - shuffled joins;
  - broadcast inner and right joins;
  - gathered left, full, semi and anti joins. Workers run the filtered
    inner (or right) join, so build rows count as matched only when the
    filter holds.

## Consequences

**Positive**
- Equi-joins with extra conditions, including correlated `EXISTS` with a
  non-equality, run on the workers like their filterless forms.

**Negative (accepted debt)**
- **Join keys must still be plain columns.** `ON a.id % 5 = b.id % 5`
  stays local.
- **Purely non-equi joins** (nested loop) are not distributed.

## Validation

- `remote::tests` round-trips `filter` on broadcasts and on joined
  reduces.
- `tests/distributed.rs::distributed_scans_match_local_ones` (Spark
  fixtures, CI's interop job), on tables with position deletes and
  deletion vectors, each returning exactly the local answer:
  - a shuffled inner join and a shuffled left join with
    `a.id + b.id > 10`;
  - a broadcast inner join with a filter;
  - a gathered left join with a filter;
  - an `EXISTS` with a non-equality (gathered semi join).

  The first run showed every filter refused (canonicalization). That
  led to the equivalence check.

## Revisit when

- Expression join keys matter: hash the key expressions on the maps (as
  aggregation keys already are, ADR 0063) and ship them as SQL.
- Non-equi joins against a small side: a broadcast nested-loop join
  with the build side attached.
