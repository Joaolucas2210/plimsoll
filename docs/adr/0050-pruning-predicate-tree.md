# ADR 0050 — Pruner as a predicate tree

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0010 (bounds pruning), ADR 0048 (nested paths), ADR 0049 (null counts)
- Scope: `plimsoll_connectors::iceberg::prune::{Pruner, Node, node, cmp}`

## Context

The pruner (ADR 0010) took the scan filters as a flat AND of
comparisons. Anything else was ignored, and ignored meant "may match".
Dashboard filters are rarely that simple:

- `region IN ('eu', 'us')`, which DataFusion often rewrites into
  `region = 'eu' OR region = 'us'`;
- `day = d1 OR day = d2` from multi-select pickers;
- `NOT (amount <= 100)`.

None of them pruned.

## Decision

- **A tree.** Filters compile into `Node = True | Cmp | Null | And | Or`.
  - `True` means unknown, so the node may match.
  - `And` drops `True` children.
  - `Or` becomes `True` as soon as any branch is unknown, because one
    unprovable branch means the row may come from it.

  The same tree is evaluated per file (bounds and counts, ADRs 0010 and
  0049) and per manifest (partition summaries; null checks are `True`
  there).
- **Shapes recognized:**
  - `a AND b`, `a OR b`;
  - comparisons with a literal on either side, and `BETWEEN`;
  - `IN (v1, …)` as an OR of equalities; `NOT IN` is unknown;
  - `IS [NOT] NULL`.
- **`NOT` is pushed in only where the result is exact under SQL's
  three-valued logic:**
  - `NOT (x > v)` becomes `x <= v`, and likewise for `>=`, `<` and `<=`,
    because both sides are NULL when `x` is NULL;
  - `NOT (x IS NULL)` becomes `x IS NOT NULL`;
  - `NOT (x = v)` (`<>`) cannot be proven empty from bounds, so it is
    unknown;
  - `NOT` over AND or OR is unknown. De Morgan is not applied, to keep
    the rule set small.

## Consequences

**Positive**
- Multi-value filters and OR chains skip files and manifests.

**Negative (accepted)**
- **`<>`, `NOT IN` and `NOT (a AND b)` never prune.**
- **No `LIKE 'prefix%'` pruning.** That would be a range on strings, and
  is a possible follow-up.

## Validation

- `prune::tests::file_bounds_prune_only_when_provably_empty`, on a file
  holding `id ∈ [10, 20]`:
  - `id > 20 OR id < 0` is pruned (previously kept);
  - an OR with a matching branch, or with an unknown branch, is kept;
  - `IN (1, 30)` is pruned, `IN (1, 15)` is kept, and `NOT IN` is kept;
  - string `IN` lists prune on their bounds;
  - `NOT (id <= 20)` is pruned, while `NOT (id < 20)` and `NOT (id = 15)`
    are kept.
- `tests/iceberg.rs::reads_a_spark_merge_on_read_table` (interop). Spark's
  MOR table has three data files: day 1's insert, day 2's insert, and the
  file holding only `id = 1` that the `UPDATE` wrote.

  | Filter | Answer | Files scanned |
  |---|---|---|
  | `id IN (1, 2)` | 2 rows | 2/3 |
  | `id IN (40, 41) OR id = 47` | 3 rows | 1/3 |
  | `NOT (id <= 23)` | 16 rows | 1/3 |
  | `id IN (2, 40)` | 2 rows | 2/3 (the `id = 1` file pruned) |

  Answers are unchanged with deletes applied.
- `tests/iceberg.rs::prunes_a_partitioned_pyiceberg_table` (CI live
  tests): `id = 3 OR id = 97` over 10 daily files used to read all 10.
  It now reads 2/10 and still answers 2 rows.
