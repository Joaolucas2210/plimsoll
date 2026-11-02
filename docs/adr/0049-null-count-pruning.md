# ADR 0049 — Null-count pruning

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0010 (bounds pruning), ADR 0048 (nested fields; its all-NULL files motivated this)
- Scope: `plimsoll_connectors::iceberg::prune::{Pruner::{null_checks, may_match_file}, Counts}`, `iceberg::count_map`

## Context

Bounds pruning (ADR 0010) has nothing to say about a file whose column is
entirely NULL: such a file has no bounds, so it is kept. That case is
common:

- columns added after old files were written (ADR 0040);
- struct fields under NULL structs (ADR 0048);
- sparse columns.

Iceberg manifests also carry `value_counts`, which include nulls, and
`null_value_counts` per column. Those counts answer two more filter
shapes exactly: `IS NULL` and `IS NOT NULL`.

## Decision

- Each data file's `value_counts` and `null_value_counts` (Iceberg
  `map<int, long>`) are read into `Counts`.
- A file is skipped when:
  - **a comparison** (`=`, `<`, `<=`, `>`, `>=`, `BETWEEN`) is on a
    column with `null_value_counts = value_counts`, because a comparison is
    never true on NULL;
  - **`col IS NOT NULL`** is on such an all-NULL column;
  - **`col IS NULL`** is on a column with `null_value_counts = 0`.
- **Missing counts never prune.** That covers v1 writers, metrics mode
  `none`, and columns without stats.
- Paths through structs (ADR 0048) work the same, since counts are per
  leaf field id.

## Consequences

**Positive**
- Filters on sparse, newly added or nested columns skip all-NULL files.
- `IS NULL` and `IS NOT NULL` queries, common for data-quality checks,
  prune for the first time.

**Negative (accepted)**
- **Only per-file counts.** Manifest-level `contains_null` summaries for
  partition fields are not used.
- **NaN counts are not read.** Floats with NaN are never pruned on that
  basis; comparisons with NaN are false anyway.

## Validation

- `prune::tests::null_counts_prune_comparisons_and_null_checks`:
  - with an all-NULL column, `= 5` and `IS NOT NULL` are pruned and
    `IS NULL` is kept;
  - with no NULLs, `IS NULL` is pruned and `IS NOT NULL` is kept;
  - with some NULLs, or no counts, everything is kept.
- `tests/iceberg.rs::reads_nested_spark_types` (interop). Spark wrote
  three files: `(1, s.a=1)`, `(2, s NULL)` and `(3, s.a=3)`.

  | Filter | ADR 0048 alone | With this ADR |
  |---|---|---|
  | `s['a'] = 3` | 2/3 | 1/3 |
  | `s['a'] > 100` | 1/3 | 0/3 |
  | `s['a'] >= 1` | 3/3 | 2/3 |
  | `s['a'] IS NULL` | — | 1/3, answer [2] |
  | `s['a'] IS NOT NULL` | — | 2/3 |

- The full Iceberg suite against Spark's tables (MOR, equality deletes,
  v3 DVs, evolution, imports): 26 of 26, with unchanged answers.
