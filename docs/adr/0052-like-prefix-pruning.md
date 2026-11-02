# ADR 0052 — `LIKE` prefix pruning

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0050 (predicate tree), ADR 0051 (truncate partitions)
- Scope: `plimsoll_connectors::iceberg::prune::{node (Like, starts_with), prefix_range}`

## Context

`code LIKE 'EU-%'` and `starts_with(sku, 'A12')` are everyday filters on
codes, SKUs and paths. Neither pruned, although every match lies in a
known string range.

## Decision

- **Shapes.** `col LIKE 'p…'` (case-sensitive, not negated) and
  `starts_with(col, 'p')` on a string column become
  `col >= p AND col < next(p)`.
  - `next(p)` increments the last byte below `0xFF`, after dropping
    trailing `0xFF` bytes. This is bytewise order, which is how Iceberg
    bounds and Arrow compare strings.
  - A prefix made entirely of `0xFF` bytes keeps only the lower bound.
- **Prefix.** It is the literal characters before the first `%`, `_` or
  backslash (escape). A shorter prefix is still a sound range.
- **Not pruned:** an empty prefix (`'%x'`), `NOT LIKE` and `ILIKE`.
- Both bounds then prune like any range:
  - on column bounds (ADR 0010);
  - on truncate partitions (ADR 0051), including the per-file partition
    check;
  - on identity partitions.

## Consequences

**Positive**
- Prefix searches skip files and truncate partitions.

**Negative (accepted)**
- **`ILIKE` and other case-insensitive matching never prune.** Bounds
  are case-sensitive.
- **Escaped wildcards end the prefix early.** This is conservative.

## Validation

- `prune::tests::like_prefixes_prune_as_ranges`, on a file whose `city`
  lies in `["b", "d"]`:
  - `e%`, `a%`, `da_%` and `starts_with(…, 'x')` are pruned;
  - `c%` and `d%` are kept, since `"d"` itself matches;
  - `%x`, `NOT LIKE` and `ILIKE` are kept.
- `tests/iceberg.rs::prunes_spark_bucket_and_truncate_partitions`
  (interop), on Spark's table partitioned by `truncate(1, code)`:
  `code LIKE 'b%'` reads exactly the files that `code >= 'b' AND code <
  'c'` reads, which are fewer than all, and returns the same 13 rows.
