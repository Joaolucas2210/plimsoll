# ADR 0055 — Exact identity-partition filters

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0006 (pushdown policy and guard), ADR 0050 (predicate tree), ADR 0054 (count from metadata)
- Scope: `plimsoll_connectors::iceberg::prune::{Exact, exact_node, identity_sources, Node::{only_on, decide}}`, `IcebergTable::{supports_filters_pushdown, scan, identity}`, the Iceberg connector's `PushdownPolicy`

## Context

For a table `PARTITIONED BY (region)`, every row of a data file has the
file's `region`, which is recorded in its partition tuple. A filter
`region = 'eu'` is therefore decided per file: true for all rows or for
none. Plimsoll pruned such files (ADR 0010, 0051), but still reported the
filter as `Inexact`. DataFusion then re-evaluated it on every row above
the scan. That wastes CPU, and it also blocked ADR 0054:
`count(*) WHERE region = 'eu'` could never come from metadata, because a
`FilterExec` sat between the count and the scan.

## Decision drivers

1. **`Exact` must be true.** If Plimsoll claims a filter, DataFusion drops
   its own check, so a wrong claim is a wrong answer.
2. **Spec evolution.** A file written under an older spec may not carry
   the field at all.
3. **SQL NULL semantics.** A NULL partition value must never satisfy
   `region = 'eu'`.

## Decision

- **Which columns.** A field id is decidable only if it is an `identity`
  source in **every** partition spec in the table's metadata
  (`identity_sources`). Then every file, whatever its spec, carries its
  value.
- **Which filters.** `Exact::new` is a strict compilation. It accepts:
  - comparisons with exact literals and `BETWEEN`;
  - `IN` lists, `IS [NOT] NULL` and `NOT` folded into a comparison;
  - `AND` and `OR` of those.

  Every leaf must be on decidable columns and of a type the partition
  tuple decodes (not decimal). Anything else (another column, a
  function, `LIKE`, `<>`, an unknown side of an AND) makes the filter
  *not* exact. Unlike the pruner's tree, nothing is dropped silently.
- **Pushdown.** `supports_filters_pushdown` answers `Exact` for those
  filters and `Inexact` for the rest.
  - The Iceberg connector's federation policy (ADR 0006) is now
    `uniform(Exact)`, so the table's own claim stands. The policy exists
    to distrust remote engines' semantics, such as Postgres collations.
  - Iceberg filters are evaluated by DataFusion itself, and anything the
    table claims `Inexact` is still re-checked.
- **Enforcement in `scan`.** For each kept file, every exact filter is
  decided from the file's partition tuple:
  - true keeps the file;
  - false drops it;
  - NULL compares as SQL NULL: comparisons are false, and `IS NULL` is
    true;
  - an undecidable file (missing tuple, or a value of another type) is
    an `InvalidMetadata` error, never an unfiltered file.

  Only inexact filters go to Parquet.
- **Counting** (ADR 0054). Exact filters no longer block the exact row
  count, and equality deletes block it only if they apply to a kept file.

## Consequences

**Positive**
- Partition filters cost nothing per row. `count(*) WHERE region = 'b'`
  is a metadata read when that partition has no deletes.
- Mixed filters split. In `region = 'b' AND id > 4`, the partition part is
  exact and only `id > 4` is re-checked.

**Negative (accepted)**
- **Temporal, bucket and truncate partitions stay `Inexact`.** Their
  transforms lose information, so a file can hold rows on both sides of
  `ts > t`.
- **A table whose history includes an unpartitioned spec** gets no exact
  filters at all. That is conservative.

## Validation

- `prune::tests::identity_partition_filters_are_exact_only_when_decidable`:
  - spec intersection: an unpartitioned spec in the history removes the
    identity field;
  - equality, `IN`, `IS NULL`, an OR with `IS NULL`, and a `NOT`-folded
    comparison decide correctly on a value and on NULL;
  - another column, `LIKE`, `<>`, and an AND or OR with a non-exact side
    are not exact;
  - a missing tuple is undecidable.
- `tests/iceberg.rs::reads_reference_equality_deletes` (interop), on the
  Iceberg Java table partitioned by `region`, with equality deletes in
  region `a`:
  - `region = 'b'` has no `FilterExec`;
  - `count(*) WHERE region = 'b'` has no scan at all and returns 2;
  - `region = 'b' AND id > 4` keeps a filter for `id` and returns [5];
  - `IN ('a', 'b')` returns all 4 live rows, and `IS NULL` returns none;
  - `count(*) WHERE region = 'a'` still scans, because of the deletes,
    and returns 2.
- The full Iceberg suite: 29 of 29.
