# ADR 0016 — Merge-on-read: equality deletes as grouped hash anti-joins

- Status: Accepted
- Date: 2026-10-30
- Owner: JoaoLucas2210
- Related: ADR 0014 (position deletes; this completes v2 row-level deletes), ADR 0007, ADR 0010, ADR 0012, ADR 0013, ADR 0011
- Scope: `plimsoll-connectors::iceberg` (`iceberg/deletes.rs`, `IcebergTable::data_files`, `scan`, `anti_join`), `scripts/spark_mor_fixture.sh`, `.github/workflows/interop.yml`

## Context

After ADR 0014, tables with *equality* delete files were still refused
(`unsupported_feature`). Equality deletes are what streaming upserts write:
Flink's Iceberg sink in upsert mode, and any writer that knows the key of a
row but not its position. A CDC pipeline landing in bronze is the textbook
lakehouse ingestion path, so these tables are common in exactly the layer
Plimsoll reads.

Spec v2 rule for an equality delete file `D` and a data file `F`:

- `D` applies to `F` iff `seq(F) < seq(D)` (**strict**, unlike position
  deletes' `<=`) **and** either `F` has the same partition spec id and
  partition tuple as `D`, or `D`'s spec is unpartitioned (global delete).
- A row of `F` is deleted iff its values in `D`'s `equality_ids` columns equal
  a row of `D`. **Nulls compare equal** (`city = NULL` in `D` deletes rows
  whose `city` is null).

Unlike position deletes, the deleted rows cannot be known without reading the
data: the cost is a join, not a skip.

## Decision drivers

1. Correctness on the three subtle rules: strict sequence order, partition
   scoping, null equality. A wrong answer here resurrects or loses rows.
2. Delete state is data-proportional: it must be under the ceiling (ADR 0003,
   0012), including during execution.
3. Files without applicable deletes must not pay for the join.
4. Validated against the reference implementation, not only hand-built files.
5. No new dependencies, no new `ExecutionPlan` to maintain if one exists.

## Considered options

**A. Hash anti-join per group of files sharing the same delete sets
(chosen).** Planning computes, per kept data file, which equality delete files
apply (sequence + partition rule). Files are grouped by that set; a group with
no sets is a plain Parquet scan, a group with sets is its scan followed by one
DataFusion `HashJoinExec` (`RightAnti`, `CollectLeft`,
`NullEquality::NullEqualsNull`) per delete file, build side = the delete keys.
Groups are combined with `UnionExec`. Pros: DataFusion's join is vectorized,
already accounted in the memory pool (the build side reserves on collect), and
null semantics are a flag. Sequence and partition rules are resolved at
planning, so the join needs no per-row metadata. Cons: plan width grows with
the number of distinct delete sets (≈ partitions with deletes).

**B. Custom filter `ExecutionPlan` with a `HashSet` of row-encoded keys.**
Same semantics, one fewer operator per delete file, but ~150 lines of
`ExecutionPlan` boilerplate and our own memory accounting for the set.
Rejected for now; it is the upgrade path if plan width becomes a problem.

**C. Convert equality deletes into position deletes at planning.** Would reuse
ADR 0014's row selections, but requires reading every affected data file's key
columns during planning: doubles I/O and moves execution into planning.

**D. Keep refusing.** Leaves CDC tables unreadable; rejected.

## Decision

Option A.

- **Planning (phase 3 of `data_files`).** Equality delete entries are kept
  with their sequence number, `partition_spec_id`, partition tuple and
  `equality_ids`. For each, the kept data files it applies to are computed
  with `EqualityDelete::applies`; deletes that apply to no kept file are never
  read. A spec counts as unpartitioned when it has no field or only `void`
  transforms.
- **Keys.** The delete file is read whole through the table's store (scoped
  credentials included), key columns projected by the current schema's names
  for `equality_ids` (renames are refused at registration, ADR 0007), cast to
  the table types. File bytes and decoded keys are reserved while planning.
  An `equality_ids` entry that is not in the current schema (dropped column) is
  `Unsupported`.
- **Scan.** Per group: key columns are added to the scan projection when the
  query did not select them, then dropped by a `ProjectionExec` after the
  joins. **No `LIMIT` is pushed into a scan below an anti-join** (it would cut
  rows before deletes); DataFusion's own limit above the scan still applies.
- **Observability.** `IcebergScan[t] … equality_deletes=<files>`; `EXPLAIN`
  shows `HashJoinExec: mode=CollectLeft, join_type=RightAnti` per set.
- **Interop CI.** `scripts/spark_mor_fixture.sh` (now one `spark-shell`
  session for both fixtures) writes a v2 table partitioned by `region`, an
  equality delete on `city` with keys `'x'` and `NULL` scoped to
  `region='a'` via Iceberg's `GenericAppenderFactory`, then a newer insert of
  a deleted key. The script `require`s Spark's own answer (`2, 4, 5, 6`)
  before printing `PLIMSOLL_ICEBERG_EQ`.

Why A over B: it reuses an operator that is tested, vectorized and already
inside the memory ceiling; B only wins on plan width, which has not been a
problem yet.

## Consequences

**Positive**
- Upsert/CDC tables are readable with Spark-identical answers, including null
  keys, partition scoping and the strict sequence rule.
- Files outside any delete set scan exactly as before (no join, `LIMIT`
  pushed).
- Join build sides are reserved in the `CeilingPool` at execution; an
  oversized delete backlog fails the query (`memory_limit_exceeded`), not the
  node.

**Negative (accepted debt)**
- Between planning and execution the key batches live in the plan
  unaccounted (the same O(deletes) window as ADR 0014's selections).
- One anti-join per delete file in a group, no merging of delete files with
  identical `equality_ids`: many small equality delete files (typical of
  frequent Flink checkpoints) widen the plan. Compaction is the remedy today.
- Delete files are read whole, per query, without a cache.
- A key column not selected by the query is still decoded for files with
  deletes.

**Operational**
- `equality_deletes=` in `EXPLAIN` is the compaction signal for upsert
  tables, alongside `deleted_rows=` for position deletes.
- OOM on a table with a large delete backlog names the hash join (execution)
  or `IcebergPlanning[t]` (planning).

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| `<` vs `<=` confusion between delete kinds resurrects or loses rows | Medium | High | Rule lives in one function (`EqualityDelete::applies`); fixture has a data file newer than the delete holding a deleted key (`c5` stays); reference test inserts after the delete. |
| Partition tuple comparison wrong (e.g. Avro union encoding) | Low | High | Tuples compared as decoded Avro values from the same writer; reference test has a deleted key in another partition (`id 4`) that must stay. |
| Null keys not matched | Low | High | `NullEqualsNull` on the join; reference test deletes a `NULL` key in one partition and keeps it in another. |
| A `LIMIT` cut below the anti-join drops live rows | Low | High | Never pushed into grouped scans; fixture test with `LIMIT`. |
| Plan width explodes with many delete files | Medium | Medium (planning latency) | Visible in `EXPLAIN`; upgrade path option B or merging sets by `equality_ids`. |

## Validation

- Fixture (`tests/iceberg.rs::equality_deletes_apply_only_to_older_data`):
  sequence rule, two groups (`UnionExec`), projection without the key column,
  `LIMIT`, `count(*)`, and `equality_deletes=1` plus the `RightAnti` join in
  `EXPLAIN`.
- Interop (`reads_reference_equality_deletes`, workflow *interop*): Iceberg
  Java-written deletes; ids, partition filter, `count(*)`, pool back to 0.
- ADR 0014 tests unchanged and green.

## Revisit when

- Upsert tables arrive with hundreds of small delete files per partition
  (merge sets by `equality_ids`, or option B).
- Format v3 equality-delete semantics change (none as of v3; DVs: ADR 0017).
- Planning profiles show delete-file reads dominating (cache per snapshot).
