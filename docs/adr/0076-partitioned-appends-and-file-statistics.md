# ADR 0076 — Partitioned appends and file statistics

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0075 (append), ADR 0050–0053 (pruning, transforms, decimal bounds), ADR 0010 (manifest pruning)
- Scope: `iceberg::write::{Layout, IcebergSink::split, ColumnStats, bounds, truncated, manifest_entry_schema, summaries, avro_writer}`, `iceberg::prune::{partition_value, Prim::encode, Datum, Transform}`

## Context

ADR 0075 appended only to unpartitioned tables, and wrote data files
without column statistics. Two problems followed:

- **Most lakehouse tables are partitioned**, by day, by region, by a
  bucket of a key. They could not be written to.
- **The new files could not be pruned.** Every reader had to open every
  new file, and the other engines lost manifest-level pruning on
  anything Plimsoll wrote.

Plimsoll's reader already implements, for pruning, the partition
transforms (identity, bucket via Murmur3, truncate, year, month, day,
hour) and the decoding of Iceberg's single-value bound serialization
(ADR 0050–0053).

## Decision drivers

1. **One definition of each transform.** A row's partition value on
   write must be exactly what the pruner, and every other engine,
   computes for a predicate. Two implementations would drift.
2. **Interop.** Spark and pyiceberg must prune Plimsoll's files by the
   partition values and bounds Plimsoll records.
3. **Bounded memory** with many partitions in one insert.

## Decision

- **Layout.** `Layout::of(meta)` reads the default partition spec.
  - Each field maps to its source column and transform, and to the type
    of its partition value: the source type for identity and truncate,
    `int` for the others.
  - It refuses, with the field named:
    - transforms the pruner does not know (void, unknown);
    - sources outside the current schema;
    - unsupported types: decimal partition values (they need Avro
      decimal types), bucketing a boolean, a temporal transform of a
      non-temporal column.
  - The commit also asserts the default spec id
    (`assert-default-spec-id`); a spec or schema change mid-insert aborts.
- **Routing.**
  - For each batch, each row's partition tuple is computed with
    `prune::partition_value`, the pruner's own transform code, from the
    row's values.
  - Rows are grouped by tuple in order of first appearance, and each
    group is `take`n into that partition's open Parquet writer.
  - The per-row work touches the partition columns only.
- **Memory.**
  - Open writers buffer row groups. Their in-progress bytes are reserved
    in the pool (`IcebergAppend[t]`).
  - A refusal finishes every open file instead of failing the insert. So
    does exceeding 512 open partitions.
  - High-cardinality inserts therefore write more, smaller files, never
    unbounded buffers.
- **Statistics, per file and column.**
  - `value_counts` and `null_value_counts` for every column.
  - `lower_bounds`/`upper_bounds` for the types the pruner orders:
    boolean, int, long, date, timestamp, string, decimal. Each bound is
    the batch minimum or maximum, computed with Arrow kernels and merged
    across batches, then encoded with `Prim::encode` (the inverse of the
    pruner's decoder).
  - Strings are cut to 16 characters, Iceberg's default `truncate(16)`:
    the lower bound is the prefix; the upper bound is the prefix with its
    last character incremented, and is dropped if that is impossible.
  - Floats get no bounds, because the pruner ignores them (NaN).
- **Manifests.**
  - The `manifest_entry` schema is built per spec. Its partition struct
    has one optional field per spec field, with the spec's `field-id`.
    The statistics are the `map<int, _>` arrays Iceberg defines.
  - The manifest list entry carries the `partitions` field summaries
    (contains-null, lower, upper per partition field), which readers
    prune whole manifests by.
- **The Avro header is written by Plimsoll.** `apache_avro`
  re-serializes the schema and drops `"logicalType": "map"` from those
  arrays. Spark then read them as lists and failed ("Not a list type:
  map<int, long>"). `avro_writer` now writes the container header itself:
  magic, the original schema text, `avro.codec: deflate`, Iceberg
  metadata and a sync marker. The library only appends data blocks.

## Consequences

**Positive**
- Day-, hour-, region- and bucket-partitioned tables take `INSERT INTO`.
- Plimsoll, Spark and pyiceberg prune Plimsoll's files by partition
  values and column bounds.
- The transforms are the pruner's code, so writes and reads cannot
  disagree.

**Negative (accepted debt)**
- **Partition values are computed row by row** (`ScalarValue` per
  partition column). It is correct and simple; vectorize if inserts
  become CPU-bound.
- **One spec only.** Appends use the default spec; tables whose spec
  changes mid-insert abort.
- **Files are not sorted** within a partition, so bounds are as tight
  as the input order makes them.
- **Not covered yet:** decimal partition values, `void` fields, float
  bounds, nested columns, v3.

## Validation

- `write::tests::manifests_round_trip_with_field_ids_partitions_and_bounds`:
  - the header keeps `"logicalType":"map"`, and the partition and
    statistics field ids;
  - the partition value reads back, as do the bounds, encoded as the
    pruner decodes them;
  - a 26-character string's upper bound is cut to 16 characters with its
    last character incremented;
  - an all-null column has counts but no bounds;
  - the manifest list summarizes the partition, and carries it on to the
    next list.
- `write::tests::layouts_accept_the_transforms_the_pruner_knows`: every
  supported transform/type pair is accepted; day of a string, `void` and
  a missing source are refused.
- `write::tests::rows_split_by_partition_value`: rows group by
  (identity, bucket) in order of first appearance, nulls included. The
  bucket of `1L` under `bucket[2]` matches the Iceberg spec's Murmur3
  vector.
- `lakehouse::tests::appends_through_the_rest_catalog` (live in CI, on
  S3) adds a table partitioned by `identity(region)`, with two inserts.
  - A filter on `region`, and one on `id` through column bounds, each
    read a single file.
  - On `region`, the manifest list's summaries skip a whole manifest
    (`manifests=1/2`).
- By hand, against the reference REST catalog, with a table partitioned
  by `identity(region)`, `bucket[4](id)`, `truncate[2](code)` and
  `hour(ts)`, plus the pyiceberg fixture's `day(ts)` table:
  - Spark 3.5 returns the right rows for `region = 'eu'`, `id = 3`,
    `id = 5` (its own bucket computation of the literal), `LIKE 'cd%'`,
    `ts >= …` and `region IS NULL`. Its `files` and `partitions`
    metadata tables show the recorded partition values and bounds.
  - pyiceberg prunes to 1 file for `region == 'eu'`, `id == 3` and
    `code == 'cdz'`; the last needs the column bounds, since `truncate`
    puts two files in partition `cd`.
  - For the `day(ts)` table, all three engines agree: 200 rows, 90 in
    February.

## Revisit when

- Insert throughput matters: vectorize the transforms (Arrow kernels
  per transform) and sort rows by partition and key before writing.
- Decimal partitions are needed: write Avro `fixed` with the `decimal`
  logical type through the same header path.
- Writes move to workers: route by partition on the workers, and commit
  once on the coordinator.
