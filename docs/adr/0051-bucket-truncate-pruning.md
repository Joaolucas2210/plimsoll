# ADR 0051 — Bucket and truncate partition pruning

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0010 (partition pruning; identity and temporal only), ADR 0050 (predicate tree)
- Scope: `plimsoll_connectors::iceberg::prune::{Transform::{Bucket, Truncate}, transformed, bucket, murmur3_32, Pruner::may_match_partition, partition_values}`, the file loop in `IcebergTable::data_files`

## Context

Iceberg's two most common non-temporal partition transforms are:

- **`bucket[N](col)`**: hash partitioning, as for `user_id`;
- **`truncate[W](col)`**: prefix or range partitioning, as for codes and
  amounts.

The pruner ignored both (ADR 0010), so `WHERE user_id = 42` on a
bucketed table read every file.

A second gap is that bucket pruning cannot work at the manifest level.
One manifest usually holds files of every bucket, so its partition
summary is `[0, N-1]`. The bucket values that matter are per file, in
each data file's `partition` tuple, and the pruner never looked there.

## Decision drivers

1. **Exactly Iceberg's bucket function.** A hash that differs in a single
   case silently drops the right file and returns a wrong answer. It must
   be validated against the spec and a real writer.
2. **Only provable mappings.** Hashes keep no order, so only `=`, and
   `IN` through the predicate tree, can map to a bucket.

## Decision

- **Bucket.** `murmur3_32` is the 32-bit x86 Murmur3 with seed 0.
  `bucket(v) = (hash & 0x7fffffff) % N`, where:
  - `int`, `long`, `date` and `timestamp` hash as 8-byte little-endian
    longs;
  - `string` hashes as its UTF-8 bytes.

  An `Eq` predicate maps to `bucket = b(v)`; other operators do not map.
- **Truncate.**
  - Integers map to `v - (v mod W)` (Euclidean mod).
  - Strings map to their first `W` code points.
  - The transform is non-decreasing, so `=` maps exactly, and `<` / `>`
    widen to `<=` / `>=`.
  - For integers, `<` stays strict when `v` starts its bucket:
    `x < 20` rules out truncated value 20.
- **Per-file partition check.** `Pruner::may_match_partition` evaluates
  the tree against each data file's partition tuple, read from the
  manifest entry: lower = upper = the file's value, and NULL values are
  unknown. It runs next to the bound and count checks, so a file is
  skipped if *either* proves it empty. Manifest summaries still use the
  same transforms.

## Consequences

**Positive**
- Point lookups and `IN` lists on bucketed columns read only the
  matching buckets' files.
- Truncate partitions prune for equality and ranges.
- The per-file partition check also helps identity and temporal
  partitions whose files share a manifest.

**Negative (accepted)**
- **Not covered:** bucket on decimal, uuid, fixed and binary columns,
  which have no prunable `Prim`; truncate on decimals; and timestamp_ns
  buckets.
- **No bucket pruning for ranges,** by nature.

## Validation

- `prune::tests::murmur3_matches_the_iceberg_spec`: the spec's hash
  examples (Appendix B) match bit for bit:

  | Input | Hash |
  |---|---|
  | int/long 34 | 2017239379 |
  | date 2017-11-16 | -653330422 |
  | timestamp 2017-11-16T22:31:08 | -2047944441 |
  | `"iceberg"` | 1210000089 |
  | bytes `00 01 02 03` | -188683207 |

- `prune::tests::bucket_and_truncate_partitions_prune_per_file`:
  - `=` and `IN` map through the bucket, and a range never prunes a
    bucket;
  - truncate equality and ranges prune, including `x < 20` at a bucket
    start.
- `tests/iceberg.rs::prunes_spark_bucket_and_truncate_partitions`
  (interop), on a Spark table (12 data files) partitioned by `bucket(4, id)` and
  `truncate(1, code)` with 40 rows:
  - each of the 40 ids, queried alone, returns that row while reading
    fewer than all files. Spark placed the rows with its own hash, so any
    disagreement would have lost a row;
  - `IN (7, 23)` returns both rows;
  - `code = 'b10'` and `code >= 'b' AND code < 'c'` return the right
    counts while reading only the `b` partitions.
