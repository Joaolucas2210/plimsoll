# ADR 0053 — Decimal pruning

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0010 (pruning; decimals were excluded), ADR 0051 (bucket and truncate)
- Scope: `plimsoll_connectors::iceberg::prune::{Prim::Decimal, Datum::Big, rescale, min_be_bytes, bucket, transformed}`

## Context

Money and quantities are `decimal(p, s)`. Filters like
`amount > 1000.00` are common, and decimals were the last major
primitive the pruner could not compare. ADR 0010 left them out to avoid
any chance of inexact comparison.

The comparison can in fact be made exact. Iceberg stores a decimal bound
as the **unscaled** value at the column's scale, in big-endian two's
complement with minimal length. Comparing unscaled integers at one scale
is exact.

## Decision

- **Type.** `Prim::Decimal(scale)` is parsed from `decimal(p, s)`.
  Bounds decode to `Datum::Big(i128)` by sign-extending the big-endian
  bytes, which covers precision up to 38.
- **Literals.** They are rescaled to the column's scale only when exact:
  - `Decimal128(x, _, s')` becomes `x · 10^(s − s')` when `s ≥ s'`;
  - when `s < s'`, it becomes `x / 10^(s' − s)` only if the division is
    exact;
  - integer literals scale up.

  So `10.005` against `decimal(10, 2)` is not pruned. Float literals
  never are.
- **Bucket.** Iceberg hashes a decimal as its minimal unscaled bytes, so
  `bucket` applies Murmur3 to `min_be_bytes`, the same serialization.
  The spec's example (14.20 → -500754589) is a test.
- **Truncate.** `truncate[W]` acts on the unscaled value:
  `v − (v mod W)`. With `truncate[1000]` at scale 2, values group in
  steps of 10.00. Ranges widen, keeping strictness at a step start, as
  for integers (ADR 0051).

## Consequences

**Positive**
- Range and equality filters on money columns prune by file bounds and by
  bucket and truncate partitions.

**Negative (accepted)**
- **`Decimal256` literals and precision above 38 are not pruned.**
- **Identity decimal partitions** prune on manifest summaries only. Their
  per-file Avro decimal values are not decoded.

## Validation

- `prune::tests::decimals_prune_on_exact_unscaled_values`:
  - `min_be_bytes` gives the spec's encoding for 1420, −1, 128 and −129;
  - the hash of 14.20 matches the spec;
  - on a file with `amount ∈ [10.00, 20.00]`, `> 20.00`, `> 20.5`,
    `< 9.9900` and `= 21` are pruned;
  - `>= 20.00` and `= 15` are kept;
  - the inexact literals 9.9999 and 10.005 never prune.
- `tests/iceberg.rs::prunes_spark_decimal_columns` (interop), on a Spark
  table with `amount DECIMAL(10, 2)` equal to `id × 1.25`, partitioned by
  `bucket(3, amount)` and `truncate(1000, amount)`:
  - each of the 40 amounts, looked up alone, returns its id while
    reading fewer than all files. A hash or truncate mismatch with Spark
    would lose rows;
  - `amount > 40.00` returns ids 33..39, as Spark does, also reading
    fewer files.
