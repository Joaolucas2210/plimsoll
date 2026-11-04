# ADR 0128 — TPC-H on a cluster, and the three bugs it found

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0066 (broadcast joins), ADR 0073 (shuffle release), ADR 0099 (broadcast limit), ADR 0123 (quickstart), ADR 0126 (TPC-H)
- Scope: `scripts/tpch_cluster.yaml`, `tpch_bench.py load`, `tables::ensure_schema`, `/v1/fragment` body reading, `remote::Release`

## Context

The single-node benchmark (ADR 0126) reads Parquet, which stays on the
coordinator. The distributed path (remote scans, partial aggregations,
shuffles, broadcast joins) reads Iceberg tables. The distributed tests
exercise it on small tables with hand-written queries; the 22 TPC-H
queries over real volumes had never run on it.

The first cluster run failed at boot, and then two queries failed:
- **Boot.** `PLIMSOLL_PARQUET_TABLES=tpch.lineitem=…` stopped startup
  with "failed to resolve schema: tpch". Environment tables only worked
  in schemas that already existed (the medallion ones, `public`).
- **Q14: a worker answered `413 Payload Too Large`.** A broadcast's
  build side (here, `part` filtered) travels in the `/v1/fragment` body,
  up to `PLIMSOLL_BROADCAST_MAX_BYTES` (64 MiB). The worker read bodies
  through axum's default extractor, capped at 2 MiB.
- **Q15: "shuffle … was released: its query has ended".** A CTE
  aggregated by a shuffle is read twice, once by a scalar subquery.
  DataFusion evaluates that subquery while planning and drops its plan
  as soon as the streams are started. The release of the shuffle's
  maps (ADR 0073) ran in the plan's `Drop`, so the workers dropped the
  maps while the reduces were still reading them.

## Decision

- **A cluster recipe.** `scripts/tpch_cluster.yaml` overlays the
  quickstart (ADR 0123):
  - the coordinator reads the generated Parquet as `tpch.*`;
  - a `public` namespace is created in the catalog;
  - every node gets a 3 GiB ceiling.
  `tpch_bench.py load` copies each table into it with `CREATE TABLE
  public.x AS SELECT * FROM tpch.x`. The unqualified queries then read
  Iceberg, over the workers, and `run` checks them as before.
- **Environment tables create their schema.** A `schema.table` name
  whose schema does not exist gets an in-memory one. Existing schemas
  (medallion, catalog namespaces) are used as they are.
- **Fragment bodies are read after authentication, up to the broadcast
  limit.**
  - `/v1/fragment` takes the body as a stream.
  - It checks the worker token first: an unauthenticated caller makes
    the worker read nothing.
  - Only then does it read up to `PLIMSOLL_BROADCAST_MAX_BYTES + 16
    MiB` (the task and its other attachments). A larger body is `413`.
  - Coordinators and workers must share the broadcast limit, as they
    share the worker token.
- **A shuffle's release outlives its plan until its streams end.**
  - The release (its workers, its id, whether it ran) is an
    `Arc<Release>`, held by the `RemoteReduceExec` and by every stream
    its `execute` returns.
  - Its `Drop` sends the releases. That happens when the plan *and*
    every read of the shuffle are gone, whichever comes last.

## Results

The quickstart's cluster (a coordinator and two workers, 3 GiB each, on
the machine of ADR 0126), SF1 loaded into Iceberg (Parquet on the
warehouse volume). Seconds, checked against DuckDB:

| Query | min | median | rows | check |
|---|---:|---:|---:|---|
| Q01 | 0.619 | 0.677 | 4 | ok |
| Q02 | 0.193 | 0.214 | 100 | ok |
| Q03 | 0.370 | 0.498 | 10 | ok |
| Q04 | 0.164 | 0.171 | 5 | ok |
| Q05 | 0.460 | 0.523 | 5 | ok |
| Q06 | 0.080 | 0.092 | 1 | ok |
| Q07 | 0.467 | 0.508 | 4 | ok |
| Q08 | 0.635 | 0.667 | 2 | ok |
| Q09 | 0.790 | 0.834 | 175 | ok |
| Q10 | 0.261 | 0.300 | 20 | ok |
| Q11 | 0.195 | 0.205 | 1048 | ok |
| Q12 | 0.197 | 0.213 | 2 | ok |
| Q13 | 0.240 | 0.259 | 42 | ok |
| Q14 | 0.107 | 0.164 | 1 | ok |
| Q15 | 0.189 | 0.222 | 1 | ok |
| Q16 | 0.184 | 0.232 | 18314 | ok |
| Q17 | 0.723 | 0.751 | 1 | ok |
| Q18 | 1.035 | 1.052 | 57 | ok |
| Q19 | 0.202 | 0.219 | 1 | ok |
| Q20 | 0.198 | 0.204 | 186 | ok |
| Q21 | 1.032 | 1.050 | 100 | ok |
| Q22 | 0.099 | 0.136 | 7 | ok |
| **Total** | **8.441** | | | 22/22 ok |

All 22 are correct. On one laptop, three containers share the same
cores and disk, so this measures correctness and the cost of
distribution (fragments, shuffles, HTTP between containers), not
speed-up. The workers ran scans, partial aggregations, shuffle maps and
reduces, broadcast probes and releases (`plimsoll_fragments_total`).

## Consequences

**Positive**
- The 22 TPC-H queries are correct on the distributed path. Two
  failures that any join with a mid-sized dimension, or any repeated
  CTE, would have hit in production are fixed.
- An unauthenticated `/v1/fragment` request no longer costs the worker
  a 2 MiB read.

**Negative (accepted debt)**
- **SF10 on the cluster was not run to the end.** It was stopped to
  free the development machine's memory and disk. It needs a host with
  room for three 3 GiB nodes and a second copy of the data.
- **The release waits for the slowest stream.** A stream a client
  abandons without dropping it would hold the maps until the TTL (ADR
  0074), as before.

## Validation

- `tables::tests::env_tables_create_their_schema`.
- `app::tests::fragment_bodies_are_read_once_authenticated_up_to_the_limit`:
  - an unauthenticated 3 MiB body is `401`;
  - an authenticated one is read, then refused as a task (`400`, not
    `413`);
  - 9 MiB against an 8 MiB limit is `413`.
- `tests/distributed.rs::distributed_scans_match_local_ones`: a Q15-like
  query (a shuffled aggregation read by a scalar subquery) plans a
  shuffle and matches the local answer. Against the previous `remote.rs`
  it fails with the release error; with the fix it passes.
- The cluster run above: Q14 and Q15 failed before the fixes and are
  correct after them.
