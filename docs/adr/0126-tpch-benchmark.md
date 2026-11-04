# ADR 0126 — A reproducible TPC-H benchmark

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0003 (memory ceiling), ADR 0123 (image)
- Scope: `scripts/tpch_bench.py`, the numbers below

## Context

The README claims an engine that keeps its node alive under a memory
ceiling. Nothing measured how fast it answers the standard analytical
workload under that ceiling, or checked that it answers it correctly.
An MVP needs a number anyone can reproduce, and a script that fails
when a result is wrong.

## Decision

- **`scripts/tpch_bench.py`** (Python standard library, plus `duckdb`
  for `generate` only):
  - `generate --sf N`: DuckDB's `tpch` extension writes the eight
    tables as Parquet and the 22 standard queries' text. It also runs
    each query once and keeps its row count and first row as the
    expected answer.
  - `tables`: the `PLIMSOLL_PARQUET_TABLES` value that serves them.
  - `run`: posts each query to `/v1/sql`, one warm-up and three timed
    runs, and prints a Markdown table (min and median). Each result is
    checked: the row count, and the first row with numbers to 1e-6
    relative. Any mismatch or error exits non-zero.
- DuckDB is the **generator and the oracle**, never the engine
  measured. The queries run unmodified, as the spec's text.
- **The measured node is the release image** (ADR 0123): one node,
  `PLIMSOLL_MEMORY_LIMIT_BYTES=4294967296`, spill to `/tmp`, data
  bind-mounted read-only, everything else default.

## Results

Intel Core i7-1255U (12 threads), 15 GiB RAM, WSL2, Docker 28; the
image built from `e5975a2`. Wall-clock seconds per query as seen by
the HTTP client (planning, execution and JSON serialization included).

### SF1 (1 GB)

| Query | min | median | rows | check |
|---|---:|---:|---:|---|
| Q01 | 0.171 | 0.172 | 4 | ok |
| Q02 | 0.039 | 0.039 | 100 | ok |
| Q03 | 0.101 | 0.109 | 10 | ok |
| Q04 | 0.040 | 0.042 | 5 | ok |
| Q05 | 0.141 | 0.141 | 5 | ok |
| Q06 | 0.043 | 0.045 | 1 | ok |
| Q07 | 0.163 | 0.168 | 4 | ok |
| Q08 | 0.091 | 0.092 | 2 | ok |
| Q09 | 0.153 | 0.159 | 175 | ok |
| Q10 | 0.119 | 0.123 | 20 | ok |
| Q11 | 0.026 | 0.026 | 1048 | ok |
| Q12 | 0.068 | 0.081 | 2 | ok |
| Q13 | 0.073 | 0.073 | 42 | ok |
| Q14 | 0.048 | 0.049 | 1 | ok |
| Q15 | 0.075 | 0.076 | 1 | ok |
| Q16 | 0.052 | 0.053 | 18314 | ok |
| Q17 | 0.250 | 0.261 | 1 | ok |
| Q18 | 0.300 | 0.307 | 57 | ok |
| Q19 | 0.093 | 0.098 | 1 | ok |
| Q20 | 0.093 | 0.093 | 186 | ok |
| Q21 | 0.266 | 0.274 | 100 | ok |
| Q22 | 0.039 | 0.043 | 7 | ok |
| **Total** | **2.443** | | | 22/22 ok |

### SF10 (10 GB)

| Query | min | median | rows | check |
|---|---:|---:|---:|---|
| Q01 | 2.175 | 2.317 | 4 | ok |
| Q02 | 0.430 | 0.439 | 100 | ok |
| Q03 | 1.119 | 1.162 | 10 | ok |
| Q04 | 0.534 | 0.553 | 5 | ok |
| Q05 | 1.693 | 1.782 | 5 | ok |
| Q06 | 0.525 | 0.528 | 1 | ok |
| Q07 | 3.228 | 3.392 | 4 | ok |
| Q08 | 1.714 | 1.891 | 2 | ok |
| Q09 | 3.273 | 3.613 | 175 | ok |
| Q10 | 1.199 | 1.232 | 20 | ok |
| Q11 | 0.342 | 0.357 | 0 | ok |
| Q12 | 0.718 | 0.761 | 2 | ok |
| Q13 | 0.989 | 1.001 | 46 | ok |
| Q14 | 0.442 | 0.453 | 1 | ok |
| Q15 | 0.924 | 0.936 | 1 | ok |
| Q16 | 0.286 | 0.290 | 27840 | ok |
| Q17 | 2.978 | 2.989 | 1 | ok |
| Q18 | 3.909 | 3.942 | 100 | ok |
| Q19 | 0.992 | 1.049 | 1 | ok |
| Q20 | 1.063 | 1.093 | 1804 | ok |
| Q21 | 3.602 | 3.605 | 100 | ok |
| Q22 | 0.170 | 0.173 | 7 | ok |
| **Total** | **32.307** | | | 22/22 ok |

## Consequences

**Positive**
- One command reproduces the numbers on any machine. A regression in
  speed or in correctness shows up as a slower or failing run.
- Every TPC-H query runs correctly at SF10 under a 4 GiB ceiling on a
  laptop.

**Negative (accepted debt)**
- **One node, Parquet only.** The distributed path reads Iceberg (ADR
  0058), so a cluster run needs the tables loaded into a catalog first
  (`CREATE TABLE … AS SELECT` through the quickstart). The script runs
  unchanged against such a coordinator; publishing those numbers is
  next.
- **Not run in CI.** It is too heavy for the paid minutes (ADR 0011).
- **The check is a smoke test.** Row counts and the first row only; a
  full result comparison is the upgrade.
- **Not an official TPC-H result.** No audit, no refresh functions.

## Revisit when

- A cluster run: the same queries over Iceberg on the quickstart's
  coordinator and workers, at SF10 and larger.
- A release changes planning or execution.
