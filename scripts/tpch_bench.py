#!/usr/bin/env python3
"""TPC-H on a running Plimsoll node (ADR 0126).

DuckDB's tpch extension is only the generator: it writes the data as
Parquet and the 22 standard queries' text, and answers each query once
so every Plimsoll result is checked (row count and first row).

    pip install duckdb
    scripts/tpch_bench.py generate --sf 1 --data /tmp/tpch
    # serve /tmp/tpch with PLIMSOLL_PARQUET_TABLES (`tables` prints it)
    scripts/tpch_bench.py run --data /tmp/tpch --url http://localhost:8080

`run` reads the token from PLIMSOLL_TOKEN and prints a Markdown table.
"""

import argparse
import json
import math
import os
import statistics
import sys
import time
import urllib.error
import urllib.request

TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]


def generate(args):
    import duckdb

    os.makedirs(args.data, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch")
    con.execute(f"CALL dbgen(sf={args.sf})")
    for t in TABLES:
        con.execute(f"COPY {t} TO '{args.data}/{t}.parquet' (FORMAT parquet)")
    expected = {}
    for nr, sql in con.execute("SELECT query_nr, query FROM tpch_queries()").fetchall():
        with open(f"{args.data}/q{nr:02}.sql", "w") as f:
            f.write(sql)
        rows = con.execute(sql).fetchall()
        expected[nr] = {"rows": len(rows), "first": [str(v) for v in rows[0]] if rows else []}
    with open(f"{args.data}/expected.json", "w") as f:
        json.dump({"sf": args.sf, "queries": expected}, f, indent=1)
    print(f"sf={args.sf}: {len(TABLES)} tables and 22 queries in {args.data}")


def tables(args):
    root = args.prefix or args.data
    print(",".join(f"public.{t}={root}/{t}.parquet" for t in TABLES))


def load(args):
    """Copies `tpch.*` into the catalog's `public` (cluster runs)."""
    token = os.environ.get("PLIMSOLL_TOKEN")
    for t in TABLES:
        started = time.monotonic()
        out = post(args.url, token, f"CREATE TABLE public.{t} AS SELECT * FROM tpch.{t}", args.timeout)
        if "error" in out:
            print(f"{t}: {out['error']['code']}: {out['error']['message']}", file=sys.stderr)
            return 1
        print(f"{t}: {out['rows'][0][0]} rows in {time.monotonic() - started:.1f}s")
    return 0


def post(url, token, sql, timeout):
    body = json.dumps({"sql": sql}).encode()
    req = urllib.request.Request(f"{url}/v1/sql", data=body, method="POST")
    req.add_header("content-type", "application/json")
    if token:
        req.add_header("authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        return json.load(e)


def same(got, want):
    """Values as DuckDB prints them, numbers to 1e-6 relative."""
    if len(got) != len(want):
        return False
    for g, w in zip(got, want):
        try:
            if not math.isclose(float(g), float(w), rel_tol=1e-6, abs_tol=1e-6):
                return False
        except (TypeError, ValueError):
            if str(g) != w and not str(w).startswith(str(g)):
                return False
    return True


def run(args):
    token = os.environ.get("PLIMSOLL_TOKEN")
    with open(f"{args.data}/expected.json") as f:
        expected = json.load(f)
    print(f"TPC-H SF{expected['sf']}, {args.runs} runs after one warm-up; seconds\n")
    print("| Query | min | median | rows | check |")
    print("|---|---:|---:|---:|---|")
    totals, failed = [], 0
    for nr in range(1, 23):
        with open(f"{args.data}/q{nr:02}.sql") as f:
            sql = f.read()
        want = expected["queries"][str(nr)]
        times, out = [], None
        for i in range(args.runs + 1):
            started = time.monotonic()
            out = post(args.url, token, sql, args.timeout)
            if i > 0:
                times.append(time.monotonic() - started)
            if "error" in out:
                break
        if "error" in out:
            failed += 1
            print(f"| Q{nr:02} | | | | {out['error']['code']}: {out['error']['message'][:60]} |")
            continue
        rows = out["rows"]
        ok = len(rows) == want["rows"] and (not rows or same(rows[0], want["first"]))
        failed += not ok
        totals.append(min(times))
        print(
            f"| Q{nr:02} | {min(times):.3f} | {statistics.median(times):.3f} "
            f"| {len(rows)} | {'ok' if ok else 'MISMATCH'} |"
        )
    print(f"| **Total** | **{sum(totals):.3f}** | | | {22 - failed}/22 ok |")
    return 1 if failed else 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--sf", type=float, default=1)
    g.add_argument("--data", required=True)
    t = sub.add_parser("tables", help="PLIMSOLL_PARQUET_TABLES for the data")
    t.add_argument("--data", required=True)
    t.add_argument("--prefix", help="where the node sees --data (e.g. /data in a container)")
    lo = sub.add_parser("load", help="copy tpch.* into the catalog's public namespace")
    lo.add_argument("--url", default="http://localhost:8080")
    lo.add_argument("--timeout", type=float, default=3600)
    r = sub.add_parser("run")
    r.add_argument("--data", required=True)
    r.add_argument("--url", default="http://localhost:8080")
    r.add_argument("--runs", type=int, default=3)
    r.add_argument("--timeout", type=float, default=600)
    args = p.parse_args()
    sys.exit({"generate": generate, "tables": tables, "load": load, "run": run}[args.cmd](args) or 0)


if __name__ == "__main__":
    main()
