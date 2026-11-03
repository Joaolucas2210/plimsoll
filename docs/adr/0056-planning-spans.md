# ADR 0056 — Planning spans in traces

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0031 (query traces; resolves part of its revisit item), ADR 0008 (per-query `loadTable`), ADR 0020/0039 (planning cost)
- Scope: `plimsoll_core::plan_trace`, `lakehouse::CatalogSchema::load`, `IcebergTable::scan`, `engine::CoreEngine::{execute, stream}`, `trace::spans`

## Context

A query's trace (ADR 0031) had one `planning` span. On a lakehouse,
planning is where small queries spend their time. Its three main costs
look the same from outside:

- the catalog's `loadTable` round trip;
- the manifest list and manifests;
- delete files.

They need different fixes: catalog latency, compaction, or the manifest
cache (ADR 0039). Operators had to guess.

That work happens deep inside DataFusion's planning, in a
`SchemaProvider::table` or a `TableProvider::scan`, with no request
context in reach.

## Decision drivers

1. Attribute planning time to catalog versus Iceberg planning, with the
   numbers that explain it: files and manifests kept or total, deletes,
   cache hits.
2. No OpenTelemetry SDK, and no context threaded through DataFusion's
   APIs.
3. Zero cost and no effect outside a traced query.

## Decision

- **`plimsoll_core::plan_trace`** is a `tokio::task_local` collector.
  - `collect(fut)` runs a query's planning and returns the events
    recorded in its task.
  - `record(name, started, attrs)` adds one event and is a no-op
    outside a collection.
  - DataFusion plans in the calling task: schema providers and `scan`
    are awaited there, and manifest fetches are `buffered`, not spawned.
    So the events of one query land in its own collector, even with
    concurrent queries.
- **Events.**
  - **`catalog.load_table`** (lakehouse) carries `table` and `found`.
  - **`iceberg.plan`** carries `table`, `snapshot`, `manifests` (read
    over total), `files` (kept over total), `delete_files`,
    `equality_deletes` and `manifest_cache_hits`.
- **Export.** `CoreEngine` wraps planning in `collect` for both the
  document and the streamed paths. The events travel in `QueryOutput`
  and `Streamed`, then in `QueryLog`. `trace::spans` emits them as
  INTERNAL spans, children of the `planning` span (or of `query` if
  planning failed), at their real start times, with attributes prefixed
  `plimsoll.`.
- They are not logged. The query log line already has `reads`
  (ADR 0046) and the scan labels are in `EXPLAIN`.

## Consequences

**Positive**
- A slow query's trace shows whether planning time was the catalog or
  Iceberg, and why: `files=1/3`, `delete_files=2`,
  `manifest_cache_hits=0`.
- The collector is reusable: any connector can `record` from inside
  planning.

**Negative (accepted)**
- **Planning only.** Execution, meaning operators and Parquet fetches,
  is not broken down. That needs `tracing` (ADR 0031's upgrade path).
- **Work DataFusion spawns into other tasks during planning** would not
  be recorded. None does today.

## Validation

- `plan_trace::tests::events_are_collected_per_task_and_ignored_outside`:
  - a `record` outside a collection is dropped;
  - an event recorded across an await is collected;
  - two concurrent collections each see only their own events.
- `lakehouse::tests::tables_resolve_per_query_through_the_catalog`:
  planning `SELECT b FROM silver.users` records `catalog.load_table` with
  `table` and `found`.
- `trace::tests::query_spans_nest_phases_back_to_back_inside_the_request`:
  an `iceberg.plan` event becomes a child of the `planning` span, at its
  start time, with its attributes.
- Manual check on the binary, exporting to a local OTLP collector, with
  Spark's MOR table and `id IN (40, 41)`:

  | Span | Parent | Duration |
  |---|---|---|
  | `query` | caller | 29.0 ms |
  | `planning` | `query` | 21.9 ms |
  | `iceberg.plan` | `planning` | 11.7 ms |

  `iceberg.plan` carried `files=1/3`, `manifests=4/4`, `delete_files=2`
  and `manifest_cache_hits=0`.
