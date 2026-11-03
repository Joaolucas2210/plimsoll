# ADR 0058 — Coordinator/worker step 1: scan fragments on workers

- Status: Accepted
- Date: 2026-11-02
- Owner: JoaoLucas2210
- Related: ADR 0002 (planes; this starts the split), ADR 0007 (Iceberg read path), ADR 0035/0037 (streams, Arrow IPC), ADR 0015 (one HTTP stack)
- Scope: `plimsoll_connectors::remote::{Workers, Fragment, RemoteScanExec}`, `IcebergConnector::fragment`, `IcebergTable::{scan, metadata_path, only_files}`, `plimsoll-server::{app::fragment, engine::QueryEngine::fragment, config::{workers, worker_token}}`

## Context

A single node bounds what Plimsoll can scan, both in throughput (object
store reads, Parquet decode, delete application) and in memory, since
everything sits under one ceiling. The planes were drawn for a split from
the start (ADR 0002). Scans are the natural first step:

- they dominate cost;
- they are embarrassingly parallel by data file;
- Iceberg makes them deterministic, because the same snapshot and the
  same file give the same rows.

## Decision drivers

1. **Same answers.** Deletes of every kind (position, equality, DV)
   must still apply exactly. A distributed answer that differs from the
   local one is a bug.
2. **Same guarantees per node.** Every node keeps its own ceiling.
   Cancellation must reach workers. A worker failure must fail the
   query, never shorten it.
3. **No new heavy dependency** (Flight, gRPC, plan serialization).
4. **No secrets on the wire** beyond what is necessary.

## Considered options

**A. File-level fragments: workers re-plan from the snapshot, over
HTTP + Arrow IPC (chosen).**
- A fragment is
  `{metadata_location, snapshot_id, files[], columns[]}`.
- The worker re-opens the table at that snapshot and runs the normal
  Iceberg scan, restricted to its files. That covers manifests, deletes
  and DVs, with the same code and the same correctness proofs as a
  local scan.
- Results travel as the Arrow IPC stream of ADR 0037.

**B. Serialize physical plans (`datafusion-proto`) and ship plan
fragments.** It is general and covers joins and aggregates, but it is a
large dependency and requires every custom node (Iceberg scan,
`AccountedExec`, anti-joins) to be serializable. That is a step for
shuffles, not for scans.

**C. Arrow Flight.** It brings tonic and prost and a second listener.
HTTP streaming already gives back-pressure and cancellation (ADR 0035).

## Decision

- **Roles.** One binary serves both.
  - **Worker.** `PLIMSOLL_WORKER_TOKEN` (at least 16 chars, secret)
    enables `POST /v1/fragment`. Without it the route is 404. A caller
    must present `Bearer <token>`, compared by SHA-256 digest; a wrong
    token gets 401.
  - **Coordinator.** `PLIMSOLL_WORKERS` lists worker base URLs and
    requires the token. Their `Workers` handle is a session config
    extension, so every query on the coordinator sees it.
- **Coordinator scan.** `IcebergTable::scan` plans as before (manifests,
  pruning, exact filters), then, if workers are configured:
  - the kept data files are split round-robin into one fragment per
    worker;
  - `RemoteScanExec` has one partition per fragment;
  - it sits inside the usual `AccountedExec`, which keeps its pinning
    (ADR 0038) and its exact count (ADR 0054). Inexact filters, limits,
    joins and aggregates still run on the coordinator, above the scan.
- **Worker.** `CoreEngine::fragment`:
  - re-opens the table via `IcebergConnector::fragment`, which reads the
    metadata, pins it with `at(snapshot)` (ADR 0028) and sets
    `only_files`;
  - plans the scan with the projected columns;
  - streams it.

  The body is Arrow IPC: schema, then batches, then the end-of-stream
  marker. A failure after the first batch aborts the body (ADR 0037).
- **Decoding.** `RemoteScanExec` feeds response chunks to Arrow's
  `StreamDecoder` as they arrive, so it holds a window, not a result.
  - Batches are re-labelled with the coordinator's schema.
  - A stream that ends without the end-of-stream marker is an error
    ("worker stream ended early").
  - HTTP errors carry the worker's status and message.
- **Not distributed:**
  - tables read with catalog-vended credentials (ADR 0009), whose
    credentials are not sent over the wire;
  - scans with no files;
  - a worker's own scans, so there is no recursion.

## Consequences

**Positive**
- Scan throughput and memory scale with workers. Each worker decodes and
  applies deletes for its own files, under its own ceiling.
- Correctness carries over by construction. Workers run the same planner
  on the same immutable snapshot.
- **Cancellation reaches workers.** Dropping the coordinator's stream
  (cancel, timeout, disconnect) drops the HTTP response, and the
  worker's body stream is dropped, which stops its scan. The `ndjson`
  path's mechanism (ADR 0035) applies unchanged.
- No new crate. reqwest gains its `stream` feature only; `Cargo.lock`
  changes by one dependency edge.

**Negative (accepted, step 2 and later)**
- **Workers do not run fragments under their admission control.** The
  ceiling protects them, but concurrency is not bounded per worker yet.
- **No query id, trace context or deadline propagation to workers.**
- **No retries, no health-based worker selection, no shuffle.**
- **Round-robin by file count, not by bytes.**
- **Workers need the same storage access as the coordinator,** because
  they read the files with their own environment credentials.
- **The worker token is a shared secret.** Run workers on a private
  network or behind TLS.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A worker's answer differs from a local one | Low | High | Workers run the identical scan code on the identical snapshot; the interop test compares distributed and local answers on Spark tables with every delete kind. |
| A failing worker shortens the result | — | High | HTTP errors and truncated IPC streams fail the query (tested with a dead worker). |
| An unauthorized node asks a worker for data | Low | High | Token required, compared by digest; 401 otherwise; the route does not exist without a token. |
| Worker overload under many coordinators | Medium | Medium | Each worker's ceiling fails queries, not the node; admission on workers is step 2. |

## Validation

- `remote::tests::fragments_round_trip_as_json`: the fragment JSON round
  trip is lossless, a missing field is refused, and `Workers`' `Debug`
  hides the token.
- `config::tests::workers_need_a_token_and_urls`: workers without a token,
  non-HTTP URLs and short tokens are refused, and errors redact the
  token.
- `tests/distributed.rs::distributed_scans_match_local_ones` (interop),
  with two in-process workers serving the real router and a coordinator
  engine:
  - on Spark's MOR (position deletes), EQ (Iceberg Java equality
    deletes) and v3 DV tables, `count`/`sum`, a filtered ordered list and
    `min`/`max` match the local engine exactly, and the coordinator's
    plan contains `RemoteScanExec`;
  - a dead worker fails the query with a `worker` error;
  - a wrong token gets 401.
- Manual check with three processes (two workers and a coordinator) over
  `/v1/sql`:
  - the DV table returned 85 706 rows summing to 4 285 685 677, and the
    MOR table 32 rows summing to 768, both as Spark;
  - `EXPLAIN` showed `RemoteScanExec: fragments=2 files=13`;
  - an unauthenticated `/v1/fragment` call got 401.
