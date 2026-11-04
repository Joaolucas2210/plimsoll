# ADR 0130 — MERGE sources over the broadcast limit merge on the coordinator

- Status: Accepted
- Date: 2026-11-04
- Owner: JoaoLucas2210
- Related: ADR 0094 (MERGE on workers), ADR 0099 (broadcast limit), ADR 0128 (fragment bodies)
- Scope: `merge::{run_remote, local}`, `RemoteScanExec::broadcast_limit`

## Context

`MERGE` on workers (ADR 0094) collects the source on the coordinator
and attaches it, as Arrow IPC, to every target-file fragment. Nothing
bounded that attachment:
- Workers read fragment bodies up to the broadcast limit plus 16 MiB
  (ADR 0128); up to 2 MiB before that. A larger source failed the
  merge with `413`.
- A source of N bytes sent to W workers costs N × W of network and of
  memory across the cluster. That is exactly the trade-off the
  broadcast limit already decides for joins (ADR 0099).

A shuffled merge (source and target hashed on the merge keys) would
remove the bound. It needs `WHEN NOT MATCHED` decided per key bucket
instead of from one global paired set. That is larger work, and it is
not needed for the merge to be correct at any size.

## Decision

- **The source's encoded size is checked against the broadcast limit**
  before it is sent (`PLIMSOLL_BROADCAST_MAX_BYTES`, the same value
  that bounds join build sides and what workers read).
- **Over the limit, the merge runs on the coordinator**, with the
  source already collected: the same three steps as the single-node
  merge (`probe`, `not_matched`, `finish`), now one `local` function
  that both paths share. Nothing is sent to the workers.
- **Under the limit, nothing changes**: target files on the workers,
  as in ADR 0094.

## Consequences

**Positive**
- A `MERGE` succeeds whatever its source size, under the coordinator's
  memory ceiling. Before, it failed on workers above 2 MiB, then above
  the broadcast limit.
- One limit governs everything attached to fragments. Operators tune
  one value.

**Negative (accepted debt)**
- **A large source merges on one node.** Reading every target file is
  then not spread. A shuffled merge (source and target files bucketed
  by the merge keys, `WHEN NOT MATCHED` per bucket) is still the way to
  scale it out.
- The decision is made at execution time, so `EXPLAIN` still shows the
  fragments planned.

## Validation

`tests/distributed.rs::writes_on_workers_through_the_rest_catalog`
(REST catalog):
- a coordinator capped at a 1-byte broadcast limit runs the same `MERGE`
  (matched updates and deletes, inserts, `NOT MATCHED BY SOURCE`
  deletes);
- it returns the single-node count, and the table matches the
  single-node one row for row;
- its workers served no `merge` fragment.

Against the previous `merge.rs`, that last assertion fails: the source
went out to the workers.
