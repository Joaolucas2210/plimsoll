# ADR 0101 — Several coordinators

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0034 (cancellation), ADR 0058/0070 (workers), ADR 0073/0074 (shuffle lifetime), ADR 0080 (orphans), ADR 0086 (scheduled maintenance)
- Scope: `QueryRegistry::starting_at`, `first_query_id`, `expire::{claim, LEASE}`, `IcebergConnector::claim_maintenance`, `schedule::{pass, every}` leases

## Context

One coordinator is a single point of failure for the query API. Running
several behind a load balancer, all sharing the catalog and the workers,
is the obvious answer, but nothing had been checked for it.

Most state was already safe to share:
- Iceberg commits are optimistic: the catalog checks requirements, and
  conflicts are retried.
- Workers keep no per-coordinator state. Shuffle maps are keyed by
  random ids (ADR 0064) and expire with their query's deadline (ADR
  0074), so a coordinator that dies mid-query leaks nothing for long.

Three things were not:

1. **Query ids collided.** Every coordinator counted from 1. A
   `DELETE /v1/queries/5` routed to the other coordinator would cancel
   *its* query 5, which is another query of the same user (owners may
   cancel their own).
2. **Scheduled maintenance ran everywhere.** Every coordinator walked
   every opted-in table on every interval (ADR 0086, accepted debt).
   Commits stayed correct, but compaction ran N times and conflicted
   with itself.
3. **Limits are per node**: admission, memory and the result cache.

## Decision drivers

1. **No new infrastructure.** No etcd, ZooKeeper or database just for
   coordination. The catalog and the object store are already shared.
2. **Correct first.** A lost race may cost duplicate work, never a
   wrong result or a cancelled stranger.
3. **Any coordinator can die at any time.** Nothing may wait on it.

## Decision

- **Query ids start at a random point.** Each coordinator starts at a
  random multiple of 2^32 below 2^53, so ids from different coordinators
  do not meet in practice (2^21 starting points, 2^32 queries each).
  They stay exact as JSON numbers. A cancel sent to the wrong
  coordinator now finds nothing and answers 404. Cancelling across
  coordinators needs sticky routing, or closing the result stream, which
  cancels the query (ADR 0035).
- **Maintenance claims each table with a lease.**
  - The lease object is `<table location>/.plimsoll-maintenance`:
    `{"holder", "until_ms"}`.
  - It is written in the table's own storage, with the credentials that
    already write its files. Being hidden, it is skipped by Plimsoll's
    and Iceberg's orphan removal.
  - A coordinator claims a table after its admission slot is granted,
    for one interval:
    - a conditional create (`If-None-Match: *`) takes a free table;
    - a lease past `until_ms` is taken over with a conditional replace
      (`If-Match` on its ETag);
    - an unexpired lease means another coordinator owns the table this
      interval, so it is skipped (`"skipped": "held by another
      coordinator"`).
  - The lease is never released. It expires one interval after the
    claim, so each table is maintained about once per interval across
    the fleet, by whichever coordinator reaches it first. A coordinator
    that dies holding a lease delays that table by one interval at most.
  - The holder is `HOSTNAME/pid`, for whoever reads the lease object.
  - On a local filesystem, which has no compare-and-swap, an expired
    lease is deleted and created again. This only races between
    coordinators on one machine's disk.
- **Limits stay per node.** Admission, memory and the result cache are
  per coordinator. Set them per coordinator, as a share of the cluster.
  Workers enforce their own limits against all coordinators together
  (ADR 0059).

## Consequences

**Positive**
- Coordinators can be replicated behind any load balancer with no
  shared state of their own and no coordination service.
- Scheduled maintenance can be enabled on every coordinator. Tables are
  spread across them, and the work survives losing any of them.

**Negative (accepted debt)**
- **Maintenance longer than its interval** lets its lease expire under
  it, and another coordinator may start the same table. That is still
  correct (commit conflict checks), just duplicated. The interval should
  exceed a table's maintenance time.
- **The store must honor conditional writes.** AWS S3, MinIO, R2 and GCS
  do. A store that ignores them makes every claim succeed, which brings
  back the duplicated, still correct, maintenance.
- **Query lists and cancels are per coordinator.**
  `system.runtime.queries` shows only the node's own queries. A
  cluster-wide view needs coordinators to know each other (ROADMAP).
- **Admission limits do not sum across coordinators.** Losing one
  coordinator lowers cluster capacity until it is replaced.

## Validation

- `expire::tests::one_claim_holds_until_it_expires` runs on an
  in-memory store (compare-and-swap) and on a local filesystem (the
  fallback). It checks that:
  - a claim holds against everyone until `until_ms`;
  - an expired lease is taken over;
  - the new claim then holds.
- `governance::tests::ids_start_where_the_node_says`.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI,
  against the REST catalog on S3-compatible storage): a pass as holder
  `a` maintains the table, and a pass as holder `b` right after
  maintains none.
- By hand, two coordinators with a 4 s maintenance interval against the
  reference REST catalog:
  - their query ids came from different ranges;
  - each pass, the opted-in table was maintained by one coordinator and
    skipped by the other;
  - queries through either one returned the same rows.

## Revisit when

- Clients need a cluster-wide query list or cancel from any node:
  coordinators discover each other (as workers are, ADR 0070) and
  forward by id range.
- Admission must be global: a shared token bucket, at the cost of a
  coordination dependency.
