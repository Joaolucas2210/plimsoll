# ADR 0086 — Scheduled maintenance by table policy

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0078 (compaction), ADR 0079 (snapshot expiration), ADR 0080 (orphan removal), ADR 0084 (`SET TBLPROPERTIES`), ADR 0023 (admission)
- Scope: `schedule::{Policy, pass, every}`, `PLIMSOLL_MAINTENANCE_INTERVAL_MS`, `config::maintenance_interval`

## Context

Compaction, snapshot expiration and orphan removal exist (ADR
0078–0080), but only on demand, through `/v1/maintenance/*`. Every
table that is written to needs them regularly:

- small files accumulate with every append;
- position deletes accumulate with every `DELETE`, `UPDATE` and `MERGE`;
- snapshots and their files accumulate with every commit.

Each deployment would otherwise have to write its own cron job, hold a
token with grants on every table, and decide per table what to run.

## Decision drivers

1. **Policy with the table.** What to run, and how much history to
   keep, belongs to the table, as Iceberg's own
   `history.expire.*` properties already do. It should not be in a
   Plimsoll config that knows table names.
2. **Opt-in.** Maintenance deletes history and files, so nothing is
   touched unless the table asks.
3. **Queries first.** Maintenance must not starve user work.

## Decision

- **Schedule.** `PLIMSOLL_MAINTENANCE_INTERVAL_MS` turns it on (`0`, the
  default, is off). Every interval after the previous pass ends, a pass
  walks every table of every single-level catalog namespace. Passes
  never overlap.
- **Policy**, from the table's properties (`Policy::of`, pure and
  unit-tested):

  | property | default |
  |---|---|
  | `plimsoll.maintenance.enabled` | `false`: the table is skipped |
  | `plimsoll.maintenance.compact` | `true` |
  | `plimsoll.maintenance.expire-snapshots` | `true` |
  | `history.expire.max-snapshot-age-ms` (Iceberg's) | 5 days |
  | `history.expire.min-snapshots-to-keep` (Iceberg's) | 1 |
  | `plimsoll.maintenance.remove-orphans` | `false` |
  | `plimsoll.maintenance.orphan-age-ms` | 3 days, never under 1 hour |

  - Properties are set with `ALTER TABLE … SET TBLPROPERTIES` (ADR 0084)
    or by any other engine.
  - A value that is not understood (`enabled = 'maybe'`, a negative age)
    leaves the table alone and logs a warning. Guessing would be worse.
- **Order per table.**
  1. Compaction.
  2. Expiration, which then reclaims the files compaction replaced
     once they age out.
  3. Orphan removal, only when asked. It is opt-in because its window
     is the only protection against writes in flight (ADR 0080).

  These are the same operations as the endpoints, run one table at a
  time.
- **Admission.**
  - Each table holds an admission slot as user `plimsoll-maintenance`
    in the default group while it is maintained. Maintenance queues
    behind queries, and per-user limits cap it.
  - No grants are checked: the operator turned scheduling on, and each
    table's owner opted in.
- **Observability.**
  - One `scheduled_maintenance` log line per table and step: the report
    on success, a warning with the error otherwise, followed by the next
    step or table.
  - One `scheduled_maintenance_pass` line per pass, counting the tables
    maintained.

## Consequences

**Positive**
- A table maintains itself once it says so. Its retention travels with
  it, and Spark's or pyiceberg's `history.expire.*` settings are
  honored.
- No external scheduler, token or per-table configuration in Plimsoll.

**Negative (accepted debt)**
- **One coordinator should run it.** With several, passes race. Commits
  stay safe (conflict checks and retries), but the work is duplicated.
  Coordinator HA would add a lease. Resolved by ADR 0101 (a lease per
  table, per interval).
- **Tables are visited serially**, and every table is loaded on every
  pass to read its properties.
- **No per-step schedule.** Compaction cannot run hourly while
  expiration runs daily. There is no backoff for a table that keeps
  failing, and no metrics yet, only logs.
- Per-ref retention (`max-ref-age-ms`, per-branch keep counts) is still
  not honored (ADR 0079).

## Validation

- `schedule::tests::policies_come_from_table_properties`:
  - no opt-in means no policy;
  - an opt-in alone uses the defaults;
  - every property is read, with retain-last at least 1 and the orphan
    window at least 1 hour;
  - malformed values are errors.
- `lakehouse::tests::writes_through_the_rest_catalog` (live in CI, on S3):
  - the partitioned table opts in with `max-snapshot-age-ms = 0`, and
    two small appends follow;
  - one `pass` compacts them (the last snapshot is a `replace`) and
    expires everything but the head and the tag;
  - the rows are unchanged.
- By hand, against the reference REST catalog, with the server's
  interval at 4 s:
  - a table opted in (with orphan removal) and got two appends; the
    first pass compacted 3 files into 1, expired every snapshot but the
    head, and scanned for orphans;
  - deleting some expired files failed because they belonged to Spark's
    container user, not the server's; the step logged that as a
    warning, and the pass went on to the next step and table;
  - a table with `enabled = 'maybe'` was skipped with a warning on
    every pass;
  - the next pass found nothing to do.

## Revisit when

- Several coordinators: take a per-table lease (a catalog property with
  an expiry, or the coordinator lease of HA) before maintaining.
- Large catalogs: maintain tables in parallel up to a limit, and skip
  tables whose metadata has not changed since their last pass.
- Operators need numbers: `plimsoll_maintenance_runs_total{step,outcome}`
  and the files and snapshots reclaimed.
