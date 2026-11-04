# ADR 0111 — Expression join keys on workers

- Status: Accepted
- Date: 2026-11-03
- Owner: JoaoLucas2210
- Related: ADR 0065 (shuffled joins), ADR 0066/0097 (broadcast joins), ADR 0098 (join filters), ADR 0063 (expressions as SQL)
- Scope: `Fragment::computed`, `remote::scan_side`, the worker's fragment projection, reducer key positions

## Context

Joins of remote scans run on the workers when their keys are columns
(ADR 0065, 0066). A key that is an expression (`ON lower(a.k) =
lower(b.k)`, `ON a.id + 1 = b.id`) kept the join on the coordinator,
with every row of both sides brought there.

DataFusion's physical planner never hands `HashJoinExec` an expression
key. It projects the side below the join, every input column then the
key under its display name (`id@0 + 1 AS "a.id + Int64(1)"`), and joins
on that column. The join itself is already one the workers run. Only
the projection between it and the remote scan stood in the way.

## Decision drivers

1. **Compute where the rows are.** The key is a function of one row of
   one side, so its fragment can compute it.
2. **Change nothing downstream.** With the projection reproduced on the
   worker, the join's key columns, output schema and projection indices
   are exactly those planned, so shuffles (ADR 0065), broadcasts
   (ADR 0066), gathered joins (ADR 0097) and join filters (ADR 0098)
   work unchanged.
3. **Old workers must refuse, not misjoin.**

## Decision

- **`Fragment::computed`: `(name, SQL)` pairs** evaluated after the
  fragment's `columns`, each over them (`with_column`). The SQL is the
  key expression in portable SQL, checked equivalent, as for aggregate
  keys (ADR 0063).
  - The field is serialized only when set. A worker that does not know
    it refuses the fragment (unknown field), so it never joins without
    the key.
- **`remote::scan_side`** accepts, as a join side, a plain remote scan
  (as before) or the planner's key projection over one. The projection
  must:
  - pass every scan column through in order, under its own name;
  - append only expressions that turn into portable SQL.

  Such a side becomes the same scan with the expressions as computed
  columns, and the projection's schema. Shuffled joins use it for both
  sides, and broadcasts for the probe side. A broadcast's build side is
  collected on the coordinator as planned, keys included.
- **Reducers find a key** among the map's columns, then among its
  computed columns, which follow them in the map's output.

## Consequences

**Positive**
- Joins on expressions of columns are shuffled or broadcast like joins
  on columns, alone or beside column keys, and with every join type and
  residual filter the workers already handle.
- No new join machinery: one fragment field, and recognizing one plan
  shape.

**Negative (accepted debt)**
- **Only the planner's shape.** A projection that also drops, reorders
  or renames columns, or computes something other than keys, keeps the
  join on the coordinator.
- **The keys travel back with the rows.** Computed columns are part of
  the side's output, as they are locally. They cost bytes on shuffles
  and broadcast results.
- **Key expressions must be portable SQL**: what `physical_sql_equivalent`
  can write and re-plan identically. UDFs unknown to the worker, or
  expressions that do not round-trip, keep the join local.

## Validation

- `remote::tests::fragments_round_trip_as_json`: `computed` round-trips.
- `tests/distributed.rs::distributed_scans_match_local_ones` (Spark
  fixtures, CI's interop job). Against the local engine:
  - shuffled: `ON a.id + 1 = b.id`, and a left join on
    `upper(a.name) = upper(b.name) AND a.id = b.id` (an expression key
    beside a column key);
  - broadcast: the expression on the build side (`b.id + 1`, computed
    on the coordinator), and on the probe side (`a.id + 1`, computed by
    the workers);
  - gathered: a left join on `a.id * 2 = b.id`;
  - each plan was checked to be shuffled or broadcast.
- Found while testing: an expression key under `ORDER BY` keeps a
  sorted probe side below the join, which stays on the coordinator as
  for column keys (ADR 0066).

## Revisit when

- Projections that also prune columns are common under joins: map the
  join's projection and filter indices through them, instead of
  requiring a pass-through.
