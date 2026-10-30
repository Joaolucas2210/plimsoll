# ADR 0005 — Federation correctness: a filter can never disappear

- Status: Accepted
- Date: 2026-10-29
- Owner: JoaoLucas2210
- Related: ADR 0006, ADR 0007, ADR 0003
- Scope: `plimsoll_core::federation` (`guard`, `policy`, `subquery`), `plimsoll_connectors::Connector`, `plimsoll_core::engine`

## Context

DataFusion lets a `TableProvider` answer `Exact | Inexact | Unsupported` per
filter. `Exact` makes the optimizer *delete* its own `Filter`. If a source
claims `Exact` and then ignores or mis-evaluates the predicate (driver bug,
collation, NaN/-0.0 ordering, a clause silently dropped), rows that should be
filtered reach the user. The query succeeds with wrong data — the worst
outcome for a platform whose promise is to fail loudly. A second gap: the
watermark pattern `WHERE ts > (SELECT max(ts) FROM local)` is rewritten into a
join by `ScalarSubqueryToJoin`, so the predicate never reaches the remote
source and the whole remote table is shipped.

## Decision drivers

1. Correctness: no filter may be lost, whatever a source claims.
2. The invariant is owned and tested by us, not by optimizer rule order.
3. Pushdown where semantics are provably identical (performance second).
4. One sanctioned registration path for sources.

## Considered options

### Option A — Provider decorator that owns re-application (chosen)

`FederationFilterGuard` wraps every source, reports pushable filters as
`Exact` and re-applies the untrusted ones itself. Pros: invariant local to the
provider; plan shape identical to Option B. Cons: projection widening and
limit handling are ours to get right.

### Option B — Report `Inexact`, let DataFusion keep its `Filter`

Correct today with no code. Cons: the guarantee depends on optimizer behaviour
outside our code and invisible in the provider; a rule change can remove it.

### Option C — Trust connector claims

Simplest. Cons: one lying driver corrupts results.

### Option D — Never push filters

Correct. Cons: ships whole remote tables over the wire.

### Subqueries: pre-pass vs optimizer rule

Optimizer rules are synchronous; executing a subquery is async. A pre-pass on
the unoptimized logical plan is simpler and explicit.

## Decision

1. **Trust is decided by Plimsoll.** Each connector carries a
   `PushdownPolicy` (ADR 0006). The effective decision per filter is the
   *weaker* of policy and source claim (`Unsupported < Inexact < Exact`,
   `policy::weakest`). A source can never upgrade itself.
2. **`FederationFilterGuard`** (`TableProvider` decorator) reports
   `Exact|Inexact → Exact` and `Unsupported → Unsupported` to DataFusion. In
   `scan`, filters not `Unsupported` are passed to the source; every filter
   not `Exact` is re-applied by a `FilterExec` placed directly on the source
   scan. Invariant: *whatever `scan` returns satisfies every filter it was
   given.*
3. Filter-only columns (dropped from `projection` because we answered
   `Exact`) are added to the source projection, filtered, then projected back
   with a `ProjectionExec`.
4. `LIMIT` reaches the source only when no filter needs a re-check *and*
   `policy.limit` is true; otherwise it becomes the `FilterExec` fetch.
5. **Registration path.** `Connector::provider()` returns the guarded
   provider and `Connector::register()` registers it; `Connector::source()` is
   documented "do not register directly". Catalog tables use `provider()`
   (`plimsoll-server::lakehouse`).
6. **Scalar subquery pre-evaluation.** `Engine::sql` calls
   `preevaluate_scalar_subqueries` on the *unoptimized* plan: each
   uncorrelated (`outer_ref_columns` empty) scalar subquery runs once and is
   replaced by a literal (0 rows → typed NULL, >1 rows → error, as in SQL).

Why over Option B: identical plan shape, but the invariant is in code we own,
read and test, rather than a property of optimizer ordering.

## Consequences

**Positive.** A wrong or partial source pushdown costs performance, never
correctness. `EXPLAIN` shows the re-check `FilterExec` and, for Postgres, the
exact SQL sent.

**Negative (accepted debt).**
- `Inexact` is sound only when the source returns a **superset** of the rows
  the engine keeps; a re-check removes extras but cannot restore dropped rows.
  Policies must use `Unsupported` where remote semantics can drop rows.
- Registering `source()` directly bypasses the guarantee; this is a
  convention plus review, not a type-level guarantee.
- The subquery pre-pass is eager and sequential with no cost guard
  (`ponytail:`): an expensive subquery runs even if the outer query is later
  cancelled or would not benefit.

**Operational.** No new metrics. A rejected/odd filter shows up only as a
`FilterExec` above the scan in `EXPLAIN`. A scalar subquery returning >1 row
fails the query as an execution error.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| New connector registered unguarded | Low | High | `Connector` trait is the only path in tree; review rule; tests use `register` |
| Policy marks a lossy class `Inexact` | Medium | High | ADR 0006 rule: lossy → `Unsupported`; semantics test per widening |
| Pre-pass runs a huge subquery | Medium | Medium | Runs under the session pool (ADR 0003); upgrade: pre-evaluate only when the target is a federated scan |
| DataFusion changes `ScalarSubqueryToJoin` timing | Low | Low | Pre-pass runs before optimization by construction (`DataFrame::into_parts`) |

## Validation

- `plimsoll_core::federation::guard::tests`:
  `federation_unguarded_lying_source_loses_the_filter` (baseline: wrong
  result without the guard), `federation_filter_cannot_disappear` (asserts
  `FilterExec: id@1 > 2` in the plan), `federation_limit_not_pushed_under_recheck`.
- `plimsoll_core::federation::subquery::tests::federation_watermark_subquery_becomes_literal`.
- `plimsoll_connectors::postgres::tests::pg_text_like_and_float_stay_local`.

## Revisit when

- DataFusion offers a provider-level "verified" pushdown contract that makes
  the decorator redundant.
- Plan-level federation (aggregates, joins pushed to a source) is needed: a
  `TableProvider` cannot receive them; that is a separate mechanism and ADR.
- Pre-evaluated subqueries show up in latency or cancellation complaints.
