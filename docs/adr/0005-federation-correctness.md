# ADR 0005 — Federation correctness: a filter can never disappear

- Status: Accepted
- Scope: `plimsoll-core::federation`, every connector

## Context

DataFusion lets a `TableProvider` answer `Exact | Inexact | Unsupported` per
filter. `Exact` makes the optimizer *delete* its own `Filter`. If a source
claims `Exact` and then ignores or mis-evaluates the predicate (bug, different
collation, NaN ordering, a driver that silently drops an unsupported clause),
rows that should have been filtered reach the user. The query "succeeds" with
wrong data — the worst failure mode for a platform whose tagline is about
failing loudly. Correctness beats maximum pushdown.

## Decision

1. **Trust is decided by Plimsoll, not by the source.** Each connector carries
   a `PushdownPolicy` (ADR 0006). The effective decision per filter is the
   *weaker* of the policy and the source's own claim
   (`Unsupported < Inexact < Exact`). A source can never upgrade itself.
2. **`FederationFilterGuard`** is a `TableProvider` decorator wrapping every
   connector source. It reports every pushable filter as `Exact` to DataFusion
   and owns re-application: filters whose effective decision is `Inexact` are
   re-applied by a `FilterExec` placed directly on the source scan. The
   invariant lives in the provider — *whatever `scan` returns satisfies every
   filter it was given* — not in an optimizer rule that could be reordered or
   bypassed.
3. The guard widens the scan projection with filter-only columns (DataFusion
   omits them for `Exact` filters), filters, then projects back.
4. `LIMIT` is passed to the source only when nothing needs a local re-check and
   the policy allows it; otherwise it becomes the `FilterExec` fetch.
5. Connectors expose the raw source and the guarded provider; only the guarded
   one is registered (`Connector::register`).
6. **Scalar subquery pre-evaluation** (watermark pattern,
   `WHERE ts > (SELECT max(ts) FROM local)`): `preevaluate_scalar_subqueries`
   walks the *unoptimized* logical plan, executes each uncorrelated
   (`outer_ref_columns` empty) scalar subquery once, and substitutes the
   literal (0 rows → NULL, >1 rows → error, as in SQL). The predicate then
   becomes an ordinary comparison the policy can federate. It must run before
   the optimizer's `ScalarSubqueryToJoin` rewrites the subquery into a join.

## Alternatives

- **Report `Inexact` and let DataFusion keep its `Filter`.** Correct today, but
  the guarantee depends on optimizer behaviour outside our code and is not
  visible in the provider. Rejected as the primary mechanism; the guard gives
  the same plan shape with an invariant we own and test.
- **Trust connector claims.** Simple; one lying driver corrupts results.
  Rejected.
- **Never push filters.** Correct but ships whole remote tables over the wire.
  Rejected; the policy keeps the safe subset.
- **Pre-evaluate subqueries inside an optimizer rule.** Optimizer rules are
  synchronous; executing a subquery needs async. A pre-pass over the logical
  plan is simpler and explicit.

## Consequences

- A wrong or partial source pushdown costs performance, never correctness.
- `Inexact` is only sound when the source returns a **superset** of the rows
  the engine would keep: re-checking removes extra rows but cannot restore
  dropped ones. Policies must use `Unsupported` where semantics can drop rows
  (e.g. float `-0.0` vs `0.0`, collations).
- Every connector source must be wrapped; registering a raw source bypasses
  the guarantee. `Connector::register` is the only sanctioned path.
- The subquery pre-pass runs subqueries eagerly and sequentially, without a
  cost guard (`ponytail:` marked in code). Upgrade: only pre-evaluate when the
  comparison target is a federated scan, and account under the memory ceiling.
- Tests: a lying source that claims `Exact` and ignores filters returns wrong
  rows unguarded and correct rows guarded; `FilterExec` is asserted in the plan.
