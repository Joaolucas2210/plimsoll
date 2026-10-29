//! Federation correctness: pushdown policy, filter guard, scalar subquery pre-eval.
//!
//! The rule this module enforces: a filter handed to a source is either applied
//! by a source we *trust* for that expression class, or it is re-applied
//! locally. It can never silently disappear. See ADR 0005 and ADR 0006.

mod guard;
mod policy;
mod subquery;

pub use guard::FederationFilterGuard;
pub use policy::PushdownPolicy;
pub use subquery::preevaluate_scalar_subqueries;

/// Re-exported so connectors and tests speak the same vocabulary as DataFusion.
pub use datafusion::logical_expr::TableProviderFilterPushDown as Pushdown;
