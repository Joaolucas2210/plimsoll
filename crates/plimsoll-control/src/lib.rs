//! Plimsoll control plane.
//!
//! Everything here decides *whether and where* a query runs, never *how*:
//!
//! - [`catalog`]: Iceberg REST catalog contract; [`rest`]: its HTTP client.
//! - [`governance`]: query registry, admission control and time limits.
//! - [`system`]: schema + snapshot source for `system.runtime.queries`.
//!
//! Invariant: this crate does not depend on DataFusion. The compute plane
//! (`plimsoll-core`) adapts these plain Rust types into table providers.

pub mod catalog;
pub mod governance;
pub mod rest;
pub mod system;
