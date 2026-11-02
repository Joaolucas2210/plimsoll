//! Plimsoll core: memory ceiling, federation policy and the engine façade.
//!
//! Invariant: this crate never depends on concrete connectors.

pub mod engine;
pub mod federation;
pub mod memory;
pub mod result_cache;
pub mod storage;

/// Re-exported so downstream crates share one DataFusion version.
pub use datafusion;
