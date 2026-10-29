//! plimsoll-server as a library: routes, config, engine seam and metrics.
//! The binary (`main.rs`) only wires them together, so tests and the
//! integrator can build the router without a network socket.

pub mod app;
pub mod config;
pub mod engine;
pub mod metrics;
pub mod system;
pub mod tables;
