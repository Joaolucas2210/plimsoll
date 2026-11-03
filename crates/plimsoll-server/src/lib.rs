//! plimsoll-server as a library: routes, config, engine seam and metrics.
//! The binary (`main.rs`) only wires them together, so tests and the
//! integrator can build the router without a network socket.

pub mod app;
pub mod auth;
pub mod config;
pub mod engine;
pub mod lakehouse;
pub mod log;
pub mod metrics;
pub mod schedule;
pub mod system;
pub mod tables;
pub mod tls;
pub mod trace;
