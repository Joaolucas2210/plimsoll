//! Server configuration. Env only, fail fast: an invalid value aborts startup
//! instead of silently falling back to a default.

use std::net::SocketAddr;
use std::time::Duration;

use plimsoll_control::governance::{AdmissionConfig, Limits};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid {var}={value:?}: {reason}")]
pub struct ConfigError {
    pub var: &'static str,
    pub value: String,
    pub reason: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub bind: SocketAddr,
    /// Ceiling for the compute plane's memory pool (CeilingPool).
    pub memory_limit_bytes: u64,
    pub admission: AdmissionConfig,
    /// Deadline covering queue wait + execution.
    pub query_timeout: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let bind = parse(
            &lookup,
            "PLIMSOLL_BIND",
            "127.0.0.1:8080".parse().ok(),
            "expected host:port",
        )?;
        let memory_limit_bytes = positive(&lookup, "PLIMSOLL_MEMORY_LIMIT_BYTES", 1 << 30)?;
        let max_running = positive(&lookup, "PLIMSOLL_MAX_RUNNING", 8)?;
        let max_queued: usize = parse(
            &lookup,
            "PLIMSOLL_MAX_QUEUED",
            Some(64),
            "expected a non-negative integer",
        )?;
        let user_running = positive(&lookup, "PLIMSOLL_MAX_RUNNING_PER_USER", max_running)?;
        let timeout_ms = positive(&lookup, "PLIMSOLL_QUERY_TIMEOUT_MS", 30_000)?;

        let global = Limits {
            max_running,
            max_queued,
        };
        Ok(Self {
            bind,
            memory_limit_bytes,
            admission: AdmissionConfig {
                global,
                // ponytail: one resource group ("default") and no group
                // limits beyond the global ones until groups are configurable.
                per_group: global,
                per_user: Limits {
                    max_running: user_running,
                    max_queued,
                },
            },
            query_timeout: Duration::from_millis(timeout_ms),
        })
    }
}

fn parse<T: std::str::FromStr>(
    lookup: &impl Fn(&str) -> Option<String>,
    var: &'static str,
    default: Option<T>,
    reason: &'static str,
) -> Result<T, ConfigError> {
    let err = |value: String| ConfigError { var, value, reason };
    match lookup(var) {
        Some(v) => v.trim().parse().map_err(|_| err(v)),
        None => default.ok_or_else(|| err(String::new())),
    }
}

fn positive<T>(
    lookup: &impl Fn(&str) -> Option<String>,
    var: &'static str,
    default: T,
) -> Result<T, ConfigError>
where
    T: std::str::FromStr + PartialEq + Default,
{
    let v: T = parse(lookup, var, Some(default), "expected a positive integer")?;
    if v == T::default() {
        return Err(ConfigError {
            var,
            value: "0".into(),
            reason: "must be greater than zero",
        });
    }
    Ok(v)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn cfg(pairs: &[(&str, &str)]) -> Result<Config, ConfigError> {
        Config::from_lookup(|k| {
            pairs
                .iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| (*v).to_owned())
        })
    }

    #[test]
    fn defaults_are_valid() {
        let c = cfg(&[]).unwrap();
        assert_eq!(c.bind.port(), 8080);
        assert_eq!(c.admission.global.max_running, 8);
        assert_eq!(c.query_timeout, Duration::from_secs(30));
    }

    #[test]
    fn invalid_values_fail_fast() {
        assert_eq!(
            cfg(&[("PLIMSOLL_MAX_RUNNING", "0")]).unwrap_err().var,
            "PLIMSOLL_MAX_RUNNING"
        );
        assert!(cfg(&[("PLIMSOLL_BIND", "nope")]).is_err());
        assert!(cfg(&[("PLIMSOLL_MEMORY_LIMIT_BYTES", "-1")]).is_err());
        assert!(cfg(&[("PLIMSOLL_QUERY_TIMEOUT_MS", "abc")]).is_err());
        assert_eq!(
            cfg(&[("PLIMSOLL_MAX_QUEUED", "0")])
                .unwrap()
                .admission
                .global
                .max_queued,
            0
        );
    }
}
