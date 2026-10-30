//! Server configuration. Env only, fail fast: an invalid value aborts startup
//! instead of silently falling back to a default.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use plimsoll_control::governance::{AdmissionConfig, Limits};

use crate::auth::Auth;

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
    /// Who may run SQL (ADR 0021).
    pub auth: Auth,
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
        let admission = AdmissionConfig {
            global,
            // `default` gets the global limits unless configured.
            per_group: global,
            per_user: Limits {
                max_running: user_running,
                max_queued,
            },
            groups: resource_groups(&lookup)?,
        };
        let auth = Auth::from_lookup(&lookup, &|g| admission.has_group(g))?;
        Ok(Self {
            bind,
            memory_limit_bytes,
            admission,
            query_timeout: Duration::from_millis(timeout_ms),
            auth,
        })
    }
}

/// `PLIMSOLL_RESOURCE_GROUPS=interactive=4/16,batch=2/64`: per group, max
/// running / max queued (ADR 0023).
fn resource_groups(
    lookup: &impl Fn(&str) -> Option<String>,
) -> Result<HashMap<String, Limits>, ConfigError> {
    const VAR: &str = "PLIMSOLL_RESOURCE_GROUPS";
    let mut groups = HashMap::new();
    for entry in lookup(VAR).unwrap_or_default().split(',').map(str::trim) {
        if entry.is_empty() {
            continue;
        }
        let err = |reason| ConfigError {
            var: VAR,
            value: entry.to_owned(),
            reason,
        };
        let shape = "expected `name=<running>/<queued>` entries";
        let (name, limits) = entry.split_once('=').ok_or_else(|| err(shape))?;
        let (running, queued) = limits.split_once('/').ok_or_else(|| err(shape))?;
        let name = name.trim();
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(err("group names are [A-Za-z0-9_-]+"));
        }
        let limits = Limits {
            max_running: running.trim().parse().map_err(|_| err(shape))?,
            max_queued: queued.trim().parse().map_err(|_| err(shape))?,
        };
        if limits.max_running == 0 {
            return Err(err("max running must be greater than zero"));
        }
        if groups.insert(name.to_owned(), limits).is_some() {
            return Err(err("group defined twice"));
        }
    }
    Ok(groups)
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

    /// Auth is required by default; these tests are about other settings.
    fn cfg(pairs: &[(&str, &str)]) -> Result<Config, ConfigError> {
        Config::from_lookup(|k| {
            pairs
                .iter()
                .chain(&[("PLIMSOLL_AUTH", "disabled")])
                .find(|(n, _)| *n == k)
                .map(|(_, v)| (*v).to_owned())
        })
    }

    #[test]
    fn resource_groups_are_parsed_and_validated() {
        let c = cfg(&[("PLIMSOLL_RESOURCE_GROUPS", " batch=2/64 , interactive=4/0")]).unwrap();
        let g = &c.admission.groups;
        assert_eq!((g["batch"].max_running, g["batch"].max_queued), (2, 64));
        assert_eq!(g["interactive"].max_queued, 0);
        assert!(c.admission.has_group("default"));
        for bad in [
            "batch",
            "batch=2",
            "batch=0/1",
            "b@d=1/1",
            "a=1/1,a=2/2",
            "x=one/1",
        ] {
            let e = cfg(&[("PLIMSOLL_RESOURCE_GROUPS", bad)]).unwrap_err();
            assert_eq!(e.var, "PLIMSOLL_RESOURCE_GROUPS", "{bad}");
        }
    }

    #[test]
    fn auth_is_required_by_default() {
        let err = Config::from_lookup(|_| None).unwrap_err();
        assert_eq!(err.var, "PLIMSOLL_AUTH_TOKENS");
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
