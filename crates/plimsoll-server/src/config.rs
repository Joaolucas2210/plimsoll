//! Server configuration. Env only, fail fast: an invalid value aborts startup
//! instead of silently falling back to a default.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use plimsoll_control::governance::{AdmissionConfig, Limits};

use crate::auth::Auth;
use crate::trace::OtlpConfig;

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
    /// SIGTERM: how long to keep serving after `/readyz` turns 503.
    pub drain_delay: Duration,
    /// Who may run SQL (ADR 0021).
    pub auth: Auth,
    /// Trace export; `None` = traces are only propagated (ADR 0031).
    pub otlp: Option<OtlpConfig>,
    /// How often catalog listings are refreshed; zero = boot only (ADR 0033).
    pub catalog_refresh: Duration,
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
        // Longer than a typical readiness probe period (Kubernetes: 10 s
        // period, often 5 s); 0 disables the delay.
        let drain_ms: u64 = parse(
            &lookup,
            "PLIMSOLL_DRAIN_DELAY_MS",
            Some(5_000),
            "expected a non-negative integer",
        )?;

        let refresh_ms: u64 = parse(
            &lookup,
            "PLIMSOLL_CATALOG_REFRESH_MS",
            Some(60_000),
            "expected a non-negative integer",
        )?;

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
            drain_delay: Duration::from_millis(drain_ms),
            auth,
            otlp: otlp(&lookup)?,
            catalog_refresh: Duration::from_millis(refresh_ms),
        })
    }
}

/// The standard OpenTelemetry variables, so a deployment's existing OTel
/// settings apply: `OTEL_EXPORTER_OTLP_ENDPOINT` (http/protobuf-style base
/// URL; Plimsoll sends JSON), `OTEL_EXPORTER_OTLP_HEADERS=k=v,k2=v2`,
/// `OTEL_SERVICE_NAME` (default `plimsoll`).
fn otlp(lookup: &impl Fn(&str) -> Option<String>) -> Result<Option<OtlpConfig>, ConfigError> {
    let Some(endpoint) = lookup("OTEL_EXPORTER_OTLP_ENDPOINT") else {
        return Ok(None);
    };
    let endpoint = endpoint.trim().to_owned();
    if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
        return Err(ConfigError {
            var: "OTEL_EXPORTER_OTLP_ENDPOINT",
            value: endpoint,
            reason: "expected an http:// or https:// URL",
        });
    }
    const VAR: &str = "OTEL_EXPORTER_OTLP_HEADERS";
    let mut headers = Vec::new();
    for entry in lookup(VAR).unwrap_or_default().split(',') {
        if entry.trim().is_empty() {
            continue;
        }
        // The value is a secret: never echoed in the error.
        let err = |reason| ConfigError {
            var: VAR,
            value: entry
                .split('=')
                .next()
                .unwrap_or_default()
                .trim()
                .to_owned(),
            reason,
        };
        let (k, v) = entry
            .split_once('=')
            .ok_or_else(|| err("expected `name=value` entries"))?;
        let (k, v) = (k.trim(), v.trim());
        // ponytail: values are taken verbatim; the spec allows %-encoding,
        // decode it when a collector's key needs reserved characters.
        if axum::http::HeaderName::from_bytes(k.as_bytes()).is_err()
            || axum::http::HeaderValue::from_str(v).is_err()
        {
            return Err(err("invalid header name or value"));
        }
        headers.push((k.to_owned(), v.to_owned()));
    }
    let service_name = lookup("OTEL_SERVICE_NAME")
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "plimsoll".to_owned());
    Ok(Some(OtlpConfig {
        endpoint,
        headers,
        service_name,
    }))
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

    #[test]
    fn otlp_export_uses_the_standard_otel_variables() {
        assert_eq!(cfg(&[]).unwrap().otlp, None);
        let c = cfg(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318"),
            ("OTEL_EXPORTER_OTLP_HEADERS", "x-api-key=s3cr3t, x-tenant=a"),
        ])
        .unwrap()
        .otlp
        .unwrap();
        assert_eq!(c.endpoint, "http://collector:4318");
        assert_eq!(c.service_name, "plimsoll");
        assert_eq!(c.headers[1], ("x-tenant".into(), "a".into()));
        assert!(!format!("{c:?}").contains("s3cr3t"));

        let e = cfg(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "collector:4318")]).unwrap_err();
        assert_eq!(e.var, "OTEL_EXPORTER_OTLP_ENDPOINT");
        let e = cfg(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "https://c"),
            ("OTEL_EXPORTER_OTLP_HEADERS", "bad header=s3cr3t"),
        ])
        .unwrap_err();
        assert_eq!(e.var, "OTEL_EXPORTER_OTLP_HEADERS");
        assert!(!e.to_string().contains("s3cr3t"), "{e}");
    }

    #[test]
    fn catalog_refresh_defaults_to_a_minute_and_zero_disables_it() {
        assert_eq!(cfg(&[]).unwrap().catalog_refresh, Duration::from_secs(60));
        let c = cfg(&[("PLIMSOLL_CATALOG_REFRESH_MS", "0")]).unwrap();
        assert!(c.catalog_refresh.is_zero());
        let e = cfg(&[("PLIMSOLL_CATALOG_REFRESH_MS", "-1")]).unwrap_err();
        assert_eq!(e.var, "PLIMSOLL_CATALOG_REFRESH_MS");
    }
}
