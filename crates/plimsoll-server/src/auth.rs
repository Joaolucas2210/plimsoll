//! Bearer-token authentication for the SQL endpoint (ADR 0021).
//!
//! `PLIMSOLL_AUTH_TOKENS=ana:sha256:<hex>,etl:sha256:<hex>` maps the SHA-256
//! digest of each token to a principal; raw tokens never sit in config, so an
//! env dump (`docker inspect`, `/proc/<pid>/environ`) yields no usable token.
//! A presented token is hashed and looked up: the lookup compares digests of
//! a secret, which leaks nothing an attacker can use, so no constant-time
//! comparison is needed. Secure by default: without tokens the server
//! refuses to start unless `PLIMSOLL_AUTH=disabled`.
//!
//! Authorization (ADR 0022): `PLIMSOLL_AUTH_OPERATORS=ana,ops` names the
//! principals that see every query in `system.runtime.queries`; everyone
//! else sees only their own.

use std::collections::{HashMap, HashSet};
use std::fmt;

use ring::digest::{SHA256, digest};

use crate::config::ConfigError;

#[derive(Clone, PartialEq, Eq)]
pub enum Auth {
    /// Development only: `user` is taken from the request body as sent.
    Disabled,
    /// Token digest -> principal, and the principals with operator rights.
    Tokens {
        tokens: HashMap<[u8; 32], String>,
        operators: HashSet<String>,
    },
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("Disabled"),
            Self::Tokens { tokens, operators } => {
                let mut users: Vec<&String> = tokens.values().collect();
                users.sort();
                users.dedup();
                let mut ops: Vec<&String> = operators.iter().collect();
                ops.sort();
                f.debug_struct("Tokens")
                    .field("users", &users)
                    .field("operators", &ops)
                    .finish()
            }
        }
    }
}

/// Why a request was not authenticated (both are HTTP 401).
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("missing bearer token")]
    Missing,
    #[error("invalid bearer token")]
    Invalid,
}

const MODE: &str = "PLIMSOLL_AUTH";
const TOKENS: &str = "PLIMSOLL_AUTH_TOKENS";
const OPERATORS: &str = "PLIMSOLL_AUTH_OPERATORS";

impl Auth {
    pub fn from_lookup(lookup: &impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let err = |var, value: &str, reason| ConfigError {
            var,
            value: value.to_owned(),
            reason,
        };
        match lookup(MODE).as_deref().map(str::trim) {
            None | Some("required") => {}
            Some("disabled") => {
                if lookup(OPERATORS).is_some_and(|o| !o.trim().is_empty()) {
                    return Err(err(OPERATORS, "", "operators need PLIMSOLL_AUTH=required"));
                }
                return Ok(Self::Disabled);
            }
            Some(other) => return Err(err(MODE, other, "expected `required` or `disabled`")),
        }
        let raw = lookup(TOKENS).unwrap_or_default();
        let mut tokens = HashMap::new();
        for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            // Never echo the entry: a raw token pasted by mistake must not
            // reach the logs through the config error.
            let redacted = |reason| err(TOKENS, "<redacted>", reason);
            let (user, hex) = entry
                .split_once(":sha256:")
                .ok_or_else(|| redacted("expected `user:sha256:<64 hex chars>` entries"))?;
            let user = user.trim();
            if user.is_empty() {
                return Err(redacted("empty user"));
            }
            let digest = parse_digest(hex.trim())
                .ok_or_else(|| redacted("expected `user:sha256:<64 hex chars>` entries"))?;
            if tokens.insert(digest, user.to_owned()).is_some() {
                return Err(redacted("two users share a token"));
            }
        }
        if tokens.is_empty() {
            return Err(err(
                TOKENS,
                "",
                "at least one token is required (or set PLIMSOLL_AUTH=disabled)",
            ));
        }
        let mut operators = HashSet::new();
        for op in lookup(OPERATORS).unwrap_or_default().split(',') {
            let op = op.trim();
            if op.is_empty() {
                continue;
            }
            // A typo would silently demote the operator: fail instead.
            if !tokens.values().any(|u| u == op) {
                return Err(err(OPERATORS, op, "not a user in PLIMSOLL_AUTH_TOKENS"));
            }
            operators.insert(op.to_owned());
        }
        Ok(Self::Tokens { tokens, operators })
    }

    /// Whose queries `principal` may see in `system.runtime.queries`.
    pub fn viewer(&self, principal: &str) -> Viewer {
        match self {
            Self::Tokens { operators, .. } if !operators.contains(principal) => Viewer {
                only: Some(principal.to_owned()),
            },
            // Operators, and everyone when auth is disabled (development).
            _ => Viewer { only: None },
        }
    }

    /// The principal for an `Authorization` header value; `None` when auth
    /// is disabled.
    pub fn principal(&self, authorization: Option<&str>) -> Result<Option<&str>, AuthError> {
        let Self::Tokens { tokens, .. } = self else {
            return Ok(None);
        };
        let header = authorization.ok_or(AuthError::Missing)?;
        let (scheme, token) = header.trim().split_once(' ').ok_or(AuthError::Missing)?;
        let token = token.trim();
        if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
            return Err(AuthError::Missing);
        }
        tokens
            .get(&token_digest(token))
            .map(|user| Some(user.as_str()))
            .ok_or(AuthError::Invalid)
    }
}

/// Who is asking, carried into the query's session config so providers can
/// scope what they return (`system.runtime.queries`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Viewer {
    /// `Some(user)`: only that user's rows. `None`: all rows.
    pub only: Option<String>,
}

/// SHA-256 of a token, as stored in `PLIMSOLL_AUTH_TOKENS`
/// (`printf %s "$TOKEN" | sha256sum`).
pub fn token_digest(token: &str) -> [u8; 32] {
    let mut out = [0; 32];
    out.copy_from_slice(digest(&SHA256, token.as_bytes()).as_ref());
    out
}

fn parse_digest(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 || !hex.is_ascii() {
        return None;
    }
    let mut out = [0; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    pub fn hex(token: &str) -> String {
        token_digest(token)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    fn auth(vars: &[(&str, String)]) -> Result<Auth, ConfigError> {
        Auth::from_lookup(&|k| vars.iter().find(|(n, _)| *n == k).map(|(_, v)| v.clone()))
    }

    #[test]
    fn digest_matches_sha256sum() {
        // printf %s secret | sha256sum
        assert_eq!(
            hex("secret"),
            "2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b"
        );
    }

    #[test]
    fn config_is_secure_by_default_and_never_echoes_tokens() {
        assert_eq!(auth(&[]).unwrap_err().var, TOKENS);
        assert_eq!(auth(&[(MODE, "disabled".into())]).unwrap(), Auth::Disabled);
        assert_eq!(auth(&[(MODE, "off".into())]).unwrap_err().var, MODE);
        // A raw token pasted instead of its digest is rejected, not logged.
        let e = auth(&[(TOKENS, "ana:my-raw-token".into())]).unwrap_err();
        assert!(!e.to_string().contains("my-raw-token"), "{e}");
        let dup = format!("ana:sha256:{h},bob:sha256:{h}", h = hex("t"));
        assert!(auth(&[(TOKENS, dup)]).is_err());
        let ok = auth(&[(TOKENS, format!(" ana:sha256:{} ", hex("t1")))]).unwrap();
        assert_eq!(
            format!("{ok:?}"),
            "Tokens { users: [\"ana\"], operators: [] }",
            "Debug shows users only"
        );
    }

    #[test]
    fn operators_see_all_queries_others_only_their_own() {
        let tokens = format!("ana:sha256:{},ops:sha256:{}", hex("t1"), hex("t2"));
        let a = auth(&[(TOKENS, tokens.clone()), (OPERATORS, "ops".into())]).unwrap();
        assert_eq!(a.viewer("ops"), Viewer { only: None });
        assert_eq!(
            a.viewer("ana"),
            Viewer {
                only: Some("ana".into())
            }
        );
        assert_eq!(Auth::Disabled.viewer("x"), Viewer { only: None });
        // Unknown operator (typo) and operators without auth fail boot.
        let e = auth(&[(TOKENS, tokens), (OPERATORS, "opz".into())]).unwrap_err();
        assert_eq!(e.var, OPERATORS);
        let e = auth(&[(MODE, "disabled".into()), (OPERATORS, "ops".into())]).unwrap_err();
        assert_eq!(e.var, OPERATORS);
    }

    #[test]
    fn principal_comes_from_the_token() {
        let a = auth(&[(
            TOKENS,
            format!("ana:sha256:{},etl:sha256:{}", hex("t1"), hex("t2")),
        )])
        .unwrap();
        assert_eq!(a.principal(Some("Bearer t1")), Ok(Some("ana")));
        assert_eq!(a.principal(Some("bearer  t2 ")), Ok(Some("etl")));
        assert_eq!(a.principal(Some("Bearer t3")), Err(AuthError::Invalid));
        assert_eq!(a.principal(Some("Basic dDE=")), Err(AuthError::Missing));
        assert_eq!(a.principal(Some("Bearer ")), Err(AuthError::Missing));
        assert_eq!(a.principal(None), Err(AuthError::Missing));
        assert_eq!(Auth::Disabled.principal(None), Ok(None));
    }
}
