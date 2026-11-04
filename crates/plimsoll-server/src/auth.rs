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
//! else sees only their own. `PLIMSOLL_AUTH_GROUPS=etl=batch,ana=interactive|batch`
//! binds principals to the resource groups they may use, the first being
//! their default; unlisted principals get `default` only (ADR 0023).
//! `PLIMSOLL_AUTH_GRANTS=ana=gold|silver,etl=*` lists the schemas each
//! principal may read; once set, unlisted principals read no data (ADR 0025).
//! `PLIMSOLL_AUTH_WRITE_GRANTS=etl=bronze|silver` lists the schemas each
//! principal may `INSERT INTO`; unlisted principals write nothing, and
//! unset means no writes at all (ADR 0075).
//! `PLIMSOLL_AUTH_DROP_GRANTS=etl=bronze` lists the schemas whose tables each
//! principal may `DROP`; a write grant does not include it (ADR 0089).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use ring::digest::{SHA256, digest};

use plimsoll_control::governance::DEFAULT_GROUP;
use plimsoll_core::datafusion::common::ResolvedTableReference;
use plimsoll_core::datafusion::error::{DataFusionError, Result as DfResult};
use plimsoll_core::engine::{AccessDenied, TableAccess};

use crate::config::ConfigError;

// ponytail: one `Auth` per process, built at boot; boxing `Tokens` would
// only move the size, not save any.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, PartialEq, Eq)]
pub enum Auth {
    /// Development only: `user` is taken from the request body as sent.
    Disabled,
    /// Token digest -> principal, and the principals with operator rights.
    Tokens {
        tokens: HashMap<[u8; 32], String>,
        operators: HashSet<String>,
        /// Principal -> resource groups it may use (first = its default).
        groups: HashMap<String, Vec<String>>,
        /// Principal -> readable schemas (`*` = all). `None`: not
        /// configured, every principal reads every schema.
        grants: Option<HashMap<String, HashSet<String>>>,
        /// Principal -> schemas it may `INSERT INTO` (ADR 0075).
        write_grants: HashMap<String, HashSet<String>>,
        /// Principal -> schemas whose tables it may `DROP` (ADR 0089).
        drop_grants: HashMap<String, HashSet<String>>,
        /// Principal -> `schema.table.column`s it may not read (ADR 0044).
        column_deny: HashMap<String, Arc<HashSet<String>>>,
        /// Principal -> `schema.table` -> SQL predicate its rows must
        /// satisfy (ADR 0045).
        row_filters: HashMap<String, Arc<HashMap<String, String>>>,
    },
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("Disabled"),
            Self::Tokens {
                tokens, operators, ..
            } => {
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
const GROUPS: &str = "PLIMSOLL_AUTH_GROUPS";
const GRANTS: &str = "PLIMSOLL_AUTH_GRANTS";
const WRITE_GRANTS: &str = "PLIMSOLL_AUTH_WRITE_GRANTS";
const DROP_GRANTS: &str = "PLIMSOLL_AUTH_DROP_GRANTS";
const COLUMN_DENY: &str = "PLIMSOLL_AUTH_COLUMN_DENY";
const ROW_FILTERS: &str = "PLIMSOLL_AUTH_ROW_FILTERS";

impl Auth {
    /// `has_group`: whether a resource group exists (bindings are checked
    /// against it at boot).
    pub fn from_lookup(
        lookup: &impl Fn(&str) -> Option<String>,
        has_group: &dyn Fn(&str) -> bool,
    ) -> Result<Self, ConfigError> {
        let err = |var, value: &str, reason| ConfigError {
            var,
            value: value.to_owned(),
            reason,
        };
        match lookup(MODE).as_deref().map(str::trim) {
            None | Some("required") => {}
            Some("disabled") => {
                for var in [
                    OPERATORS,
                    GROUPS,
                    GRANTS,
                    WRITE_GRANTS,
                    DROP_GRANTS,
                    COLUMN_DENY,
                    ROW_FILTERS,
                ] {
                    if lookup(var).is_some_and(|o| !o.trim().is_empty()) {
                        return Err(err(var, "", "needs PLIMSOLL_AUTH=required"));
                    }
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
        let mut groups = HashMap::new();
        for entry in lookup(GROUPS).unwrap_or_default().split(',').map(str::trim) {
            if entry.is_empty() {
                continue;
            }
            let shape = "expected `user=group|group` entries";
            let (user, list) = entry
                .split_once('=')
                .ok_or_else(|| err(GROUPS, entry, shape))?;
            let user = user.trim();
            if !tokens.values().any(|u| u == user) {
                return Err(err(GROUPS, user, "not a user in PLIMSOLL_AUTH_TOKENS"));
            }
            let list: Vec<String> = list.split('|').map(|g| g.trim().to_owned()).collect();
            if let Some(g) = list.iter().find(|g| !has_group(g)) {
                return Err(err(
                    GROUPS,
                    g,
                    "not a resource group (PLIMSOLL_RESOURCE_GROUPS)",
                ));
            }
            if groups.insert(user.to_owned(), list).is_some() {
                return Err(err(GROUPS, user, "user bound twice"));
            }
        }
        // `user=schema|schema` lists: readable (ADR 0025) and writable
        // (ADR 0075) schemas.
        let schema_grants = |var: &'static str| -> Result<_, ConfigError> {
            let Some(raw) = lookup(var) else {
                return Ok(None);
            };
            let mut grants = HashMap::new();
            for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
                let (user, list) = entry
                    .split_once('=')
                    .ok_or_else(|| err(var, entry, "expected `user=schema|schema` entries"))?;
                let user = user.trim();
                if !tokens.values().any(|u| u == user) {
                    return Err(err(var, user, "not a user in PLIMSOLL_AUTH_TOKENS"));
                }
                let schemas: HashSet<String> = list
                    .split('|')
                    .map(|s| s.trim().to_owned())
                    .filter(|s| !s.is_empty())
                    .collect();
                if grants.insert(user.to_owned(), schemas).is_some() {
                    return Err(err(var, user, "user granted twice"));
                }
            }
            Ok(Some(grants))
        };
        let grants = schema_grants(GRANTS)?;
        let write_grants = schema_grants(WRITE_GRANTS)?.unwrap_or_default();
        let drop_grants = schema_grants(DROP_GRANTS)?.unwrap_or_default();
        let mut column_deny = HashMap::new();
        for entry in lookup(COLUMN_DENY)
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
        {
            if entry.is_empty() {
                continue;
            }
            let shape = "expected `user=schema.table.column|...` entries";
            let (user, list) = entry
                .split_once('=')
                .ok_or_else(|| err(COLUMN_DENY, entry, shape))?;
            let user = user.trim();
            if !tokens.values().any(|u| u == user) {
                return Err(err(COLUMN_DENY, user, "not a user in PLIMSOLL_AUTH_TOKENS"));
            }
            let mut columns = HashSet::new();
            for column in list.split('|').map(str::trim).filter(|c| !c.is_empty()) {
                // A typo here would leave the column readable: fail instead.
                if column.split('.').count() != 3 || column.split('.').any(str::is_empty) {
                    return Err(err(COLUMN_DENY, column, shape));
                }
                columns.insert(column.to_owned());
            }
            if column_deny
                .insert(user.to_owned(), Arc::new(columns))
                .is_some()
            {
                return Err(err(COLUMN_DENY, user, "user listed twice"));
            }
        }
        let mut row_filters = HashMap::new();
        if let Some(raw) = lookup(ROW_FILTERS).filter(|r| !r.trim().is_empty()) {
            let shape = r#"expected {"user": {"schema.table": "<SQL predicate>"}}"#;
            let parsed: HashMap<String, HashMap<String, String>> =
                serde_json::from_str(&raw).map_err(|_| err(ROW_FILTERS, "", shape))?;
            for (user, tables) in parsed {
                if !tokens.values().any(|u| *u == user) {
                    return Err(err(
                        ROW_FILTERS,
                        &user,
                        "not a user in PLIMSOLL_AUTH_TOKENS",
                    ));
                }
                for (table, predicate) in &tables {
                    if table.split('.').count() != 2 || table.split('.').any(str::is_empty) {
                        return Err(err(ROW_FILTERS, table, "tables are `schema.table`"));
                    }
                    // Syntax now; columns are checked against the table at
                    // query time (a wrong one is an internal error).
                    let dialect =
                        plimsoll_core::datafusion::sql::sqlparser::dialect::GenericDialect {};
                    let parses =
                        plimsoll_core::datafusion::sql::sqlparser::parser::Parser::new(&dialect)
                            .try_with_sql(predicate)
                            .and_then(|mut p| p.parse_expr())
                            .is_ok();
                    if !parses {
                        return Err(err(
                            ROW_FILTERS,
                            table,
                            "the predicate is not a SQL expression",
                        ));
                    }
                }
                row_filters.insert(user, Arc::new(tables));
            }
        }
        Ok(Self::Tokens {
            tokens,
            operators,
            groups,
            grants,
            write_grants,
            drop_grants,
            column_deny,
            row_filters,
        })
    }

    /// Resource groups `principal` may use, its default first; `None` when
    /// auth is disabled (any existing group).
    pub fn groups_for(&self, principal: &str) -> Option<Vec<&str>> {
        let Self::Tokens { groups, .. } = self else {
            return None;
        };
        Some(match groups.get(principal) {
            Some(list) => list.iter().map(String::as_str).collect(),
            None => vec![DEFAULT_GROUP],
        })
    }

    /// Whose queries `principal` may see in `system.runtime.queries`.
    /// Whether data access is restricted per principal (ADR 0025).
    pub fn grants_enforced(&self) -> bool {
        matches!(
            self,
            Self::Tokens {
                grants: Some(_),
                ..
            }
        )
    }

    /// What `principal` may see: its rows in `system.runtime.queries`
    /// (operators: all) and the schemas it may read.
    pub fn viewer(&self, principal: &str) -> Viewer {
        let Self::Tokens {
            operators,
            grants,
            write_grants,
            drop_grants,
            column_deny,
            row_filters,
            ..
        } = self
        else {
            // Development: everything.
            return Viewer {
                only: None,
                schemas: None,
                writable: Some(["*".to_owned()].into()),
                droppable: Some(["*".to_owned()].into()),
                group: None,
                denied_columns: None,
                row_filters: None,
                tag: None,
                spill: Default::default(),
            };
        };
        Viewer {
            only: (!operators.contains(principal)).then(|| principal.to_owned()),
            schemas: grants
                .as_ref()
                .map(|g| g.get(principal).cloned().unwrap_or_default()),
            writable: write_grants.get(principal).cloned(),
            droppable: drop_grants.get(principal).cloned(),
            group: None,
            denied_columns: column_deny.get(principal).cloned(),
            row_filters: row_filters.get(principal).cloned(),
            tag: None,
            spill: Default::default(),
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
/// scope what they return (`system.runtime.queries`), and checked by the
/// engine on every table scan (ADR 0025).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Viewer {
    /// `Some(user)`: only that user's rows. `None`: all rows.
    pub only: Option<String>,
    /// Readable schemas (`*` = all). `None`: grants not configured.
    pub schemas: Option<HashSet<String>>,
    /// Schemas it may `INSERT INTO` (`*` = all). `None`: none (ADR 0075).
    pub writable: Option<HashSet<String>>,
    /// Schemas whose tables it may `DROP` (`*` = all). `None`: none (ADR
    /// 0089).
    pub droppable: Option<HashSet<String>>,
    /// The resource group the query runs in (memory share, ADR 0036).
    pub group: Option<String>,
    /// `schema.table.column`s this principal may not read (ADR 0044).
    pub denied_columns: Option<Arc<HashSet<String>>>,
    /// `schema.table` -> predicate on every row it reads (ADR 0045).
    pub row_filters: Option<Arc<HashMap<String, String>>>,
    /// The query's identity for its fragments on workers (ADR 0059).
    pub tag: Option<plimsoll_core::engine::QueryTag>,
    /// Where the query's spill is counted, finished or failed (ADR 0118).
    pub spill: plimsoll_core::engine::SpillSink,
}

impl TableAccess for Viewer {
    fn resource_group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    fn tag(&self) -> Option<plimsoll_core::engine::QueryTag> {
        self.tag.clone()
    }

    fn spill_sink(&self) -> Option<plimsoll_core::engine::SpillSink> {
        Some(self.spill.clone())
    }

    fn row_filter(&self, table: &ResolvedTableReference) -> Option<String> {
        // Time travel reads the same table (ADR 0028).
        let name = table.table.split('@').next().unwrap_or_default();
        self.row_filters
            .as_ref()?
            .get(&format!("{}.{name}", table.schema))
            .cloned()
    }

    fn check_columns(&self, table: &ResolvedTableReference, columns: &[&str]) -> DfResult<()> {
        let Some(denied) = &self.denied_columns else {
            return Ok(());
        };
        // `users@123` and `users@audit` are the same table's data (ADR 0028).
        let name = table.table.split('@').next().unwrap_or_default();
        let deny = |what: String| {
            Err(DataFusionError::External(Box::new(AccessDenied {
                table: format!("{}.{what}", table.catalog),
            })))
        };
        // `users$files` shows partition values, which may be a denied
        // column's (a table partitioned by it): withhold them (ADR 0032).
        if let Some(base) = name
            .strip_suffix("$files")
            .or_else(|| name.strip_suffix("$partitions"))
        {
            let prefix = format!("{}.{base}.", table.schema);
            if columns.contains(&"partition") && denied.iter().any(|c| c.starts_with(&prefix)) {
                return deny(format!("{}.{name}.partition (column)", table.schema));
            }
            return Ok(());
        }
        for column in columns {
            let qualified = format!("{}.{name}.{column}", table.schema);
            if denied.contains(&qualified) {
                return deny(format!("{qualified} (column)"));
            }
        }
        Ok(())
    }

    fn check_write(&self, table: &ResolvedTableReference) -> DfResult<()> {
        let allowed = self
            .writable
            .as_ref()
            .is_some_and(|w| w.contains("*") || w.contains(table.schema.as_ref()));
        if allowed && table.catalog.as_ref() != "system" {
            return Ok(());
        }
        Err(DataFusionError::External(Box::new(AccessDenied {
            table: format!("{table} (write)"),
        })))
    }

    fn check_drop(&self, table: &ResolvedTableReference) -> DfResult<()> {
        let allowed = self
            .droppable
            .as_ref()
            .is_some_and(|w| w.contains("*") || w.contains(table.schema.as_ref()));
        if allowed && table.catalog.as_ref() != "system" {
            return Ok(());
        }
        Err(DataFusionError::External(Box::new(AccessDenied {
            table: format!("{table} (drop)"),
        })))
    }

    fn check(&self, table: &ResolvedTableReference) -> DfResult<()> {
        // `orders$files` / `$snapshots` describe every tenant's data (file
        // paths, partition values, row counts): not for row-filtered
        // principals (ADR 0045).
        if let (Some(filters), Some((base, _))) = (&self.row_filters, table.table.split_once('$')) {
            let base = base.split('@').next().unwrap_or_default();
            if filters.contains_key(&format!("{}.{base}", table.schema)) {
                return Err(DataFusionError::External(Box::new(AccessDenied {
                    table: format!("{table} (metadata of a row-filtered table)"),
                })));
            }
        }
        let Some(schemas) = &self.schemas else {
            return Ok(());
        };
        // System tables scope their own rows (ADR 0022); information_schema
        // exposes names only (accepted, ADR 0025).
        let open =
            table.catalog.as_ref() == "system" || table.schema.as_ref() == "information_schema";
        if open || schemas.contains("*") || schemas.contains(table.schema.as_ref()) {
            return Ok(());
        }
        Err(DataFusionError::External(Box::new(AccessDenied {
            table: table.to_string(),
        })))
    }
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
pub(crate) mod tests {
    use super::*;

    pub fn hex(token: &str) -> String {
        token_digest(token)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    fn auth(vars: &[(&str, String)]) -> Result<Auth, ConfigError> {
        Auth::from_lookup(
            &|k| vars.iter().find(|(n, _)| *n == k).map(|(_, v)| v.clone()),
            &|g| ["default", "batch", "interactive"].contains(&g),
        )
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
        assert_eq!(a.viewer("ops").only, None);
        assert_eq!(a.viewer("ana").only, Some("ana".into()));
        // Development: sees and reads everything (and writes, ADR 0075).
        assert_eq!(
            Auth::Disabled.viewer("x"),
            Viewer {
                writable: Some(["*".to_owned()].into()),
                droppable: Some(["*".to_owned()].into()),
                ..Viewer::default()
            }
        );
        // Unknown operator (typo) and operators without auth fail boot.
        let e = auth(&[(TOKENS, tokens), (OPERATORS, "opz".into())]).unwrap_err();
        assert_eq!(e.var, OPERATORS);
        let e = auth(&[(MODE, "disabled".into()), (OPERATORS, "ops".into())]).unwrap_err();
        assert_eq!(e.var, OPERATORS);
    }

    #[test]
    fn principals_are_bound_to_resource_groups() {
        let tokens = format!("ana:sha256:{},etl:sha256:{}", hex("t1"), hex("t2"));
        let a = auth(&[
            (TOKENS, tokens.clone()),
            (GROUPS, "etl=batch, ana=interactive|batch".into()),
        ])
        .unwrap();
        assert_eq!(a.groups_for("etl"), Some(vec!["batch"]));
        assert_eq!(a.groups_for("ana"), Some(vec!["interactive", "batch"]));
        assert_eq!(a.groups_for("bob"), Some(vec!["default"]));
        assert_eq!(Auth::Disabled.groups_for("x"), None);
        for bad in ["etl=nightly", "zed=batch", "etl", "etl=batch,etl=default"] {
            let e = auth(&[(TOKENS, tokens.clone()), (GROUPS, bad.into())]).unwrap_err();
            assert_eq!(e.var, GROUPS, "{bad}");
        }
    }

    #[test]
    fn grants_limit_readable_schemas() {
        let tokens = format!(
            "ana:sha256:{},etl:sha256:{},bob:sha256:{}",
            hex("t1"),
            hex("t2"),
            hex("t3")
        );
        let t = |schema: &str| ResolvedTableReference {
            catalog: "datafusion".into(),
            schema: schema.into(),
            table: "t".into(),
        };
        let sys = ResolvedTableReference {
            catalog: "system".into(),
            schema: "runtime".into(),
            table: "queries".into(),
        };
        // Not configured: everyone reads everything.
        let open = auth(&[(TOKENS, tokens.clone())]).unwrap();
        assert!(open.viewer("bob").check(&t("gold")).is_ok());
        let a = auth(&[
            (TOKENS, tokens.clone()),
            (GRANTS, "ana=gold|silver, etl=*".into()),
        ])
        .unwrap();
        assert!(a.viewer("ana").check(&t("gold")).is_ok());
        assert!(a.viewer("ana").check(&t("bronze")).is_err());
        assert!(a.viewer("etl").check(&t("bronze")).is_ok());
        // Configured but unlisted: no data, still system and information_schema.
        assert!(a.viewer("bob").check(&t("gold")).is_err());
        assert!(a.viewer("bob").check(&sys).is_ok());
        assert!(a.viewer("bob").check(&t("information_schema")).is_ok());
        for bad in ["zed=gold", "ana", "ana=gold,ana=silver"] {
            let e = auth(&[(TOKENS, tokens.clone()), (GRANTS, bad.into())]).unwrap_err();
            assert_eq!(e.var, GRANTS, "{bad}");
        }
    }

    #[test]
    fn write_grants_name_the_schemas_a_principal_may_insert_into() {
        let tokens = format!("ana:sha256:{},etl:sha256:{}", hex("t1"), hex("t2"));
        let t = |catalog: &str, schema: &str| ResolvedTableReference {
            catalog: catalog.into(),
            schema: schema.into(),
            table: "t".into(),
        };
        // Unset: nobody writes, readers included.
        let none = auth(&[(TOKENS, tokens.clone())]).unwrap();
        assert!(
            none.viewer("ana")
                .check_write(&t("datafusion", "bronze"))
                .is_err()
        );
        let a = auth(&[
            (TOKENS, tokens.clone()),
            (WRITE_GRANTS, "etl=bronze|silver".into()),
        ])
        .unwrap();
        let etl = a.viewer("etl");
        assert!(etl.check_write(&t("datafusion", "bronze")).is_ok());
        assert!(etl.check_write(&t("datafusion", "gold")).is_err());
        // System tables are never written.
        assert!(etl.check_write(&t("system", "bronze")).is_err());
        assert!(
            a.viewer("ana")
                .check_write(&t("datafusion", "bronze"))
                .is_err()
        );
        // Development mode writes, as it reads, everything.
        let dev = Auth::Disabled.viewer("anyone");
        assert!(dev.check_write(&t("datafusion", "bronze")).is_ok());
        for bad in ["zed=bronze", "etl", "etl=a,etl=b"] {
            let e = auth(&[(TOKENS, tokens.clone()), (WRITE_GRANTS, bad.into())]).unwrap_err();
            assert_eq!(e.var, WRITE_GRANTS, "{bad}");
        }
        // Dropping is its own grant (ADR 0089): writing does not imply it.
        assert!(etl.check_drop(&t("datafusion", "bronze")).is_err());
        let d = auth(&[
            (TOKENS, tokens.clone()),
            (WRITE_GRANTS, "etl=bronze".into()),
            (DROP_GRANTS, "etl=bronze".into()),
        ])
        .unwrap();
        assert!(
            d.viewer("etl")
                .check_drop(&t("datafusion", "bronze"))
                .is_ok()
        );
        assert!(
            d.viewer("etl")
                .check_drop(&t("datafusion", "gold"))
                .is_err()
        );
        assert!(dev.check_drop(&t("datafusion", "bronze")).is_ok());
        let e = auth(&[(TOKENS, tokens.clone()), (DROP_GRANTS, "zed=bronze".into())]).unwrap_err();
        assert_eq!(e.var, DROP_GRANTS);
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

    #[test]
    fn column_deny_is_parsed_and_checked_per_principal() {
        let lookup = |extra: &'static str| {
            move |k: &str| match k {
                "PLIMSOLL_AUTH_TOKENS" => {
                    Some(format!("ana:sha256:{},bob:sha256:{}", hex("ta"), hex("tb")))
                }
                "PLIMSOLL_AUTH_COLUMN_DENY" => Some(extra.to_owned()),
                _ => None,
            }
        };
        let auth =
            Auth::from_lookup(&lookup("ana=gold.users.email|gold.users.ssn"), &|_| true).unwrap();
        let table = |t: &str| ResolvedTableReference {
            catalog: "datafusion".into(),
            schema: "gold".into(),
            table: t.into(),
        };
        let ana = auth.viewer("ana");
        assert!(ana.check_columns(&table("users"), &["id", "name"]).is_ok());
        let err = ana
            .check_columns(&table("users"), &["id", "email"])
            .unwrap_err();
        assert!(
            err.to_string().contains("gold.users.email (column)"),
            "{err}"
        );
        // Time travel reads the same data; other tables are unaffected.
        assert!(ana.check_columns(&table("users@123"), &["ssn"]).is_err());
        assert!(ana.check_columns(&table("orders"), &["email"]).is_ok());
        // Metadata tables: file paths are fine, partition values are not.
        assert!(
            ana.check_columns(&table("users$files"), &["file_path"])
                .is_ok()
        );
        assert!(
            ana.check_columns(&table("users$files"), &["partition"])
                .is_err()
        );
        assert!(
            auth.viewer("bob")
                .check_columns(&table("users"), &["email"])
                .is_ok()
        );

        for bad in [
            "ana=gold.users",
            "ana=email",
            "carol=gold.users.email",
            "ana=a..b",
        ] {
            let e = Auth::from_lookup(&lookup(bad), &|_| true).unwrap_err();
            assert_eq!(e.var, "PLIMSOLL_AUTH_COLUMN_DENY", "{bad}");
        }
    }

    #[test]
    fn row_filters_are_validated_and_cover_time_travel_and_metadata() {
        let lookup = |filters: &'static str| {
            move |k: &str| match k {
                "PLIMSOLL_AUTH_TOKENS" => Some(format!("ana:sha256:{}", hex("ta"))),
                "PLIMSOLL_AUTH_ROW_FILTERS" => Some(filters.to_owned()),
                _ => None,
            }
        };
        let auth = Auth::from_lookup(
            &lookup(r#"{"ana": {"gold.orders": "region = 'eu' AND NOT deleted"}}"#),
            &|_| true,
        )
        .unwrap();
        let t = |name: &str| ResolvedTableReference {
            catalog: "datafusion".into(),
            schema: "gold".into(),
            table: name.into(),
        };
        let ana = auth.viewer("ana");
        assert_eq!(
            ana.row_filter(&t("orders@123")).as_deref(),
            Some("region = 'eu' AND NOT deleted")
        );
        assert_eq!(ana.row_filter(&t("users")), None);
        assert!(ana.check(&t("orders$files")).is_err());
        assert!(ana.check(&t("orders$snapshots")).is_err());
        assert!(ana.check(&t("users$files")).is_ok());
        for bad in [
            "not json",
            r#"{"carol": {"gold.orders": "true"}}"#,
            r#"{"ana": {"orders": "true"}}"#,
            r#"{"ana": {"gold.orders": "region = "}}"#,
        ] {
            let e = Auth::from_lookup(&lookup(bad), &|_| true).unwrap_err();
            assert_eq!(e.var, "PLIMSOLL_AUTH_ROW_FILTERS", "{bad}");
        }
    }
}
