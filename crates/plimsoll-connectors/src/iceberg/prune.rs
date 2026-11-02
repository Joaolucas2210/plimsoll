//! Scan planning: skip manifests and data files that cannot match.
//!
//! Only provably-safe pruning (ADR 0010): a manifest or file is skipped when
//! its bounds show that *no* row can satisfy one of the AND-ed predicates.
//! Anything we cannot evaluate exactly (OR, casts, floats, decimals, sub-µs
//! literals, missing bounds) keeps the file. Filters are still re-applied
//! above the scan, so pruning only ever changes cost, never results.

use std::cmp::Ordering;
use std::collections::HashMap;

use chrono::{DateTime, Datelike};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Between, BinaryExpr, Expr, Operator};

/// Iceberg primitive types we can compare bounds for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Prim {
    Bool,
    Int,
    Long,
    Date,
    /// Microseconds since epoch (`timestamp` and `timestamptz`).
    Timestamp,
    String,
}

impl Prim {
    pub(super) fn from_iceberg(ty: &str) -> Option<Self> {
        Some(match ty {
            "boolean" => Self::Bool,
            "int" => Self::Int,
            "long" => Self::Long,
            "date" => Self::Date,
            "timestamp" | "timestamptz" => Self::Timestamp,
            "string" => Self::String,
            // float/double: Iceberg bounds exclude NaN, DataFusion orders NaN
            // above everything, so `x > 5` matches NaN rows the bounds hide.
            _ => return None,
        })
    }

    /// Iceberg single-value binary serialization -> comparable datum.
    fn decode(self, bytes: &[u8]) -> Option<Datum> {
        Some(match self {
            Self::Bool => Datum::Int(i64::from(*bytes.first()? != 0)),
            Self::Int | Self::Date => {
                Datum::Int(i64::from(i32::from_le_bytes(bytes.try_into().ok()?)))
            }
            Self::Long | Self::Timestamp => Datum::Int(i64::from_le_bytes(bytes.try_into().ok()?)),
            Self::String => Datum::Bytes(bytes.to_vec()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
enum Datum {
    Int(i64),
    /// UTF-8 bytes; Iceberg and Arrow both order strings bytewise.
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl Op {
    fn flip(self) -> Self {
        match self {
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
            Self::Eq => Self::Eq,
        }
    }

    /// Can some value in `[lo, hi]` satisfy `value <op> v`? Unknown bounds
    /// (None) or incomparable datums answer yes.
    fn may_match(self, lo: Option<&Datum>, hi: Option<&Datum>, v: &Datum) -> bool {
        let cmp = |b: Option<&Datum>| b.and_then(|b| b.partial_cmp(v));
        let (lo, hi) = (cmp(lo), cmp(hi));
        match self {
            Self::Eq => lo != Some(Ordering::Greater) && hi != Some(Ordering::Less),
            Self::Lt => lo != Some(Ordering::Greater) && lo != Some(Ordering::Equal),
            Self::LtEq => lo != Some(Ordering::Greater),
            Self::Gt => hi != Some(Ordering::Less) && hi != Some(Ordering::Equal),
            Self::GtEq => hi != Some(Ordering::Less),
        }
    }
}

#[derive(Debug, Clone)]
struct Pred {
    field_id: i64,
    prim: Prim,
    op: Op,
    value: Datum,
}

/// Monotonic partition transforms (non-decreasing in the source value).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
}

/// Lower/upper bound of one partition field across a manifest (raw bytes).
pub(super) type Summary = (Option<Vec<u8>>, Option<Vec<u8>>);

#[derive(Debug, Clone)]
pub(super) struct PartitionField {
    source_id: i64,
    transform: Transform,
}

/// `partition-specs[].fields` -> per position, `None` when not prunable
/// (bucket, truncate, void, ...).
pub(super) fn partition_spec(fields: &serde_json::Value) -> Vec<Option<PartitionField>> {
    fields
        .as_array()
        .into_iter()
        .flatten()
        .map(|f| {
            let transform = match f["transform"].as_str()? {
                "identity" => Transform::Identity,
                "year" => Transform::Year,
                "month" => Transform::Month,
                "day" => Transform::Day,
                "hour" => Transform::Hour,
                _ => return None,
            };
            Some(PartitionField {
                source_id: f["source-id"].as_i64()?,
                transform,
            })
        })
        .collect()
}

/// AND of the prunable predicates found in the scan filters.
#[derive(Debug, Default)]
pub(super) struct Pruner {
    preds: Vec<Pred>,
}

impl Pruner {
    /// `columns`: column name -> (field id, type) for the current schema.
    pub(super) fn new(filters: &[Expr], columns: &HashMap<String, (i64, Prim)>) -> Self {
        let mut preds = Vec::new();
        for f in filters {
            collect(f, columns, &mut preds);
        }
        Self { preds }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.preds.is_empty()
    }

    /// File-level: `lower`/`upper` are the data file's column bounds.
    pub(super) fn may_match_file(
        &self,
        lower: &HashMap<i64, Vec<u8>>,
        upper: &HashMap<i64, Vec<u8>>,
    ) -> bool {
        self.preds.iter().all(|p| {
            let lo = lower.get(&p.field_id).and_then(|b| p.prim.decode(b));
            let hi = upper.get(&p.field_id).and_then(|b| p.prim.decode(b));
            p.op.may_match(lo.as_ref(), hi.as_ref(), &p.value)
        })
    }

    /// Manifest-level: `summaries[i]` = (lower, upper) bound of partition
    /// field `spec[i]` across the manifest.
    pub(super) fn may_match_manifest(
        &self,
        spec: &[Option<PartitionField>],
        summaries: &[Summary],
    ) -> bool {
        self.preds.iter().all(|p| {
            spec.iter().zip(summaries).all(|(field, (lo, hi))| {
                let Some(field) = field.as_ref().filter(|f| f.source_id == p.field_id) else {
                    return true;
                };
                let Some((op, v, prim)) = transformed(p, field.transform) else {
                    return true;
                };
                let lo = lo.as_deref().and_then(|b| prim.decode(b));
                let hi = hi.as_deref().and_then(|b| prim.decode(b));
                op.may_match(lo.as_ref(), hi.as_ref(), &v)
            })
        })
    }
}

/// `col <op> v` on the source column -> `T(col) <op'> T(v)` on the partition
/// value. For a non-decreasing T, strict bounds become inclusive.
fn transformed(p: &Pred, t: Transform) -> Option<(Op, Datum, Prim)> {
    if t == Transform::Identity {
        return Some((p.op, p.value.clone(), p.prim));
    }
    let Datum::Int(v) = p.value else { return None };
    const DAY_US: i64 = 86_400_000_000;
    let days = match p.prim {
        Prim::Date => v,
        Prim::Timestamp => v.div_euclid(DAY_US),
        _ => return None,
    };
    let date = || DateTime::from_timestamp(days.checked_mul(86_400)?, 0).map(|d| d.date_naive());
    // Is `v` the first instant of its bucket (midnight, top of the hour, the
    // 1st of the month, Jan 1st)? Then `col < v` <=> `T(col) < T(v)`.
    let midnight = p.prim == Prim::Date || v.rem_euclid(DAY_US) == 0;
    let (tv, at_start) = match t {
        Transform::Day => (days, midnight),
        Transform::Hour if p.prim == Prim::Timestamp => (
            v.div_euclid(3_600_000_000),
            v.rem_euclid(3_600_000_000) == 0,
        ),
        Transform::Month => {
            let d = date()?;
            (
                i64::from(d.year() - 1970) * 12 + i64::from(d.month0()),
                midnight && d.day() == 1,
            )
        }
        Transform::Year => {
            let d = date()?;
            (i64::from(d.year() - 1970), midnight && d.ordinal() == 1)
        }
        _ => return None,
    };
    // Non-decreasing T: `>` can still match later in the same bucket.
    let op = match p.op {
        Op::Lt if at_start => Op::Lt,
        Op::Lt => Op::LtEq,
        Op::Gt => Op::GtEq,
        op => op,
    };
    // Temporal partition values are serialized as `int`.
    Some((op, Datum::Int(tv), Prim::Int))
}

fn collect(e: &Expr, columns: &HashMap<String, (i64, Prim)>, out: &mut Vec<Pred>) {
    match e {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            collect(left, columns, out);
            collect(right, columns, out);
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let op = match op {
                Operator::Eq => Op::Eq,
                Operator::Lt => Op::Lt,
                Operator::LtEq => Op::LtEq,
                Operator::Gt => Op::Gt,
                Operator::GtEq => Op::GtEq,
                _ => return,
            };
            match (left.as_ref(), right.as_ref()) {
                (e, Expr::Literal(v, _)) => {
                    if let Some(name) = path(e) {
                        push(&name, op, v, columns, out);
                    }
                }
                (Expr::Literal(v, _), e) => {
                    if let Some(name) = path(e) {
                        push(&name, op.flip(), v, columns, out);
                    }
                }
                _ => {}
            }
        }
        Expr::Between(Between {
            expr,
            negated: false,
            low,
            high,
        }) => {
            if let (Some(name), Expr::Literal(lo, _), Expr::Literal(hi, _)) =
                (path(expr), low.as_ref(), high.as_ref())
            {
                push(&name, Op::GtEq, lo, columns, out);
                push(&name, Op::LtEq, hi, columns, out);
            }
        }
        _ => {}
    }
}

/// `col` -> `"col"`, `col['a']['b']` (`get_field`) -> `"col.a.b"`: the keys
/// of the prunable-column map, struct children included (ADR 0048).
fn path(e: &Expr) -> Option<String> {
    match e {
        Expr::Column(c) => Some(c.name.clone()),
        Expr::ScalarFunction(f) if f.func.name() == "get_field" => {
            let [inner, keys @ ..] = f.args.as_slice() else {
                return None;
            };
            let mut out = path(inner)?;
            for key in keys {
                let Expr::Literal(ScalarValue::Utf8(Some(k)), _) = key else {
                    return None;
                };
                out.push('.');
                out.push_str(k);
            }
            Some(out)
        }
        _ => None,
    }
}

fn push(
    name: &str,
    op: Op,
    v: &ScalarValue,
    columns: &HashMap<String, (i64, Prim)>,
    out: &mut Vec<Pred>,
) {
    let Some(&(field_id, prim)) = columns.get(name) else {
        return;
    };
    if let Some(value) = literal(prim, v) {
        out.push(Pred {
            field_id,
            prim,
            op,
            value,
        });
    }
}

/// Literal -> datum in the column's bound encoding, only when exact.
fn literal(prim: Prim, v: &ScalarValue) -> Option<Datum> {
    use ScalarValue as S;
    let int = |x: Option<i64>| x.map(Datum::Int);
    match (prim, v) {
        (Prim::Bool, S::Boolean(b)) => int(b.map(i64::from)),
        (Prim::Int | Prim::Long, S::Int8(x)) => int(x.map(i64::from)),
        (Prim::Int | Prim::Long, S::Int16(x)) => int(x.map(i64::from)),
        (Prim::Int | Prim::Long, S::Int32(x)) => int(x.map(i64::from)),
        (Prim::Int | Prim::Long, S::Int64(x)) => int(*x),
        (Prim::Int | Prim::Long, S::UInt8(x)) => int(x.map(i64::from)),
        (Prim::Int | Prim::Long, S::UInt16(x)) => int(x.map(i64::from)),
        (Prim::Int | Prim::Long, S::UInt32(x)) => int(x.map(i64::from)),
        (Prim::Int | Prim::Long, S::UInt64(x)) => int(x.and_then(|x| i64::try_from(x).ok())),
        (Prim::Date, S::Date32(d)) => int(d.map(i64::from)),
        (Prim::Timestamp, S::TimestampSecond(x, _)) => {
            int(x.and_then(|x| x.checked_mul(1_000_000)))
        }
        (Prim::Timestamp, S::TimestampMillisecond(x, _)) => {
            int(x.and_then(|x| x.checked_mul(1_000)))
        }
        (Prim::Timestamp, S::TimestampMicrosecond(x, _)) => int(*x),
        // Only exact conversions: a sub-µs literal would need op rewriting.
        (Prim::Timestamp, S::TimestampNanosecond(x, _)) => {
            int(x.filter(|x| x % 1_000 == 0).map(|x| x / 1_000))
        }
        (Prim::String, S::Utf8(s) | S::LargeUtf8(s) | S::Utf8View(s)) => {
            s.as_ref().map(|s| Datum::Bytes(s.as_bytes().to_vec()))
        }
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    fn columns() -> HashMap<String, (i64, Prim)> {
        HashMap::from([
            ("id".to_owned(), (1, Prim::Long)),
            ("city".to_owned(), (2, Prim::String)),
            ("ts".to_owned(), (3, Prim::Timestamp)),
            ("score".to_owned(), (4, Prim::Long)),
        ])
    }

    fn bounds(pairs: &[(i64, Vec<u8>)]) -> HashMap<i64, Vec<u8>> {
        pairs.iter().cloned().collect()
    }

    fn long(v: i64) -> Vec<u8> {
        v.to_le_bytes().to_vec()
    }

    #[test]
    fn file_bounds_prune_only_when_provably_empty() {
        // File holds id in [10, 20], city in ["b", "d"].
        let lo = bounds(&[(1, long(10)), (2, b"b".to_vec())]);
        let hi = bounds(&[(1, long(20)), (2, b"d".to_vec())]);
        let keep = |e: Expr| Pruner::new(&[e], &columns()).may_match_file(&lo, &hi);

        assert!(!keep(col("id").gt(lit(20i64))));
        assert!(keep(col("id").gt_eq(lit(20i64))));
        assert!(!keep(col("id").lt(lit(10i64))));
        assert!(keep(col("id").lt_eq(lit(10i64))));
        assert!(!keep(col("id").eq(lit(21i64))));
        assert!(keep(col("id").eq(lit(15i32)))); // narrower literal type
        assert!(!keep(lit(5i64).gt(col("id")))); // literal on the left
        assert!(!keep(col("id").between(lit(30i64), lit(40i64))));
        assert!(!keep(col("id").gt(lit(5i64)).and(col("city").eq(lit("z")))));
        assert!(keep(col("city").eq(lit("c"))));

        // Not provable -> keep.
        assert!(keep(col("id").gt(lit(20i64)).or(col("id").lt(lit(0i64)))));
        assert!(keep(col("score").gt(lit(1_000i64)))); // no bounds for score
        assert!(keep(col("nope").gt(lit(1i64))));
        assert!(keep(col("id").not_eq(lit(15i64))));
    }

    #[test]
    fn nanosecond_literals_are_only_used_when_exact() {
        let us = 1_700_000_000_000_000i64;
        let lo = bounds(&[(3, long(us))]);
        let hi = bounds(&[(3, long(us))]);
        let ts = |ns: i64| lit(ScalarValue::TimestampNanosecond(Some(ns), None));
        let keep = |e: Expr| Pruner::new(&[e], &columns()).may_match_file(&lo, &hi);
        assert!(!keep(col("ts").gt(ts(us * 1_000))));
        assert!(keep(col("ts").lt(ts(us * 1_000 + 1)))); // sub-µs: not pruned
    }

    #[test]
    fn manifest_summaries_prune_through_monotonic_transforms() {
        let spec = partition_spec(&serde_json::json!([
            {"source-id": 3, "field-id": 1000, "name": "ts_day", "transform": "day"},
            {"source-id": 2, "field-id": 1001, "name": "city_bucket", "transform": "bucket[4]"},
        ]));
        let day = |d: i32| Some(d.to_le_bytes().to_vec());
        // Manifest covers days [19_700, 19_710] since epoch (December 2023).
        let summaries = [(day(19_700), day(19_710)), (None, None)];
        let at_day = |d: i64| {
            lit(ScalarValue::TimestampMicrosecond(
                Some(d * 86_400_000_000),
                None,
            ))
        };
        let keep = |e: Expr| Pruner::new(&[e], &columns()).may_match_manifest(&spec, &summaries);

        assert!(!keep(col("ts").gt_eq(at_day(19_711))));
        // ts > start-of-day 19_710 can still match later that same day.
        assert!(keep(col("ts").gt(at_day(19_710))));
        assert!(!keep(col("ts").lt(at_day(19_700)))); // midnight: strict stays strict
        assert!(keep(col("ts").lt_eq(at_day(19_700))));
        let noon = |d: i64| {
            lit(ScalarValue::TimestampMicrosecond(
                Some(d * 86_400_000_000 + 43_200_000_000),
                None,
            ))
        };
        assert!(keep(col("ts").lt(noon(19_700))));
        assert!(!keep(col("ts").lt(noon(19_699))));
        assert!(keep(col("city").eq(lit("x")))); // bucket: not prunable

        let month_spec = partition_spec(&serde_json::json!([
            {"source-id": 3, "field-id": 1000, "name": "ts_month", "transform": "month"}]));
        // Months since 1970: 647 = 2023-12.
        let m = [(day(647), day(647))];
        let keep_m = |e: Expr| Pruner::new(&[e], &columns()).may_match_manifest(&month_spec, &m);
        assert!(keep_m(col("ts").eq(at_day(19_700))));
        assert!(!keep_m(col("ts").gt_eq(at_day(19_723)))); // day 19_723 = 2024-01-01
    }

    #[test]
    fn struct_children_are_paths() {
        use datafusion::functions::core::expr_fn::get_field;
        use datafusion::prelude::col;
        assert_eq!(path(&col("id")).as_deref(), Some("id"));
        assert_eq!(path(&get_field(col("s"), "a")).as_deref(), Some("s.a"));
        assert_eq!(
            path(&get_field(get_field(col("s"), "t"), "b")).as_deref(),
            Some("s.t.b")
        );
        assert_eq!(path(&(col("a") + col("b"))), None);
    }
}
