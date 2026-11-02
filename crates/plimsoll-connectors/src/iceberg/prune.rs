//! Scan planning: skip manifests and data files that cannot match.
//!
//! Only provably-safe pruning (ADR 0010): a manifest or file is skipped when
//! its statistics show that *no* row can satisfy the filters (an And/Or
//! tree, ADR 0050). Anything we cannot evaluate exactly (casts, floats,
//! decimals, sub-µs literals, missing bounds) keeps the file. Filters are still re-applied
//! above the scan, so pruning only ever changes cost, never results.

use std::cmp::Ordering;
use std::collections::HashMap;

use chrono::{DateTime, Datelike};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::{InList, Like};
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
    /// `bucket[N]`: Murmur3 hash mod N; only equality maps (ADR 0051).
    Bucket(u32),
    /// `truncate[W]`: monotonic, so ranges map too (ADR 0051).
    Truncate(i64),
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
                t => {
                    let arg = |name: &str| {
                        t.strip_prefix(name)?
                            .strip_prefix('[')?
                            .strip_suffix(']')?
                            .trim()
                            .parse::<i64>()
                            .ok()
                            .filter(|n| *n > 0)
                    };
                    if let Some(n) = arg("bucket") {
                        Transform::Bucket(u32::try_from(n).ok()?)
                    } else {
                        Transform::Truncate(arg("truncate")?)
                    }
                }
            };
            Some(PartitionField {
                source_id: f["source-id"].as_i64()?,
                transform,
            })
        })
        .collect()
}

/// The scan filters as a tree of what can be checked against file and
/// manifest statistics (ADR 0050). Anything else is `True` ("may match"),
/// so the tree only ever prunes what is provably empty.
#[derive(Debug, Default)]
pub(super) struct Pruner {
    root: Node,
}

#[derive(Debug, Default)]
enum Node {
    /// Unknown: may match.
    #[default]
    True,
    Cmp(Pred),
    /// `col IS NULL` (true) / `col IS NOT NULL` (false), by field id
    /// (ADR 0049).
    Null(i64, bool),
    And(Vec<Node>),
    Or(Vec<Node>),
}

impl Node {
    fn and(nodes: Vec<Node>) -> Node {
        let mut nodes: Vec<Node> = nodes
            .into_iter()
            .filter(|n| !matches!(n, Node::True))
            .collect();
        match nodes.len() {
            0 => Node::True,
            1 => nodes.remove(0),
            _ => Node::And(nodes),
        }
    }

    fn or(nodes: Vec<Node>) -> Node {
        // One unknown branch makes the whole OR unknown.
        if nodes.is_empty() || nodes.iter().any(|n| matches!(n, Node::True)) {
            return Node::True;
        }
        Node::Or(nodes)
    }

    fn file(
        &self,
        lower: &HashMap<i64, Vec<u8>>,
        upper: &HashMap<i64, Vec<u8>>,
        counts: &Counts,
    ) -> bool {
        match self {
            Node::True => true,
            // A comparison is never true on NULL: an all-NULL column
            // matches nothing (ADR 0049).
            Node::Cmp(p) => {
                if counts.all_null(p.field_id) {
                    return false;
                }
                let lo = lower.get(&p.field_id).and_then(|b| p.prim.decode(b));
                let hi = upper.get(&p.field_id).and_then(|b| p.prim.decode(b));
                p.op.may_match(lo.as_ref(), hi.as_ref(), &p.value)
            }
            Node::Null(id, true) => !counts.no_null(*id),
            Node::Null(id, false) => !counts.all_null(*id),
            Node::And(n) => n.iter().all(|n| n.file(lower, upper, counts)),
            Node::Or(n) => n.iter().any(|n| n.file(lower, upper, counts)),
        }
    }

    /// Against partition values: `bounds(i, prim)` is the (lower, upper)
    /// of partition field `i` (a manifest's summary, or one file's value).
    fn partition(
        &self,
        spec: &[Option<PartitionField>],
        bounds: &dyn Fn(usize, Prim) -> (Option<Datum>, Option<Datum>),
    ) -> bool {
        match self {
            Node::True | Node::Null(..) => true,
            Node::Cmp(p) => spec.iter().enumerate().all(|(i, field)| {
                let Some(field) = field.as_ref().filter(|f| f.source_id == p.field_id) else {
                    return true;
                };
                let Some((op, v, prim)) = transformed(p, field.transform) else {
                    return true;
                };
                let (lo, hi) = bounds(i, prim);
                op.may_match(lo.as_ref(), hi.as_ref(), &v)
            }),
            Node::And(n) => n.iter().all(|n| n.partition(spec, bounds)),
            Node::Or(n) => n.iter().any(|n| n.partition(spec, bounds)),
        }
    }
}

impl Pruner {
    /// `columns`: column path -> (field id, type) for the current schema.
    pub(super) fn new(filters: &[Expr], columns: &HashMap<String, (i64, Prim)>) -> Self {
        Self {
            root: Node::and(filters.iter().map(|f| node(f, columns)).collect()),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        matches!(self.root, Node::True)
    }

    /// File-level: `lower`/`upper` are the data file's column bounds,
    /// `counts` its value and null counts.
    pub(super) fn may_match_file(
        &self,
        lower: &HashMap<i64, Vec<u8>>,
        upper: &HashMap<i64, Vec<u8>>,
        counts: &Counts,
    ) -> bool {
        self.root.file(lower, upper, counts)
    }

    /// Manifest-level: `summaries[i]` = (lower, upper) bound of partition
    /// field `spec[i]` across the manifest.
    pub(super) fn may_match_manifest(
        &self,
        spec: &[Option<PartitionField>],
        summaries: &[Summary],
    ) -> bool {
        self.root
            .partition(spec, &|i, prim| match summaries.get(i) {
                Some((lo, hi)) => (
                    lo.as_deref().and_then(|b| prim.decode(b)),
                    hi.as_deref().and_then(|b| prim.decode(b)),
                ),
                None => (None, None),
            })
    }

    /// File-level, on the file's partition tuple (ADR 0051): what lets
    /// `bucket` partitions prune, since one manifest spans every bucket.
    pub(super) fn may_match_partition(
        &self,
        spec: &[Option<PartitionField>],
        partition: Option<&apache_avro::types::Value>,
    ) -> bool {
        let values = partition_values(partition);
        self.root.partition(spec, &|i, _| {
            let v = values.get(i).cloned().flatten();
            (v.clone(), v)
        })
    }
}

/// A data file's partition record as datums, in spec order. Unknown or
/// NULL values are `None` (no pruning on them).
fn partition_values(partition: Option<&apache_avro::types::Value>) -> Vec<Option<Datum>> {
    use apache_avro::types::Value as A;
    let Some(A::Record(fields)) = partition else {
        return Vec::new();
    };
    fields
        .iter()
        .map(|(_, v)| {
            let v = match v {
                A::Union(_, inner) => inner.as_ref(),
                v => v,
            };
            match v {
                A::Int(x) | A::Date(x) => Some(Datum::Int(i64::from(*x))),
                A::Long(x) | A::TimestampMicros(x) => Some(Datum::Int(*x)),
                A::Boolean(b) => Some(Datum::Int(i64::from(*b))),
                A::String(s) => Some(Datum::Bytes(s.as_bytes().to_vec())),
                _ => None,
            }
        })
        .collect()
}

/// Per-column counts of a data file (`value_counts` includes nulls).
#[derive(Debug, Default)]
pub(super) struct Counts {
    pub(super) values: HashMap<i64, i64>,
    pub(super) nulls: HashMap<i64, i64>,
}

impl Counts {
    /// Every value of `id` in the file is NULL (counts known and equal).
    fn all_null(&self, id: i64) -> bool {
        matches!((self.values.get(&id), self.nulls.get(&id)), (Some(v), Some(n)) if v == n)
    }

    fn no_null(&self, id: i64) -> bool {
        self.nulls.get(&id) == Some(&0)
    }
}

/// `col <op> v` on the source column -> `T(col) <op'> T(v)` on the partition
/// value. For a non-decreasing T, strict bounds become inclusive.
fn transformed(p: &Pred, t: Transform) -> Option<(Op, Datum, Prim)> {
    match t {
        Transform::Identity => return Some((p.op, p.value.clone(), p.prim)),
        // A hash keeps no order: only `=` (and so `IN`) maps.
        Transform::Bucket(n) => {
            if p.op != Op::Eq {
                return None;
            }
            return Some((Op::Eq, Datum::Int(bucket(p.prim, &p.value, n)?), Prim::Int));
        }
        Transform::Truncate(w) => {
            let v = match (&p.value, p.prim) {
                (Datum::Int(v), Prim::Int | Prim::Long) => Datum::Int(v - v.rem_euclid(w)),
                (Datum::Bytes(b), Prim::String) => {
                    let s = std::str::from_utf8(b).ok()?;
                    let end = s
                        .char_indices()
                        .nth(usize::try_from(w).ok()?)
                        .map_or(s.len(), |(i, _)| i);
                    Datum::Bytes(b[..end].to_vec())
                }
                _ => return None,
            };
            // Non-decreasing: a strict bound can still match inside the
            // same truncated value, unless `v` starts its bucket (`x < 20`
            // with width 10 rules out bucket 20 entirely).
            let at_start = matches!(&p.value, Datum::Int(x) if x.rem_euclid(w) == 0);
            let op = match p.op {
                Op::Lt if at_start => Op::Lt,
                Op::Lt => Op::LtEq,
                Op::Gt => Op::GtEq,
                op => op,
            };
            return Some((op, v, p.prim));
        }
        _ => {}
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

/// Iceberg's bucket function: 32-bit Murmur3 (x86, seed 0) of the value's
/// hash bytes, sign bit cleared, mod `n`. Ints, dates and timestamps hash
/// as 8-byte little-endian longs; strings as UTF-8 (spec, "Bucket
/// transform details").
fn bucket(prim: Prim, v: &Datum, n: u32) -> Option<i64> {
    let hash = match (prim, v) {
        (Prim::Int | Prim::Long | Prim::Date | Prim::Timestamp, Datum::Int(x)) => {
            murmur3_32(&x.to_le_bytes())
        }
        (Prim::String, Datum::Bytes(b)) => murmur3_32(b),
        _ => return None,
    };
    Some(i64::from((hash & 0x7fff_ffff) % n))
}

fn murmur3_32(data: &[u8]) -> u32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let mix = |k: u32| k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
    let mut h = 0u32;
    let chunks = data.chunks_exact(4);
    let tail = chunks.remainder();
    for c in chunks {
        h ^= mix(u32::from_le_bytes([c[0], c[1], c[2], c[3]]));
        h = h.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    if !tail.is_empty() {
        let k = tail
            .iter()
            .enumerate()
            .fold(0u32, |k, (i, b)| k | (u32::from(*b) << (8 * i)));
        h ^= mix(k);
    }
    // Lengths beyond u32 do not occur for scalar values.
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^ (h >> 16)
}

fn node(e: &Expr, columns: &HashMap<String, (i64, Prim)>) -> Node {
    match e {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => Node::and(vec![node(left, columns), node(right, columns)]),
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => Node::or(vec![node(left, columns), node(right, columns)]),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let Some(op) = cmp_op(*op) else {
                return Node::True;
            };
            match (left.as_ref(), right.as_ref()) {
                (e, Expr::Literal(v, _)) => cmp(e, op, v, columns),
                (Expr::Literal(v, _), e) => cmp(e, op.flip(), v, columns),
                _ => Node::True,
            }
        }
        Expr::Between(Between {
            expr,
            negated: false,
            low,
            high,
        }) => match (low.as_ref(), high.as_ref()) {
            (Expr::Literal(lo, _), Expr::Literal(hi, _)) => Node::and(vec![
                cmp(expr, Op::GtEq, lo, columns),
                cmp(expr, Op::LtEq, hi, columns),
            ]),
            _ => Node::True,
        },
        // `col IN (a, b)`: an OR of equalities (ADR 0050).
        Expr::InList(InList {
            expr,
            list,
            negated: false,
        }) => Node::or(
            list.iter()
                .map(|v| match v {
                    Expr::Literal(v, _) => cmp(expr, Op::Eq, v, columns),
                    _ => Node::True,
                })
                .collect(),
        ),
        // `col LIKE 'abc%'` (ADR 0052): every match starts with the literal
        // prefix, so it lies in ['abc', 'abd').
        Expr::Like(Like {
            negated: false,
            expr,
            pattern,
            case_insensitive: false,
            ..
        }) => match pattern.as_ref() {
            Expr::Literal(ScalarValue::Utf8(Some(p)) | ScalarValue::Utf8View(Some(p)), _) => {
                // Stop at the first wildcard or escape: a shorter prefix is
                // still a sound range.
                let prefix: String = p
                    .chars()
                    .take_while(|c| !matches!(c, '%' | '_' | '\\'))
                    .collect();
                prefix_range(expr, &prefix, columns)
            }
            _ => Node::True,
        },
        Expr::ScalarFunction(f) if f.func.name() == "starts_with" => match f.args.as_slice() {
            [
                expr,
                Expr::Literal(ScalarValue::Utf8(Some(p)) | ScalarValue::Utf8View(Some(p)), _),
            ] => prefix_range(expr, p, columns),
            _ => Node::True,
        },
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            match path(inner).and_then(|p| columns.get(&p)) {
                Some(&(id, _)) => Node::Null(id, matches!(e, Expr::IsNull(_))),
                None => Node::True,
            }
        }
        // `NOT` only where the negation is exact under SQL's three-valued
        // logic: NOT (x > 5) is x <= 5 for non-NULL x, and NULL either way.
        Expr::Not(inner) => match inner.as_ref() {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let negated = match cmp_op(*op) {
                    Some(Op::Gt) => Op::LtEq,
                    Some(Op::GtEq) => Op::Lt,
                    Some(Op::Lt) => Op::GtEq,
                    Some(Op::LtEq) => Op::Gt,
                    _ => return Node::True,
                };
                match (left.as_ref(), right.as_ref()) {
                    (e, Expr::Literal(v, _)) => cmp(e, negated, v, columns),
                    (Expr::Literal(v, _), e) => cmp(e, negated.flip(), v, columns),
                    _ => Node::True,
                }
            }
            Expr::IsNull(x) => node(&Expr::IsNotNull(x.clone()), columns),
            Expr::IsNotNull(x) => node(&Expr::IsNull(x.clone()), columns),
            _ => Node::True,
        },
        _ => Node::True,
    }
}

/// `e` starts with `prefix`: `e >= prefix AND e < next(prefix)`, where
/// `next` increments the last byte that can be (bytewise order, as Iceberg
/// and Arrow compare strings). An empty prefix says nothing.
fn prefix_range(e: &Expr, prefix: &str, columns: &HashMap<String, (i64, Prim)>) -> Node {
    let Some(&(field_id, Prim::String)) = path(e).and_then(|p| columns.get(&p)) else {
        return Node::True;
    };
    if prefix.is_empty() {
        return Node::True;
    }
    let pred = |op, bytes: Vec<u8>| {
        Node::Cmp(Pred {
            field_id,
            prim: Prim::String,
            op,
            value: Datum::Bytes(bytes),
        })
    };
    let mut upper = prefix.as_bytes().to_vec();
    while let Some(last) = upper.pop() {
        if last < 0xff {
            upper.push(last + 1);
            break;
        }
    }
    let lower = pred(Op::GtEq, prefix.as_bytes().to_vec());
    if upper.is_empty() {
        return lower;
    }
    Node::and(vec![lower, pred(Op::Lt, upper)])
}

fn cmp_op(op: Operator) -> Option<Op> {
    Some(match op {
        Operator::Eq => Op::Eq,
        Operator::Lt => Op::Lt,
        Operator::LtEq => Op::LtEq,
        Operator::Gt => Op::Gt,
        Operator::GtEq => Op::GtEq,
        _ => return None,
    })
}

/// `e <op> v` when `e` is a prunable column path and `v` converts exactly.
fn cmp(e: &Expr, op: Op, v: &ScalarValue, columns: &HashMap<String, (i64, Prim)>) -> Node {
    let Some(&(field_id, prim)) = path(e).and_then(|p| columns.get(&p)) else {
        return Node::True;
    };
    match literal(prim, v) {
        Some(value) => Node::Cmp(Pred {
            field_id,
            prim,
            op,
            value,
        }),
        None => Node::True,
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
    use std::ops::Not;

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
        let keep =
            |e: Expr| Pruner::new(&[e], &columns()).may_match_file(&lo, &hi, &Counts::default());

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

        // OR: pruned when every branch is (ADR 0050).
        assert!(!keep(col("id").gt(lit(20i64)).or(col("id").lt(lit(0i64)))));
        assert!(keep(col("id").gt(lit(20i64)).or(col("id").eq(lit(15i64)))));
        // An unknown branch makes the OR unknown.
        assert!(keep(col("id").gt(lit(20i64)).or(col("nope").eq(lit(0i64)))));
        // IN lists: an OR of equalities.
        assert!(!keep(col("id").in_list(vec![lit(1i64), lit(30i64)], false)));
        assert!(keep(col("id").in_list(vec![lit(1i64), lit(15i64)], false)));
        assert!(keep(col("id").in_list(vec![lit(1i64), lit(30i64)], true))); // NOT IN
        assert!(!keep(col("city").in_list(vec![lit("a"), lit("z")], false)));
        // NOT on comparisons, exact under three-valued logic.
        assert!(!keep(col("id").lt_eq(lit(20i64)).not()));
        assert!(keep(col("id").lt(lit(20i64)).not()));
        assert!(keep(col("id").eq(lit(15i64)).not())); // NOT = is `<>`: not provable

        // Not provable -> keep.

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
        let keep =
            |e: Expr| Pruner::new(&[e], &columns()).may_match_file(&lo, &hi, &Counts::default());
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

    #[test]
    fn null_counts_prune_comparisons_and_null_checks() {
        let (lo, hi) = (HashMap::new(), HashMap::new());
        // Field 1 is `id` (see `columns()`): 10 values.
        let counts = |nulls: i64| Counts {
            values: HashMap::from([(1, 10)]),
            nulls: HashMap::from([(1, nulls)]),
        };
        let keep = |e: Expr, c: &Counts| Pruner::new(&[e], &columns()).may_match_file(&lo, &hi, c);
        // All NULL: no comparison can be true; IS NOT NULL neither.
        assert!(!keep(col("id").eq(lit(5i64)), &counts(10)));
        assert!(!keep(col("id").is_not_null(), &counts(10)));
        assert!(keep(col("id").is_null(), &counts(10)));
        // No NULL: IS NULL cannot match.
        assert!(!keep(col("id").is_null(), &counts(0)));
        assert!(keep(col("id").is_not_null(), &counts(0)));
        // Some NULLs, or unknown counts: keep.
        assert!(keep(col("id").is_null(), &counts(3)));
        assert!(keep(col("id").is_not_null(), &counts(3)));
        assert!(keep(col("id").is_null(), &Counts::default()));
        assert!(keep(col("id").eq(lit(5i64)), &Counts::default()));
    }

    /// The Iceberg spec's hash examples (Appendix B), bit for bit.
    #[test]
    fn murmur3_matches_the_iceberg_spec() {
        let long = |v: i64| murmur3_32(&v.to_le_bytes()) as i32;
        assert_eq!(long(34), 2_017_239_379); // int and long 34
        assert_eq!(long(17_486), -653_330_422); // date 2017-11-16
        assert_eq!(long(1_510_871_468_000_000), -2_047_944_441); // timestamp 2017-11-16T22:31:08
        assert_eq!(murmur3_32(b"iceberg") as i32, 1_210_000_089);
        assert_eq!(murmur3_32(&[0, 1, 2, 3]) as i32, -188_683_207); // binary
    }

    #[test]
    fn bucket_and_truncate_partitions_prune_per_file() {
        use apache_avro::types::Value as A;
        let spec = partition_spec(&serde_json::json!([
            {"source-id": 1, "field-id": 1000, "name": "id_bucket", "transform": "bucket[4]"},
            {"source-id": 2, "field-id": 1001, "name": "city_trunc", "transform": "truncate[2]"},
            {"source-id": 4, "field-id": 1002, "name": "score_trunc", "transform": "truncate[10]"},
        ]));
        let b = |id: i64| bucket(Prim::Long, &Datum::Int(id), 4).unwrap() as i32;
        let file = |bucket: i32, city: &str, score: i64| {
            A::Record(vec![
                ("id_bucket".into(), A::Int(bucket)),
                (
                    "city_trunc".into(),
                    A::Union(1, Box::new(A::String(city.into()))),
                ),
                ("score_trunc".into(), A::Long(score)),
            ])
        };
        let keep =
            |e: Expr, f: &A| Pruner::new(&[e], &columns()).may_match_partition(&spec, Some(f));
        let f = file(b(7), "li", 20);
        // bucket: = and IN map; ranges do not.
        assert!(keep(col("id").eq(lit(7i64)), &f));
        let other = (0..100).find(|i| b(*i) != b(7)).unwrap();
        assert!(!keep(col("id").eq(lit(other)), &f));
        assert!(keep(
            col("id").in_list(vec![lit(other), lit(7i64)], false),
            &f
        ));
        assert!(keep(col("id").gt(lit(1_000i64)), &f));
        // truncate: equality and (widened) ranges.
        assert!(keep(col("city").eq(lit("lisbon")), &f));
        assert!(!keep(col("city").eq(lit("porto")), &f));
        assert!(!keep(col("city").lt(lit("la")), &f));
        assert!(keep(col("score").gt_eq(lit(25i64)), &f)); // 25 -> 20
        assert!(!keep(col("score").gt_eq(lit(30i64)), &f));
        assert!(keep(col("score").lt(lit(21i64)), &f));
        assert!(!keep(col("score").lt(lit(20i64)), &f)); // 20 starts the bucket
    }

    #[test]
    fn like_prefixes_prune_as_ranges() {
        // File holds city in ["b", "d"].
        let lo = bounds(&[(2, b"b".to_vec())]);
        let hi = bounds(&[(2, b"d".to_vec())]);
        let keep =
            |e: Expr| Pruner::new(&[e], &columns()).may_match_file(&lo, &hi, &Counts::default());
        assert!(!keep(col("city").like(lit("e%"))));
        assert!(!keep(col("city").like(lit("a%"))));
        assert!(keep(col("city").like(lit("c%"))));
        assert!(keep(col("city").like(lit("d%")))); // "d" itself matches
        assert!(!keep(col("city").like(lit("da_%")))); // "da..." > "d"
        assert!(keep(col("city").like(lit("%x")))); // no prefix: unknown
        assert!(keep(col("city").not_like(lit("e%"))));
        assert!(keep(col("city").ilike(lit("E%")))); // case-insensitive: unknown
        assert!(!keep(datafusion::functions::string::expr_fn::starts_with(
            col("city"),
            lit("x")
        )));
    }
}
