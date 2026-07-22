//! Row -> Arrow encoder for the ArrowArrayStream sink.
//!
//! The wasm guest returns table-function batches as row-major
//! [`reg::DuckValue`] (same wire format as the scalar/call-table hot path),
//! but DuckDB's `duckdb_result_arrow_array` / table function ArrowArrayStream
//! consumer wants a column-major Arrow batch handed across the C Data
//! Interface. This module builds one column at a time using arrow-rs's typed
//! `*Builder` API, packages the columns into a top-level `StructArray`
//! (Arrow's canonical "record batch as one array" shape used by the C Data
//! Interface), and hands DuckDB the resulting `(FFI_ArrowArray,
//! FFI_ArrowSchema)` pair via [`arrow::ffi::to_ffi`].
//!
//! Release semantics are inherited from arrow-rs: `FFI_ArrowArray::new` /
//! `FFI_ArrowSchema::try_from` install release callbacks that free the
//! buffers when DuckDB (the consumer) calls them, so the host stays
//! memory-safe without a hand-rolled release path.
//!
//! Nested/complex types (`reg::LogicalType::Complex(_)`) are pre-existing
//! Gap 1 from the audit and are rejected here with a clean `Err`; adding
//! nested-type support belongs to a later phase that also plumbs
//! precision/scale into `ColumnDef` (see the Decimal note below).

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, IntervalMonthDayNanoBuilder, StringBuilder, StructArray,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, UInt16Builder, UInt32Builder,
    UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, IntervalMonthDayNano, IntervalUnit, Schema, TimeUnit};
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};

use ducklink_runtime::reg::{ColumnDef, DuckValue, LogicalType};

/// Precision/scale defaulted for a `LogicalType::Decimal` column.
///
/// `reg::LogicalType::Decimal` currently carries no width/scale (the audit's
/// "Gap 2" — per-column decimal precision needs to flow through the neutral
/// registration model). Until that lands, the encoder declares the column as
/// `DECIMAL(38, 0)` — the widest supported precision and DuckDB's neutral
/// default when a scale isn't specified — and rejects rows whose value scale
/// disagrees. Downstream code (guests emitting DECIMAL) must currently emit
/// scale-0 values or accept an `Err`.
const DECIMAL_DEFAULT_PRECISION: u8 = 38;
const DECIMAL_DEFAULT_SCALE: i8 = 0;

/// Encodes a batch of `Vec<Vec<DuckValue>>` rows into an Arrow C Data
/// Interface `(FFI_ArrowArray, FFI_ArrowSchema)` pair matching the target
/// column schema.
pub struct ArrowEncoder {
    /// The Arrow schema derived from the target column defs. Held as an `Arc`
    /// so multiple encodings can share it without rebuilding the field list.
    schema: Arc<Schema>,
    /// The neutral column defs. Kept alongside the Arrow schema because the
    /// per-column encoder needs to see the original `LogicalType` (to know
    /// which `DuckValue` arm to expect) — the Arrow `DataType` is lossy for
    /// this (e.g. `DataType::Int64` doesn't tell us we came from
    /// `LogicalType::Int64` vs some future signed integer).
    columndefs: Vec<ColumnDef>,
}

impl ArrowEncoder {
    /// Build an encoder for the given target column schema.
    ///
    /// Returns `Err` if any column's `LogicalType` cannot be mapped to an
    /// Arrow `DataType` — today that's just `LogicalType::Complex(_)`.
    pub fn new(columndefs: &[ColumnDef]) -> Result<Self, String> {
        let fields: Vec<Field> = columndefs
            .iter()
            .map(|c| {
                let dt = logical_to_arrow(&c.logical)?;
                // Fields are nullable to mirror `DuckValue::Null`; DuckDB's
                // table-function contract does not distinguish nullable vs
                // non-nullable columns at the ArrowArrayStream boundary.
                Ok(Field::new(c.name.clone(), dt, true))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            schema: Arc::new(Schema::new(fields)),
            columndefs: columndefs.to_vec(),
        })
    }

    /// The Arrow schema as an FFI-safe `FFI_ArrowSchema`. Consumed by
    /// `duckdb_arrow_stream_get_schema` on the DuckDB side.
    ///
    /// This can panic in principle (all `FFI_ArrowSchema` conversions in
    /// arrow-rs are fallible), but every field's `DataType` was already
    /// validated in `new()`, so `try_from(&Schema)` cannot fail here.
    pub fn schema_ffi(&self) -> FFI_ArrowSchema {
        FFI_ArrowSchema::try_from(self.schema.as_ref())
            .expect("all column DataTypes validated at ArrowEncoder::new")
    }

    /// The Arrow schema (as a shared `Arc`), useful for callers that want to
    /// inspect it without going through the C ABI (tests, diagnostics).
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Encode a row-major batch into an FFI-safe Arrow array.
    ///
    /// Empty batches are supported and produce a length-0 struct array — the
    /// standard ArrowArrayStream EOF signal.
    pub fn encode_batch(&self, rows: &[Vec<DuckValue>]) -> Result<FFI_ArrowArray, String> {
        let ncols = self.columndefs.len();
        // Row-width validation happens up-front so we can report the offending
        // row index cleanly instead of surfacing an opaque type-mismatch
        // partway through the per-column loop.
        for (i, r) in rows.iter().enumerate() {
            if r.len() != ncols {
                return Err(format!(
                    "row {i} has {} columns, expected {ncols}",
                    r.len()
                ));
            }
        }

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ncols);
        for (col_idx, col) in self.columndefs.iter().enumerate() {
            arrays.push(encode_column(col_idx, col, rows)?);
        }

        let fields = self.schema.fields().clone();
        // `try_new_with_length` lets us set the row count explicitly, so an
        // empty batch (no rows, no arrays) still ends up with a valid
        // length-0 struct — `try_new` would fail on an empty `arrays` because
        // it infers length from the first array.
        let struct_array = StructArray::try_new_with_length(fields, arrays, None, rows.len())
            .map_err(|e| format!("StructArray build failed: {e}"))?;

        // to_ffi installs the release callback on both the array and the
        // schema; we throw away the schema copy because `schema_ffi()` builds
        // its own owned copy per call (the C Data Interface treats the
        // schema and array releases as independent).
        let (ffi_arr, _ffi_schema) =
            to_ffi(&struct_array.to_data()).map_err(|e| format!("to_ffi failed: {e}"))?;
        Ok(ffi_arr)
    }
}

/// Map a neutral `LogicalType` to its Arrow `DataType`. Called both when
/// building the top-level schema and (implicitly, via each builder's declared
/// data type) when encoding columns.
fn logical_to_arrow(t: &LogicalType) -> Result<DataType, String> {
    Ok(match t {
        LogicalType::Boolean => DataType::Boolean,
        LogicalType::Int8 => DataType::Int8,
        LogicalType::Int16 => DataType::Int16,
        LogicalType::Int32 => DataType::Int32,
        LogicalType::Int64 => DataType::Int64,
        LogicalType::Uint8 => DataType::UInt8,
        LogicalType::Uint16 => DataType::UInt16,
        LogicalType::Uint32 => DataType::UInt32,
        LogicalType::Uint64 => DataType::UInt64,
        LogicalType::Float32 => DataType::Float32,
        LogicalType::Float64 => DataType::Float64,
        LogicalType::Text => DataType::Utf8,
        LogicalType::Blob => DataType::Binary,
        // DuckDB's TIMESTAMP is microseconds since 1970-01-01 (no timezone).
        LogicalType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        // TIMESTAMPTZ carries an implicit UTC timezone in DuckDB.
        LogicalType::Timestamptz => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        // DuckDB's DATE is days since 1970-01-01, matches Arrow Date32 exactly.
        LogicalType::Date => DataType::Date32,
        // DuckDB's TIME is microseconds since midnight; Arrow Time64(Micro) matches.
        LogicalType::Time => DataType::Time64(TimeUnit::Microsecond),
        // See DECIMAL_DEFAULT_* — `ColumnDef` doesn't yet carry precision/scale.
        LogicalType::Decimal => {
            DataType::Decimal128(DECIMAL_DEFAULT_PRECISION, DECIMAL_DEFAULT_SCALE)
        }
        // DuckDB INTERVAL is months + days + micros; the closest Arrow type
        // is IntervalMonthDayNano — we widen micros to nanos on encode.
        LogicalType::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
        // Arrow has no first-class UUID; the pyarrow/duckdb convention is a
        // 16-byte fixed-size binary.
        LogicalType::Uuid => DataType::FixedSizeBinary(16),
        LogicalType::Complex(expr) => {
            return Err(format!(
                "nested/complex Arrow types not yet supported (LogicalType::Complex({expr:?}))"
            ));
        }
    })
}

/// Build the Arrow array for one column by walking `rows` and appending the
/// matching `DuckValue` arm (or a null) through the type-appropriate builder.
///
/// A per-row type mismatch (e.g. `DuckValue::Int32` in a `LogicalType::Int64`
/// column) is a hard `Err` — silent coercion would hide guest bugs.
fn encode_column(
    col_idx: usize,
    col: &ColumnDef,
    rows: &[Vec<DuckValue>],
) -> Result<ArrayRef, String> {
    let n = rows.len();

    // The primitive columns all follow the same pattern:
    //   builder = Type::with_capacity(n)
    //   for each row: append_null OR append_value(v)
    // A macro captures that so the 15+ primitive arms don't duplicate the
    // null-handling / mismatch-error boilerplate.
    macro_rules! prim_col {
        ($builder:ident, $variant:ident, $tyname:literal) => {{
            let mut b = $builder::with_capacity(n);
            for (i, r) in rows.iter().enumerate() {
                match &r[col_idx] {
                    DuckValue::Null => b.append_null(),
                    DuckValue::$variant(v) => b.append_value(*v),
                    other => return Err(mismatch_error(col_idx, i, $tyname, other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }

    let arr = match &col.logical {
        LogicalType::Boolean => prim_col!(BooleanBuilder, Boolean, "Boolean"),
        LogicalType::Int8 => prim_col!(Int8Builder, Int8, "Int8"),
        LogicalType::Int16 => prim_col!(Int16Builder, Int16, "Int16"),
        LogicalType::Int32 => prim_col!(Int32Builder, Int32, "Int32"),
        LogicalType::Int64 => prim_col!(Int64Builder, Int64, "Int64"),
        LogicalType::Uint8 => prim_col!(UInt8Builder, Uint8, "Uint8"),
        LogicalType::Uint16 => prim_col!(UInt16Builder, Uint16, "Uint16"),
        LogicalType::Uint32 => prim_col!(UInt32Builder, Uint32, "Uint32"),
        LogicalType::Uint64 => prim_col!(UInt64Builder, Uint64, "Uint64"),
        LogicalType::Float32 => prim_col!(Float32Builder, Float32, "Float32"),
        LogicalType::Float64 => prim_col!(Float64Builder, Float64, "Float64"),
        LogicalType::Date => prim_col!(Date32Builder, Date, "Date"),
        LogicalType::Time => prim_col!(Time64MicrosecondBuilder, Time, "Time"),
        LogicalType::Timestamp => {
            prim_col!(TimestampMicrosecondBuilder, Timestamp, "Timestamp")
        }
        LogicalType::Timestamptz => {
            // Same underlying storage as Timestamp, but we tag the array's
            // declared `DataType` with the UTC timezone so the FFI schema
            // stays consistent with what `schema_ffi()` published.
            let mut b = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
            for (i, r) in rows.iter().enumerate() {
                match &r[col_idx] {
                    DuckValue::Null => b.append_null(),
                    DuckValue::Timestamptz(v) => b.append_value(*v),
                    other => return Err(mismatch_error(col_idx, i, "Timestamptz", other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        LogicalType::Text => {
            // Rough capacity heuristic: assume ~8-byte average string. Not
            // load-bearing (StringBuilder grows), just a mild alloc hint.
            let mut b = StringBuilder::with_capacity(n, n * 8);
            for (i, r) in rows.iter().enumerate() {
                match &r[col_idx] {
                    DuckValue::Null => b.append_null(),
                    DuckValue::Text(s) => b.append_value(s),
                    other => return Err(mismatch_error(col_idx, i, "Text", other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        LogicalType::Blob => {
            let mut b = BinaryBuilder::with_capacity(n, n * 8);
            for (i, r) in rows.iter().enumerate() {
                match &r[col_idx] {
                    DuckValue::Null => b.append_null(),
                    DuckValue::Blob(v) => b.append_value(v),
                    other => return Err(mismatch_error(col_idx, i, "Blob", other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        LogicalType::Decimal => {
            let mut b = Decimal128Builder::with_capacity(n)
                .with_precision_and_scale(DECIMAL_DEFAULT_PRECISION, DECIMAL_DEFAULT_SCALE)
                .map_err(|e| format!("invalid decimal precision/scale: {e}"))?;
            for (i, r) in rows.iter().enumerate() {
                match &r[col_idx] {
                    DuckValue::Null => b.append_null(),
                    DuckValue::Decimal {
                        lower,
                        upper,
                        width: _,
                        scale,
                    } => {
                        if *scale as i8 != DECIMAL_DEFAULT_SCALE {
                            return Err(format!(
                                "row {i} col {col_idx}: Decimal value scale {} != schema \
                                 default {}. Per-column (precision, scale) plumbing on \
                                 reg::LogicalType::Decimal is not yet implemented; the encoder \
                                 currently declares DECIMAL({}, {}).",
                                scale,
                                DECIMAL_DEFAULT_SCALE,
                                DECIMAL_DEFAULT_PRECISION,
                                DECIMAL_DEFAULT_SCALE
                            ));
                        }
                        // Reassemble the 128-bit two's-complement value. The
                        // guest split it into two u64 halves; we go through
                        // u128 first so a "negative" upper half preserves
                        // its bit pattern instead of getting sign-extended
                        // by an intermediate i64 cast.
                        let raw =
                            (((*upper as u128) << 64) | (*lower as u128)) as i128;
                        b.append_value(raw);
                    }
                    other => return Err(mismatch_error(col_idx, i, "Decimal", other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        LogicalType::Interval => {
            let mut b = IntervalMonthDayNanoBuilder::with_capacity(n);
            for (i, r) in rows.iter().enumerate() {
                match &r[col_idx] {
                    DuckValue::Null => b.append_null(),
                    DuckValue::Interval {
                        months,
                        days,
                        micros,
                    } => {
                        // DuckDB stores interval sub-day precision as
                        // microseconds; Arrow wants nanoseconds. Saturating
                        // multiply keeps a pathological micros value from
                        // wrapping instead of erroring — the resulting
                        // interval is still directionally correct.
                        let nanos = micros.saturating_mul(1_000);
                        b.append_value(IntervalMonthDayNano::new(*months, *days, nanos));
                    }
                    other => return Err(mismatch_error(col_idx, i, "Interval", other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        LogicalType::Uuid => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(n, 16);
            for (i, r) in rows.iter().enumerate() {
                match &r[col_idx] {
                    DuckValue::Null => b.append_null(),
                    DuckValue::Uuid { hi, lo } => {
                        // Big-endian layout matches the pyarrow / duckdb
                        // Python UUID convention: the most-significant byte
                        // of `hi` at offset 0. `to_be_bytes` on each u64
                        // half keeps the halves stable regardless of host
                        // endianness.
                        let mut buf = [0u8; 16];
                        buf[..8].copy_from_slice(&hi.to_be_bytes());
                        buf[8..].copy_from_slice(&lo.to_be_bytes());
                        b.append_value(buf).map_err(|e| e.to_string())?;
                    }
                    other => return Err(mismatch_error(col_idx, i, "Uuid", other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        LogicalType::Complex(expr) => {
            return Err(format!(
                "nested/complex Arrow types not yet supported (LogicalType::Complex({expr:?}))"
            ));
        }
    };
    Ok(arr)
}

/// Format the "row `i` col `col_idx` expected `expected` but got …" message
/// consistently for every column type. Kept as a free function so the macro
/// and the special-case columns share one wording.
fn mismatch_error(col_idx: usize, row: usize, expected: &str, got: &DuckValue) -> String {
    format!(
        "row {row} col {col_idx}: expected {expected} DuckValue, got {}",
        duckvalue_kind(got)
    )
}

/// Short kind label used in error messages.
fn duckvalue_kind(v: &DuckValue) -> &'static str {
    match v {
        DuckValue::Null => "Null",
        DuckValue::Boolean(_) => "Boolean",
        DuckValue::Int64(_) => "Int64",
        DuckValue::Uint64(_) => "Uint64",
        DuckValue::Float64(_) => "Float64",
        DuckValue::Text(_) => "Text",
        DuckValue::Blob(_) => "Blob",
        DuckValue::Int32(_) => "Int32",
        DuckValue::Timestamp(_) => "Timestamp",
        DuckValue::Int8(_) => "Int8",
        DuckValue::Int16(_) => "Int16",
        DuckValue::Uint8(_) => "Uint8",
        DuckValue::Uint16(_) => "Uint16",
        DuckValue::Uint32(_) => "Uint32",
        DuckValue::Float32(_) => "Float32",
        DuckValue::Date(_) => "Date",
        DuckValue::Time(_) => "Time",
        DuckValue::Timestamptz(_) => "Timestamptz",
        DuckValue::Decimal { .. } => "Decimal",
        DuckValue::Interval { .. } => "Interval",
        DuckValue::Uuid { .. } => "Uuid",
        DuckValue::Complex { .. } => "Complex",
    }
}

#[cfg(test)]
mod tests {
    //! Round-trip tests re-import the FFI array via `arrow::ffi::from_ffi` so
    //! we exercise the release semantics too, not just the encode path.

    use super::*;
    use arrow::array::{
        FixedSizeBinaryArray, Int64Array, IntervalMonthDayNanoArray, StringArray, StructArray,
    };
    use arrow::ffi::from_ffi;

    /// Rebuild a `StructArray` from the FFI pair the encoder returns. The
    /// tests all go through this helper so the FFI round-trip is exercised
    /// once, in one place.
    fn round_trip(enc: &ArrowEncoder, rows: &[Vec<DuckValue>]) -> StructArray {
        let arr = enc.encode_batch(rows).expect("encode failed");
        let schema = enc.schema_ffi();
        let data = unsafe { from_ffi(arr, &schema) }.expect("from_ffi failed");
        StructArray::from(data)
    }

    fn col(name: &str, ty: LogicalType) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            logical: ty,
        }
    }

    #[test]
    fn empty_batch_is_valid_length_zero_array() {
        let enc = ArrowEncoder::new(&[col("x", LogicalType::Int64)]).unwrap();
        let arr = enc.encode_batch(&[]).expect("empty encode");
        // A length-0 struct is what the ArrowArrayStream contract treats as
        // EOF. Rehydrate through the FFI to confirm the release path is
        // also happy with an empty payload.
        let schema = enc.schema_ffi();
        let data = unsafe { from_ffi(arr, &schema) }.expect("from_ffi");
        let struct_arr = StructArray::from(data);
        assert_eq!(struct_arr.len(), 0);
        assert_eq!(struct_arr.num_columns(), 1);
    }

    #[test]
    fn int64_column_round_trips() {
        let enc = ArrowEncoder::new(&[col("i", LogicalType::Int64)]).unwrap();
        let rows = vec![
            vec![DuckValue::Int64(1)],
            vec![DuckValue::Int64(-2)],
            vec![DuckValue::Int64(i64::MAX)],
        ];
        let s = round_trip(&enc, &rows);
        let c = s.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(c.values(), &[1, -2, i64::MAX]);
        assert_eq!(c.null_count(), 0);
    }

    #[test]
    fn text_column_round_trips_including_empty_string() {
        let enc = ArrowEncoder::new(&[col("s", LogicalType::Text)]).unwrap();
        let rows = vec![
            vec![DuckValue::Text("hello".into())],
            vec![DuckValue::Text("".into())],
            vec![DuckValue::Text("arrow".into())],
        ];
        let s = round_trip(&enc, &rows);
        let c = s.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(c.value(0), "hello");
        assert_eq!(c.value(1), "");
        assert_eq!(c.value(2), "arrow");
    }

    #[test]
    fn null_in_middle_is_preserved() {
        let enc = ArrowEncoder::new(&[col("i", LogicalType::Int64)]).unwrap();
        let rows = vec![
            vec![DuckValue::Int64(10)],
            vec![DuckValue::Null],
            vec![DuckValue::Int64(30)],
        ];
        let s = round_trip(&enc, &rows);
        let c = s.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(c.len(), 3);
        assert!(!c.is_null(0));
        assert!(c.is_null(1));
        assert!(!c.is_null(2));
        assert_eq!(c.value(0), 10);
        assert_eq!(c.value(2), 30);
    }

    #[test]
    fn mismatched_row_width_returns_err() {
        let enc = ArrowEncoder::new(&[
            col("a", LogicalType::Int64),
            col("b", LogicalType::Text),
        ])
        .unwrap();
        // Row 1 only has one value; schema wants two.
        let rows = vec![
            vec![DuckValue::Int64(1), DuckValue::Text("ok".into())],
            vec![DuckValue::Int64(2)],
        ];
        let err = enc.encode_batch(&rows).unwrap_err();
        assert!(
            err.contains("row 1") && err.contains("expected 2"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn mismatched_value_type_returns_err() {
        let enc = ArrowEncoder::new(&[col("i", LogicalType::Int64)]).unwrap();
        let rows = vec![vec![DuckValue::Int32(5)]];
        let err = enc.encode_batch(&rows).unwrap_err();
        assert!(
            err.contains("expected Int64") && err.contains("Int32"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn complex_type_returns_not_yet_supported_err() {
        // `ArrowEncoder` isn't `Debug`, so we can't use `.unwrap_err()`; a
        // manual `match` keeps this test standalone.
        let err = match ArrowEncoder::new(&[col("c", LogicalType::Complex("INTEGER[]".into()))]) {
            Err(e) => e,
            Ok(_) => panic!("expected Err for Complex column"),
        };
        assert!(
            err.contains("nested/complex Arrow types not yet supported"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn uuid_column_round_trips_big_endian() {
        let enc = ArrowEncoder::new(&[col("u", LogicalType::Uuid)]).unwrap();
        let rows = vec![vec![DuckValue::Uuid {
            hi: 0x0011_2233_4455_6677,
            lo: 0x8899_aabb_ccdd_eeff,
        }]];
        let s = round_trip(&enc, &rows);
        let c = s
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let expected: [u8; 16] = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ];
        assert_eq!(c.value(0), &expected);
    }

    #[test]
    fn interval_micros_widen_to_nanos() {
        let enc = ArrowEncoder::new(&[col("iv", LogicalType::Interval)]).unwrap();
        let rows = vec![vec![DuckValue::Interval {
            months: 1,
            days: 2,
            micros: 3,
        }]];
        let s = round_trip(&enc, &rows);
        let c = s
            .column(0)
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .unwrap();
        let v = c.value(0);
        assert_eq!(v.months, 1);
        assert_eq!(v.days, 2);
        assert_eq!(v.nanoseconds, 3_000, "3us should widen to 3000ns");
    }
}
