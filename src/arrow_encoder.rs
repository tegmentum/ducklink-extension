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
//! Nested/complex types (`reg::LogicalType::Complex(_)`) remain the last
//! escape hatch and are rejected here with a clean `Err`. The structural
//! nested arms added in @5 (`List`, `Struct`, `Map`, `Array`) are handled
//! natively via arrow-rs's `ListArray`/`StructArray`/`MapArray`/
//! `FixedSizeListArray` constructors, with recursive per-child encoding.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, FixedSizeListArray, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, Int8Builder, IntervalMonthDayNanoBuilder, ListArray, MapArray,
    NullBufferBuilder, StringBuilder, StructArray, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{
    DataType, Field, FieldRef, Fields, IntervalMonthDayNano, IntervalUnit, Schema, TimeUnit,
};
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};

use ducklink_runtime::reg::{ColumnDef, DuckValue, LogicalType};

/// HUGEINT is mapped to Arrow `Decimal128(38, 0)`.
///
/// Rationale: DuckDB's own Arrow bridge lowers HUGEINT the same way, so
/// pyarrow/pandas consumers get a first-class 128-bit integer that round-trips
/// through IPC/Parquet unchanged. The alternative (`FixedSizeBinary(16)`)
/// would need custom parsing on the consumer side. The trade-off is that
/// `Decimal128(38, 0)` can hold values up to `10^38 - 1` (≈ 9.9×10^37), which
/// is just below `i128::MAX` (≈ 1.7×10^38) — a tiny sliver near
/// `±i128::MAX/MIN` is unrepresentable. Values in that sliver are exotic
/// (>9.9×10^37) and DuckDB's own HUGEINT->Arrow path has the same limit.
const HUGEINT_PRECISION: u8 = 38;

/// UHUGEINT has no native Arrow type — Arrow's Decimal128 is signed, so a
/// full u128 in the top-bit-set range wouldn't fit there either. We
/// therefore lower to a `FixedSizeBinary(16)` payload with the two u64
/// halves laid out big-endian (`upper` then `lower`), matching the
/// convention we already use for `UUID`. Consumers that need arithmetic
/// have to reassemble the u128 from those 16 bytes themselves.
const UHUGEINT_BYTES: i32 = 16;

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
        // @5: (width, scale) now flow through structurally, so we honour the
        // real precision/scale instead of the old "default to (38, 0)" hack.
        LogicalType::Decimal { width, scale } => DataType::Decimal128(*width, *scale as i8),
        // DuckDB INTERVAL is months + days + micros; the closest Arrow type
        // is IntervalMonthDayNano — we widen micros to nanos on encode.
        LogicalType::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
        // Arrow has no first-class UUID; the pyarrow/duckdb convention is a
        // 16-byte fixed-size binary.
        LogicalType::Uuid => DataType::FixedSizeBinary(16),
        // See HUGEINT_PRECISION for the choice rationale (Decimal128 vs
        // FixedSizeBinary).
        LogicalType::Hugeint => DataType::Decimal128(HUGEINT_PRECISION, 0),
        // See UHUGEINT_BYTES — Arrow has no unsigned 128, so we ship the
        // 16 raw bytes big-endian and let the consumer reassemble.
        LogicalType::UHugeint => DataType::FixedSizeBinary(UHUGEINT_BYTES),
        // Nested arms recurse into `logical_to_arrow` for the child type.
        // The child field is named "item" — this is arrow-rs's default and
        // matches what pyarrow/pandas expect on the other side of the FFI.
        LogicalType::List(elem) => {
            let elem_dt = logical_to_arrow(elem)?;
            DataType::List(Arc::new(Field::new("item", elem_dt, true)))
        }
        LogicalType::Struct(fields) => {
            let arrow_fields: Vec<Field> = fields
                .iter()
                .map(|(name, ft)| logical_to_arrow(ft).map(|dt| Field::new(name, dt, true)))
                .collect::<Result<Vec<_>, String>>()?;
            DataType::Struct(Fields::from(arrow_fields))
        }
        LogicalType::Map(kt, vt) => {
            let kt_dt = logical_to_arrow(kt)?;
            let vt_dt = logical_to_arrow(vt)?;
            // Arrow Map convention: an "entries" struct child whose two
            // fields are the non-nullable key + nullable value. We use the
            // arrow-rs default names ("entries" / "keys" / "values") so
            // pyarrow-style consumers recognize the shape.
            let entry_fields = Fields::from(vec![
                Field::new("keys", kt_dt, false),
                Field::new("values", vt_dt, true),
            ]);
            let entries_field = Arc::new(Field::new(
                "entries",
                DataType::Struct(entry_fields),
                false,
            ));
            DataType::Map(entries_field, /*keys_sorted=*/ false)
        }
        LogicalType::Array(size, elem) => {
            let elem_dt = logical_to_arrow(elem)?;
            DataType::FixedSizeList(Arc::new(Field::new("item", elem_dt, true)), *size as i32)
        }
        LogicalType::Complex(expr) => {
            return Err(format!(
                "nested/complex Arrow types not yet supported (LogicalType::Complex({expr:?}))"
            ));
        }
    })
}

/// Build the Arrow array for one column by extracting the per-row values and
/// delegating to `encode_flat`.
fn encode_column(
    col_idx: usize,
    col: &ColumnDef,
    rows: &[Vec<DuckValue>],
) -> Result<ArrayRef, String> {
    // Clone-per-row keeps the per-value handling in `encode_flat` uniform
    // (values-owned) regardless of whether we entered from the top-level
    // row-major batch or a nested-type recursion that already produced an
    // owned flat vec. For typical extension batch sizes (thousands of rows)
    // the clone cost is negligible next to the Arrow buffer build.
    let values: Vec<DuckValue> = rows.iter().map(|r| r[col_idx].clone()).collect();
    encode_flat(&col.logical, &values, col_idx)
}

/// Encode a flat vec of `DuckValue`s into an Arrow array of the given logical
/// type. Used both by `encode_column` (via cloning the column out of the
/// row-major batch) and by the nested-type arms (List/Struct/Map/Array) which
/// flatten their child values and recurse.
///
/// A per-row type mismatch (e.g. `DuckValue::Int32` in a `LogicalType::Int64`
/// column) is a hard `Err` — silent coercion would hide guest bugs.
fn encode_flat(
    lt: &LogicalType,
    values: &[DuckValue],
    col_idx: usize,
) -> Result<ArrayRef, String> {
    let n = values.len();

    // The primitive columns all follow the same pattern:
    //   builder = Type::with_capacity(n)
    //   for each row: append_null OR append_value(v)
    // A macro captures that so the 15+ primitive arms don't duplicate the
    // null-handling / mismatch-error boilerplate.
    macro_rules! prim_col {
        ($builder:ident, $variant:ident, $tyname:literal) => {{
            let mut b = $builder::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    DuckValue::Null => b.append_null(),
                    DuckValue::$variant(x) => b.append_value(*x),
                    other => return Err(mismatch_error(col_idx, i, $tyname, other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }

    let arr = match lt {
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
            for (i, v) in values.iter().enumerate() {
                match v {
                    DuckValue::Null => b.append_null(),
                    DuckValue::Timestamptz(x) => b.append_value(*x),
                    other => return Err(mismatch_error(col_idx, i, "Timestamptz", other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        LogicalType::Text => {
            // Rough capacity heuristic: assume ~8-byte average string. Not
            // load-bearing (StringBuilder grows), just a mild alloc hint.
            let mut b = StringBuilder::with_capacity(n, n * 8);
            for (i, v) in values.iter().enumerate() {
                match v {
                    DuckValue::Null => b.append_null(),
                    DuckValue::Text(s) => b.append_value(s),
                    other => return Err(mismatch_error(col_idx, i, "Text", other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        LogicalType::Blob => {
            let mut b = BinaryBuilder::with_capacity(n, n * 8);
            for (i, v) in values.iter().enumerate() {
                match v {
                    DuckValue::Null => b.append_null(),
                    DuckValue::Blob(x) => b.append_value(x),
                    other => return Err(mismatch_error(col_idx, i, "Blob", other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        LogicalType::Decimal { width, scale } => {
            let precision = *width;
            let column_scale = *scale;
            let mut b = Decimal128Builder::with_capacity(n)
                .with_precision_and_scale(precision, column_scale as i8)
                .map_err(|e| {
                    format!("invalid decimal precision/scale ({precision}, {column_scale}): {e}")
                })?;
            for (i, v) in values.iter().enumerate() {
                match v {
                    DuckValue::Null => b.append_null(),
                    DuckValue::Decimal {
                        lower,
                        upper,
                        width: vw,
                        scale: vs,
                    } => {
                        // @5: LogicalType::Decimal now carries (width, scale)
                        // structurally, so we can validate the incoming
                        // value's (width, scale) matches the column and stop
                        // silently accepting mismatched values.
                        if *vw != precision || *vs != column_scale {
                            return Err(format!(
                                "row {i} col {col_idx}: Decimal value ({vw}, {vs}) doesn't match \
                                 schema DECIMAL({precision}, {column_scale})"
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
            for (i, v) in values.iter().enumerate() {
                match v {
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
            for (i, v) in values.iter().enumerate() {
                match v {
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
        LogicalType::Hugeint => {
            let mut b = Decimal128Builder::with_capacity(n)
                .with_precision_and_scale(HUGEINT_PRECISION, 0)
                .map_err(|e| {
                    format!("invalid Hugeint→Decimal128({HUGEINT_PRECISION},0): {e}")
                })?;
            for (i, v) in values.iter().enumerate() {
                match v {
                    DuckValue::Null => b.append_null(),
                    DuckValue::Hugeint { lower, upper } => {
                        // upper is i64 (sign-extends to i128), lower is u64
                        // (zero-extends via u128). Same OR-then-cast trick as
                        // the Decimal arm so a "negative" lower half doesn't
                        // sign-extend en route through i128.
                        let raw = (((*upper as i128) << 64) as u128
                            | (*lower as u128)) as i128;
                        b.append_value(raw);
                    }
                    other => return Err(mismatch_error(col_idx, i, "Hugeint", other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        LogicalType::UHugeint => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(n, UHUGEINT_BYTES);
            for (i, v) in values.iter().enumerate() {
                match v {
                    DuckValue::Null => b.append_null(),
                    DuckValue::UHugeint { lower, upper } => {
                        // Big-endian: upper at bytes 0..8, lower at 8..16 —
                        // same convention as UUID above so downstream tools
                        // that already know the UUID layout parse UHUGEINT
                        // consistently.
                        let mut buf = [0u8; 16];
                        buf[..8].copy_from_slice(&upper.to_be_bytes());
                        buf[8..].copy_from_slice(&lower.to_be_bytes());
                        b.append_value(buf).map_err(|e| e.to_string())?;
                    }
                    other => return Err(mismatch_error(col_idx, i, "UHugeint", other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        LogicalType::List(elem_ty) => {
            // Accumulate a flat child vector + per-row offsets. The child
            // vector is later handed to `encode_flat(elem_ty, …)` — this is
            // where the recursion happens, so `List<Struct<…>>` etc. all
            // work as a natural consequence.
            let mut lengths: Vec<usize> = Vec::with_capacity(n);
            let mut flat: Vec<DuckValue> = Vec::new();
            let mut nulls = NullBufferBuilder::new(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    DuckValue::Null => {
                        nulls.append_null();
                        lengths.push(0);
                    }
                    DuckValue::List(elems) => {
                        nulls.append_non_null();
                        lengths.push(elems.len());
                        flat.extend(elems.iter().cloned());
                    }
                    other => return Err(mismatch_error(col_idx, i, "List", other)),
                }
            }
            let child = encode_flat(elem_ty, &flat, col_idx)?;
            let elem_dt = logical_to_arrow(elem_ty)?;
            let field: FieldRef = Arc::new(Field::new("item", elem_dt, true));
            let offsets = OffsetBuffer::<i32>::from_lengths(lengths);
            let arr = ListArray::try_new(field, offsets, child, nulls.finish())
                .map_err(|e| format!("ListArray build failed: {e}"))?;
            Arc::new(arr) as ArrayRef
        }
        LogicalType::Struct(schema_fields) => {
            // For each row we either:
            //   - see a Null   -> emit Null in the struct's null buffer AND
            //                     push DuckValue::Null into every per-field
            //                     slot (Arrow requires the child arrays to
            //                     still have `n` entries), OR
            //   - see a Struct -> emit non-null, and route each field's value
            //                     into its per-field vec after checking the
            //                     field name matches the schema.
            let mut nulls = NullBufferBuilder::new(n);
            let mut per_field: Vec<Vec<DuckValue>> = schema_fields
                .iter()
                .map(|_| Vec::with_capacity(n))
                .collect();
            for (i, v) in values.iter().enumerate() {
                match v {
                    DuckValue::Null => {
                        nulls.append_null();
                        for pf in per_field.iter_mut() {
                            pf.push(DuckValue::Null);
                        }
                    }
                    DuckValue::Struct(field_vals) => {
                        nulls.append_non_null();
                        if field_vals.len() != schema_fields.len() {
                            return Err(format!(
                                "row {i} col {col_idx}: Struct value has {} fields, expected {}",
                                field_vals.len(),
                                schema_fields.len()
                            ));
                        }
                        for (j, (schema_name, _)) in schema_fields.iter().enumerate() {
                            let (val_name, val) = &field_vals[j];
                            if val_name != schema_name {
                                return Err(format!(
                                    "row {i} col {col_idx}: Struct field {j} name '{val_name}' \
                                     doesn't match schema '{schema_name}'"
                                ));
                            }
                            per_field[j].push(val.clone());
                        }
                    }
                    other => return Err(mismatch_error(col_idx, i, "Struct", other)),
                }
            }
            let mut child_fields: Vec<Field> = Vec::with_capacity(schema_fields.len());
            let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(schema_fields.len());
            for (j, (name, ft)) in schema_fields.iter().enumerate() {
                let child = encode_flat(ft, &per_field[j], col_idx)?;
                let dt = logical_to_arrow(ft)?;
                child_fields.push(Field::new(name, dt, true));
                child_arrays.push(child);
            }
            let arr = StructArray::try_new(
                Fields::from(child_fields),
                child_arrays,
                nulls.finish(),
            )
            .map_err(|e| format!("StructArray build failed: {e}"))?;
            Arc::new(arr) as ArrayRef
        }
        LogicalType::Array(size, elem_ty) => {
            let sz = *size as usize;
            let mut nulls = NullBufferBuilder::new(n);
            // For a fixed-size list, Arrow requires the child array to have
            // exactly `n * size` entries: nulls at the parent level still
            // need `size` placeholder entries in the child. We push
            // `DuckValue::Null` placeholders on a parent-Null so the child
            // encoder produces those as Arrow-level nulls.
            let mut flat: Vec<DuckValue> = Vec::with_capacity(n * sz);
            for (i, v) in values.iter().enumerate() {
                match v {
                    DuckValue::Null => {
                        nulls.append_null();
                        for _ in 0..sz {
                            flat.push(DuckValue::Null);
                        }
                    }
                    DuckValue::Array(elems) => {
                        nulls.append_non_null();
                        if elems.len() != sz {
                            return Err(format!(
                                "row {i} col {col_idx}: Array value has {} elements, expected {sz}",
                                elems.len()
                            ));
                        }
                        flat.extend(elems.iter().cloned());
                    }
                    other => return Err(mismatch_error(col_idx, i, "Array", other)),
                }
            }
            let child = encode_flat(elem_ty, &flat, col_idx)?;
            let elem_dt = logical_to_arrow(elem_ty)?;
            let field: FieldRef = Arc::new(Field::new("item", elem_dt, true));
            let arr =
                FixedSizeListArray::try_new(field, *size as i32, child, nulls.finish())
                    .map_err(|e| format!("FixedSizeListArray build failed: {e}"))?;
            Arc::new(arr) as ArrayRef
        }
        LogicalType::Map(kt, vt) => {
            // Map = List<Struct<key, value>>. We flatten to two parallel
            // child vecs (keys + values), recurse on each, then wrap them in
            // a Struct entries array and hand that to MapArray.
            let mut lengths: Vec<usize> = Vec::with_capacity(n);
            let mut keys: Vec<DuckValue> = Vec::new();
            let mut vals: Vec<DuckValue> = Vec::new();
            let mut nulls = NullBufferBuilder::new(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    DuckValue::Null => {
                        nulls.append_null();
                        lengths.push(0);
                    }
                    DuckValue::Map(pairs) => {
                        nulls.append_non_null();
                        lengths.push(pairs.len());
                        for (k, val) in pairs {
                            keys.push(k.clone());
                            vals.push(val.clone());
                        }
                    }
                    other => return Err(mismatch_error(col_idx, i, "Map", other)),
                }
            }
            let key_arr = encode_flat(kt, &keys, col_idx)?;
            let val_arr = encode_flat(vt, &vals, col_idx)?;
            let kt_dt = logical_to_arrow(kt)?;
            let vt_dt = logical_to_arrow(vt)?;
            let entry_fields = Fields::from(vec![
                Field::new("keys", kt_dt, false),
                Field::new("values", vt_dt, true),
            ]);
            // The entries StructArray must have no top-level nulls (Arrow
            // Map spec requires every entry present). Nulls at the *map*
            // level go on the MapArray itself, not the entries struct.
            let entries =
                StructArray::try_new(entry_fields.clone(), vec![key_arr, val_arr], None)
                    .map_err(|e| format!("Map entries StructArray build failed: {e}"))?;
            let entries_field: FieldRef = Arc::new(Field::new(
                "entries",
                DataType::Struct(entry_fields),
                false,
            ));
            let offsets = OffsetBuffer::<i32>::from_lengths(lengths);
            let arr = MapArray::try_new(
                entries_field,
                offsets,
                entries,
                nulls.finish(),
                /*ordered=*/ false,
            )
            .map_err(|e| format!("MapArray build failed: {e}"))?;
            Arc::new(arr) as ArrayRef
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
        DuckValue::Hugeint { .. } => "Hugeint",
        DuckValue::UHugeint { .. } => "UHugeint",
        DuckValue::List(_) => "List",
        DuckValue::Struct(_) => "Struct",
        DuckValue::Map(_) => "Map",
        DuckValue::Array(_) => "Array",
        DuckValue::Complex { .. } => "Complex",
    }
}

#[cfg(test)]
mod tests {
    //! Round-trip tests re-import the FFI array via `arrow::ffi::from_ffi` so
    //! we exercise the release semantics too, not just the encode path.

    use super::*;
    use arrow::array::{
        Decimal128Array, FixedSizeBinaryArray, Int32Array, Int64Array, IntervalMonthDayNanoArray,
        ListArray, StringArray, StructArray,
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

    #[test]
    fn hugeint_column_round_trips_as_decimal128() {
        let enc = ArrowEncoder::new(&[col("h", LogicalType::Hugeint)]).unwrap();
        let rows = vec![
            // Small positive: 42.
            vec![DuckValue::Hugeint {
                lower: 42,
                upper: 0,
            }],
            // Negative: upper=-1 lower=0 → i128 = -1 << 64 = -(1<<64).
            vec![DuckValue::Hugeint {
                lower: 0,
                upper: -1,
            }],
            // Null passthrough.
            vec![DuckValue::Null],
        ];
        let s = round_trip(&enc, &rows);
        let c = s
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(c.len(), 3);
        assert_eq!(c.value(0), 42_i128);
        assert_eq!(c.value(1), -(1_i128 << 64));
        assert!(c.is_null(2));
        assert_eq!(c.precision(), HUGEINT_PRECISION);
        assert_eq!(c.scale(), 0);
    }

    #[test]
    fn list_of_int32_round_trips() {
        let enc = ArrowEncoder::new(&[col(
            "l",
            LogicalType::List(Box::new(LogicalType::Int32)),
        )])
        .unwrap();
        let rows = vec![
            vec![DuckValue::List(vec![
                DuckValue::Int32(1),
                DuckValue::Int32(2),
            ])],
            vec![DuckValue::List(vec![])],
            vec![DuckValue::Null],
            vec![DuckValue::List(vec![DuckValue::Int32(3)])],
        ];
        let s = round_trip(&enc, &rows);
        let c = s.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(c.len(), 4);
        assert!(!c.is_null(0));
        assert!(!c.is_null(1));
        assert!(c.is_null(2));
        assert!(!c.is_null(3));
        // Row 0: [1, 2]
        let v0 = c.value(0);
        let a0 = v0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a0.values(), &[1, 2]);
        // Row 1: []
        let v1 = c.value(1);
        assert_eq!(v1.len(), 0);
        // Row 3: [3]
        let v3 = c.value(3);
        let a3 = v3.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a3.values(), &[3]);
    }

    #[test]
    fn struct_of_int_and_text_round_trips() {
        let enc = ArrowEncoder::new(&[col(
            "s",
            LogicalType::Struct(vec![
                ("a".to_string(), LogicalType::Int32),
                ("b".to_string(), LogicalType::Text),
            ]),
        )])
        .unwrap();
        let rows = vec![
            vec![DuckValue::Struct(vec![
                ("a".to_string(), DuckValue::Int32(1)),
                ("b".to_string(), DuckValue::Text("hi".into())),
            ])],
            vec![DuckValue::Struct(vec![
                ("a".to_string(), DuckValue::Int32(2)),
                ("b".to_string(), DuckValue::Text("bye".into())),
            ])],
        ];
        let s = round_trip(&enc, &rows);
        let struct_col = s.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_col.len(), 2);
        let a = struct_col
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let b = struct_col
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.values(), &[1, 2]);
        assert_eq!(b.value(0), "hi");
        assert_eq!(b.value(1), "bye");
    }
}
