//! The Direction-2 DuckDB sink: register the scalar functions a wasm component
//! declared as real DuckDB scalar functions, dispatching each call back into the
//! component via [`crate::engine::Engine2`].
//!
//! MVP slice: unary `BIGINT -> BIGINT` scalars. The public duckdb-rs
//! registration path (`register_scalar_function_with_state`) derives the SQL
//! signature from a static `VScalar::signatures()`, so one `VScalar` impl serves
//! one fixed shape; the per-function callback handle is injected through the
//! function's `State`. Other shapes are skipped with a logged note — extending
//! to them is more `VScalar` impls keyed by `reg::LogicalType`.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use duckdb::core::{DataChunkHandle, FlatVector, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::ffi::duckdb_string_t;
use duckdb::types::DuckString;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::vtab::arrow::WritableVector;
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab, Value};
use duckdb::Connection;

use ducklink_runtime::reg;
// The WIT value type the component dispatcher consumes/produces. The scalar hot
// path marshals DuckDB vectors straight to/from this type, so no per-chunk
// neutral(reg::DuckValue) <-> WIT rebuild happens inside the engine (measured at
// ~15% of dispatch). The cold table/aggregate paths still use reg::DuckValue.
use ducklink_runtime::duckdb_extension_bindings::duckdb::extension::types::{
    Complexvalue as WitComplex, Decimalvalue as WitDecimal, Duckvalue as WitVal,
    Intervalvalue as WitInterval, Uuidvalue as WitUuid,
};
// Columnar dispatch types for the scalar hot path. `read_col_to_colvec` builds
// these directly from DuckDB flat vectors (per-column memcpy for primitives),
// hands them to `dispatch_scalar_batch_col`, and `write_colvec` lowers the
// result column back to a DuckDB flat vector — no row-major pivot anywhere.
use ducklink_runtime::duckdb_extension_bindings::duckdb::extension::column_types::{
    Colvec, Column as ColvecColumn, Complexvalue as ColvecComplex,
    Decimalvalue as ColvecDecimal, Intervalvalue as ColvecInterval, Uuidvalue as ColvecUuid,
};

use crate::engine::{AggregateFunc, Engine2, ScalarFunc, TableFunc};

/// Convert DuckDB's physical UUID hugeint storage (sign-flipped: the high bit is
/// inverted so values sort correctly as a signed i128) into the logical 128-bit
/// UUID value the WIT contract carries. Self-inverse, so the same function maps
/// logical -> physical.
#[inline]
fn uuid_storage_to_logical(stored: i128) -> u128 {
    (stored as u128) ^ (1u128 << 127)
}

/// Format a caught panic payload as a one-line message.
fn panic_msg(p: Box<dyn std::any::Any + Send>, what: &str) -> String {
    let detail = p
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown".to_string());
    format!("internal panic in wasm {what}: {detail}")
}

/// Run a fallible FFI-callback body, catching any panic so it cannot unwind
/// across DuckDB's `extern "C"` call boundary — which (duckdb-rs installs no
/// catch of its own) would abort the entire host process. A caught panic becomes
/// an `Err`, surfaced to DuckDB as a normal query error. These bodies are
/// panic-free in normal operation; this is a last line of defence so that an
/// unexpected panic (a future bug, an internal dependency panic) fails one query
/// instead of tearing down every connection in the process.
fn guard<T>(
    what: &str,
    f: impl FnOnce() -> Result<T, Box<dyn std::error::Error>>,
) -> Result<T, Box<dyn std::error::Error>> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(p) => Err(panic_msg(p, what).into()),
    }
}

/// Per-function state DuckDB hands back to `invoke`: which component callback to
/// dispatch to, the shared engine, and the function's argument / return type
/// codes (so one `WasmScalar` serves every signature).
#[derive(Clone)]
struct WasmScalarState {
    callback_handle: u32,
    engine: Arc<Engine2>,
    arg_codes: Vec<u8>,
    ret_code: u8,
}

// Bridge type codes — one per DuckDB logical type the scalar bridge marshals.
const T_I64: u8 = 0;
const T_U64: u8 = 1;
const T_F64: u8 = 2;
const T_BOOL: u8 = 3;
const T_TEXT: u8 = 4;
const T_BLOB: u8 = 5;
const T_I8: u8 = 6;
const T_I16: u8 = 7;
const T_I32: u8 = 8;
const T_U8: u8 = 9;
const T_U16: u8 = 10;
const T_U32: u8 = 11;
const T_F32: u8 = 12;
const T_TIMESTAMP: u8 = 13;
const T_DATE: u8 = 14;
const T_TIME: u8 = 15;
const T_TIMESTAMPTZ: u8 = 16;
const T_DECIMAL: u8 = 17;
const T_INTERVAL: u8 = 18;
const T_UUID: u8 = 19;
// ESCAPE-HATCH: a component-declared composite type (LIST/STRUCT/...). The
// native bridge has no full nested-vector marshaller yet, so a Complex value is
// carried as its JSON rendering in a VARCHAR column (best-effort). The declared
// type-expression is not reconstructed into a real LIST/STRUCT vector here.
const T_COMPLEX: u8 = 20;

/// Map a neutral logical type to a bridge type code. All current `reg`
/// logical types are supported. Borrows `lt` because the `Complex` arm carries
/// an owned `String`, so `reg::LogicalType` is no longer `Copy`.
pub(crate) fn type_code(lt: &reg::LogicalType) -> u8 {
    match lt {
        reg::LogicalType::Int64 => T_I64,
        reg::LogicalType::Uint64 => T_U64,
        reg::LogicalType::Float64 => T_F64,
        reg::LogicalType::Boolean => T_BOOL,
        reg::LogicalType::Text => T_TEXT,
        reg::LogicalType::Blob => T_BLOB,
        reg::LogicalType::Int8 => T_I8,
        reg::LogicalType::Int16 => T_I16,
        reg::LogicalType::Int32 => T_I32,
        reg::LogicalType::Uint8 => T_U8,
        reg::LogicalType::Uint16 => T_U16,
        reg::LogicalType::Uint32 => T_U32,
        reg::LogicalType::Float32 => T_F32,
        reg::LogicalType::Timestamp => T_TIMESTAMP,
        reg::LogicalType::Date => T_DATE,
        reg::LogicalType::Time => T_TIME,
        reg::LogicalType::Timestamptz => T_TIMESTAMPTZ,
        reg::LogicalType::Decimal => T_DECIMAL,
        reg::LogicalType::Interval => T_INTERVAL,
        reg::LogicalType::Uuid => T_UUID,
        reg::LogicalType::Complex(_) => T_COMPLEX,
    }
}

fn logical_type(code: u8) -> LogicalTypeHandle {
    let id = match code {
        T_I64 => LogicalTypeId::Bigint,
        T_U64 => LogicalTypeId::UBigint,
        T_F64 => LogicalTypeId::Double,
        T_BOOL => LogicalTypeId::Boolean,
        T_TEXT => LogicalTypeId::Varchar,
        T_BLOB => LogicalTypeId::Blob,
        T_I8 => LogicalTypeId::Tinyint,
        T_I16 => LogicalTypeId::Smallint,
        T_I32 => LogicalTypeId::Integer,
        T_U8 => LogicalTypeId::UTinyint,
        T_U16 => LogicalTypeId::USmallint,
        T_U32 => LogicalTypeId::UInteger,
        T_F32 => LogicalTypeId::Float,
        T_TIMESTAMP => LogicalTypeId::Timestamp,
        T_DATE => LogicalTypeId::Date,
        T_TIME => LogicalTypeId::Time,
        T_TIMESTAMPTZ => LogicalTypeId::TimestampTZ,
        T_INTERVAL => LogicalTypeId::Interval,
        T_UUID => LogicalTypeId::Uuid,
        // Complex crosses as JSON text -> declare a VARCHAR column.
        T_COMPLEX => LogicalTypeId::Varchar,
        // DECIMAL needs a (width, scale) and is built directly below; the value's
        // own width/scale is only known per-value, so the column is declared with
        // DuckDB's default-precision DECIMAL(18, 3). A column whose values carry a
        // different width/scale is a known limitation (see write_ret Decimal arm).
        T_DECIMAL => return LogicalTypeHandle::decimal(18, 3),
        _ => unreachable!("type code out of range"),
    };
    LogicalTypeHandle::from(id)
}

/// Render a neutral logical type as the DuckDB SQL type name the bridge declares
/// it as (matching `logical_type` above). Used by the discovery views to show a
/// function's argument/return types the way a user would write them in SQL
/// (e.g. `VARCHAR`, `BIGINT`, `BOOLEAN`), rather than the internal `reg` names
/// (`TEXT`, `INT64`) that `reg::LogicalType::describe` yields.
pub(crate) fn sql_type_name(lt: &reg::LogicalType) -> String {
    match lt {
        reg::LogicalType::Int64 => "BIGINT".to_string(),
        reg::LogicalType::Uint64 => "UBIGINT".to_string(),
        reg::LogicalType::Float64 => "DOUBLE".to_string(),
        reg::LogicalType::Boolean => "BOOLEAN".to_string(),
        reg::LogicalType::Text => "VARCHAR".to_string(),
        reg::LogicalType::Blob => "BLOB".to_string(),
        reg::LogicalType::Int8 => "TINYINT".to_string(),
        reg::LogicalType::Int16 => "SMALLINT".to_string(),
        reg::LogicalType::Int32 => "INTEGER".to_string(),
        reg::LogicalType::Uint8 => "UTINYINT".to_string(),
        reg::LogicalType::Uint16 => "USMALLINT".to_string(),
        reg::LogicalType::Uint32 => "UINTEGER".to_string(),
        reg::LogicalType::Float32 => "FLOAT".to_string(),
        reg::LogicalType::Timestamp => "TIMESTAMP".to_string(),
        reg::LogicalType::Date => "DATE".to_string(),
        reg::LogicalType::Time => "TIME".to_string(),
        reg::LogicalType::Timestamptz => "TIMESTAMP WITH TIME ZONE".to_string(),
        reg::LogicalType::Decimal => "DECIMAL(18, 3)".to_string(),
        reg::LogicalType::Interval => "INTERVAL".to_string(),
        reg::LogicalType::Uuid => "UUID".to_string(),
        // The Complex escape-hatch carries the declared type-expression verbatim.
        reg::LogicalType::Complex(expr) => expr.clone(),
    }
}

/// Render a function's argument list as a comma-separated SQL signature, e.g.
/// `VARCHAR` or `name VARCHAR, digits INTEGER`. Anonymous positional args show
/// only their type.
fn render_arg_signature(args: &[reg::FuncArg]) -> String {
    args.iter()
        .map(|a| match &a.name {
            Some(n) if !n.is_empty() => format!("{n} {}", sql_type_name(&a.logical)),
            _ => sql_type_name(&a.logical),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// True if row `r` is valid (non-NULL) under DuckDB's bitmask layout. A null
/// `validity` pointer means the whole column is valid.
///
/// # Safety
/// `validity`, when non-null, must point to a DuckDB validity mask with at least
/// `r + 1` rows.
#[inline]
unsafe fn row_valid(validity: *const u64, r: usize) -> bool {
    *validity.add(r / 64) & (1u64 << (r % 64)) != 0
}

/// Copy a DuckDB validity bit-mask into the byte-packed form the WIT `Colvec`
/// expects. DuckDB packs one bit per row into u64 words; `Colvec::validity` is
/// `Vec<u8>` packed 8 rows to a byte, using the same bit ordering (bit `i & 7`
/// of byte `i >> 3`). On little-endian hosts (x86_64 / aarch64 — the only
/// platforms ducklink supports) the u64 word bytes ALREADY carry the bits in
/// that order, so this is a straight `memcpy` of `(len + 7) / 8` bytes. A null
/// `validity` pointer returns an empty `Vec<u8>` (the "no NULLs" fast-path the
/// guest recognises).
///
/// # Safety
/// `validity`, when non-null, must point to a DuckDB validity mask covering
/// at least `len` rows.
unsafe fn duckdb_validity_to_colvec_bytes(validity: *const u64, len: usize) -> Vec<u8> {
    if validity.is_null() {
        return Vec::new();
    }
    let nbytes = (len + 7) / 8;
    let src = std::slice::from_raw_parts(validity as *const u8, nbytes);
    src.to_vec()
}

/// Read column `j` of the DataChunk directly into a WIT `Colvec` — the columnar
/// arg the guest's `call-scalar-batch-col` consumes. Fixed-width arms are one
/// `.to_vec()` off the DuckDB slice (a single per-column memcpy, no per-cell
/// enum wrap); TEXT/BLOB/COMPLEX walk row-by-row because they own their
/// storage. Contrast with [`read_col_into`], which materialises a row-major
/// `Vec<Vec<WitVal>>` scratch and forces the runtime to re-pivot at the
/// wasmtime boundary.
///
/// # Safety
/// `validity`, when non-null, must point to a DuckDB validity mask covering
/// at least `len` rows. `vec` must be a DuckDB flat vector storing `code` type
/// values.
unsafe fn read_col_to_colvec(
    code: u8,
    vec: &FlatVector,
    validity: *const u64,
    len: usize,
) -> Colvec {
    let validity_bytes = duckdb_validity_to_colvec_bytes(validity, len);
    let is_null = |i: usize| !validity.is_null() && !row_valid(validity, i);
    macro_rules! prim {
        ($ty:ty, $variant:ident) => {{
            let s = vec.as_slice_with_len::<$ty>(len);
            ColvecColumn::$variant(s.to_vec())
        }};
    }
    let data = match code {
        T_I64 => prim!(i64, Int64),
        T_U64 => prim!(u64, Uint64),
        T_F64 => prim!(f64, Float64),
        T_BOOL => prim!(bool, Boolean),
        T_I8 => prim!(i8, Int8),
        T_I16 => prim!(i16, Int16),
        T_I32 => prim!(i32, Int32),
        T_U8 => prim!(u8, Uint8),
        T_U16 => prim!(u16, Uint16),
        T_U32 => prim!(u32, Uint32),
        T_F32 => prim!(f32, Float32),
        T_TIMESTAMP => prim!(i64, Timestamp),
        T_DATE => prim!(i32, Date),
        T_TIME => prim!(i64, Time),
        T_TIMESTAMPTZ => prim!(i64, Timestamptz),
        T_INTERVAL => {
            let s = vec.as_slice_with_len::<ffi::duckdb_interval>(len);
            let out: Vec<ColvecInterval> = s
                .iter()
                .map(|iv| ColvecInterval {
                    months: iv.months,
                    days: iv.days,
                    micros: iv.micros,
                })
                .collect();
            ColvecColumn::Interval(out)
        }
        T_DECIMAL => {
            let s = vec.as_slice_with_len::<i128>(len);
            let out: Vec<ColvecDecimal> = s
                .iter()
                .map(|&raw| {
                    let u = raw as u128;
                    ColvecDecimal {
                        lower: u as u64,
                        upper: (u >> 64) as u64,
                        // The value's width/scale is not available from the flat
                        // vector here; the registration declared DECIMAL(18, 3).
                        width: 18,
                        scale: 3,
                    }
                })
                .collect();
            ColvecColumn::Decimal(out)
        }
        T_UUID => {
            let s = vec.as_slice_with_len::<i128>(len);
            let out: Vec<ColvecUuid> = s
                .iter()
                .map(|&raw| {
                    let logical = uuid_storage_to_logical(raw);
                    ColvecUuid {
                        hi: (logical >> 64) as u64,
                        lo: logical as u64,
                    }
                })
                .collect();
            ColvecColumn::Uuid(out)
        }
        T_COMPLEX => {
            let s = vec.as_slice_with_len::<duckdb_string_t>(len);
            let out: Vec<ColvecComplex> = (0..len)
                .map(|i| {
                    if is_null(i) {
                        ColvecComplex {
                            type_expr: String::new(),
                            json: String::new(),
                        }
                    } else {
                        let mut t = s[i];
                        ColvecComplex {
                            type_expr: String::new(),
                            json: DuckString::new(&mut t).as_str().into_owned(),
                        }
                    }
                })
                .collect();
            ColvecColumn::Complex(out)
        }
        T_TEXT => {
            let s = vec.as_slice_with_len::<duckdb_string_t>(len);
            let out: Vec<String> = (0..len)
                .map(|i| {
                    if is_null(i) {
                        String::new()
                    } else {
                        let mut t = s[i];
                        DuckString::new(&mut t).as_str().into_owned()
                    }
                })
                .collect();
            ColvecColumn::Text(out)
        }
        T_BLOB => {
            let s = vec.as_slice_with_len::<duckdb_string_t>(len);
            let out: Vec<Vec<u8>> = (0..len)
                .map(|i| {
                    if is_null(i) {
                        Vec::new()
                    } else {
                        let mut t = s[i];
                        DuckString::new(&mut t).as_bytes().to_vec()
                    }
                })
                .collect();
            ColvecColumn::Blob(out)
        }
        _ => unreachable!("type code out of range"),
    };
    Colvec {
        data,
        validity: validity_bytes,
        rows: len as u32,
    }
}

/// Lower a result `Colvec` from the guest back into a DuckDB flat output
/// vector. Fixed-width arms use `slice::copy_from_slice` — a single memcpy per
/// column when the result has no NULLs and no input row masked as NULL — so
/// the whole write side of the scalar dispatch is one column-major copy.
/// TEXT/BLOB/COMPLEX still walk row-by-row (variable-length data cannot be
/// bulk-copied into the DuckDB string arena). Any row masked by `null_mask`
/// (an INPUT null) or by the `Colvec`'s validity mask is written as NULL.
fn write_colvec(
    code: u8,
    out: &mut FlatVector,
    colvec: Colvec,
    null_mask: Option<&[bool]>,
    len: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if colvec.rows as usize != len {
        return Err(format!(
            "component returned {} rows, expected {len}",
            colvec.rows
        )
        .into());
    }
    let colvec_validity = colvec.validity;
    let result_null = |i: usize| -> bool {
        // Empty validity = all-valid (the same shortcut colvec_to_values uses).
        !colvec_validity.is_empty()
            && (i >> 3) < colvec_validity.len()
            && (colvec_validity[i >> 3] >> (i & 7)) & 1 == 0
    };
    let input_null = |i: usize| null_mask.is_some_and(|nm| nm[i]);
    let is_null = |i: usize| input_null(i) || result_null(i);
    // Whether ANY row is masked NULL — lets the primitive arms take the pure
    // memcpy path when the result and the inputs are all-valid.
    let any_input_null = null_mask.map_or(false, |nm| nm.iter().any(|&b| b));
    let has_null = !colvec_validity.is_empty() || any_input_null;
    macro_rules! prim {
        ($ty:ty, $variant:ident) => {{
            match colvec.data {
                ColvecColumn::$variant(src) => {
                    if src.len() != len {
                        return Err(format!(
                            "component returned column of {} values, expected {len}",
                            src.len()
                        )
                        .into());
                    }
                    let s = unsafe { out.as_mut_slice_with_len::<$ty>(len) };
                    if !has_null {
                        // Column-major memcpy — one call per column, per chunk.
                        s.copy_from_slice(&src);
                    } else {
                        let mut nulls: Vec<usize> = Vec::new();
                        for (i, x) in src.into_iter().enumerate() {
                            if is_null(i) {
                                nulls.push(i);
                            } else {
                                s[i] = x;
                            }
                        }
                        for i in nulls {
                            out.set_null(i);
                        }
                    }
                    Ok(())
                }
                other => Err(format!(
                    "component returned column {} incompatible with declared return type",
                    describe_column(&other)
                )
                .into()),
            }
        }};
    }
    match code {
        T_I64 => prim!(i64, Int64),
        T_U64 => prim!(u64, Uint64),
        T_F64 => prim!(f64, Float64),
        T_BOOL => prim!(bool, Boolean),
        T_I8 => prim!(i8, Int8),
        T_I16 => prim!(i16, Int16),
        T_I32 => prim!(i32, Int32),
        T_U8 => prim!(u8, Uint8),
        T_U16 => prim!(u16, Uint16),
        T_U32 => prim!(u32, Uint32),
        T_F32 => prim!(f32, Float32),
        T_TIMESTAMP => prim!(i64, Timestamp),
        T_DATE => prim!(i32, Date),
        T_TIME => prim!(i64, Time),
        T_TIMESTAMPTZ => prim!(i64, Timestamptz),
        // Variable-width and HUGEINT-backed types (TEXT/BLOB/COMPLEX/DECIMAL/
        // INTERVAL/UUID) can't take the primitive memcpy path; lower the Colvec
        // to Vec<WitVal> and reuse the existing per-row writer. Colder path.
        _ => {
            let vals = colvec_to_witvals(Colvec {
                data: colvec.data,
                validity: colvec_validity,
                rows: len as u32,
            });
            for (i, r) in vals.iter().enumerate() {
                if input_null(i) {
                    out.set_null(i);
                } else {
                    write_ret(code, out, i, len, r)?;
                }
            }
            Ok(())
        }
    }
}

/// Human-readable Column variant name, for error messages when the guest
/// returned a column type incompatible with the declared return type.
fn describe_column(c: &ColvecColumn) -> &'static str {
    match c {
        ColvecColumn::Boolean(_) => "Boolean",
        ColvecColumn::Int64(_) => "Int64",
        ColvecColumn::Uint64(_) => "Uint64",
        ColvecColumn::Float64(_) => "Float64",
        ColvecColumn::Int32(_) => "Int32",
        ColvecColumn::Int16(_) => "Int16",
        ColvecColumn::Int8(_) => "Int8",
        ColvecColumn::Uint32(_) => "Uint32",
        ColvecColumn::Uint16(_) => "Uint16",
        ColvecColumn::Uint8(_) => "Uint8",
        ColvecColumn::Float32(_) => "Float32",
        ColvecColumn::Timestamp(_) => "Timestamp",
        ColvecColumn::Time(_) => "Time",
        ColvecColumn::Timestamptz(_) => "Timestamptz",
        ColvecColumn::Date(_) => "Date",
        ColvecColumn::Text(_) => "Text",
        ColvecColumn::Blob(_) => "Blob",
        ColvecColumn::Decimal(_) => "Decimal",
        ColvecColumn::Interval(_) => "Interval",
        ColvecColumn::Uuid(_) => "Uuid",
        ColvecColumn::Complex(_) => "Complex",
    }
}

/// Lower a Colvec whose contents are variable-width or HUGEINT-backed
/// (TEXT/BLOB/COMPLEX/DECIMAL/INTERVAL/UUID) back to a row-major `Vec<WitVal>`.
/// The primitive arms take the memcpy path in [`write_colvec`] and never call
/// this — this only runs for the cold fallback where per-row lowering is
/// unavoidable anyway.
fn colvec_to_witvals(c: Colvec) -> Vec<WitVal> {
    let n = c.rows as usize;
    let is_valid = |i: usize| -> bool {
        c.validity.is_empty()
            || (i >> 3 >= c.validity.len())
            || (c.validity[i >> 3] >> (i & 7)) & 1 != 0
    };
    let mut out = Vec::with_capacity(n);
    macro_rules! emit {
        ($v:expr, $ctor:expr) => {{
            for (i, x) in $v.into_iter().enumerate() {
                out.push(if is_valid(i) { $ctor(x) } else { WitVal::Null });
            }
        }};
    }
    match c.data {
        ColvecColumn::Boolean(v) => emit!(v, WitVal::Boolean),
        ColvecColumn::Int64(v) => emit!(v, WitVal::Int64),
        ColvecColumn::Uint64(v) => emit!(v, WitVal::Uint64),
        ColvecColumn::Float64(v) => emit!(v, WitVal::Float64),
        ColvecColumn::Int32(v) => emit!(v, WitVal::Int32),
        ColvecColumn::Int16(v) => emit!(v, WitVal::Int16),
        ColvecColumn::Int8(v) => emit!(v, WitVal::Int8),
        ColvecColumn::Uint32(v) => emit!(v, WitVal::Uint32),
        ColvecColumn::Uint16(v) => emit!(v, WitVal::Uint16),
        ColvecColumn::Uint8(v) => emit!(v, WitVal::Uint8),
        ColvecColumn::Float32(v) => emit!(v, WitVal::Float32),
        ColvecColumn::Timestamp(v) => emit!(v, WitVal::Timestamp),
        ColvecColumn::Time(v) => emit!(v, WitVal::Time),
        ColvecColumn::Timestamptz(v) => emit!(v, WitVal::Timestamptz),
        ColvecColumn::Date(v) => emit!(v, WitVal::Date),
        ColvecColumn::Text(v) => emit!(v, WitVal::Text),
        ColvecColumn::Blob(v) => emit!(v, WitVal::Blob),
        ColvecColumn::Decimal(v) => {
            for (i, d) in v.into_iter().enumerate() {
                out.push(if is_valid(i) {
                    WitVal::Decimal(WitDecimal {
                        lower: d.lower,
                        upper: d.upper,
                        width: d.width,
                        scale: d.scale,
                    })
                } else {
                    WitVal::Null
                });
            }
        }
        ColvecColumn::Interval(v) => {
            for (i, d) in v.into_iter().enumerate() {
                out.push(if is_valid(i) {
                    WitVal::Interval(WitInterval {
                        months: d.months,
                        days: d.days,
                        micros: d.micros,
                    })
                } else {
                    WitVal::Null
                });
            }
        }
        ColvecColumn::Uuid(v) => {
            for (i, d) in v.into_iter().enumerate() {
                out.push(if is_valid(i) {
                    WitVal::Uuid(WitUuid { hi: d.hi, lo: d.lo })
                } else {
                    WitVal::Null
                });
            }
        }
        ColvecColumn::Complex(v) => {
            for (i, d) in v.into_iter().enumerate() {
                out.push(if is_valid(i) {
                    WitVal::Complex(WitComplex {
                        type_expr: d.type_expr,
                        json: d.json,
                    })
                } else {
                    WitVal::Null
                });
            }
        }
    }
    out
}

/// Write a component-returned WIT value into row `i` of a flat output column.
fn write_ret(
    code: u8,
    vec: &mut FlatVector,
    i: usize,
    len: usize,
    v: &WitVal,
) -> Result<(), Box<dyn std::error::Error>> {
    match (code, v) {
        // A component may return SQL NULL for any declared return type (e.g. a
        // validator on bad input) — mark the output row invalid.
        (_, WitVal::Null) => vec.set_null(i),
        (T_I64, WitVal::Int64(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i64>(len) };
            s[i] = *x;
        }
        (T_U64, WitVal::Uint64(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<u64>(len) };
            s[i] = *x;
        }
        (T_F64, WitVal::Float64(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<f64>(len) };
            s[i] = *x;
        }
        (T_BOOL, WitVal::Boolean(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<bool>(len) };
            s[i] = *x;
        }
        (T_I8, WitVal::Int8(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i8>(len) };
            s[i] = *x;
        }
        (T_I16, WitVal::Int16(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i16>(len) };
            s[i] = *x;
        }
        (T_I32, WitVal::Int32(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i32>(len) };
            s[i] = *x;
        }
        (T_U8, WitVal::Uint8(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<u8>(len) };
            s[i] = *x;
        }
        (T_U16, WitVal::Uint16(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<u16>(len) };
            s[i] = *x;
        }
        (T_U32, WitVal::Uint32(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<u32>(len) };
            s[i] = *x;
        }
        (T_F32, WitVal::Float32(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<f32>(len) };
            s[i] = *x;
        }
        // Temporal types share their underlying integer storage.
        (T_TIMESTAMP, WitVal::Timestamp(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i64>(len) };
            s[i] = *x;
        }
        (T_DATE, WitVal::Date(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i32>(len) };
            s[i] = *x;
        }
        (T_TIME, WitVal::Time(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i64>(len) };
            s[i] = *x;
        }
        (T_TIMESTAMPTZ, WitVal::Timestamptz(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i64>(len) };
            s[i] = *x;
        }
        (T_INTERVAL, WitVal::Interval(iv)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<ffi::duckdb_interval>(len) };
            s[i] = ffi::duckdb_interval {
                months: iv.months,
                days: iv.days,
                micros: iv.micros,
            };
        }
        (T_DECIMAL, WitVal::Decimal(d)) => {
            // HUGEINT-backed. The declared column is DECIMAL(18, 3); a value whose
            // width/scale differ would be misinterpreted (known limitation).
            let s = unsafe { vec.as_mut_slice_with_len::<i128>(len) };
            s[i] = (((d.upper as u128) << 64) | d.lower as u128) as i128;
        }
        (T_UUID, WitVal::Uuid(u)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i128>(len) };
            let logical = ((u.hi as u128) << 64) | u.lo as u128;
            // logical -> physical sign-flipped hugeint storage.
            s[i] = uuid_storage_to_logical(logical as i128) as i128;
        }
        // No nested-vector writer: emit the JSON rendering into the VARCHAR column.
        (T_COMPLEX, WitVal::Complex(c)) => vec.insert(i, c.json.as_str()),
        (T_TEXT, WitVal::Text(x)) => vec.insert(i, x.as_str()),
        (T_BLOB, WitVal::Blob(x)) => vec.insert(i, x.as_slice()),
        (_, other) => {
            return Err(format!(
                "component returned {other:?}, incompatible with declared return type"
            )
            .into());
        }
    }
    Ok(())
}

/// Write a whole column of component-returned WIT values into the flat output
/// vector. Symmetric to [`read_col_into`]: for the fixed-width result types the
/// output's typed slice and the return-type match are derived ONCE per chunk,
/// leaving only an index + value store per row — rather than re-deriving the
/// slice and re-matching `(code, value)` on every row, as a per-row [`write_ret`]
/// does. This is the write-side counterpart to the column-major read hoist.
///
/// `null_mask[i] == true` (an input NULL the bridge propagates) and a
/// component-returned `Null` both mark row `i` invalid. The output's typed slice
/// and `set_null` both need `&mut` access to the same vector, so the null rows are
/// recorded while the slice is held and invalidated only after it is dropped.
/// The `nulls` scratch never allocates unless a NULL actually occurs, so the
/// all-valid common case stays allocation-free. Variable-width and rarer fixed
/// types (TEXT/BLOB/COMPLEX/DECIMAL/INTERVAL/UUID) fall back to the per-row
/// [`write_ret`], preserving its exact behaviour.
fn write_col_from(
    code: u8,
    out: &mut FlatVector,
    results: &[WitVal],
    null_mask: Option<&[bool]>,
    len: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let is_null = |i: usize| null_mask.is_some_and(|nm| nm[i]);
    // Hoist the typed output slice + the variant match out of the per-row loop.
    macro_rules! hoist {
        ($ty:ty, $variant:ident) => {{
            let s = unsafe { out.as_mut_slice_with_len::<$ty>(len) };
            let mut nulls: Vec<usize> = Vec::new();
            for (i, r) in results.iter().enumerate() {
                if is_null(i) {
                    nulls.push(i);
                    continue;
                }
                match r {
                    WitVal::$variant(x) => s[i] = *x,
                    // A component may return SQL NULL for any declared type.
                    WitVal::Null => nulls.push(i),
                    other => {
                        return Err(format!(
                            "component returned {other:?}, incompatible with declared return type"
                        )
                        .into());
                    }
                }
            }
            // The slice borrow has ended; now invalidate the recorded NULL rows.
            for i in nulls {
                out.set_null(i);
            }
            Ok(())
        }};
    }
    match code {
        T_I64 => hoist!(i64, Int64),
        T_U64 => hoist!(u64, Uint64),
        T_F64 => hoist!(f64, Float64),
        T_BOOL => hoist!(bool, Boolean),
        T_I8 => hoist!(i8, Int8),
        T_I16 => hoist!(i16, Int16),
        T_I32 => hoist!(i32, Int32),
        T_U8 => hoist!(u8, Uint8),
        T_U16 => hoist!(u16, Uint16),
        T_U32 => hoist!(u32, Uint32),
        T_F32 => hoist!(f32, Float32),
        // Temporal types share their underlying integer storage.
        T_TIMESTAMP => hoist!(i64, Timestamp),
        T_DATE => hoist!(i32, Date),
        T_TIME => hoist!(i64, Time),
        T_TIMESTAMPTZ => hoist!(i64, Timestamptz),
        // Variable-width (TEXT/BLOB/COMPLEX) and the rarer HUGEINT-backed fixed
        // types (DECIMAL/INTERVAL/UUID) keep the per-row writer.
        _ => {
            for (i, r) in results.iter().enumerate() {
                if is_null(i) {
                    out.set_null(i);
                } else {
                    write_ret(code, out, i, len, r)?;
                }
            }
            Ok(())
        }
    }
}

// The signature for the next `register_scalar_function_with_state` call.
// `VScalar::signatures()` is a static method with no access to the function's
// state, so the per-function signature is handed to it through this thread-local,
// set immediately before the (synchronous) registration call.
thread_local! {
    static PENDING_SIGNATURE: RefCell<Option<(Vec<u8>, u8)>> = const { RefCell::new(None) };
}

/// One `VScalar` impl serving every component scalar. The argument / return
/// types come from the state (for dispatch) and from `PENDING_SIGNATURE` (for
/// the SQL signature), so any arity and any supported type combination works.
struct WasmScalar;

impl VScalar for WasmScalar {
    type State = WasmScalarState;

    fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Never let a panic in marshalling/dispatch unwind into DuckDB's C call.
        guard("scalar dispatch", || {
            let len = input.len();
            let cols: Vec<FlatVector> = (0..state.arg_codes.len())
                .map(|j| input.flat_vector(j))
                .collect();
            let mut out = output.flat_vector();
            // Raw chunk handle, so each column's validity mask can be fetched once
            // (FlatVector exposes only a per-row null check, which would re-fetch the
            // mask every cell). NULL-free columns -> null mask -> branch-free reads.
            let raw_chunk = input.get_ptr();

            // Marshal the whole chunk column-natively, then cross into the
            // component once. Each input column becomes ONE `Colvec` — for the
            // primitive arms (I64/F64/BOOL/temporal/...) that's a single
            // `.to_vec()` (memcpy) off the DuckDB slice. No row-major
            // `Vec<Vec<WitVal>>` scratch is materialised, and the runtime
            // hands the colvecs straight to `call-scalar-batch-col` without
            // its own `rows_to_colvecs` pivot.
            let arity = state.arg_codes.len();
            // DuckDB does not propagate input NULLs to the output for these scalars
            // (it invokes the function on NULL rows and keeps the result), so the
            // bridge enforces SQL semantics itself: any row with a NULL input yields
            // a NULL result, overriding whatever the component computed from the
            // placeholder it was fed. `None` until a NULL-bearing column is seen, so
            // the all-valid common case allocates nothing and skips the scan.
            let mut null_mask: Option<Vec<bool>> = None;
            let mut args: Vec<Colvec> = Vec::with_capacity(arity);
            for (j, &code) in state.arg_codes.iter().enumerate() {
                // Fetch the column's validity mask once (null when no NULLs).
                let validity = unsafe {
                    let v = ffi::duckdb_data_chunk_get_vector(raw_chunk, j as u64);
                    ffi::duckdb_vector_get_validity(v) as *const u64
                };
                args.push(unsafe { read_col_to_colvec(code, &cols[j], validity, len) });
                if !validity.is_null() {
                    let nm = null_mask.get_or_insert_with(|| vec![false; len]);
                    for (i, slot) in nm.iter_mut().enumerate() {
                        if unsafe { !row_valid(validity, i) } {
                            *slot = true;
                        }
                    }
                }
            }
            let result = {
                let engine = &state.engine;
                engine
                    .dispatch_scalar_batch_col(state.callback_handle, 0, &args)
                    .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?
            };

            // Write the whole result column at once. The primitive arms of
            // `write_colvec` take a `slice::copy_from_slice` fast-path (one
            // memcpy per chunk) when neither the input null_mask nor the
            // Colvec's validity mask is set.
            write_colvec(state.ret_code, &mut out, result, null_mask.as_deref(), len)?;
            Ok(())
        })
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        let (arg_codes, ret_code) = PENDING_SIGNATURE
            .with(|s| s.borrow().clone())
            .expect("PENDING_SIGNATURE must be set before registration");
        vec![ScalarFunctionSignature::exact(
            arg_codes.into_iter().map(logical_type).collect(),
            logical_type(ret_code),
        )]
    }
}

/// Register every component scalar on `con`. Returns the count registered. All
/// `reg` logical types are supported across any arity.
pub fn register_scalars(
    con: &Connection,
    engine: Arc<Engine2>,
    scalars: &[ScalarFunc],
) -> duckdb::Result<usize> {
    let mut registered = 0usize;
    for f in scalars {
        let arg_codes: Vec<u8> = f.arguments.iter().map(|a| type_code(&a.logical)).collect();
        let ret_code = type_code(&f.returns);
        let state = WasmScalarState {
            callback_handle: f.callback_handle,
            engine: engine.clone(),
            arg_codes: arg_codes.clone(),
            ret_code,
        };
        // Hand the signature to `WasmScalar::signatures()` for this one call.
        PENDING_SIGNATURE.with(|s| *s.borrow_mut() = Some((arg_codes, ret_code)));
        let result = con.register_scalar_function_with_state::<WasmScalar>(&f.name, &state);
        PENDING_SIGNATURE.with(|s| *s.borrow_mut() = None);
        // IDEMPOTENCY: a function of this name already in the catalog (a re-load
        // of the same component, or a name another component already claimed) is
        // NOT a hard error — DuckDB rejects the duplicate registration, which we
        // treat as "already present" and skip, so `ducklink_load` can be called
        // again without failing.
        match result {
            Ok(()) => registered += 1,
            Err(e) => {
                eprintln!("[ducklink] scalar '{}' not registered (already present?): {e}", f.name);
            }
        }
    }
    Ok(registered)
}

// ---------------------------------------------------------------------------
// Table functions
// ---------------------------------------------------------------------------

/// Convert a DuckDB call-parameter value (from `BindInfo::get_parameter`) into a
/// neutral value, extracting it as the function's declared argument type `code`.
fn param_to_neutral(code: u8, v: &Value) -> reg::DuckValue {
    if v.is_null() {
        return reg::DuckValue::Null;
    }
    match code {
        T_I64 => reg::DuckValue::Int64(v.to_int64()),
        T_U64 => reg::DuckValue::Uint64(v.to_uint64()),
        T_F64 => reg::DuckValue::Float64(v.to_double()),
        T_BOOL => reg::DuckValue::Boolean(v.to_bool()),
        T_TEXT => reg::DuckValue::Text(v.to_string()),
        // No raw blob getter on the param value; fall back to its text form.
        T_BLOB => reg::DuckValue::Blob(v.to_string().into_bytes()),
        _ => reg::DuckValue::Null,
    }
}

/// Per-function table data, passed to the static `VTab` callbacks via DuckDB's
/// extra-info slot.
#[derive(Clone)]
struct WasmTableExtra {
    callback_handle: u32,
    engine: Arc<Engine2>,
    arg_codes: Vec<u8>,
    col_codes: Vec<u8>,
    col_names: Vec<String>,
}

/// Bind result: the full set of rows the component produced for this call,
/// PIVOTED at bind time from row-major into column-major WitVal storage. Each
/// `func` chunk then hands `write_col_from` a `&[WitVal]` slice per output
/// column and the fixed-width types write via the hoisted memcpy path; the
/// per-row `neutral_to_wit + clone + write_ret` of the previous shape is gone.
struct WasmTableBind {
    columns: Vec<Vec<WitVal>>,
    total_rows: usize,
    col_codes: Vec<u8>,
}

/// Init state: a cursor over `WasmTableBind::columns` across `func` chunks.
struct WasmTableInit {
    cursor: AtomicUsize,
}

// The parameter types for the next table-function registration — handed to the
// static `VTab::parameters()` the same way `PENDING_SIGNATURE` feeds scalars.
thread_local! {
    static PENDING_TABLE_PARAMS: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
}

/// One `VTab` impl serving every component table function. `bind` runs the
/// component once with the call parameters and buffers all rows; `func` streams
/// them back in DuckDB vector-sized chunks.
struct WasmTable;

impl VTab for WasmTable {
    type InitData = WasmTableInit;
    type BindData = WasmTableBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("table bind", || {
            let extra = unsafe { &*bind.get_extra_info::<WasmTableExtra>() };
            for (name, &code) in extra.col_names.iter().zip(&extra.col_codes) {
                bind.add_result_column(name, logical_type(code));
            }
            let args: Vec<reg::DuckValue> = extra
                .arg_codes
                .iter()
                .enumerate()
                .map(|(j, &code)| param_to_neutral(code, &bind.get_parameter(j as u64)))
                .collect();
            let rows = {
                let engine = &extra.engine;
                engine
                    .dispatch_table(extra.callback_handle, args)
                    .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?
            };
            // Pivot row-major -> column-major ONCE per bind. This lets `func`
            // chunks hand `write_col_from` a `&[WitVal]` slice per column and
            // reap the fixed-width hoist (memcpy), which the per-row-per-cell
            // write_ret loop of the previous shape did not.
            let total_rows = rows.len();
            let ncols = extra.col_codes.len();
            let mut columns: Vec<Vec<WitVal>> =
                (0..ncols).map(|_| Vec::with_capacity(total_rows)).collect();
            for row in rows {
                if row.len() != ncols {
                    return Err(format!(
                        "table function returned {} cols, expected {ncols}",
                        row.len()
                    )
                    .into());
                }
                for (c, v) in row.into_iter().enumerate() {
                    columns[c].push(v);
                }
            }
            Ok(WasmTableBind {
                columns,
                total_rows,
                col_codes: extra.col_codes.clone(),
            })
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(WasmTableInit {
            cursor: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        guard("table scan", || {
            let bind = func.get_bind_data();
            let init = func.get_init_data();
            let start = init.cursor.load(Ordering::Relaxed);
            let n = bind.total_rows.saturating_sub(start).min(2048);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            for (c, &code) in bind.col_codes.iter().enumerate() {
                let mut col = output.flat_vector(c);
                let slice = &bind.columns[c][start..start + n];
                write_col_from(code, &mut col, slice, None, n)?;
            }
            init.cursor.store(start + n, Ordering::Relaxed);
            output.set_len(n);
            Ok(())
        })
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        PENDING_TABLE_PARAMS
            .with(|s| s.borrow().clone())
            .map(|codes| codes.into_iter().map(logical_type).collect())
    }
}

/// Register every component table function on `con`. Returns the count
/// registered. Parameter and column types use the same `reg` logical-type set as
/// scalars.
pub fn register_tables(
    con: &Connection,
    engine: Arc<Engine2>,
    tables: &[TableFunc],
) -> duckdb::Result<usize> {
    let mut registered = 0usize;
    for t in tables {
        let arg_codes: Vec<u8> = t.arguments.iter().map(|a| type_code(&a.logical)).collect();
        let col_codes: Vec<u8> = t.columns.iter().map(|c| type_code(&c.logical)).collect();
        let col_names: Vec<String> = t.columns.iter().map(|c| c.name.clone()).collect();
        let extra = WasmTableExtra {
            callback_handle: t.callback_handle,
            engine: engine.clone(),
            arg_codes: arg_codes.clone(),
            col_codes,
            col_names,
        };
        PENDING_TABLE_PARAMS.with(|s| *s.borrow_mut() = Some(arg_codes));
        let result = con
            .register_table_function_with_extra_info::<WasmTable, WasmTableExtra>(&t.name, &extra);
        PENDING_TABLE_PARAMS.with(|s| *s.borrow_mut() = None);
        // IDEMPOTENCY: a duplicate table-function name (re-load) is skipped, not
        // a hard error. See `register_scalars`.
        match result {
            Ok(()) => registered += 1,
            Err(e) => {
                eprintln!("[ducklink] table function '{}' not registered (already present?): {e}", t.name);
            }
        }
    }
    Ok(registered)
}

// ---------------------------------------------------------------------------
// Aggregate functions (raw C API)
// ---------------------------------------------------------------------------
//
// duckdb-rs has no safe aggregate wrapper, so aggregates go through the raw C
// API. DuckDB's aggregate is incremental (init/update/combine/finalize); the
// wasm component computes over ALL rows at once, so each group's state simply
// accumulates its input rows and `finalize` hands them to the component.

use std::ffi::CString;
use std::os::raw::{c_char, c_void};

use duckdb::ffi;

/// Per-group aggregate state: the input rows accumulated for this group, each a
/// tuple of the function's argument values.
type AggState = Vec<Vec<reg::DuckValue>>;

/// Run an aggregate FFI-callback body, converting a panic into a DuckDB
/// aggregate error rather than letting it unwind across the `extern "C"`
/// boundary (which would abort the host process). Mirrors [`guard`] for the
/// raw aggregate callbacks, which return `void` and report failure via the
/// function info.
///
/// # Safety
/// `info` must be the valid `duckdb_function_info` for the running aggregate.
unsafe fn agg_guard(info: ffi::duckdb_function_info, what: &str, f: impl FnOnce()) {
    if let Err(p) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        if let Ok(c) = CString::new(panic_msg(p, what)) {
            ffi::duckdb_aggregate_function_set_error(info, c.as_ptr());
        }
    }
}

/// Per-function data DuckDB hands to the aggregate callbacks via extra-info.
struct AggExtra {
    callback_handle: u32,
    engine: Arc<Engine2>,
    arg_codes: Vec<u8>,
    ret_code: u8,
}

fn duckdb_type_of(code: u8) -> ffi::duckdb_type {
    match code {
        T_I64 => ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
        T_U64 => ffi::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
        T_F64 => ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
        T_BOOL => ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
        T_TEXT => ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
        T_BLOB => ffi::DUCKDB_TYPE_DUCKDB_TYPE_BLOB,
        T_I8 => ffi::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT,
        T_I16 => ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
        T_I32 => ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
        T_U8 => ffi::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
        T_U16 => ffi::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
        T_U32 => ffi::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER,
        T_F32 => ffi::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
        T_TIMESTAMP => ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
        T_DATE => ffi::DUCKDB_TYPE_DUCKDB_TYPE_DATE,
        T_TIME => ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIME,
        T_TIMESTAMPTZ => ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ,
        T_INTERVAL => ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL,
        T_UUID => ffi::DUCKDB_TYPE_DUCKDB_TYPE_UUID,
        // DECIMAL via this enum has no width/scale; aggregates over DECIMAL are a
        // known limitation (the scalar/table path declares DECIMAL(18, 3) via a
        // dedicated constructor instead). COMPLEX falls back to VARCHAR/JSON.
        T_COMPLEX => ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
        _ => ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    }
}

/// Read row `i` of a raw input vector (type `code`) into a neutral value.
unsafe fn read_arg_raw(code: u8, vector: ffi::duckdb_vector, i: usize) -> reg::DuckValue {
    let validity = ffi::duckdb_vector_get_validity(vector);
    if !validity.is_null() && !ffi::duckdb_validity_row_is_valid(validity, i as u64) {
        return reg::DuckValue::Null;
    }
    let data = ffi::duckdb_vector_get_data(vector);
    match code {
        T_I64 => reg::DuckValue::Int64(*(data as *const i64).add(i)),
        T_U64 => reg::DuckValue::Uint64(*(data as *const u64).add(i)),
        T_F64 => reg::DuckValue::Float64(*(data as *const f64).add(i)),
        T_BOOL => reg::DuckValue::Boolean(*(data as *const bool).add(i)),
        T_TEXT => {
            let mut s = *(data as *const duckdb_string_t).add(i);
            reg::DuckValue::Text(DuckString::new(&mut s).as_str().into_owned())
        }
        T_BLOB => {
            let mut s = *(data as *const duckdb_string_t).add(i);
            reg::DuckValue::Blob(DuckString::new(&mut s).as_bytes().to_vec())
        }
        T_I8 => reg::DuckValue::Int8(*(data as *const i8).add(i)),
        T_I16 => reg::DuckValue::Int16(*(data as *const i16).add(i)),
        T_I32 => reg::DuckValue::Int32(*(data as *const i32).add(i)),
        T_U8 => reg::DuckValue::Uint8(*(data as *const u8).add(i)),
        T_U16 => reg::DuckValue::Uint16(*(data as *const u16).add(i)),
        T_U32 => reg::DuckValue::Uint32(*(data as *const u32).add(i)),
        T_F32 => reg::DuckValue::Float32(*(data as *const f32).add(i)),
        T_TIMESTAMP => reg::DuckValue::Timestamp(*(data as *const i64).add(i)),
        T_DATE => reg::DuckValue::Date(*(data as *const i32).add(i)),
        T_TIME => reg::DuckValue::Time(*(data as *const i64).add(i)),
        T_TIMESTAMPTZ => reg::DuckValue::Timestamptz(*(data as *const i64).add(i)),
        T_INTERVAL => {
            let iv = *(data as *const ffi::duckdb_interval).add(i);
            reg::DuckValue::Interval {
                months: iv.months,
                days: iv.days,
                micros: iv.micros,
            }
        }
        T_UUID => {
            let logical = uuid_storage_to_logical(*(data as *const i128).add(i));
            reg::DuckValue::Uuid {
                hi: (logical >> 64) as u64,
                lo: logical as u64,
            }
        }
        // DECIMAL (needs per-value width/scale) and COMPLEX (nested) over the raw
        // aggregate path are not yet marshalled; surface as NULL.
        _ => reg::DuckValue::Null,
    }
}

/// Write a neutral value into row `i` of a raw result vector (type `code`). Takes
/// the value by reference so the caller (advanced-tier pushdown scan) can walk
/// its `Vec<Vec<DuckValue>>` without cloning each cell.
pub(crate) unsafe fn write_ret_raw(
    code: u8,
    vector: ffi::duckdb_vector,
    i: usize,
    v: &reg::DuckValue,
) -> Result<(), String> {
    if matches!(v, reg::DuckValue::Null) {
        ffi::duckdb_vector_ensure_validity_writable(vector);
        let validity = ffi::duckdb_vector_get_validity(vector);
        ffi::duckdb_validity_set_row_validity(validity, i as u64, false);
        return Ok(());
    }
    let data = ffi::duckdb_vector_get_data(vector);
    match (code, v) {
        (T_I64, reg::DuckValue::Int64(x)) => *(data as *mut i64).add(i) = *x,
        (T_U64, reg::DuckValue::Uint64(x)) => *(data as *mut u64).add(i) = *x,
        (T_F64, reg::DuckValue::Float64(x)) => *(data as *mut f64).add(i) = *x,
        (T_BOOL, reg::DuckValue::Boolean(x)) => *(data as *mut bool).add(i) = *x,
        (T_TEXT, reg::DuckValue::Text(s)) => {
            ffi::duckdb_vector_assign_string_element_len(
                vector,
                i as u64,
                s.as_ptr() as *const c_char,
                s.len() as u64,
            );
        }
        (T_BLOB, reg::DuckValue::Blob(b)) => {
            ffi::duckdb_vector_assign_string_element_len(
                vector,
                i as u64,
                b.as_ptr() as *const c_char,
                b.len() as u64,
            );
        }
        (T_I8, reg::DuckValue::Int8(x)) => *(data as *mut i8).add(i) = *x,
        (T_I16, reg::DuckValue::Int16(x)) => *(data as *mut i16).add(i) = *x,
        (T_I32, reg::DuckValue::Int32(x)) => *(data as *mut i32).add(i) = *x,
        (T_U8, reg::DuckValue::Uint8(x)) => *(data as *mut u8).add(i) = *x,
        (T_U16, reg::DuckValue::Uint16(x)) => *(data as *mut u16).add(i) = *x,
        (T_U32, reg::DuckValue::Uint32(x)) => *(data as *mut u32).add(i) = *x,
        (T_F32, reg::DuckValue::Float32(x)) => *(data as *mut f32).add(i) = *x,
        (T_TIMESTAMP, reg::DuckValue::Timestamp(x)) => *(data as *mut i64).add(i) = *x,
        (T_DATE, reg::DuckValue::Date(x)) => *(data as *mut i32).add(i) = *x,
        (T_TIME, reg::DuckValue::Time(x)) => *(data as *mut i64).add(i) = *x,
        (T_TIMESTAMPTZ, reg::DuckValue::Timestamptz(x)) => *(data as *mut i64).add(i) = *x,
        (T_INTERVAL, reg::DuckValue::Interval { months, days, micros }) => {
            *(data as *mut ffi::duckdb_interval).add(i) = ffi::duckdb_interval {
                months: *months,
                days: *days,
                micros: *micros,
            };
        }
        (T_UUID, reg::DuckValue::Uuid { hi, lo }) => {
            let logical = ((*hi as u128) << 64) | *lo as u128;
            *(data as *mut i128).add(i) = uuid_storage_to_logical(logical as i128) as i128;
        }
        (T_COMPLEX, reg::DuckValue::Complex { json, .. }) => {
            ffi::duckdb_vector_assign_string_element_len(
                vector,
                i as u64,
                json.as_ptr() as *const c_char,
                json.len() as u64,
            );
        }
        (_, other) => {
            return Err(format!(
                "component returned {other:?}, incompatible with declared aggregate return type"
            ));
        }
    }
    Ok(())
}

/// Column-hoisted analog of [`write_ret_raw`] for the advanced-tier pushdown
/// scan: derive the typed data pointer + match on the return type ONCE per
/// column, then walk the values in-place. Compared to the previous per-cell
/// `write_ret_raw` loop, this saves `ncols * (nrows - 1)` FFI derefs of
/// `duckdb_vector_get_data` and the same number of `(code, val)` pattern
/// matches. For fixed-width columns it collapses to a straight pointer write
/// per row; the variable-width TEXT/BLOB/COMPLEX arms still iterate rows
/// because their storage is per-element in the DuckDB string arena.
///
/// # Safety
/// `vector` must be a valid `duckdb_vector` whose column type is `code` and
/// whose capacity is at least `len`. `vals.len()` must equal `len`.
pub(crate) unsafe fn write_col_from_raw(
    code: u8,
    vector: ffi::duckdb_vector,
    vals: &[WitVal],
    len: usize,
) -> Result<(), String> {
    debug_assert_eq!(vals.len(), len, "write_col_from_raw len mismatch");
    let data = ffi::duckdb_vector_get_data(vector);
    // Any NULL in the column upgrades the validity mask; lazy so no-NULL
    // columns pay nothing.
    let mut validity_hot: Option<*mut u64> = None;
    macro_rules! ensure_validity {
        () => {{
            match validity_hot {
                Some(v) => v,
                None => {
                    ffi::duckdb_vector_ensure_validity_writable(vector);
                    let v = ffi::duckdb_vector_get_validity(vector);
                    validity_hot = Some(v);
                    v
                }
            }
        }};
    }
    macro_rules! hoist {
        ($ty:ty, $variant:ident) => {{
            let s = data as *mut $ty;
            for (i, v) in vals.iter().enumerate() {
                match v {
                    WitVal::$variant(x) => *s.add(i) = *x,
                    WitVal::Null => {
                        let validity = ensure_validity!();
                        ffi::duckdb_validity_set_row_validity(validity, i as u64, false);
                    }
                    other => {
                        return Err(format!(
                            "component returned {other:?}, incompatible with declared return type"
                        ));
                    }
                }
            }
            Ok(())
        }};
    }
    match code {
        T_I64 => hoist!(i64, Int64),
        T_U64 => hoist!(u64, Uint64),
        T_F64 => hoist!(f64, Float64),
        T_BOOL => hoist!(bool, Boolean),
        T_I8 => hoist!(i8, Int8),
        T_I16 => hoist!(i16, Int16),
        T_I32 => hoist!(i32, Int32),
        T_U8 => hoist!(u8, Uint8),
        T_U16 => hoist!(u16, Uint16),
        T_U32 => hoist!(u32, Uint32),
        T_F32 => hoist!(f32, Float32),
        // Temporal types share underlying integer storage.
        T_TIMESTAMP => hoist!(i64, Timestamp),
        T_DATE => hoist!(i32, Date),
        T_TIME => hoist!(i64, Time),
        T_TIMESTAMPTZ => hoist!(i64, Timestamptz),
        T_TEXT => {
            for (i, v) in vals.iter().enumerate() {
                match v {
                    WitVal::Text(s) => ffi::duckdb_vector_assign_string_element_len(
                        vector,
                        i as u64,
                        s.as_ptr() as *const c_char,
                        s.len() as u64,
                    ),
                    WitVal::Null => {
                        let validity = ensure_validity!();
                        ffi::duckdb_validity_set_row_validity(validity, i as u64, false);
                    }
                    other => {
                        return Err(format!(
                            "component returned {other:?}, incompatible with declared TEXT column"
                        ));
                    }
                }
            }
            Ok(())
        }
        T_BLOB => {
            for (i, v) in vals.iter().enumerate() {
                match v {
                    WitVal::Blob(b) => ffi::duckdb_vector_assign_string_element_len(
                        vector,
                        i as u64,
                        b.as_ptr() as *const c_char,
                        b.len() as u64,
                    ),
                    WitVal::Null => {
                        let validity = ensure_validity!();
                        ffi::duckdb_validity_set_row_validity(validity, i as u64, false);
                    }
                    other => {
                        return Err(format!(
                            "component returned {other:?}, incompatible with declared BLOB column"
                        ));
                    }
                }
            }
            Ok(())
        }
        T_COMPLEX => {
            for (i, v) in vals.iter().enumerate() {
                match v {
                    WitVal::Complex(c) => ffi::duckdb_vector_assign_string_element_len(
                        vector,
                        i as u64,
                        c.json.as_ptr() as *const c_char,
                        c.json.len() as u64,
                    ),
                    WitVal::Null => {
                        let validity = ensure_validity!();
                        ffi::duckdb_validity_set_row_validity(validity, i as u64, false);
                    }
                    other => {
                        return Err(format!(
                            "component returned {other:?}, incompatible with declared COMPLEX column"
                        ));
                    }
                }
            }
            Ok(())
        }
        T_INTERVAL => {
            let s = data as *mut ffi::duckdb_interval;
            for (i, v) in vals.iter().enumerate() {
                match v {
                    WitVal::Interval(iv) => {
                        *s.add(i) = ffi::duckdb_interval {
                            months: iv.months,
                            days: iv.days,
                            micros: iv.micros,
                        };
                    }
                    WitVal::Null => {
                        let validity = ensure_validity!();
                        ffi::duckdb_validity_set_row_validity(validity, i as u64, false);
                    }
                    other => {
                        return Err(format!(
                            "component returned {other:?}, incompatible with declared INTERVAL column"
                        ));
                    }
                }
            }
            Ok(())
        }
        T_UUID => {
            let s = data as *mut i128;
            for (i, v) in vals.iter().enumerate() {
                match v {
                    WitVal::Uuid(u) => {
                        let logical = ((u.hi as u128) << 64) | u.lo as u128;
                        *s.add(i) = uuid_storage_to_logical(logical as i128) as i128;
                    }
                    WitVal::Null => {
                        let validity = ensure_validity!();
                        ffi::duckdb_validity_set_row_validity(validity, i as u64, false);
                    }
                    other => {
                        return Err(format!(
                            "component returned {other:?}, incompatible with declared UUID column"
                        ));
                    }
                }
            }
            Ok(())
        }
        T_DECIMAL => {
            let s = data as *mut i128;
            for (i, v) in vals.iter().enumerate() {
                match v {
                    WitVal::Decimal(d) => {
                        *s.add(i) = (((d.upper as u128) << 64) | d.lower as u128) as i128;
                    }
                    WitVal::Null => {
                        let validity = ensure_validity!();
                        ffi::duckdb_validity_set_row_validity(validity, i as u64, false);
                    }
                    other => {
                        return Err(format!(
                            "component returned {other:?}, incompatible with declared DECIMAL column"
                        ));
                    }
                }
            }
            Ok(())
        }
        _ => Err(format!(
            "write_col_from_raw: unsupported column type code {code}"
        )),
    }
}

unsafe extern "C" fn agg_state_size(_info: ffi::duckdb_function_info) -> ffi::idx_t {
    std::mem::size_of::<*mut AggState>() as ffi::idx_t
}

unsafe extern "C" fn agg_init(
    _info: ffi::duckdb_function_info,
    state: ffi::duckdb_aggregate_state,
) {
    let slot = state as *mut *mut AggState;
    *slot = Box::into_raw(Box::new(AggState::new()));
}

unsafe extern "C" fn agg_update(
    info: ffi::duckdb_function_info,
    input: ffi::duckdb_data_chunk,
    states: *mut ffi::duckdb_aggregate_state,
) {
    agg_guard(info, "aggregate update", || {
        let extra = &*(ffi::duckdb_aggregate_function_get_extra_info(info) as *const AggExtra);
        let n = ffi::duckdb_data_chunk_get_size(input) as usize;
        let ncols = extra.arg_codes.len();
        let vectors: Vec<ffi::duckdb_vector> = (0..ncols)
            .map(|c| ffi::duckdb_data_chunk_get_vector(input, c as u64))
            .collect();
        for row in 0..n {
            // The state for this input row (states is parallel to the input chunk).
            let st = *states.add(row);
            let group = &mut **(st as *mut *mut AggState);
            let argrow: Vec<reg::DuckValue> = (0..ncols)
                .map(|c| read_arg_raw(extra.arg_codes[c], vectors[c], row))
                .collect();
            group.push(argrow);
        }
    });
}

unsafe extern "C" fn agg_combine(
    info: ffi::duckdb_function_info,
    source: *mut ffi::duckdb_aggregate_state,
    target: *mut ffi::duckdb_aggregate_state,
    count: ffi::idx_t,
) {
    agg_guard(info, "aggregate combine", || {
        for i in 0..count as usize {
            let s = &mut **(*source.add(i) as *mut *mut AggState);
            let t = &mut **(*target.add(i) as *mut *mut AggState);
            t.append(s);
        }
    });
}

unsafe extern "C" fn agg_finalize(
    info: ffi::duckdb_function_info,
    source: *mut ffi::duckdb_aggregate_state,
    result: ffi::duckdb_vector,
    count: ffi::idx_t,
    offset: ffi::idx_t,
) {
    agg_guard(info, "aggregate finalize", || {
        let extra = &*(ffi::duckdb_aggregate_function_get_extra_info(info) as *const AggExtra);
        for i in 0..count as usize {
            let group = &mut **(*source.add(i) as *mut *mut AggState);
            let rows = std::mem::take(group);
            let dispatched = {
                let engine = &extra.engine;
                engine.dispatch_aggregate(extra.callback_handle, rows)
            };
            let out = offset as usize + i;
            let write = dispatched
                .map_err(|e| e.to_string())
                .and_then(|v| write_ret_raw(extra.ret_code, result, out, &v));
            if let Err(msg) = write {
                if let Ok(c) = CString::new(msg) {
                    ffi::duckdb_aggregate_function_set_error(info, c.as_ptr());
                }
                return;
            }
        }
    });
}

unsafe extern "C" fn agg_destroy(states: *mut ffi::duckdb_aggregate_state, count: ffi::idx_t) {
    for i in 0..count as usize {
        let p = *(*states.add(i) as *mut *mut AggState);
        if !p.is_null() {
            drop(Box::from_raw(p));
        }
    }
}

unsafe extern "C" fn agg_extra_destroy(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut AggExtra));
    }
}

/// Register every component aggregate function on the raw connection `raw_con`
/// via the DuckDB C API. Functions registered on any connection of a database
/// are visible to all its connections, so `raw_con` need only share the database
/// with the connection used for queries.
///
/// # Safety
/// `raw_con` must be a valid `duckdb_connection`.
pub unsafe fn register_aggregates(
    raw_con: ffi::duckdb_connection,
    engine: Arc<Engine2>,
    aggregates: &[AggregateFunc],
) -> duckdb::Result<usize> {
    let mut registered = 0usize;
    for f in aggregates {
        let arg_codes: Vec<u8> = f.arguments.iter().map(|a| type_code(&a.logical)).collect();
        let ret_code = type_code(&f.returns);

        let func = ffi::duckdb_create_aggregate_function();
        let cname = CString::new(f.name.as_str())
            .map_err(|_| duckdb::Error::DuckDBFailure(ffi::Error::new(ffi::DuckDBError), None))?;
        ffi::duckdb_aggregate_function_set_name(func, cname.as_ptr());
        for &code in &arg_codes {
            let mut lt = ffi::duckdb_create_logical_type(duckdb_type_of(code));
            ffi::duckdb_aggregate_function_add_parameter(func, lt);
            ffi::duckdb_destroy_logical_type(&mut lt);
        }
        let mut rlt = ffi::duckdb_create_logical_type(duckdb_type_of(ret_code));
        ffi::duckdb_aggregate_function_set_return_type(func, rlt);
        ffi::duckdb_destroy_logical_type(&mut rlt);

        let extra = Box::into_raw(Box::new(AggExtra {
            callback_handle: f.callback_handle,
            engine: engine.clone(),
            arg_codes,
            ret_code,
        })) as *mut c_void;
        ffi::duckdb_aggregate_function_set_extra_info(func, extra, Some(agg_extra_destroy));
        ffi::duckdb_aggregate_function_set_functions(
            func,
            Some(agg_state_size),
            Some(agg_init),
            Some(agg_update),
            Some(agg_combine),
            Some(agg_finalize),
        );
        ffi::duckdb_aggregate_function_set_destructor(func, Some(agg_destroy));

        let rc = ffi::duckdb_register_aggregate_function(raw_con, func);
        let mut func_mut = func;
        ffi::duckdb_destroy_aggregate_function(&mut func_mut);
        // IDEMPOTENCY: a duplicate aggregate name (re-load) is skipped, not a
        // hard error — the C API returns failure for an already-present name,
        // which we treat as "already registered". See `register_scalars`.
        if rc != ffi::DuckDBSuccess {
            eprintln!("[ducklink] aggregate '{}' not registered (already present?)", f.name);
            continue;
        }
        registered += 1;
    }
    Ok(registered)
}

/// A component to load at extension-load time: a display name and a path to the
/// `.wasm` artifact.
#[derive(Clone, Debug)]
pub struct ComponentSpec {
    pub name: String,
    pub path: PathBuf,
}

/// Parse the `DUCKLINK_COMPONENTS` environment variable into specs. The value is
/// a `:`-separated list; each entry is either `name=path` or a bare `path`
/// (whose file stem becomes the name). Empty / unset yields no specs.
///
/// This is how a deployment selects which components `LOAD ducklink` exposes —
/// catalog registration is a load-time operation, so components are named up
/// front rather than via an in-query `CALL`.
pub fn component_specs_from_env() -> Vec<ComponentSpec> {
    let raw = std::env::var("DUCKLINK_COMPONENTS").unwrap_or_default();
    raw.split(':')
        .filter(|entry| !entry.is_empty())
        .map(|entry| match entry.split_once('=') {
            Some((name, path)) => ComponentSpec {
                name: name.to_string(),
                path: PathBuf::from(path),
            },
            None => {
                let path = PathBuf::from(entry);
                let name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("component")
                    .to_string();
                ComponentSpec { name, path }
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// `ducklink_load(path)` — RUNTIME component loading from SQL
// ---------------------------------------------------------------------------
//
// `LOAD ducklink` (the extension entry point) loads the components named in
// `DUCKLINK_COMPONENTS` up front. `ducklink_load(path)` is the in-SQL analogue
// of DuckDB's own `LOAD`: it loads ONE component and registers its functions on
// the live database so they are callable in SUBSEQUENT statements of the same
// session.
//
// Surface: a TABLE function (`SELECT * FROM ducklink_load('path')` /
// `CALL ducklink_load('path')`). Registration is done in `VTab::bind`, which
// DuckDB runs during the BIND/PLAN phase of the query — the phase where catalog
// access happens — rather than in a scalar `invoke` (execution phase, after the
// plan is already bound and while the executing query may hold catalog locks).
// The component's functions land in the catalog as a side effect of binding the
// `ducklink_load` call, so the NEXT statement's binder resolves them.
//
// The registration itself is performed on a FRESH duckdb-rs `Connection` opened
// from the process-wide `duckdb_database` handle (captured at `LOAD ducklink`
// time), NOT on the connection currently executing the `ducklink_load` query.
// Registrations are database-wide, so every connection — including the caller's
// — sees the new functions on the next statement. The fresh connection also
// gives the registration its own catalog/transaction context, avoiding any
// re-entrancy against the connection that is mid-query.

/// Process-wide ducklink runtime, captured once at `LOAD ducklink` time so the
/// `ducklink_load` table function (whose static `VTab::bind` has no `self` and
/// no connection of its own) can reach the shared `Engine2` and re-open a
/// `Connection` on the live database to register newly loaded functions.
///
/// The `duckdb_database` is a raw pointer that DuckDB owns for the whole process
/// lifetime; we only ever read it (to `open_from_raw`), never free it. The raw
/// pointer is not `Send`/`Sync`, so it is wrapped — DuckDB serialises bind, and
/// the registration goes through a fresh connection, so concurrent loads are
/// guarded by the engine `Mutex`.
struct DucklinkRuntime {
    /// The `duckdb_database` handle captured at `LOAD ducklink` time. KEPT FOR
    /// REFERENCE/diagnostics only — it is NOT safe to re-`duckdb_connect` later:
    /// in the real loadable host this points at a `DatabaseWrapper` owned by the
    /// stack-local `DuckDBExtensionLoadState`, which is FREED when the extension's
    /// `init` returns (see duckdb `extension_load.cpp`: `database_data` is a
    /// `unique_ptr` member of a stack `load_state`). Reconnecting through it later
    /// is a use-after-free that surfaces as `Binder Error: connect error`. Runtime
    /// registration therefore reuses `con` below (a connection opened ONCE at init,
    /// while the handle was still alive), never this handle.
    #[allow(dead_code)]
    db: ffi::duckdb_database,
    /// A duckdb-rs connection opened ONCE at init (while `db` was still valid) and
    /// kept alive for the whole process. A `Connection` owns a `shared_ptr` to the
    /// live `DatabaseInstance`, so it stays valid after the load state is freed.
    /// Runtime function registration (`ducklink_load` / `LOAD WASM`) goes through
    /// this connection: registration only touches the already-open
    /// `duckdb_connection` (no reconnect), and is database-wide so the functions
    /// are visible to every connection on the NEXT statement.
    con: Mutex<Connection>,
    /// A raw sibling `duckdb_connection`, also opened once at init and kept alive,
    /// for the aggregate registration path (which needs the raw C handle). Null if
    /// the init-time connect failed. Disconnected at process exit (we never free
    /// it explicitly — it lives as long as the runtime).
    raw_con: RawConnHandle,
    engine: Arc<Engine2>,
    /// Components loaded in THIS session (by `ducklink_load`), tracked so
    /// `ducklink_modules().loaded` can report them and so a re-load is idempotent.
    loaded: Mutex<Vec<LoadedRecord>>,
}

/// A kept-alive raw `duckdb_connection` (opened at init, valid for the process).
struct RawConnHandle(ffi::duckdb_connection);

/// A summary of one component loaded this session. Surfaced through the `loaded`
/// / count columns of `ducklink_modules()` and the LIVE per-function signatures
/// of `ducklink_functions()`.
#[derive(Clone)]
struct LoadedRecord {
    name: String,
    scalars: usize,
    tables: usize,
    aggregates: usize,
    #[allow(dead_code)]
    path: String,
    /// The component's live registered functions, kept so `ducklink_functions()`
    /// can render exact engine signatures (name/kind/arguments/returns) for a
    /// loaded module rather than falling back to catalog names.
    funcs: Vec<LoadedFuncSig>,
}

/// A single live function signature captured from a loaded component, for the
/// `ducklink_functions()` view.
#[derive(Clone)]
struct LoadedFuncSig {
    name: String,
    kind: &'static str,
    arguments: String,
    returns: String,
}

/// Capture the live signatures of every function a component registered, so the
/// `ducklink_functions()` view can render exact argument/return types for loaded
/// modules (as opposed to the catalog's name-only fallback for unloaded ones).
fn capture_live_sigs(loaded: &crate::engine::LoadedComponent) -> Vec<LoadedFuncSig> {
    let mut out = Vec::new();
    for f in &loaded.scalars {
        out.push(LoadedFuncSig {
            name: f.name.clone(),
            kind: "scalar",
            arguments: render_arg_signature(&f.arguments),
            returns: sql_type_name(&f.returns),
        });
    }
    for f in &loaded.tables {
        let cols = f
            .columns
            .iter()
            .map(|c| format!("{} {}", c.name, sql_type_name(&c.logical)))
            .collect::<Vec<_>>()
            .join(", ");
        out.push(LoadedFuncSig {
            name: f.name.clone(),
            kind: "table",
            arguments: render_arg_signature(&f.arguments),
            returns: format!("TABLE({cols})"),
        });
    }
    for f in &loaded.aggregates {
        out.push(LoadedFuncSig {
            name: f.name.clone(),
            kind: "aggregate",
            arguments: render_arg_signature(&f.arguments),
            returns: sql_type_name(&f.returns),
        });
    }
    out
}

/// The host capability profile captured at `LOAD ducklink` time: which tiers are
/// active on THIS artifact + host, so `ducklink_host_capabilities()` can report them
/// and `ducklink_modules().compatible` can be decided. Set once by the entry
/// point via [`set_host_caps`]; read by the capability + module views.
#[derive(Clone, Default)]
pub struct HostCaps {
    /// The advanced tier (parser / optimizer / internal-ABI) is active: the
    /// artifact was built with `--features advanced` AND the host DuckDB version
    /// matched the built-against gate.
    pub advanced_enabled: bool,
    /// The advanced tier was compiled into this artifact at all (`cfg(advanced_tier)`).
    pub advanced_built: bool,
    /// The host DuckDB library version string, if known.
    pub host_version: Option<String>,
    /// The DuckDB version this artifact's advanced tier was built against (the
    /// exact-version gate). The advanced tier is active only when `host_version`
    /// equals this.
    pub built_against: String,
    /// The wasm component ABI / WIT contract version this host speaks.
    pub abi_version: String,
}

static HOST_CAPS: std::sync::OnceLock<HostCaps> = std::sync::OnceLock::new();

/// Record the host capability profile (called once from the entry point after the
/// tier gate is decided). Later calls are ignored (the first `LOAD` wins), mirror-
/// ing `RUNTIME`.
pub fn set_host_caps(caps: HostCaps) {
    let _ = HOST_CAPS.set(caps);
}

fn host_caps() -> HostCaps {
    HOST_CAPS.get().cloned().unwrap_or_default()
}

/// The host's wasm contract-generation MAJOR (e.g. `4` for `wasm_abi 4.0.0`),
/// parsed from `HostCaps.abi_version`. Drives per-generation provider selection
/// in the catalog. Defaults to the runtime's own contract major when the caps
/// are somehow unset or the version is unparseable, so selection stays sane.
fn host_generation_major() -> u64 {
    let caps = host_caps();
    crate::catalog::abi_major_of(&caps.abi_version)
        .or_else(|| crate::catalog::abi_major_of(ducklink_runtime::CONTRACT_VERSION))
        .unwrap_or(0)
}

/// The capability kinds the COMMON tier (always present, stable C API) satisfies.
/// A module whose `requires` are all in this set is compatible on any host; a
/// module requiring anything else needs the advanced tier.
const COMMON_TIER_KINDS: &[&str] = &[
    "scalar",
    "table",
    "aggregate",
    "macro",
    "cast",
    "network",
    "compose-dynlink",
];

/// The capability kinds the ADVANCED tier adds (internal C++ ABI). Present only
/// when the advanced tier is active on this host.
const ADVANCED_TIER_KINDS: &[&str] =
    &["parser", "optimizer", "storage", "index", "catalog", "query", "window"];

/// True when THIS host can satisfy every capability `kind` in `requires`. The
/// common tier is always available; advanced-only kinds need the advanced tier
/// active. An unknown kind is treated as advanced-only (conservative: report it
/// incompatible unless the advanced tier is up).
fn module_compatible(requires: &[String], caps: &HostCaps) -> bool {
    requires.iter().all(|r| {
        if COMMON_TIER_KINDS.contains(&r.as_str()) {
            true
        } else if ADVANCED_TIER_KINDS.contains(&r.as_str()) {
            caps.advanced_enabled
        } else {
            // Unknown / host-component requirement: satisfiable only if advanced.
            caps.advanced_enabled
        }
    })
}

// SAFETY: `db` is a stable, process-lifetime handle DuckDB owns; we only read it
// to open sibling connections (a database-wide, thread-safe C-API operation).
unsafe impl Send for DucklinkRuntime {}
unsafe impl Sync for DucklinkRuntime {}

static RUNTIME: std::sync::OnceLock<DucklinkRuntime> = std::sync::OnceLock::new();

/// Register the `ducklink_load(path)` table function and capture the
/// process-wide runtime handle it needs. Idempotent: the handle is set once
/// (the first `LOAD ducklink` in the process wins); the table function is
/// registered on `con` each call (a no-op duplicate is tolerated by DuckDB).
pub fn register_load_function(
    con: &Connection,
    db: ffi::duckdb_database,
    engine: Arc<Engine2>,
) -> duckdb::Result<()> {
    // Open a PERSISTENT connection NOW, while `db` is still valid (we are inside
    // `init`). `try_clone` performs the `duckdb_connect` here; the resulting
    // `Connection` owns a shared_ptr to the live `DatabaseInstance`, so it
    // outlives the soon-to-be-freed load-state `DatabaseWrapper` that `db` points
    // at. Later runtime registration reuses THIS connection (no reconnect through
    // the dangling handle). If the clone somehow fails we still install the table
    // functions; runtime loading would then surface a clear error.
    let persistent = con.try_clone()?;

    // Also keep a raw sibling connection alive for the aggregate path (it needs
    // the raw C handle). Opened here while `db` is valid.
    let mut raw: ffi::duckdb_connection = std::ptr::null_mut();
    let raw_ok = unsafe { ffi::duckdb_connect(db, &mut raw) } == ffi::DuckDBSuccess && !raw.is_null();
    let raw_con = RawConnHandle(if raw_ok { raw } else { std::ptr::null_mut() });

    // First loader in the process captures the runtime; later ones reuse it.
    let _ = RUNTIME.set(DucklinkRuntime {
        db,
        con: Mutex::new(persistent),
        raw_con,
        engine,
        loaded: Mutex::new(Vec::new()),
    });
    con.register_table_function::<WasmLoad>("ducklink_load")?;
    // The INTERNAL discovery table functions backing the public `ducklink.*`
    // views. Users query the views (`SELECT * FROM ducklink.modules`); these
    // `ducklink_*()` TFs are the implementation the views select from.
    con.register_table_function::<WasmModules>("ducklink_modules")?;
    con.register_table_function::<WasmFunctions>("ducklink_functions")?;
    con.register_table_function::<WasmHostCapabilities>("ducklink_host_capabilities")?;
    con.register_table_function::<WasmCache>("ducklink_cache")?;
    con.register_table_function::<WasmModuleCompatibility>("ducklink_module_compatibility")?;
    con.register_table_function::<WasmEvents>("ducklink_events")?;
    con.register_table_function::<WasmHost>("ducklink_host")?;

    // Create the public `ducklink` SCHEMA of system VIEWS over the internal TFs.
    // This is the discovery API surface: `SELECT * FROM ducklink.modules`, etc.
    // Done here on the init connection — which is still valid (only LATER
    // reconnection through the dangling `db` handle is unsafe; creating objects
    // on this live init connection at load time is fine). NON-FATAL: a failure to
    // create the schema/views must never break `LOAD ducklink` (the raw
    // `ducklink_*()` TFs remain callable directly as a fallback).
    //
    // NOTE on catalog placement: with no explicit catalog qualifier the schema
    // lands in the currently-active catalog (the default in-memory/attached
    // database for the session). `SELECT * FROM ducklink.modules` then resolves
    // in a normal session. Under a foreign `ATTACH ... AS other; USE other;` the
    // schema would be created in whichever catalog is active at LOAD time; the
    // views are recreated (CREATE OR REPLACE) on each LOAD, so a later LOAD in a
    // different active catalog re-materialises them there.
    if let Err(e) = create_ducklink_schema(con) {
        eprintln!("[ducklink] could not create ducklink schema/views: {e}");
    }
    Ok(())
}

/// Create the `ducklink` schema and its system views over the internal discovery
/// table functions. Idempotent (`CREATE SCHEMA IF NOT EXISTS` + `CREATE OR
/// REPLACE VIEW`), so re-running on every `LOAD ducklink` is safe.
fn create_ducklink_schema(con: &Connection) -> duckdb::Result<()> {
    con.execute_batch(
        "CREATE SCHEMA IF NOT EXISTS ducklink;
         CREATE OR REPLACE VIEW ducklink.modules AS SELECT * FROM ducklink_modules();
         CREATE OR REPLACE VIEW ducklink.functions AS SELECT * FROM ducklink_functions();
         CREATE OR REPLACE VIEW ducklink.host_capabilities AS SELECT * FROM ducklink_host_capabilities();
         CREATE OR REPLACE VIEW ducklink.cache AS SELECT * FROM ducklink_cache();
         CREATE OR REPLACE VIEW ducklink.module_compatibility AS SELECT * FROM ducklink_module_compatibility();
         CREATE OR REPLACE VIEW ducklink.events AS SELECT * FROM ducklink_events();
         CREATE OR REPLACE VIEW ducklink.host AS SELECT * FROM ducklink_host();",
    )
}

/// Load a component (by path or catalog name) into the GIVEN database handle and
/// register its functions. The advanced-tier `LOAD WASM '<name>'` statement
/// routes here from the C++ parser shim, which hands us a `duckdb_database`
/// derived from the PARSER's live `ClientContext` (`context.db`) — the connection
/// the statement actually runs on — instead of the process-captured `rt.db`. In
/// the real loadable/CLI context the init-time `rt.db` handle does not survive to
/// re-`duckdb_connect` later (observed: "connect error"), so reusing the live
/// context db is what makes runtime loading work end to end there.
///
/// Returns Ok((name, scalars, tables, aggregates)) on success.
///
/// # Safety
/// `db` must be a valid `duckdb_database` for the live database the statement is
/// executing on.
pub unsafe fn load_wasm_into_db(
    db: ffi::duckdb_database,
    arg: &str,
) -> Result<(String, usize, usize, usize), String> {
    let rt = RUNTIME
        .get()
        .ok_or_else(|| "LOAD WASM: runtime not initialised (LOAD ducklink first)".to_string())?;

    // Resolve the arg to (display name, on-disk .wasm path): a path-looking arg
    // is a filesystem path; anything else is a catalog NAME (live catalog with a
    // bundled-snapshot fallback). Mirrors `ducklink_load`'s heuristic.
    let looks_like_path = arg.contains('/')
        || arg.contains('\\')
        || arg.ends_with(".wasm")
        || Path::new(arg).exists();
    let (name, path): (String, PathBuf) = if looks_like_path {
        let path = PathBuf::from(arg);
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("component")
            .to_string();
        (name, path)
    } else {
        let cached =
            crate::catalog::resolve_name_to_blob(arg, host_generation_major()).map_err(|e| e.to_string())?;
        (arg.to_string(), cached)
    };
    let path_str = path.to_string_lossy().into_owned();

    crate::events::emit("load_start", Some(&name), path_str.clone());
    let loaded = {
        let e = &rt.engine;
        match e.load(&name, &path) {
            Ok(l) => l,
            Err(err) => {
                let msg = err.to_string();
                crate::events::emit("load_error", Some(&name), msg.clone());
                return Err(msg);
            }
        }
    };

    // Register on the PERSISTENT init connection (database-wide; valid for the
    // process). Same handle the common-tier `ducklink_load` path uses — see the
    // use-after-free note on `DucklinkRuntime::db`. (`db`, the live context db the
    // advanced parser hands us, also works, but reusing the persistent connection
    // keeps a single registration path for both tiers.)
    let _ = db;
    let con = rt.con.lock().unwrap_or_else(|e| e.into_inner());
    let scalars =
        register_scalars(&con, rt.engine.clone(), &loaded.scalars).map_err(|e| e.to_string())?;
    let tables =
        register_tables(&con, rt.engine.clone(), &loaded.tables).map_err(|e| e.to_string())?;
    drop(con);

    let mut agg = 0usize;
    if !loaded.aggregates.is_empty() {
        let raw_con = rt.raw_con.0;
        if !raw_con.is_null() {
            agg = register_aggregates(raw_con, rt.engine.clone(), &loaded.aggregates)
                .map_err(|e| e.to_string())?;
        }
    }

    {
        let mut loaded_list = rt.loaded.lock().unwrap_or_else(|e| e.into_inner());
        let rec = LoadedRecord {
            name: name.clone(),
            scalars,
            tables,
            aggregates: agg,
            path: path_str,
            funcs: capture_live_sigs(&loaded),
        };
        match loaded_list.iter_mut().find(|r| r.name == name) {
            Some(existing) => *existing = rec,
            None => loaded_list.push(rec),
        }
    }

    crate::events::emit(
        "load_ok",
        Some(&name),
        format!("{scalars} scalars, {tables} tables, {agg} aggregates"),
    );
    Ok((name, scalars, tables, agg))
}

/// One-row bind result for `ducklink_load`: a summary of what was loaded.
struct WasmLoadBind {
    name: String,
    path: String,
    scalars: usize,
    tables: usize,
    aggregates: usize,
}

/// Init cursor for `ducklink_load` (single row).
struct WasmLoadInit {
    done: AtomicUsize,
}

/// The `ducklink_load(path)` table function. Its `bind` performs the load +
/// registration side effect; `func` streams back the single summary row.
struct WasmLoad;

impl VTab for WasmLoad {
    type InitData = WasmLoadInit;
    type BindData = WasmLoadBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("ducklink_load bind", || {
            let rt = RUNTIME.get().ok_or_else(|| -> Box<dyn std::error::Error> {
                "ducklink_load: runtime not initialised (LOAD ducklink first)".into()
            })?;

            // Argument 0: EITHER a filesystem path to a `.wasm` component OR a
            // catalog NAME (e.g. 'aba'). Heuristic: an argument that contains a
            // path separator, ends in `.wasm`, or names an existing file is a
            // PATH; anything else is a catalog NAME resolved against the
            // published catalog (live, with a bundled-snapshot fallback).
            let arg_val = bind.get_parameter(0);
            let arg_str = arg_val.to_string();

            let looks_like_path = arg_str.contains('/')
                || arg_str.contains('\\')
                || arg_str.ends_with(".wasm")
                || Path::new(&arg_str).exists();

            // Resolve to (display name, on-disk .wasm path). For a path arg the
            // name defaults to the file stem (overridable via `name :=`); for a
            // catalog name the arg IS the name and the path is the cached blob.
            let (name, path): (String, PathBuf) = if looks_like_path {
                let path = PathBuf::from(&arg_str);
                let name = match bind.get_named_parameter("name") {
                    Some(v) => v.to_string(),
                    None => path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("component")
                        .to_string(),
                };
                (name, path)
            } else {
                // Catalog NAME -> resolve + fetch/cache/verify the blob.
                let cached = crate::catalog::resolve_name_to_blob(&arg_str, host_generation_major())
                    .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
                let name = match bind.get_named_parameter("name") {
                    Some(v) => v.to_string(),
                    None => arg_str.clone(),
                };
                (name, cached)
            };
            let path_str = path.to_string_lossy().into_owned();

            crate::events::emit("load_start", Some(&name), path_str.clone());
            // Load the component through the shared engine.
            let loaded = {
                let e = &rt.engine;
                match e.load(&name, &path) {
                    Ok(l) => l,
                    Err(err) => {
                        let msg = err.to_string();
                        crate::events::emit("load_error", Some(&name), msg.clone());
                        return Err(msg.into());
                    }
                }
            };

            // Register on the PERSISTENT connection captured at init (NOT a fresh
            // reconnect through `rt.db`: that handle points at the load-state's
            // `DatabaseWrapper`, freed once `init` returned, so reconnecting is a
            // use-after-free that DuckDB reports as `connect error`). Registration
            // only touches the already-open `duckdb_connection` (no reconnect) and
            // is database-wide, so the functions are visible to the caller's next
            // statement. The init connection is a SEPARATE connection from the one
            // binding this call, so this is not catalog re-entrancy.
            let con = rt.con.lock().unwrap_or_else(|e| e.into_inner());
            let scalars = register_scalars(&con, rt.engine.clone(), &loaded.scalars)
                .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
            let tables = register_tables(&con, rt.engine.clone(), &loaded.tables)
                .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
            drop(con);

            // Aggregates need the raw C connection: reuse the persistent raw
            // sibling captured at init (also valid for the process lifetime).
            let mut agg = 0usize;
            if !loaded.aggregates.is_empty() {
                let raw_con = rt.raw_con.0;
                let ok = !raw_con.is_null();
                if ok {
                    agg = unsafe {
                        register_aggregates(raw_con, rt.engine.clone(), &loaded.aggregates)
                    }
                    .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
                    // Do NOT disconnect: this raw connection is the persistent
                    // sibling, reused across loads for the process lifetime.
                }
            }

            bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("path", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("scalars", LogicalTypeHandle::from(LogicalTypeId::Bigint));
            bind.add_result_column("tables", LogicalTypeHandle::from(LogicalTypeId::Bigint));
            bind.add_result_column("aggregates", LogicalTypeHandle::from(LogicalTypeId::Bigint));

            // Track this load for `ducklink_modules().loaded`. A re-load of the same name
            // updates the existing record rather than appending a duplicate.
            {
                let mut loaded_list = rt.loaded.lock().unwrap_or_else(|e| e.into_inner());
                let rec = LoadedRecord {
                    name: name.clone(),
                    scalars,
                    tables,
                    aggregates: agg,
                    path: path_str.clone(),
                    funcs: capture_live_sigs(&loaded),
                };
                match loaded_list.iter_mut().find(|r| r.name == name) {
                    Some(existing) => *existing = rec,
                    None => loaded_list.push(rec),
                }
            }

            crate::events::emit(
                "load_ok",
                Some(&name),
                format!("{scalars} scalars, {tables} tables, {agg} aggregates"),
            );
            Ok(WasmLoadBind {
                name,
                path: path_str,
                scalars,
                tables,
                aggregates: agg,
            })
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(WasmLoadInit {
            done: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        guard("ducklink_load scan", || {
            let bind = func.get_bind_data();
            let init = func.get_init_data();
            if init.done.swap(1, Ordering::Relaxed) != 0 {
                output.set_len(0);
                return Ok(());
            }
            output.flat_vector(0).insert(0, bind.name.as_str());
            output.flat_vector(1).insert(0, bind.path.as_str());
            // SAFETY: BIGINT result columns; row 0 is in range (set_len(1) below).
            unsafe {
                output.flat_vector(2).as_mut_slice::<i64>()[0] = bind.scalars as i64;
                output.flat_vector(3).as_mut_slice::<i64>()[0] = bind.tables as i64;
                output.flat_vector(4).as_mut_slice::<i64>()[0] = bind.aggregates as i64;
            }
            output.set_len(1);
            Ok(())
        })
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        // Positional arg 0: filesystem path to the component .wasm.
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        // Optional `name :=` override; defaults to the file stem.
        Some(vec![(
            "name".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )])
    }
}

// ---------------------------------------------------------------------------
// Discovery table functions backing the public `ducklink.*` views
// ---------------------------------------------------------------------------
//
// These `ducklink_*()` table functions are the INTERNAL implementation of the
// public discovery API. Users query the `ducklink` schema of system views
// (`SELECT * FROM ducklink.modules`, `ducklink.functions`, `ducklink.host_capabilities`,
// `ducklink.cache`), which are `CREATE OR REPLACE VIEW`s over these TFs (see
// `create_ducklink_schema`). The TFs remain individually callable as a fallback.

/// Snapshot the set of module names loaded THIS session (from the runtime), for
/// the `loaded` column of `ducklink_modules()`.
fn loaded_names() -> std::collections::HashSet<String> {
    match RUNTIME.get() {
        Some(rt) => rt
            .loaded
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|r| r.name.clone())
            .collect(),
        None => std::collections::HashSet::new(),
    }
}

/// Look up the live per-function-class counts for a loaded module (used to fill
/// the `scalars`/`tables`/`aggregates` columns with EXACT counts when a module
/// is loaded). Returns `None` for an unloaded module.
fn loaded_counts(name: &str) -> Option<(usize, usize, usize)> {
    let rt = RUNTIME.get()?;
    let list = rt.loaded.lock().unwrap_or_else(|e| e.into_inner());
    list.iter()
        .find(|r| r.name == name)
        .map(|r| (r.scalars, r.tables, r.aggregates))
}

// --- ducklink_modules() -----------------------------------------------------

/// One catalog module row materialised for `ducklink_modules()`.
struct ModuleRow {
    name: String,
    version: String,
    description: String,
    categories: String,
    loaded: bool,
    scalars: i32,
    tables: i32,
    aggregates: i32,
    capabilities: String,
    compatible: bool,
}

struct WasmModulesBind {
    rows: Vec<ModuleRow>,
}

/// `ducklink_modules()` — one row per CATALOG module (the full published
/// catalog), with `loaded` reflecting this session's runtime state and
/// `compatible` reflecting whether THIS host's tiers satisfy the module's
/// required capability kinds.
struct WasmModules;

impl VTab for WasmModules {
    type InitData = WasmTableInit;
    type BindData = WasmModulesBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("ducklink_modules bind", || {
            let vc = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
            let int = || LogicalTypeHandle::from(LogicalTypeId::Integer);
            let boolean = || LogicalTypeHandle::from(LogicalTypeId::Boolean);
            bind.add_result_column("name", vc());
            bind.add_result_column("version", vc());
            bind.add_result_column("description", vc());
            bind.add_result_column("categories", vc());
            bind.add_result_column("loaded", boolean());
            bind.add_result_column("scalars", int());
            bind.add_result_column("tables", int());
            bind.add_result_column("aggregates", int());
            bind.add_result_column("capabilities", vc());
            bind.add_result_column("compatible", boolean());

            let caps = host_caps();
            let loaded = loaded_names();
            let catalog = crate::catalog::resolve_catalog();
            let rows = catalog
                .extensions
                .iter()
                .map(|e| {
                    let is_loaded = loaded.contains(&e.name);
                    // Loaded modules report EXACT live counts; unloaded ones show
                    // a coarse presence-flag (1/0) inferred from `requires`, since
                    // the catalog carries no per-function counts.
                    let (scalars, tables, aggregates) = match loaded_counts(&e.name) {
                        Some((s, t, a)) => (s as i32, t as i32, a as i32),
                        None => (
                            e.requires_kind("scalar") as i32,
                            e.requires_kind("table") as i32,
                            e.requires_kind("aggregate") as i32,
                        ),
                    };
                    ModuleRow {
                        name: e.name.clone(),
                        version: e.version.clone().unwrap_or_default(),
                        description: e.description.clone().unwrap_or_default(),
                        categories: e.categories.join(", "),
                        loaded: is_loaded,
                        scalars,
                        tables,
                        aggregates,
                        capabilities: e.requires.join(", "),
                        compatible: module_compatible(&e.requires, &caps),
                    }
                })
                .collect();
            Ok(WasmModulesBind { rows })
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(WasmTableInit {
            cursor: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        guard("ducklink_modules scan", || {
            let bind = func.get_bind_data();
            let init = func.get_init_data();
            let start = init.cursor.load(Ordering::Relaxed);
            let n = bind.rows.len().saturating_sub(start).min(2048);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            for r in 0..n {
                let row = &bind.rows[start + r];
                output.flat_vector(0).insert(r, row.name.as_str());
                output.flat_vector(1).insert(r, row.version.as_str());
                output.flat_vector(2).insert(r, row.description.as_str());
                output.flat_vector(3).insert(r, row.categories.as_str());
                output.flat_vector(8).insert(r, row.capabilities.as_str());
            }
            // Fixed-width columns: fill the typed slices after the string inserts.
            unsafe {
                let mut lv = output.flat_vector(4);
                let l = lv.as_mut_slice::<bool>();
                let mut sv = output.flat_vector(5);
                let s = sv.as_mut_slice::<i32>();
                let mut tv = output.flat_vector(6);
                let t = tv.as_mut_slice::<i32>();
                let mut av = output.flat_vector(7);
                let a = av.as_mut_slice::<i32>();
                let mut cv = output.flat_vector(9);
                let c = cv.as_mut_slice::<bool>();
                for r in 0..n {
                    let row = &bind.rows[start + r];
                    l[r] = row.loaded;
                    s[r] = row.scalars;
                    t[r] = row.tables;
                    a[r] = row.aggregates;
                    c[r] = row.compatible;
                }
            }
            init.cursor.store(start + n, Ordering::Relaxed);
            output.set_len(n);
            Ok(())
        })
    }
}

// --- ducklink_functions() ---------------------------------------------------

/// One function row for `ducklink_functions()`.
struct FunctionRow {
    module: String,
    name: String,
    kind: String,
    arguments: String,
    returns: String,
    loaded: bool,
}

struct WasmFunctionsBind {
    rows: Vec<FunctionRow>,
}

/// `ducklink_functions()` — one row per function across ALL catalog modules. For
/// LOADED modules the live engine signatures (exact argument/return types) are
/// used; for unloaded modules the catalog `functions` enrichment is used when
/// present, else the bare `exports` names (with empty argument/return columns).
struct WasmFunctions;

impl VTab for WasmFunctions {
    type InitData = WasmTableInit;
    type BindData = WasmFunctionsBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("ducklink_functions bind", || {
            let vc = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
            let boolean = || LogicalTypeHandle::from(LogicalTypeId::Boolean);
            bind.add_result_column("module", vc());
            bind.add_result_column("name", vc());
            bind.add_result_column("kind", vc());
            bind.add_result_column("arguments", vc());
            bind.add_result_column("returns", vc());
            bind.add_result_column("loaded", boolean());

            // Live signatures for loaded modules, keyed by module name.
            let live: std::collections::HashMap<String, Vec<LoadedFuncSig>> = match RUNTIME.get() {
                Some(rt) => rt
                    .loaded
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .iter()
                    .map(|r| (r.name.clone(), r.funcs.clone()))
                    .collect(),
                None => std::collections::HashMap::new(),
            };

            let catalog = crate::catalog::resolve_catalog();
            let mut rows: Vec<FunctionRow> = Vec::new();
            for e in &catalog.extensions {
                if let Some(funcs) = live.get(&e.name) {
                    // LOADED: exact live engine signatures.
                    for f in funcs {
                        rows.push(FunctionRow {
                            module: e.name.clone(),
                            name: f.name.clone(),
                            kind: f.kind.to_string(),
                            arguments: f.arguments.clone(),
                            returns: f.returns.clone(),
                            loaded: true,
                        });
                    }
                } else if !e.functions.is_empty() {
                    // UNLOADED with catalog enrichment: render the catalog sigs.
                    for f in &e.functions {
                        let name = f.name.clone().unwrap_or_default();
                        let args = f
                            .arguments
                            .iter()
                            .map(|a| match (&a.name, &a.type_name) {
                                (Some(n), Some(t)) if !n.is_empty() => format!("{n} {t}"),
                                (_, Some(t)) => t.clone(),
                                (Some(n), None) => n.clone(),
                                (None, None) => String::new(),
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        rows.push(FunctionRow {
                            module: e.name.clone(),
                            name,
                            kind: f.kind.clone().unwrap_or_default(),
                            arguments: args,
                            returns: f.returns.clone().unwrap_or_default(),
                            loaded: false,
                        });
                    }
                } else {
                    // UNLOADED, no enrichment: the bare export names, no signatures.
                    for name in &e.exports {
                        rows.push(FunctionRow {
                            module: e.name.clone(),
                            name: name.clone(),
                            kind: String::new(),
                            arguments: String::new(),
                            returns: String::new(),
                            loaded: false,
                        });
                    }
                }
            }
            Ok(WasmFunctionsBind { rows })
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(WasmTableInit {
            cursor: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        guard("ducklink_functions scan", || {
            let bind = func.get_bind_data();
            let init = func.get_init_data();
            let start = init.cursor.load(Ordering::Relaxed);
            let n = bind.rows.len().saturating_sub(start).min(2048);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            for r in 0..n {
                let row = &bind.rows[start + r];
                output.flat_vector(0).insert(r, row.module.as_str());
                output.flat_vector(1).insert(r, row.name.as_str());
                output.flat_vector(2).insert(r, row.kind.as_str());
                output.flat_vector(3).insert(r, row.arguments.as_str());
                output.flat_vector(4).insert(r, row.returns.as_str());
            }
            unsafe {
                let mut lv = output.flat_vector(5);
                let l = lv.as_mut_slice::<bool>();
                for r in 0..n {
                    l[r] = bind.rows[start + r].loaded;
                }
            }
            init.cursor.store(start + n, Ordering::Relaxed);
            output.set_len(n);
            Ok(())
        })
    }
}

// --- ducklink_host_capabilities() ------------------------------------------------

/// One capability row for `ducklink_host_capabilities()`.
struct HostCapabilityRow {
    name: String,
    available: bool,
    detail: String,
}

struct WasmHostCapabilitiesBind {
    rows: Vec<HostCapabilityRow>,
}

/// `ducklink_host_capabilities()` — the HOST's capabilities: which capability kinds
/// this artifact + host can satisfy. The row-set is the DEDUPED union of
/// `COMMON_TIER_KINDS` and `ADVANCED_TIER_KINDS` — the exact vocabulary
/// `module_compatible()` checks against — so anything that appears in a
/// module's `kinds` column is guaranteed to have a row here.
struct WasmHostCapabilities;

impl VTab for WasmHostCapabilities {
    type InitData = WasmTableInit;
    type BindData = WasmHostCapabilitiesBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("ducklink_host_capabilities bind", || {
            bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("available", LogicalTypeHandle::from(LogicalTypeId::Boolean));
            bind.add_result_column("detail", LogicalTypeHandle::from(LogicalTypeId::Varchar));

            let caps = host_caps();
            let adv_detail = if caps.advanced_enabled {
                format!(
                    "advanced tier: active (host DuckDB {})",
                    caps.host_version.as_deref().unwrap_or("unknown"),
                )
            } else if caps.advanced_built {
                format!(
                    "advanced tier: built but inactive (requires osx/linux + host DuckDB {}; host reports {})",
                    caps.built_against,
                    caps.host_version.as_deref().unwrap_or("unknown")
                )
            } else {
                "advanced tier: not built in this artifact (build with --features advanced to enable)".to_string()
            };

            // One row per DEDUPED tier kind. Common-tier kinds are always
            // available; advanced-tier kinds gate on the tier state captured
            // at load. `catalog` is intentionally listed in both tier
            // constants (any host provides it), so it emits a single
            // common-tier row here.
            let mut rows: Vec<HostCapabilityRow> = Vec::new();
            let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for name in COMMON_TIER_KINDS {
                if seen.insert(*name) {
                    rows.push(HostCapabilityRow {
                        name: (*name).to_string(),
                        available: true,
                        detail: "common tier".to_string(),
                    });
                }
            }
            for name in ADVANCED_TIER_KINDS {
                if seen.insert(*name) {
                    rows.push(HostCapabilityRow {
                        name: (*name).to_string(),
                        available: caps.advanced_enabled,
                        detail: adv_detail.clone(),
                    });
                }
            }
            // `LOAD WASM 'name'` — the SQL statement — is gated on the parser
            // hook installed only when the advanced tier is active. Not in
            // the tier-kind constants (it's a host feature, not something a
            // module declares), but useful to advertise here alongside the
            // kinds that gate on the same thing.
            rows.push(HostCapabilityRow {
                name: "load_wasm".to_string(),
                available: caps.advanced_enabled,
                detail: adv_detail,
            });
            rows.sort_by(|a, b| a.name.cmp(&b.name));

            Ok(WasmHostCapabilitiesBind { rows })
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(WasmTableInit {
            cursor: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        guard("ducklink_host_capabilities scan", || {
            let bind = func.get_bind_data();
            let init = func.get_init_data();
            let start = init.cursor.load(Ordering::Relaxed);
            let n = bind.rows.len().saturating_sub(start).min(2048);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            for r in 0..n {
                let row = &bind.rows[start + r];
                output.flat_vector(0).insert(r, row.name.as_str());
                output.flat_vector(2).insert(r, row.detail.as_str());
            }
            unsafe {
                let mut av = output.flat_vector(1);
                let a = av.as_mut_slice::<bool>();
                for r in 0..n {
                    a[r] = bind.rows[start + r].available;
                }
            }
            init.cursor.store(start + n, Ordering::Relaxed);
            output.set_len(n);
            Ok(())
        })
    }
}

// --- ducklink_host() --------------------------------------------------------

/// One-row view carrying pure HOST metadata: things that describe this
/// ducklink process/artifact rather than any module. Split from
/// `ducklink.host_capabilities` because these are single-value facts, not yes/no
/// availability flags.
struct HostRow {
    wasm_abi: String,
    duckdb_version: String,
    duckdb_built_against: String,
    advanced_tier: String,
}

struct WasmHostBind {
    row: HostRow,
}

/// `ducklink_host()` — a single-row view of host metadata: the WIT contract
/// version this host speaks (`wasm_abi`, in `duckdb:extension@X.Y.Z` form),
/// the host DuckDB version, the DuckDB version the advanced tier was compiled
/// against, and the advanced tier's current state.
struct WasmHost;

impl VTab for WasmHost {
    type InitData = WasmTableInit;
    type BindData = WasmHostBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("ducklink_host bind", || {
            let vc = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
            bind.add_result_column("wasm_abi", vc());
            bind.add_result_column("duckdb_version", vc());
            bind.add_result_column("duckdb_built_against", vc());
            bind.add_result_column("advanced_tier", vc());

            let caps = host_caps();
            let wasm_abi = normalize_generation(Some(caps.abi_version.clone()));
            let duckdb_version = caps.host_version.clone().unwrap_or_default();
            let advanced_tier = if caps.advanced_enabled {
                "active"
            } else if caps.advanced_built {
                "inactive"
            } else {
                "not_built"
            }
            .to_string();

            Ok(WasmHostBind {
                row: HostRow {
                    wasm_abi,
                    duckdb_version,
                    duckdb_built_against: caps.built_against.clone(),
                    advanced_tier,
                },
            })
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(WasmTableInit {
            cursor: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        guard("ducklink_host scan", || {
            let bind = func.get_bind_data();
            let init = func.get_init_data();
            // Exactly one row, emitted on the first scan.
            let start = init.cursor.load(Ordering::Relaxed);
            let n = 1usize.saturating_sub(start);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            output.flat_vector(0).insert(0, bind.row.wasm_abi.as_str());
            output.flat_vector(1).insert(0, bind.row.duckdb_version.as_str());
            output.flat_vector(2).insert(0, bind.row.duckdb_built_against.as_str());
            output.flat_vector(3).insert(0, bind.row.advanced_tier.as_str());
            init.cursor.store(1, Ordering::Relaxed);
            output.set_len(1);
            Ok(())
        })
    }
}

// --- ducklink_cache() -------------------------------------------------------

/// One cached-blob row for `ducklink_cache()`.
struct CacheRow {
    digest: String,
    name: String,
    bytes: i64,
    /// Modification time as MICROSECONDS since the Unix epoch, matching DuckDB's
    /// TIMESTAMP physical storage (i64 micros). 0 when the mtime is unavailable.
    modified_micros: i64,
    path: String,
}

/// Scan the on-disk cache root (`<cache>/wasm/sha256/<digest>/<name>.wasm`) and
/// collect one row per cached blob. Best-effort: an unreadable dir yields no
/// rows rather than an error.
fn scan_cache() -> Vec<CacheRow> {
    let mut rows = Vec::new();
    let Some(root) = crate::catalog::cache_root() else {
        return rows;
    };
    let sha_dir = root.join("wasm").join("sha256");
    let Ok(digests) = std::fs::read_dir(&sha_dir) else {
        return rows;
    };
    for dent in digests.flatten() {
        if !dent.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let digest = dent.file_name().to_string_lossy().into_owned();
        let Ok(blobs) = std::fs::read_dir(dent.path()) else {
            continue;
        };
        for blob in blobs.flatten() {
            let path = blob.path();
            if path.extension().and_then(|s| s.to_str()) != Some("wasm") {
                continue;
            }
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string();
            let meta = match blob.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            let bytes = meta.len() as i64;
            // mtime -> micros since epoch (TIMESTAMP storage). Awkward via the C
            // API to build a real TIMESTAMP value, so we write the raw i64 micros
            // into a TIMESTAMP-typed column, which is exactly its physical form.
            let modified_micros = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_micros() as i64)
                .unwrap_or(0);
            rows.push(CacheRow {
                digest: digest.clone(),
                name,
                bytes,
                modified_micros,
                path: path.to_string_lossy().into_owned(),
            });
        }
    }
    rows
}

struct WasmCacheBind {
    rows: Vec<CacheRow>,
}

/// `ducklink_cache()` — one row per cached component blob on disk.
struct WasmCache;

impl VTab for WasmCache {
    type InitData = WasmTableInit;
    type BindData = WasmCacheBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("ducklink_cache bind", || {
            bind.add_result_column("digest", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("bytes", LogicalTypeHandle::from(LogicalTypeId::Bigint));
            bind.add_result_column("modified", LogicalTypeHandle::from(LogicalTypeId::Timestamp));
            bind.add_result_column("path", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            Ok(WasmCacheBind { rows: scan_cache() })
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(WasmTableInit {
            cursor: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        guard("ducklink_cache scan", || {
            let bind = func.get_bind_data();
            let init = func.get_init_data();
            let start = init.cursor.load(Ordering::Relaxed);
            let n = bind.rows.len().saturating_sub(start).min(2048);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            for r in 0..n {
                let row = &bind.rows[start + r];
                output.flat_vector(0).insert(r, row.digest.as_str());
                output.flat_vector(1).insert(r, row.name.as_str());
                output.flat_vector(4).insert(r, row.path.as_str());
            }
            unsafe {
                let mut bv = output.flat_vector(2);
                let b = bv.as_mut_slice::<i64>();
                // TIMESTAMP stores i64 micros-since-epoch; write the raw micros.
                let mut mv = output.flat_vector(3);
                let m = mv.as_mut_slice::<i64>();
                for r in 0..n {
                    b[r] = bind.rows[start + r].bytes;
                    m[r] = bind.rows[start + r].modified_micros;
                }
            }
            init.cursor.store(start + n, Ordering::Relaxed);
            output.set_len(n);
            Ok(())
        })
    }
}

// --- ducklink_events() ------------------------------------------------------

struct WasmEventsBind {
    rows: Vec<crate::events::Event>,
}

/// `ducklink_events()` — a snapshot of the process-wide runtime event log
/// ([`crate::events`]), one row per recorded event, ordered by `seq`. Backs the
/// `ducklink.events` system view: an in-process audit trail of catalog fetches,
/// cache hits/misses, downloads, sha256 verification, provider selection, and
/// the load lifecycle. The snapshot is taken at bind so a single scan sees a
/// consistent view even if concurrent loads keep emitting.
struct WasmEvents;

impl VTab for WasmEvents {
    type InitData = WasmTableInit;
    type BindData = WasmEventsBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("ducklink_events bind", || {
            bind.add_result_column("seq", LogicalTypeHandle::from(LogicalTypeId::Bigint));
            bind.add_result_column("ts", LogicalTypeHandle::from(LogicalTypeId::Timestamp));
            bind.add_result_column("kind", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("module", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("detail", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            // Snapshot is already oldest-first (ascending seq); the emit path
            // assigns seq monotonically, so no re-sort is needed.
            Ok(WasmEventsBind {
                rows: crate::events::snapshot(),
            })
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(WasmTableInit {
            cursor: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        guard("ducklink_events scan", || {
            let bind = func.get_bind_data();
            let init = func.get_init_data();
            let start = init.cursor.load(Ordering::Relaxed);
            let n = bind.rows.len().saturating_sub(start).min(2048);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            for r in 0..n {
                let row = &bind.rows[start + r];
                output.flat_vector(2).insert(r, row.kind.as_str());
                match row.module.as_deref() {
                    Some(m) => output.flat_vector(3).insert(r, m),
                    // NULL module column when the event is not module-scoped
                    // (e.g. catalog_fetch / catalog_fallback).
                    None => output.flat_vector(3).set_null(r),
                }
                output.flat_vector(4).insert(r, row.detail.as_str());
            }
            unsafe {
                let mut sv = output.flat_vector(0);
                let s = sv.as_mut_slice::<i64>();
                // TIMESTAMP stores i64 micros-since-epoch; write the raw micros
                // straight into the physical column (same technique as
                // `ducklink_cache().modified`).
                let mut tv = output.flat_vector(1);
                let t = tv.as_mut_slice::<i64>();
                for r in 0..n {
                    s[r] = bind.rows[start + r].seq as i64;
                    t[r] = bind.rows[start + r].ts_micros;
                }
            }
            init.cursor.store(start + n, Ordering::Relaxed);
            output.set_len(n);
            Ok(())
        })
    }
}

// --- ducklink_module_compatibility() -------------------------------------

/// One (module, generation) row for `ducklink_module_compatibility()`.
struct ModuleCompatibilityRow {
    module: String,
    /// The module's WIT contract version, always as `duckdb:extension@X.Y.Z`.
    /// Paired with `host_generation` so the direct visual comparison shows why
    /// `runnable` is what it is.
    module_generation: String,
    /// The HOST's WIT contract version in the same format. Constant per row
    /// (the host doesn't change mid-session); repeated so a user glancing at
    /// any row sees the pair without a separate lookup.
    host_generation: String,
    /// Catalog author's lifecycle claim from `providers[].status` (`supported`,
    /// `deprecated`, …), else `unknown`. This is metadata, NOT a health/
    /// runnability signal — `runnable` is the runtime answer.
    lifecycle: String,
    /// Whether THIS host can load this module. Per the STRICT same-major model,
    /// a host loads ONLY modules whose OWN generation major equals the host
    /// generation major.
    runnable: bool,
    /// Whether this row is the provider `ducklink_load('<module>')` would
    /// resolve to on THIS host — the same choice `select_provider(host_major)`
    /// makes. Exactly one row per module is `true` (or zero, if no provider
    /// matches).
    selected: bool,
}

/// Build the `ducklink_module_compatibility()` rows from the resolved
/// catalog: one row per (module, generation). Entries that carry a
/// `providers[]` array emit one row per WASM provider (generation from
/// `providers[].abi`); entries without providers emit a single synthetic row
/// for their top-level default artifact (generation from `wit_contract_version`).
/// `runnable` and `selected` are decided against the host generation
/// `host_major`; the `host_generation` label is derived from
/// `ducklink_runtime::CONTRACT_VERSION`.
fn build_module_compatibility_rows(host_major: u64) -> Vec<ModuleCompatibilityRow> {
    let catalog = crate::catalog::resolve_catalog();
    let host_generation =
        normalize_generation(Some(ducklink_runtime::CONTRACT_VERSION.to_string()));
    let mut rows = Vec::new();
    for e in &catalog.extensions {
        let selected_digest = e
            .select_provider(host_major)
            .and_then(|p| p.content_digest.clone())
            .or_else(|| e.content_digest.clone());
        // STRICT same-major: a module is runnable iff its OWN generation
        // (`wit_contract_version`) equals the host generation. The provider `abi`
        // is stale build metadata (gen-4 artifacts stamped `@2.2.0`/`@3.1.0`) and
        // is NOT used to decide runnability — only to label the generation row.
        let entry_runnable = e.generation_major().map(|m| m == host_major).unwrap_or(false);
        let wasm: Vec<_> = e.wasm_providers().collect();
        if wasm.is_empty() {
            // No per-generation providers: one synthetic row for the default
            // artifact, labelled with the entry's contract version if known.
            let module_generation = normalize_generation(e.wit_contract_version.clone());
            rows.push(ModuleCompatibilityRow {
                module: e.name.clone(),
                module_generation,
                host_generation: host_generation.clone(),
                lifecycle: "unknown".to_string(),
                runnable: entry_runnable,
                selected: true,
            });
        } else {
            for p in wasm {
                let module_generation = normalize_generation(p.abi.clone());
                let selected = p.content_digest.is_some()
                    && p.content_digest == selected_digest;
                rows.push(ModuleCompatibilityRow {
                    module: e.name.clone(),
                    module_generation,
                    host_generation: host_generation.clone(),
                    lifecycle: p.status.clone().unwrap_or_else(|| "unknown".to_string()),
                    runnable: entry_runnable,
                    selected,
                });
            }
        }
    }
    rows
}

/// Present every `generation` cell in the canonical WIT-package form
/// `duckdb:extension@X.Y.Z`. The catalog is inconsistent: entries with a
/// structured `providers[]` array carry the fully-qualified `abi` string
/// (`duckdb:extension@4.0.0`), while entries without one fall back to
/// `wit_contract_version` (bare `4.0.0`). Two rows semantically identical to
/// gen-4 would otherwise render as two different strings, so users can't
/// group / filter by generation reliably. `None` and any placeholder that
/// looks like a sentinel (no dot, empty, "unknown") stay as `"unknown"`.
fn normalize_generation(raw: Option<String>) -> String {
    let Some(raw) = raw else {
        return "unknown".to_string();
    };
    if raw.is_empty() || raw == "unknown" {
        "unknown".to_string()
    } else if raw.contains('@') {
        // Already fully-qualified.
        raw
    } else if raw.contains('.') {
        // Bare semver → prefix with the canonical WIT package name.
        format!("duckdb:extension@{raw}")
    } else {
        // Not a recognizable version string; leave as-is so the caller can see
        // whatever oddity the catalog carried.
        raw
    }
}

struct WasmModuleCompatibilityBind {
    rows: Vec<ModuleCompatibilityRow>,
}

/// `ducklink_module_compatibility()` — one row per (module, generation),
/// reflecting the per-generation providers in the resolved catalog and whether
/// THIS host (whose own WIT generation is repeated in the `host_generation`
/// column) can load each one.
struct WasmModuleCompatibility;

impl VTab for WasmModuleCompatibility {
    type InitData = WasmTableInit;
    type BindData = WasmModuleCompatibilityBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("ducklink_module_compatibility bind", || {
            let vc = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
            let boolean = || LogicalTypeHandle::from(LogicalTypeId::Boolean);
            bind.add_result_column("module", vc());
            bind.add_result_column("module_generation", vc());
            bind.add_result_column("host_generation", vc());
            bind.add_result_column("lifecycle", vc());
            bind.add_result_column("runnable", boolean());
            bind.add_result_column("selected", boolean());
            Ok(WasmModuleCompatibilityBind {
                rows: build_module_compatibility_rows(host_generation_major()),
            })
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(WasmTableInit {
            cursor: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        guard("ducklink_module_compatibility scan", || {
            let bind = func.get_bind_data();
            let init = func.get_init_data();
            let start = init.cursor.load(Ordering::Relaxed);
            let n = bind.rows.len().saturating_sub(start).min(2048);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            for r in 0..n {
                let row = &bind.rows[start + r];
                output.flat_vector(0).insert(r, row.module.as_str());
                output.flat_vector(1).insert(r, row.module_generation.as_str());
                output.flat_vector(2).insert(r, row.host_generation.as_str());
                output.flat_vector(3).insert(r, row.lifecycle.as_str());
            }
            unsafe {
                let mut rv = output.flat_vector(4);
                let run = rv.as_mut_slice::<bool>();
                let mut sv = output.flat_vector(5);
                let sel = sv.as_mut_slice::<bool>();
                for r in 0..n {
                    let row = &bind.rows[start + r];
                    run[r] = row.runnable;
                    sel[r] = row.selected;
                }
            }
            init.cursor.store(start + n, Ordering::Relaxed);
            output.set_len(n);
            Ok(())
        })
    }
}

/// Load each component and register its functions, sharing one `engine`. Scalars
/// and table functions register on the duckdb-rs `con`; aggregates (which need
/// the raw C API) register on `raw_con` when supplied — pass `None` (e.g. the
/// loadable entry point, which has no raw connection) to skip them with a note.
/// Returns the total number of functions registered. The `engine` `Arc` is
/// cloned into every registered function's state, so the loaded components stay
/// alive as long as the functions remain in the catalog.
///
/// # Safety
/// When `Some`, `raw_con` must be a valid `duckdb_connection` sharing the
/// database with `con` (so the aggregates it registers are visible to `con`).
pub fn register_components(
    con: &Connection,
    raw_con: Option<ffi::duckdb_connection>,
    db: Option<ffi::duckdb_database>,
    engine: Arc<Engine2>,
    specs: &[ComponentSpec],
) -> anyhow::Result<usize> {
    // The advanced-tier `db` handle is unused when the advanced module is not
    // compiled in (the default community build, or Windows); reference it so the
    // common-tier-only build is clean.
    #[cfg(not(advanced_tier))]
    let _ = &db;
    let mut total = 0usize;
    for spec in specs {
        let loaded = {
            let e = &engine;
            e.load(&spec.name, &spec.path)?
        };
        total += register_scalars(con, engine.clone(), &loaded.scalars)?;
        total += register_tables(con, engine.clone(), &loaded.tables)?;
        // Advanced tier: wire any PARSER / OPTIMIZER / filterable-table markers
        // through the internal-ABI C++ shim. Needs the raw `db` handle; the
        // bundled tests (which use a duckdb-rs Connection) pass `None`. Compiled
        // in only for `advanced_tier` builds; on the default community build and
        // on Windows the C++ shim and the `advanced` module do not exist (callers
        // there always pass `None` anyway).
        #[cfg(advanced_tier)]
        if let Some(db) = db {
            crate::advanced::register(db, &engine, &loaded);
        }
        match raw_con {
            Some(rc) => {
                total += unsafe { register_aggregates(rc, engine.clone(), &loaded.aggregates)? };
            }
            None if !loaded.aggregates.is_empty() => {
                eprintln!(
                    "[ducklink] skipping {} aggregate function(s) from '{}': no raw connection available",
                    loaded.aggregates.len(),
                    spec.name
                );
            }
            None => {}
        }
    }
    Ok(total)
}

#[cfg(all(test, feature = "bundled"))]
mod tests {
    use super::*;
    use crate::engine::Engine2;
    use std::path::PathBuf;

    fn sample_component() -> PathBuf {
        // Defaults to the monorepo layout; overridable with `DUCKLINK_CORPUS_DIR`
        // so the bundled tests run from the standalone repo checkout too.
        let dir = match std::env::var_os("DUCKLINK_CORPUS_DIR") {
            Some(dir) => PathBuf::from(dir),
            None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../artifacts/extensions"),
        };
        dir.join("sample_extension.wasm")
    }

    /// End-to-end: load the sample wasm component, register its
    /// `sample_plus_one(BIGINT)->BIGINT` scalar into a real in-process DuckDB,
    /// and confirm the +1 is computed inside the wasm component.
    #[test]
    fn sample_plus_one_dispatches_into_wasm() {
        let mut engine = Engine2::new().expect("engine");
        let loaded = engine
            .load("sample_extension", &sample_component())
            .expect("load component");
        let engine = Arc::new(engine);

        let con = Connection::open_in_memory().expect("open duckdb");
        let n = register_scalars(&con, engine.clone(), &loaded.scalars).expect("register");
        assert!(
            n >= 1,
            "expected at least one BIGINT->BIGINT scalar, got {n}"
        );

        let v: i64 = con
            .query_row("SELECT sample_plus_one(41)", [], |r| r.get(0))
            .expect("query");
        assert_eq!(v, 42, "sample_plus_one(41) should be 42 (computed in wasm)");

        // A batch, to exercise the per-row dispatch loop.
        let sum: i64 = con
            .query_row(
                "SELECT sum(sample_plus_one(i)) FROM range(5) t(i)",
                [],
                |r| r.get(0),
            )
            .expect("query batch");
        assert_eq!(sum, 1 + 2 + 3 + 4 + 5, "sum of (i+1) for i in 0..5");
    }

    /// `register_components` — the path the loadable entry point takes — loads a
    /// component by spec and registers its scalars.
    #[test]
    fn register_components_exposes_scalar() {
        let engine = Arc::new(Engine2::new().expect("engine"));
        let specs = vec![ComponentSpec {
            name: "sample_extension".to_string(),
            path: sample_component(),
        }];
        let con = Connection::open_in_memory().expect("open duckdb");
        let n =
            register_components(&con, None, None, engine, &specs).expect("register components");
        assert!(n >= 1, "expected >=1 scalar registered, got {n}");

        let v: i64 = con
            .query_row("SELECT sample_plus_one(7)", [], |r| r.get(0))
            .expect("query");
        assert_eq!(v, 8);
    }

    /// End-to-end table function: `sample_emit_sequence(limit)` emits rows
    /// `0..limit` from inside the wasm component, streamed back through the VTab
    /// bridge.
    #[test]
    fn sample_emit_sequence_streams_from_wasm() {
        let engine = Arc::new(Engine2::new().expect("engine"));
        let specs = vec![ComponentSpec {
            name: "sample_extension".to_string(),
            path: sample_component(),
        }];
        let con = Connection::open_in_memory().expect("open duckdb");
        register_components(&con, None, None, engine, &specs).expect("register components");

        let count: i64 = con
            .query_row("SELECT count(*) FROM sample_emit_sequence(5)", [], |r| {
                r.get(0)
            })
            .expect("count query");
        assert_eq!(count, 5, "sample_emit_sequence(5) emits 5 rows");

        let sum: i64 = con
            .query_row("SELECT sum(value) FROM sample_emit_sequence(5)", [], |r| {
                r.get(0)
            })
            .expect("sum query");
        assert_eq!(sum, 1 + 2 + 3 + 4, "sum of values 0..5");
    }

    /// Serialises the tests that drive the process-wide `RUNTIME` OnceLock via the
    /// raw `duckdb_open` entry-point path. `RUNTIME` is first-write-wins and holds
    /// ONE `db` handle for the whole process, so two such tests must not run
    /// concurrently (the second would register `ducklink_load` against the first's
    /// db). Whichever test acquires this lock FIRST also wins the OnceLock; a later
    /// test that finds `RUNTIME` already bound to a different db must skip (its
    /// logic is independently covered in isolation + by the `catalog` unit tests).
    static RUNTIME_LOCK: Mutex<()> = Mutex::new(());

    /// True once the process-wide `RUNTIME` is bound to `db` (this test won the
    /// first-write). A later raw-open test that finds it bound elsewhere skips.
    fn runtime_is_ours(db: ffi::duckdb_database) -> bool {
        RUNTIME.get().map(|rt| rt.db) == Some(db)
    }

    /// THE LINCHPIN TEST: prove `ducklink_load(path)` loads a component AT
    /// RUNTIME from a SQL statement and registers its functions so a SEPARATE,
    /// SUBSEQUENT statement in the same session can call them.
    ///
    /// This mirrors the real loadable entry point exactly: open a raw
    /// `duckdb_database`, wrap it in a `Connection`, seed the process-wide runtime
    /// + register `ducklink_load` (what `register_load_function` does at
    /// `LOAD ducklink` time). Then issue `SELECT * FROM ducklink_load(<wasm>)` and,
    /// in a LATER `query_row`, call the freshly-registered `sample_plus_one`.
    #[test]
    fn ducklink_load_registers_at_runtime_for_later_statements() {
        let _guard = RUNTIME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            // Raw database, exactly as DuckDB hands the loadable entry point one.
            let mut db: ffi::duckdb_database = std::ptr::null_mut();
            let r = ffi::duckdb_open(c":memory:".as_ptr(), &mut db);
            assert_eq!(r, ffi::DuckDBSuccess, "duckdb_open failed");

            let con = Connection::open_from_raw(db.cast()).expect("open_from_raw");
            let engine = Arc::new(Engine2::new().expect("engine"));

            // What the entry point does: register ducklink_load + seed RUNTIME.
            register_load_function(&con, db, engine).expect("register ducklink_load");

            // `RUNTIME` is process-wide first-write-wins; if an earlier raw-open
            // test already bound it to a different db, this run cannot proceed
            // (registrations would target the wrong database). Skip cleanly.
            if !runtime_is_ours(db) {
                eprintln!("[test] RUNTIME already bound elsewhere; skipping (covered in isolation)");
                drop(con);
                ffi::duckdb_close(&mut db);
                return;
            }

            // BEFORE the load, the component's function must NOT exist yet.
            let pre = con.query_row("SELECT sample_plus_one(1)", [], |r| r.get::<_, i64>(0));
            assert!(
                pre.is_err(),
                "sample_plus_one must not exist before ducklink_load"
            );

            // STATEMENT 1: load the component at runtime from SQL.
            let path = sample_component();
            let path_str = path.to_str().expect("utf8 path").to_string();
            let (got_name, n_scalars): (String, i64) = con
                .query_row(
                    "SELECT name, scalars FROM ducklink_load(?)",
                    [&path_str],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .expect("ducklink_load query");
            assert_eq!(got_name, "sample_extension", "name defaults to file stem");
            assert!(n_scalars >= 1, "expected >=1 scalar registered, got {n_scalars}");

            // STATEMENT 2 (separate, subsequent): call the newly-registered fn.
            let v: i64 = con
                .query_row("SELECT sample_plus_one(41)", [], |r| r.get(0))
                .expect("call sample_plus_one AFTER runtime load");
            assert_eq!(v, 42, "sample_plus_one(41) computed in wasm after runtime load");

            // STATEMENT 3: also visible on a SIBLING connection (db-wide catalog).
            let con2 = con.try_clone().expect("clone connection");
            let v2: i64 = con2
                .query_row("SELECT sample_plus_one(7)", [], |r| r.get(0))
                .expect("call on sibling connection after runtime load");
            assert_eq!(v2, 8);

            drop(con2);
            drop(con);
            ffi::duckdb_close(&mut db);
        }
    }

    /// NAME-BASED end-to-end: `CALL ducklink_load('aba')` (by catalog NAME, not a
    /// path) must resolve the catalog, obtain the `aba.wasm` blob (cache hit after
    /// seeding), register `aba_validate`, and have it callable in a LATER
    /// statement. Also exercises the two discovery table functions
    /// (`ducklink_extensions()` 199 rows; `ducklink_loaded()` shows aba).
    ///
    /// To stay deterministic regardless of the sandbox's network, the cache is
    /// pre-seeded from the local artifact at the catalog digest so the resolver
    /// finds it WITHOUT downloading. The catalog itself resolves live-or-bundled
    /// (the bundled snapshot carries the aba entry + digest), so name lookup works
    /// offline too. The download+verify+fallback logic is covered by the
    /// `catalog::tests` unit tests.
    #[test]
    fn ducklink_load_by_name_registers_aba_and_discovery_lists_it() {
        let _guard = RUNTIME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // A private cache root for this test (does not touch the user's cache).
        let cache_root = std::env::temp_dir().join(format!("ducklink_nm_{}", std::process::id()));
        unsafe {
            std::env::set_var("XDG_CACHE_HOME", &cache_root);
        }

        // Seed the cache from the committed aba fixture at its catalog digest, so
        // the name resolver returns it as a cache hit (no network needed). The
        // fixture is the gen-4 production blob, matching the bundled snapshot's
        // aba content_digest, so the sha256 verify succeeds offline (and a strict
        // gen-4 host accepts it).
        let digest = "068b47e3ea5df366637eb3726e7efaa6bfb4ddd00564bf75c821956572c76a15";
        let seed_target = cache_root
            .join("ducklink")
            .join("wasm")
            .join("sha256")
            .join(digest)
            .join("aba.wasm");
        // The committed fixture (production gen-4 aba blob at the snapshot digest).
        let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/aba.wasm");
        if !src.is_file() {
            eprintln!("[test] skipping name-based test: aba fixture not found at {}", src.display());
            return;
        }
        std::fs::create_dir_all(seed_target.parent().unwrap()).expect("mk cache dir");
        std::fs::copy(&src, &seed_target).expect("seed cache");

        unsafe {
            let mut db: ffi::duckdb_database = std::ptr::null_mut();
            assert_eq!(ffi::duckdb_open(c":memory:".as_ptr(), &mut db), ffi::DuckDBSuccess);
            let con = Connection::open_from_raw(db.cast()).expect("open_from_raw");
            let engine = Arc::new(Engine2::new().expect("engine"));
            register_load_function(&con, db, engine).expect("register ducklink_load + discovery");

            // `RUNTIME` is process-wide first-write-wins; if another raw-open test
            // already bound it, this run can't register against its own db. Skip
            // (this test's logic is proven when it runs first / in isolation, and
            // the resolve/verify/fallback path is covered by `catalog::tests`).
            if !runtime_is_ours(db) {
                eprintln!("[test] RUNTIME already bound elsewhere; skipping name-based E2E (covered in isolation)");
                drop(con);
                ffi::duckdb_close(&mut db);
                std::env::remove_var("XDG_CACHE_HOME");
                let _ = std::fs::remove_dir_all(&cache_root);
                return;
            }

            // The public `ducklink` schema of views resolves in a normal session.
            // Discovery BEFORE load: the published catalog lists 199 modules.
            let n_ext: i64 = con
                .query_row("SELECT count(*) FROM ducklink.modules", [], |r| r.get(0))
                .expect("ducklink.modules count");
            assert!(
                n_ext > 150,
                "expected 199 catalog rows, got {n_ext}"
            );
            // aba is one of them and is NOT loaded yet.
            let is_loaded: bool = con
                .query_row(
                    "SELECT loaded FROM ducklink.modules WHERE name = 'aba'",
                    [],
                    |r| r.get(0),
                )
                .expect("aba row in ducklink.modules");
            assert!(!is_loaded, "aba must not be loaded before ducklink_load");

            // ducklink.functions lists aba's exported function name pre-load.
            let has_fn: i64 = con
                .query_row(
                    "SELECT count(*) FROM ducklink.functions WHERE module='aba' AND name='aba_validate'",
                    [],
                    |r| r.get(0),
                )
                .expect("aba_validate in ducklink.functions");
            assert!(has_fn >= 1, "aba_validate should appear in ducklink.functions");

            // Nothing is loaded yet: no module has loaded=true.
            let n_loaded_before: i64 = con
                .query_row("SELECT count(*) FROM ducklink.modules WHERE loaded", [], |r| r.get(0))
                .expect("loaded count before");
            assert_eq!(n_loaded_before, 0, "nothing loaded yet");

            // STATEMENT 1: load BY NAME (resolves catalog -> cached blob).
            let (got_name, n_scalars): (String, i64) = con
                .query_row(
                    "SELECT name, scalars FROM ducklink_load('aba')",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .expect("ducklink_load('aba') by name");
            assert_eq!(got_name, "aba", "name defaults to the catalog name");
            assert!(n_scalars >= 1, "aba should register >=1 scalar, got {n_scalars}");

            // STATEMENT 2 (separate): call the freshly-registered aba_validate.
            // A valid ABA routing number (well-known example): 021000021.
            let valid: bool = con
                .query_row("SELECT aba_validate('021000021')", [], |r| r.get(0))
                .expect("call aba_validate after name-based load");
            assert!(valid, "021000021 is a valid ABA routing number");
            let invalid: bool = con
                .query_row("SELECT aba_validate('021000020')", [], |r| r.get(0))
                .expect("call aba_validate (invalid)");
            assert!(!invalid, "021000020 fails the ABA checksum");

            // ducklink.modules now shows aba as loaded with its live scalar count.
            let (lname, lloaded, lscalars): (String, bool, i32) = con
                .query_row(
                    "SELECT name, loaded, scalars FROM ducklink.modules WHERE name = 'aba'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .expect("aba in ducklink.modules");
            assert_eq!(lname, "aba");
            assert!(lloaded, "aba should be loaded now");
            assert!(lscalars >= 1);

            // ducklink.functions now renders aba's LIVE signature.
            let (fkind, fargs, frets): (String, String, String) = con
                .query_row(
                    "SELECT kind, arguments, returns FROM ducklink.functions \
                     WHERE module='aba' AND name='aba_validate'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .expect("aba_validate live signature");
            assert_eq!(fkind, "scalar");
            assert!(fargs.contains("VARCHAR"), "args should be VARCHAR, got: {fargs}");
            assert_eq!(frets, "BOOLEAN", "aba_validate returns BOOLEAN");

            // ducklink.cache shows the seeded aba blob.
            let cache_hit: i64 = con
                .query_row(
                    "SELECT count(*) FROM ducklink.cache WHERE name='aba'",
                    [],
                    |r| r.get(0),
                )
                .expect("aba in ducklink.cache");
            assert!(cache_hit >= 1, "aba blob should appear in ducklink.cache");

            // ducklink.events recorded the aba load lifecycle. The load above
            // resolved a seeded/cached blob, so at minimum load_start + load_ok
            // were emitted for module 'aba', ordered by monotonic seq, and the
            // ts column is a real TIMESTAMP.
            let (start_seq, ok_seq): (i64, i64) = con
                .query_row(
                    "SELECT \
                       max(seq) FILTER (WHERE kind='load_start'), \
                       max(seq) FILTER (WHERE kind='load_ok') \
                     FROM ducklink.events WHERE module='aba'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .expect("aba load_start/load_ok in ducklink.events");
            assert!(ok_seq > start_seq, "load_ok must follow load_start by seq");
            let ts_type: String = con
                .query_row(
                    "SELECT typeof(ts) FROM ducklink.events LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .expect("events ts type");
            assert_eq!(ts_type, "TIMESTAMP", "events.ts must be a TIMESTAMP");

            // ducklink.host_capabilities always has the common-tier rows.
            let n_caps: i64 = con
                .query_row(
                    "SELECT count(*) FROM ducklink.host_capabilities WHERE name IN ('scalar','load_wasm')",
                    [],
                    |r| r.get(0),
                )
                .expect("capabilities rows");
            assert_eq!(n_caps, 2, "scalar + load_wasm capability rows present");

            // ducklink.module_compatibility lists aba's provider row (its
            // provider carries a stale @2.2.0 abi label). aba is a gen-4 ENTRY,
            // so on this gen-4 host it is runnable under strict same-major, and
            // it is the default (its gen-4 top-level digest is the resolved blob).
            let (vgen, vhost, vrun, vsel): (String, String, bool, bool) = con
                .query_row(
                    "SELECT module_generation, host_generation, runnable, selected \
                     FROM ducklink.module_compatibility \
                     WHERE module='aba' AND module_generation LIKE '%@2.2.0'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .expect("aba provider row in ducklink.module_compatibility");
            assert_eq!(vgen, "duckdb:extension@2.2.0");
            assert!(
                vhost.starts_with("duckdb:extension@"),
                "host_generation is normalized to duckdb:extension@X.Y.Z, got {vhost}"
            );
            assert!(vrun, "gen-4 aba runs on a gen-4 host (strict same-major)");
            assert!(vsel, "aba's gen-4 digest is the selected provider");

            // IDEMPOTENCY: re-loading aba must NOT hard-error.
            let again = con.query_row(
                "SELECT name FROM ducklink_load('aba')",
                [],
                |r| r.get::<_, String>(0),
            );
            assert!(again.is_ok(), "re-load of aba should not error: {again:?}");

            drop(con);
            ffi::duckdb_close(&mut db);
        }

        unsafe { std::env::remove_var("XDG_CACHE_HOME") };
        let _ = std::fs::remove_dir_all(&cache_root);
    }

    // `DUCKLINK_COMPONENTS` is a process-global; the tests that mutate it must not
    // run concurrently (cargo runs unit tests multi-threaded). Serialize them on a
    // shared lock so each sets/reads/clears the var in isolation.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn env_specs_parse_name_and_bare_path() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("DUCKLINK_COMPONENTS", "sample=/a/b.wasm:/c/isin.wasm");
        }
        let specs = component_specs_from_env();
        unsafe {
            std::env::remove_var("DUCKLINK_COMPONENTS");
        }
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "sample");
        assert_eq!(specs[0].path, PathBuf::from("/a/b.wasm"));
        assert_eq!(specs[1].name, "isin", "bare path -> file stem as name");
        assert_eq!(specs[1].path, PathBuf::from("/c/isin.wasm"));
    }

    /// Feasibility probe for the aggregate bridge: are functions registered via
    /// the C API visible across connections of the same database? (Aggregate
    /// registration needs a raw `duckdb_connection`, which the duckdb-rs
    /// `Connection` doesn't expose — so we'd register on a separate connection of
    /// the same db and rely on db-wide visibility.)
    #[test]
    fn registered_function_visible_across_connections() {
        let mut engine = Engine2::new().expect("engine");
        let loaded = engine
            .load("sample_extension", &sample_component())
            .expect("load");
        let engine = Arc::new(engine);
        let con = Connection::open_in_memory().expect("open");
        register_scalars(&con, engine, &loaded.scalars).expect("register");

        // sample_plus_one is registered on `con`; query it from a clone.
        let con2 = con.try_clone().expect("clone connection");
        let v: i64 = con2
            .query_row("SELECT sample_plus_one(41)", [], |r| r.get(0))
            .expect("query on cloned connection");
        assert_eq!(
            v, 42,
            "C-API function should be visible on a sibling connection"
        );
    }

    // --- FFI panic guard ---------------------------------------------------

    #[test]
    fn guard_passes_through_ok() {
        let r: Result<i32, Box<dyn std::error::Error>> = guard("scalar dispatch", || Ok(7));
        assert_eq!(r.expect("ok"), 7);
    }

    #[test]
    fn guard_passes_through_declared_error_verbatim() {
        // A normal Err is returned unchanged — only panics get the wrapper prefix.
        let r: Result<(), Box<dyn std::error::Error>> =
            guard("scalar dispatch", || Err("declared failure".into()));
        assert_eq!(r.unwrap_err().to_string(), "declared failure");
    }

    #[test]
    fn guard_converts_panic_to_error() {
        // The crux: a panic in the body becomes an Err (carrying the message)
        // instead of unwinding across the C FFI boundary and aborting the host.
        let r: Result<(), Box<dyn std::error::Error>> =
            guard("scalar dispatch", || panic!("kaboom {}", 42));
        let msg = r.unwrap_err().to_string();
        assert!(
            msg.contains("internal panic in wasm scalar dispatch"),
            "missing context, got: {msg}"
        );
        assert!(msg.contains("kaboom 42"), "missing payload, got: {msg}");
    }

    // --- Pure mapping / codec helpers (no wasm) ----------------------------

    #[test]
    fn uuid_storage_logical_is_self_inverse() {
        for &stored in &[0i128, 1, -1, i128::MAX, i128::MIN, 0x0123_4567_89ab_cdef] {
            let logical = uuid_storage_to_logical(stored);
            // Applying the (self-inverse) transform again returns the storage bits.
            assert_eq!(
                uuid_storage_to_logical(logical as i128),
                stored as u128,
                "uuid transform not self-inverse for {stored}"
            );
        }
    }

    #[test]
    fn type_codes_are_distinct_per_logical_type() {
        use ducklink_runtime::reg::LogicalType::*;
        let types = [
            Int64,
            Uint64,
            Float64,
            Boolean,
            Text,
            Blob,
            Int8,
            Int16,
            Int32,
            Uint8,
            Uint16,
            Uint32,
            Float32,
            Timestamp,
            Date,
            Time,
            Timestamptz,
            Decimal,
            Interval,
            Uuid,
            Complex(String::new()),
        ];
        let codes: Vec<u8> = types.iter().map(type_code).collect();
        let mut uniq = codes.clone();
        uniq.sort_unstable();
        uniq.dedup();
        assert_eq!(
            uniq.len(),
            codes.len(),
            "every logical type maps to a distinct bridge code"
        );
    }

    #[test]
    fn logical_type_and_duckdb_type_cover_every_code() {
        // Every defined code (0..=T_COMPLEX) must build a column logical type and a
        // raw duckdb_type without hitting the `unreachable!` arm.
        for code in 0u8..=T_COMPLEX {
            let _ = logical_type(code); // must not panic
            let _ = duckdb_type_of(code); // must not panic
        }
    }

    // --- DUCKLINK_COMPONENTS parsing edge cases ----------------------------

    #[test]
    fn env_specs_skip_empty_entries_and_keep_order() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("DUCKLINK_COMPONENTS", ":a=/x.wasm::/y/z.wasm:");
        }
        let specs = component_specs_from_env();
        unsafe {
            std::env::remove_var("DUCKLINK_COMPONENTS");
        }
        // Leading / trailing / doubled colons produce empty entries that are dropped.
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "a");
        assert_eq!(specs[0].path, PathBuf::from("/x.wasm"));
        // Bare path -> file stem as the name.
        assert_eq!(specs[1].name, "z");
        assert_eq!(specs[1].path, PathBuf::from("/y/z.wasm"));
    }
}
