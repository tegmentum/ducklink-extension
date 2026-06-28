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
use std::path::PathBuf;
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
    engine: Arc<Mutex<Engine2>>,
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
fn type_code(lt: &reg::LogicalType) -> u8 {
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

/// Marshal an entire input column (type `code`) into argument slot `j` of every
/// row, ready to hand straight to the component dispatcher with no further
/// conversion. Column-major: the typed slice is derived and the type code is
/// matched once per column (not once per cell), leaving only an index and an
/// enum wrap per row in the hot loop.
///
/// DuckDB uses default NULL handling for these scalars: it still invokes the
/// function on rows whose inputs are NULL, then overwrites those result rows
/// with NULL. So the component must receive a *type-valid* value for every row,
/// including NULL ones (handing it `WitVal::Null` makes a type-checking
/// component reject the whole batch). For numeric types the raw slot value is
/// already type-valid (and discarded), so they read unconditionally — the
/// fast path. For TEXT/BLOB a NULL row's `duckdb_string_t` holds no valid
/// pointer, so reading it would dereference garbage; those rows are instead
/// given an empty (but valid) string/blob. `validity` is the column's mask
/// (null when the column has no NULLs) and is consulted only for TEXT/BLOB.
fn read_col_into(
    code: u8,
    vec: &FlatVector,
    validity: *const u64,
    len: usize,
    rows: &mut [Vec<WitVal>],
    j: usize,
) {
    macro_rules! fill {
        ($ty:ty, $variant:ident) => {{
            let s = unsafe { vec.as_slice_with_len::<$ty>(len) };
            for (i, row) in rows.iter_mut().enumerate() {
                row[j] = WitVal::$variant(s[i]);
            }
        }};
    }
    // For TEXT/BLOB: true when row `i` is NULL and its string slot must not be
    // read. Numeric columns never call this.
    let is_null = |i: usize| !validity.is_null() && unsafe { !row_valid(validity, i) };
    match code {
        T_I64 => fill!(i64, Int64),
        T_U64 => fill!(u64, Uint64),
        T_F64 => fill!(f64, Float64),
        T_BOOL => fill!(bool, Boolean),
        T_I8 => fill!(i8, Int8),
        T_I16 => fill!(i16, Int16),
        T_I32 => fill!(i32, Int32),
        T_U8 => fill!(u8, Uint8),
        T_U16 => fill!(u16, Uint16),
        T_U32 => fill!(u32, Uint32),
        T_F32 => fill!(f32, Float32),
        // Temporal types are stored as plain integers (Date = i32 days, the rest
        // = i64 micros), so they marshal exactly like the numeric arms above.
        T_TIMESTAMP => fill!(i64, Timestamp),
        T_DATE => fill!(i32, Date),
        T_TIME => fill!(i64, Time),
        T_TIMESTAMPTZ => fill!(i64, Timestamptz),
        // INTERVAL is a {months: i32, days: i32, micros: i64} struct in storage
        // (duckdb_interval). Read the three components per row.
        T_INTERVAL => {
            let s = unsafe { vec.as_slice_with_len::<ffi::duckdb_interval>(len) };
            for (i, row) in rows.iter_mut().enumerate() {
                row[j] = WitVal::Interval(WitInterval {
                    months: s[i].months,
                    days: s[i].days,
                    micros: s[i].micros,
                });
            }
        }
        // DECIMAL and UUID are both HUGEINT-backed (i128) in storage. UUID's
        // physical storage is the sign-flipped hugeint; convert to the logical
        // big-endian hi/lo halves the WIT contract expects.
        T_DECIMAL => {
            let s = unsafe { vec.as_slice_with_len::<i128>(len) };
            for (i, row) in rows.iter_mut().enumerate() {
                let raw = s[i] as u128;
                row[j] = WitVal::Decimal(WitDecimal {
                    lower: raw as u64,
                    upper: (raw >> 64) as u64,
                    // The value's width/scale is not available from the flat
                    // vector here; the registration declared DECIMAL(18, 3).
                    width: 18,
                    scale: 3,
                });
            }
        }
        T_UUID => {
            let s = unsafe { vec.as_slice_with_len::<i128>(len) };
            for (i, row) in rows.iter_mut().enumerate() {
                let logical = uuid_storage_to_logical(s[i]);
                row[j] = WitVal::Uuid(WitUuid {
                    hi: (logical >> 64) as u64,
                    lo: logical as u64,
                });
            }
        }
        T_COMPLEX => {
            // No nested-vector reader: surface the VARCHAR/JSON form as a Complex
            // value with an empty (unknown) type-expression.
            let s = unsafe { vec.as_slice_with_len::<duckdb_string_t>(len) };
            for (i, row) in rows.iter_mut().enumerate() {
                row[j] = if is_null(i) {
                    WitVal::Complex(WitComplex {
                        type_expr: String::new(),
                        json: String::new(),
                    })
                } else {
                    let mut t = s[i];
                    WitVal::Complex(WitComplex {
                        type_expr: String::new(),
                        json: DuckString::new(&mut t).as_str().into_owned(),
                    })
                };
            }
        }
        T_TEXT => {
            let s = unsafe { vec.as_slice_with_len::<duckdb_string_t>(len) };
            for (i, row) in rows.iter_mut().enumerate() {
                row[j] = if is_null(i) {
                    WitVal::Text(String::new())
                } else {
                    let mut t = s[i];
                    WitVal::Text(DuckString::new(&mut t).as_str().into_owned())
                };
            }
        }
        T_BLOB => {
            let s = unsafe { vec.as_slice_with_len::<duckdb_string_t>(len) };
            for (i, row) in rows.iter_mut().enumerate() {
                row[j] = if is_null(i) {
                    WitVal::Blob(Vec::new())
                } else {
                    let mut t = s[i];
                    WitVal::Blob(DuckString::new(&mut t).as_bytes().to_vec())
                };
            }
        }
        _ => unreachable!("type code out of range"),
    }
}

/// Convert a neutral value into the WIT value `write_ret` consumes. Used only by
/// the cold table path (one materialized result set per query); the hot scalar
/// path skips it entirely -- `read_arg` yields `WitVal` and the dispatcher
/// returns `WitVal`, so nothing on that path touches `reg::DuckValue`.
fn neutral_to_wit(v: reg::DuckValue) -> WitVal {
    match v {
        reg::DuckValue::Null => WitVal::Null,
        reg::DuckValue::Boolean(b) => WitVal::Boolean(b),
        reg::DuckValue::Int64(i) => WitVal::Int64(i),
        reg::DuckValue::Uint64(u) => WitVal::Uint64(u),
        reg::DuckValue::Float64(f) => WitVal::Float64(f),
        reg::DuckValue::Text(s) => WitVal::Text(s),
        reg::DuckValue::Blob(b) => WitVal::Blob(b),
        reg::DuckValue::Int8(i) => WitVal::Int8(i),
        reg::DuckValue::Int16(i) => WitVal::Int16(i),
        reg::DuckValue::Int32(i) => WitVal::Int32(i),
        reg::DuckValue::Uint8(u) => WitVal::Uint8(u),
        reg::DuckValue::Uint16(u) => WitVal::Uint16(u),
        reg::DuckValue::Uint32(u) => WitVal::Uint32(u),
        reg::DuckValue::Float32(f) => WitVal::Float32(f),
        reg::DuckValue::Timestamp(t) => WitVal::Timestamp(t),
        reg::DuckValue::Date(d) => WitVal::Date(d),
        reg::DuckValue::Time(t) => WitVal::Time(t),
        reg::DuckValue::Timestamptz(t) => WitVal::Timestamptz(t),
        reg::DuckValue::Decimal {
            lower,
            upper,
            width,
            scale,
        } => WitVal::Decimal(WitDecimal {
            lower,
            upper,
            width,
            scale,
        }),
        reg::DuckValue::Interval {
            months,
            days,
            micros,
        } => WitVal::Interval(WitInterval {
            months,
            days,
            micros,
        }),
        reg::DuckValue::Uuid { hi, lo } => WitVal::Uuid(WitUuid { hi, lo }),
        reg::DuckValue::Complex { type_expr, json } => {
            WitVal::Complex(WitComplex { type_expr, json })
        }
    }
}

/// Write a component-returned WIT value into row `i` of a flat output column.
fn write_ret(
    code: u8,
    vec: &mut FlatVector,
    i: usize,
    len: usize,
    v: WitVal,
) -> Result<(), Box<dyn std::error::Error>> {
    match (code, v) {
        // A component may return SQL NULL for any declared return type (e.g. a
        // validator on bad input) — mark the output row invalid.
        (_, WitVal::Null) => vec.set_null(i),
        (T_I64, WitVal::Int64(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i64>(len) };
            s[i] = x;
        }
        (T_U64, WitVal::Uint64(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<u64>(len) };
            s[i] = x;
        }
        (T_F64, WitVal::Float64(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<f64>(len) };
            s[i] = x;
        }
        (T_BOOL, WitVal::Boolean(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<bool>(len) };
            s[i] = x;
        }
        (T_I8, WitVal::Int8(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i8>(len) };
            s[i] = x;
        }
        (T_I16, WitVal::Int16(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i16>(len) };
            s[i] = x;
        }
        (T_I32, WitVal::Int32(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i32>(len) };
            s[i] = x;
        }
        (T_U8, WitVal::Uint8(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<u8>(len) };
            s[i] = x;
        }
        (T_U16, WitVal::Uint16(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<u16>(len) };
            s[i] = x;
        }
        (T_U32, WitVal::Uint32(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<u32>(len) };
            s[i] = x;
        }
        (T_F32, WitVal::Float32(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<f32>(len) };
            s[i] = x;
        }
        // Temporal types share their underlying integer storage.
        (T_TIMESTAMP, WitVal::Timestamp(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i64>(len) };
            s[i] = x;
        }
        (T_DATE, WitVal::Date(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i32>(len) };
            s[i] = x;
        }
        (T_TIME, WitVal::Time(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i64>(len) };
            s[i] = x;
        }
        (T_TIMESTAMPTZ, WitVal::Timestamptz(x)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i64>(len) };
            s[i] = x;
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
    results: Vec<WitVal>,
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
                        .into())
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
            for (i, r) in results.into_iter().enumerate() {
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

    /// Per-thread reusable marshalling buffer for scalar dispatch. DuckDB calls
    /// `invoke` once per data chunk, possibly from several threads; each thread
    /// keeps its own `Vec<Vec<WitVal>>` whose inner row Vecs retain capacity
    /// between chunks, so steady-state scalar evaluation allocates no per-row
    /// marshalling memory. Borrowed only for the duration of one `invoke`.
    static SCALAR_SCRATCH: RefCell<Vec<Vec<WitVal>>> = const { RefCell::new(Vec::new()) };
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

            // Marshal the whole chunk into a reused per-thread scratch, then cross
            // into the component once. DuckDB hands us a chunk of up to
            // STANDARD_VECTOR_SIZE rows; dispatching each row individually pays a WIT
            // boundary crossing per row, which dominates for cheap scalars, so we
            // build every row's argument tuple up front and call the batched
            // dispatcher a single time. The scratch's inner row Vecs keep their
            // capacity across chunks, so steady-state evaluation allocates no per-row
            // marshalling memory.
            let arity = state.arg_codes.len();
            // DuckDB does not propagate input NULLs to the output for these scalars
            // (it invokes the function on NULL rows and keeps the result), so the
            // bridge enforces SQL semantics itself: any row with a NULL input yields
            // a NULL result, overriding whatever the component computed from the
            // placeholder it was fed. `None` until a NULL-bearing column is seen, so
            // the all-valid common case allocates nothing and skips the scan.
            let mut null_mask: Option<Vec<bool>> = None;
            let results = SCALAR_SCRATCH.with(|cell| {
                let mut rows = cell.borrow_mut();
                // Shape the scratch to exactly `len` rows of `arity` slots, reusing
                // the existing inner Vecs' capacity. The slots are overwritten in
                // full by the column fills below, so the placeholder value is never
                // observed.
                if rows.len() < len {
                    rows.resize_with(len, Vec::new);
                } else {
                    rows.truncate(len);
                }
                for row in rows.iter_mut() {
                    // Shape to `arity` slots, reusing capacity. Each slot is
                    // overwritten in full by the column fills below (which drop the
                    // prior value), so surviving entries need not be cleared first.
                    row.resize(arity, WitVal::Null);
                }
                for (j, &code) in state.arg_codes.iter().enumerate() {
                    // Fetch the column's validity mask once (null when no NULLs).
                    let validity = unsafe {
                        let v = ffi::duckdb_data_chunk_get_vector(raw_chunk, j as u64);
                        ffi::duckdb_vector_get_validity(v) as *const u64
                    };
                    read_col_into(code, &cols[j], validity, len, &mut rows, j);
                    if !validity.is_null() {
                        let nm = null_mask.get_or_insert_with(|| vec![false; len]);
                        for (i, slot) in nm.iter_mut().enumerate() {
                            if unsafe { !row_valid(validity, i) } {
                                *slot = true;
                            }
                        }
                    }
                }
                let mut engine = state.engine.lock().expect("engine mutex poisoned");
                engine
                    .dispatch_scalar_batch(state.callback_handle, 0, &rows)
                    .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })
                // `&rows` (RefMut) deref-coerces to `&Vec<Vec<WitVal>>`.
            })?;

            if results.len() != len {
                return Err(format!(
                    "scalar (callback {}) returned {} results for {} input rows",
                    state.callback_handle,
                    results.len(),
                    len
                )
                .into());
            }
            // Write the whole result column at once: the fixed-width hot types
            // derive the typed output slice and match the return type a single
            // time per chunk (column-major), mirroring the read side.
            write_col_from(state.ret_code, &mut out, results, null_mask.as_deref(), len)?;
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
    engine: Arc<Mutex<Engine2>>,
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
        result?;
        registered += 1;
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
    engine: Arc<Mutex<Engine2>>,
    arg_codes: Vec<u8>,
    col_codes: Vec<u8>,
    col_names: Vec<String>,
}

/// Bind result: the full set of rows the component produced for this call, plus
/// the column type codes used to write them out.
struct WasmTableBind {
    rows: Vec<Vec<reg::DuckValue>>,
    col_codes: Vec<u8>,
}

/// Init state: a cursor over `WasmTableBind::rows` across `func` chunks.
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
                let mut engine = extra.engine.lock().expect("engine mutex poisoned");
                engine
                    .dispatch_table(extra.callback_handle, args)
                    .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?
            };
            Ok(WasmTableBind {
                rows,
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
            let n = bind.rows.len().saturating_sub(start).min(2048);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            for (c, &code) in bind.col_codes.iter().enumerate() {
                let mut col = output.flat_vector(c);
                for r in 0..n {
                    let val = neutral_to_wit(bind.rows[start + r][c].clone());
                    write_ret(code, &mut col, r, n, val)?;
                }
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
    engine: Arc<Mutex<Engine2>>,
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
        result?;
        registered += 1;
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
    engine: Arc<Mutex<Engine2>>,
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

/// Write a neutral value into row `i` of a raw result vector (type `code`).
unsafe fn write_ret_raw(
    code: u8,
    vector: ffi::duckdb_vector,
    i: usize,
    v: reg::DuckValue,
) -> Result<(), String> {
    if matches!(v, reg::DuckValue::Null) {
        ffi::duckdb_vector_ensure_validity_writable(vector);
        let validity = ffi::duckdb_vector_get_validity(vector);
        ffi::duckdb_validity_set_row_validity(validity, i as u64, false);
        return Ok(());
    }
    let data = ffi::duckdb_vector_get_data(vector);
    match (code, v) {
        (T_I64, reg::DuckValue::Int64(x)) => *(data as *mut i64).add(i) = x,
        (T_U64, reg::DuckValue::Uint64(x)) => *(data as *mut u64).add(i) = x,
        (T_F64, reg::DuckValue::Float64(x)) => *(data as *mut f64).add(i) = x,
        (T_BOOL, reg::DuckValue::Boolean(x)) => *(data as *mut bool).add(i) = x,
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
        (T_I8, reg::DuckValue::Int8(x)) => *(data as *mut i8).add(i) = x,
        (T_I16, reg::DuckValue::Int16(x)) => *(data as *mut i16).add(i) = x,
        (T_I32, reg::DuckValue::Int32(x)) => *(data as *mut i32).add(i) = x,
        (T_U8, reg::DuckValue::Uint8(x)) => *(data as *mut u8).add(i) = x,
        (T_U16, reg::DuckValue::Uint16(x)) => *(data as *mut u16).add(i) = x,
        (T_U32, reg::DuckValue::Uint32(x)) => *(data as *mut u32).add(i) = x,
        (T_F32, reg::DuckValue::Float32(x)) => *(data as *mut f32).add(i) = x,
        (T_TIMESTAMP, reg::DuckValue::Timestamp(x)) => *(data as *mut i64).add(i) = x,
        (T_DATE, reg::DuckValue::Date(x)) => *(data as *mut i32).add(i) = x,
        (T_TIME, reg::DuckValue::Time(x)) => *(data as *mut i64).add(i) = x,
        (T_TIMESTAMPTZ, reg::DuckValue::Timestamptz(x)) => *(data as *mut i64).add(i) = x,
        (T_INTERVAL, reg::DuckValue::Interval { months, days, micros }) => {
            *(data as *mut ffi::duckdb_interval).add(i) = ffi::duckdb_interval {
                months,
                days,
                micros,
            };
        }
        (T_UUID, reg::DuckValue::Uuid { hi, lo }) => {
            let logical = ((hi as u128) << 64) | lo as u128;
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
                let mut engine = extra.engine.lock().expect("engine mutex poisoned");
                engine.dispatch_aggregate(extra.callback_handle, rows)
            };
            let out = offset as usize + i;
            let write = dispatched
                .map_err(|e| e.to_string())
                .and_then(|v| write_ret_raw(extra.ret_code, result, out, v));
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
    engine: Arc<Mutex<Engine2>>,
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
        if rc != ffi::DuckDBSuccess {
            return Err(duckdb::Error::DuckDBFailure(
                ffi::Error::new(ffi::DuckDBError),
                Some(format!("failed to register aggregate '{}'", f.name)),
            ));
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
    engine: Arc<Mutex<Engine2>>,
    specs: &[ComponentSpec],
) -> anyhow::Result<usize> {
    let mut total = 0usize;
    for spec in specs {
        let loaded = {
            let mut e = engine.lock().expect("engine mutex poisoned");
            e.load(&spec.name, &spec.path)?
        };
        total += register_scalars(con, engine.clone(), &loaded.scalars)?;
        total += register_tables(con, engine.clone(), &loaded.tables)?;
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
        let engine = Arc::new(Mutex::new(engine));

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
        let engine = Arc::new(Mutex::new(Engine2::new().expect("engine")));
        let specs = vec![ComponentSpec {
            name: "sample_extension".to_string(),
            path: sample_component(),
        }];
        let con = Connection::open_in_memory().expect("open duckdb");
        let n = register_components(&con, None, engine, &specs).expect("register components");
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
        let engine = Arc::new(Mutex::new(Engine2::new().expect("engine")));
        let specs = vec![ComponentSpec {
            name: "sample_extension".to_string(),
            path: sample_component(),
        }];
        let con = Connection::open_in_memory().expect("open duckdb");
        register_components(&con, None, engine, &specs).expect("register components");

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
        let engine = Arc::new(Mutex::new(engine));
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
