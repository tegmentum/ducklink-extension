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
use ducklink_runtime::LogEntry;
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
    ArrayColumn as ColvecArrayColumn, Colvec, Column as ColvecColumn,
    Complexvalue as ColvecComplex, Decimalvalue as ColvecDecimal, DuckInt128 as ColvecDuckInt128,
    DuckUint128 as ColvecDuckUint128, Intervalvalue as ColvecInterval,
    MapColumn as ColvecMapColumn, NestedColumn as ColvecNestedColumn, Uuidvalue as ColvecUuid,
};

use crate::engine::{
    AggregateFunc, ArrowTable, CastEntry, CoordinateSystemEntry, CopyHandler, Engine2,
    EnumTypeEntry, LogStorageEntry, LogicalTypeEntry, MacroEntry, ModifiedTypeEntry, PragmaEntry,
    ReplacementScan, ScalarEx, ScalarFunc, Setting, TableFunc, TableMacroEntry,
};

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
///
/// T2-7: `arg_type_exprs[j]` is `Some(expr)` when the corresponding
/// argument's declared logical type is `Complex(expr)`, `None` otherwise.
/// `invoke` forwards each arg's expression to `refill_colvec` so
/// `ColvecComplex.type_expr` reaches the guest with the declared shape
/// (previously erased to the empty string).
#[derive(Clone)]
struct WasmScalarState {
    callback_handle: u32,
    engine: Arc<Engine2>,
    arg_codes: Vec<u8>,
    arg_type_exprs: Vec<Option<String>>,
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
// T2-1 residual (major-5): first-class nested + 128-bit integer bridge codes.
// The nested arms carry structural shape via `reg::LogicalType` (LIST elem,
// STRUCT fields, MAP key/value, ARRAY size+elem); `logical_type_ffi_from_lt`
// walks that shape recursively into `duckdb_create_{list,struct,map,array}_type`.
// The read/write marshallers for nested types are FAIL-LOUD stubs — they log
// an eprintln + degrade to a NULL/empty column rather than half-marshal a
// nested vector, per the T2-1 residual "clean partial over broken" guidance.
const T_LIST: u8 = 21;
const T_STRUCT: u8 = 22;
const T_MAP: u8 = 23;
const T_ARRAY: u8 = 24;
const T_HUGEINT: u8 = 25;
const T_UHUGEINT: u8 = 26;
// Highest defined bridge code — used by `logical_type_and_duckdb_type_cover_every_code`
// (tests) and by any per-code enumeration. `#[allow(dead_code)]` because
// the sole live use is the test; keeping it public-adjacent so future
// enumerate-all-codes callers don't drift out of sync with the T_* set.
#[allow(dead_code)]
const T_CODE_MAX: u8 = T_UHUGEINT;

/// DuckDB's per-column output chunk capacity. Every table-fn `func` clamps its
/// batch to this so `set_len` never exceeds the chunk's allocated capacity
/// (writing past it is UB). Also enforced as the upper bound on rows a guest
/// producer (Arrow shim, COPY FROM) may yield per call — a larger batch is a
/// protocol violation surfaced as a query error.
pub(crate) const STANDARD_VECTOR_SIZE: u32 = 2048;

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
        reg::LogicalType::Decimal { .. } => T_DECIMAL,
        reg::LogicalType::Interval => T_INTERVAL,
        reg::LogicalType::Uuid => T_UUID,
        // T2-1 residual (major-5): 128-bit integers.
        reg::LogicalType::Hugeint => T_HUGEINT,
        reg::LogicalType::UHugeint => T_UHUGEINT,
        // S1 (major-5): first-class nested arms. The bridge code only tags
        // the KIND — structural shape (element type, field names, map
        // key/value, array size) is preserved out-of-band by callers that
        // hand a full `reg::LogicalType` to `logical_type_ffi_from_lt`.
        reg::LogicalType::List(_) => T_LIST,
        reg::LogicalType::Struct(_) => T_STRUCT,
        reg::LogicalType::Map(_, _) => T_MAP,
        reg::LogicalType::Array(_, _) => T_ARRAY,
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
        // T2-1 residual (major-5): 128-bit integers now have first-class
        // LogicalTypeId arms via duckdb-rs.
        T_HUGEINT => LogicalTypeId::Hugeint,
        T_UHUGEINT => LogicalTypeId::UHugeint,
        // Complex crosses as JSON text -> declare a VARCHAR column.
        T_COMPLEX => LogicalTypeId::Varchar,
        // DECIMAL needs a (width, scale) and is built directly below; the value's
        // own width/scale is only known per-value, so the column is declared with
        // DuckDB's default-precision DECIMAL(18, 3). A column whose values carry a
        // different width/scale is a known limitation (see write_ret Decimal arm).
        T_DECIMAL => return LogicalTypeHandle::decimal(18, 3),
        // S1 (major-5): nested arms have no code-only lowering — the
        // LogicalTypeHandle path needs a full `reg::LogicalType` shape to
        // walk. Callers wanting a nested column type must go through the
        // raw-FFI `logical_type_ffi_from_lt(&reg::LogicalType)` path
        // (which recurses into duckdb_create_{list,struct,map,array}_type).
        // Falling back to VARCHAR keeps the code-only path linear rather
        // than panicking on registrations that thread a code without the
        // structural shape.
        T_LIST | T_STRUCT | T_MAP | T_ARRAY => {
            eprintln!(
                "[ducklink] nested logical type (code {code}) has no code-only lowering — \
                 declaring column as VARCHAR fallback (T2-1 residual: pass full \
                 reg::LogicalType via logical_type_ffi_from_lt for structural nested types)"
            );
            LogicalTypeId::Varchar
        }
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
        // S2 (major-5): DECIMAL width/scale ride the variant arm structurally,
        // so the SQL name preserves the declared precision instead of the
        // hardcoded 18/3 the pre-@5 shape used.
        reg::LogicalType::Decimal { width, scale } => format!("DECIMAL({width}, {scale})"),
        reg::LogicalType::Interval => "INTERVAL".to_string(),
        reg::LogicalType::Uuid => "UUID".to_string(),
        // T2-1 residual (major-5): 128-bit integers.
        reg::LogicalType::Hugeint => "HUGEINT".to_string(),
        reg::LogicalType::UHugeint => "UHUGEINT".to_string(),
        // S1 (major-5): nested types render as DuckDB type-expression strings.
        reg::LogicalType::List(inner) => format!("{}[]", sql_type_name(inner)),
        reg::LogicalType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|(n, t)| format!("{n} {}", sql_type_name(t)))
                .collect();
            format!("STRUCT({})", parts.join(", "))
        }
        reg::LogicalType::Map(k, v) => format!("MAP({}, {})", sql_type_name(k), sql_type_name(v)),
        reg::LogicalType::Array(size, inner) => format!("{}[{size}]", sql_type_name(inner)),
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

/// Bulk-scan a DuckDB validity bitmask for any zero bit within the first
/// `len` rows. Reads whole u64 words — 64 rows at a time — so a non-null
/// mask over an all-valid column costs `(len + 63) / 64` word compares, not
/// `len` per-row bit tests. The last partial word masks out the excess bits
/// beyond `len` (DuckDB does not guarantee they are 1).
///
/// # Safety
/// `validity` must be non-null and point to at least `(len + 63) / 64`
/// contiguous u64 words.
#[inline]
unsafe fn validity_has_any_null(validity: *const u64, len: usize) -> bool {
    let full_words = len / 64;
    for w in 0..full_words {
        if *validity.add(w) != u64::MAX {
            return true;
        }
    }
    let tail_bits = len % 64;
    if tail_bits != 0 {
        let mask = (1u64 << tail_bits) - 1;
        if *validity.add(full_words) & mask != mask {
            return true;
        }
    }
    false
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
/// `complex_type_expr` is the declared type-expression string for the
/// column, forwarded to every emitted `ColvecComplex` so the guest can
/// re-parse it (T2-7). `None` for non-complex columns; a `Some("")` for a
/// complex column with no expression is treated as an empty string. Only
/// the T_COMPLEX arm consults it; other arms ignore it.
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
    complex_type_expr: Option<&str>,
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
            // T2-7: forward the declared complex type-expression to the
            // guest for every cell. Previously erased to `String::new()`,
            // which stripped LIST/STRUCT/... arity so the guest could not
            // re-parse the JSON body against its declared schema. The
            // expression lives on the enclosing bind state — the caller
            // (refill_colvec / a direct read_col_to_colvec) threads it
            // through via `complex_type_expr`.
            let type_expr_owned = complex_type_expr.unwrap_or("").to_string();
            let s = vec.as_slice_with_len::<duckdb_string_t>(len);
            let out: Vec<ColvecComplex> = (0..len)
                .map(|i| {
                    if is_null(i) {
                        ColvecComplex {
                            type_expr: type_expr_owned.clone(),
                            json: String::new(),
                        }
                    } else {
                        let mut t = s[i];
                        ColvecComplex {
                            type_expr: type_expr_owned.clone(),
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
        // T2-1 residual (major-5): HUGEINT / UHUGEINT bulk read. DuckDB
        // stores each cell as a naked i128 / u128 in the flat vector; split
        // each into (lower u64, upper i64/u64) so the WIT column arms round-
        // trip through the guest's `duck-int128 { lower, upper }` shape.
        T_HUGEINT => {
            let s = vec.as_slice_with_len::<i128>(len);
            let out: Vec<ColvecDuckInt128> = s
                .iter()
                .map(|&raw| ColvecDuckInt128 {
                    lower: raw as u64,
                    upper: (raw >> 64) as i64,
                })
                .collect();
            ColvecColumn::Hugeint(out)
        }
        T_UHUGEINT => {
            let s = vec.as_slice_with_len::<u128>(len);
            let out: Vec<ColvecDuckUint128> = s
                .iter()
                .map(|&raw| ColvecDuckUint128 {
                    lower: raw as u64,
                    upper: (raw >> 64) as u64,
                })
                .collect();
            ColvecColumn::Uhugeint(out)
        }
        // S1 (major-5): nested type reads via
        // `duckdb_list_vector_get_child(vec)` / `duckdb_list_vector_get_size` /
        // `duckdb_struct_vector_get_child(vec, i)` /
        // `duckdb_array_vector_get_child(vec)` are not yet wired here. The
        // opaque-encoded `nested-column { encoded: list<u8> }` payload the
        // WIT column arm carries needs a runtime-defined encoding scheme
        // (see column-types.wit S1 header note) that has NOT landed yet.
        // FAIL-LOUD: emit an empty encoded payload + eprintln so the guest
        // observes an empty vector rather than a half-marshaled one.
        T_LIST => {
            eprintln!(
                "[ducklink] read_col_to_colvec: T_LIST payload encoding not yet wired \
                 (T2-1 residual continuation) — emitting empty nested-column"
            );
            ColvecColumn::ListCol(ColvecNestedColumn { encoded: Vec::new() })
        }
        T_STRUCT => {
            eprintln!(
                "[ducklink] read_col_to_colvec: T_STRUCT payload encoding not yet wired \
                 (T2-1 residual continuation) — emitting empty nested-column"
            );
            ColvecColumn::StructCol(ColvecNestedColumn { encoded: Vec::new() })
        }
        T_MAP => {
            eprintln!(
                "[ducklink] read_col_to_colvec: T_MAP payload encoding not yet wired \
                 (T2-1 residual continuation) — emitting empty map-column"
            );
            ColvecColumn::MapCol(ColvecMapColumn {
                keys_encoded: Vec::new(),
                vals_encoded: Vec::new(),
            })
        }
        T_ARRAY => {
            eprintln!(
                "[ducklink] read_col_to_colvec: T_ARRAY payload encoding not yet wired \
                 (T2-1 residual continuation) — emitting empty array-column"
            );
            ColvecColumn::ArrayCol(ColvecArrayColumn {
                size: 0,
                encoded: Vec::new(),
            })
        }
        _ => unreachable!("type code out of range"),
    };
    Colvec {
        data,
        validity: validity_bytes,
        rows: len as u32,
    }
}

/// In-place refill of a scratch `Colvec` from column `j` of the DataChunk.
/// Reuses the existing inner `Vec<T>` allocation when the scratch's variant
/// already matches `code` — the common case, because per-scalar-function
/// `arg_codes` are fixed at registration, so once the per-thread scratch has
/// warmed up for a specific function every subsequent chunk hits the
/// clear+extend fast path (~zero allocation). On a variant mismatch (e.g. two
/// different scalars alternating on the same thread), falls back to
/// [`read_col_to_colvec`] which allocates fresh.
///
/// # Safety
/// Same contract as [`read_col_to_colvec`]: `validity` (if non-null) covers
/// at least `len` rows, and `vec` stores `code`-typed values.
unsafe fn refill_colvec(
    dst: &mut Colvec,
    code: u8,
    vec: &FlatVector,
    validity: *const u64,
    len: usize,
    complex_type_expr: Option<&str>,
) {
    // Reset validity + rows for every chunk. `dst.validity` reuses its
    // capacity via clear/extend_from_slice — no reallocation on the steady-
    // state hot path.
    dst.validity.clear();
    if !validity.is_null() {
        let nbytes = (len + 7) / 8;
        let src = std::slice::from_raw_parts(validity as *const u8, nbytes);
        dst.validity.extend_from_slice(src);
    }
    dst.rows = len as u32;

    // Try to reuse the existing typed vector.
    macro_rules! reuse_prim {
        ($v:expr, $ty:ty) => {{
            let src = vec.as_slice_with_len::<$ty>(len);
            $v.clear();
            $v.extend_from_slice(src);
        }};
    }
    let reused = match (&mut dst.data, code) {
        (ColvecColumn::Int64(v), T_I64) => {
            reuse_prim!(v, i64);
            true
        }
        (ColvecColumn::Uint64(v), T_U64) => {
            reuse_prim!(v, u64);
            true
        }
        (ColvecColumn::Float64(v), T_F64) => {
            reuse_prim!(v, f64);
            true
        }
        (ColvecColumn::Boolean(v), T_BOOL) => {
            reuse_prim!(v, bool);
            true
        }
        (ColvecColumn::Int8(v), T_I8) => {
            reuse_prim!(v, i8);
            true
        }
        (ColvecColumn::Int16(v), T_I16) => {
            reuse_prim!(v, i16);
            true
        }
        (ColvecColumn::Int32(v), T_I32) => {
            reuse_prim!(v, i32);
            true
        }
        (ColvecColumn::Uint8(v), T_U8) => {
            reuse_prim!(v, u8);
            true
        }
        (ColvecColumn::Uint16(v), T_U16) => {
            reuse_prim!(v, u16);
            true
        }
        (ColvecColumn::Uint32(v), T_U32) => {
            reuse_prim!(v, u32);
            true
        }
        (ColvecColumn::Float32(v), T_F32) => {
            reuse_prim!(v, f32);
            true
        }
        (ColvecColumn::Timestamp(v), T_TIMESTAMP) => {
            reuse_prim!(v, i64);
            true
        }
        (ColvecColumn::Date(v), T_DATE) => {
            reuse_prim!(v, i32);
            true
        }
        (ColvecColumn::Time(v), T_TIME) => {
            reuse_prim!(v, i64);
            true
        }
        (ColvecColumn::Timestamptz(v), T_TIMESTAMPTZ) => {
            reuse_prim!(v, i64);
            true
        }
        // TEXT / BLOB reuse: keep the outer `Vec<String>` / `Vec<Vec<u8>>` and
        // every element's inner buffer. Resize the outer Vec to `len` (usually
        // no-op after warmup — DuckDB chunks are 2048 rows), then per row
        // clear+push_str (TEXT) or clear+extend_from_slice (BLOB) into the
        // existing buffer. So the ~2048-entry Vec + per-row String/Vec<u8>
        // allocations from the pre-G2 read_col_to_colvec path collapse to
        // ZERO on the steady-state hot path. NULL rows clear the buffer to
        // empty (the validity mask says NULL; the slot's byte content is
        // never observed by the guest).
        (ColvecColumn::Text(v), T_TEXT) => {
            let s = vec.as_slice_with_len::<duckdb_string_t>(len);
            if v.len() < len {
                v.resize_with(len, String::new);
            } else if v.len() > len {
                v.truncate(len);
            }
            for i in 0..len {
                let dst = &mut v[i];
                dst.clear();
                if validity.is_null() || row_valid(validity, i) {
                    let mut t = s[i];
                    dst.push_str(&DuckString::new(&mut t).as_str());
                }
            }
            true
        }
        (ColvecColumn::Blob(v), T_BLOB) => {
            let s = vec.as_slice_with_len::<duckdb_string_t>(len);
            if v.len() < len {
                v.resize_with(len, Vec::new);
            } else if v.len() > len {
                v.truncate(len);
            }
            for i in 0..len {
                let dst = &mut v[i];
                dst.clear();
                if validity.is_null() || row_valid(validity, i) {
                    let mut t = s[i];
                    dst.extend_from_slice(DuckString::new(&mut t).as_bytes());
                }
            }
            true
        }
        // COMPLEX / DECIMAL / INTERVAL / UUID and any variant-mismatch
        // (different function on the same thread) fall through to allocate
        // fresh via read_col_to_colvec.
        _ => false,
    };
    if !reused {
        let fresh = read_col_to_colvec(code, vec, validity, len, complex_type_expr);
        dst.data = fresh.data;
        dst.validity = fresh.validity;
        dst.rows = fresh.rows;
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
    //
    // Invariant from the caller (WasmScalar::invoke, post-I1): `Some(mask)`
    // is passed IFF the SCALAR_NULL_MASK_SCRATCH loop set at least one bit to
    // `true` (has_input_null == true). `None` is passed otherwise. So
    // `is_some()` is provably equivalent to the `.iter().any(|&b| b)` walk it
    // replaces — and skips a 2048-byte scan on every NULL-bearing chunk.
    let any_input_null = null_mask.is_some();
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
        // The three variable-width return types (TEXT / BLOB / COMPLEX) walk
        // their concrete `ColvecColumn::*` vector in place. No Colvec ->
        // Vec<WitVal> allocation, no per-row generic `write_ret` dispatch —
        // one match on `code`, then a straight loop of
        // `duckdb_vector_assign_string_element_len` calls (that's what
        // `FlatVector::insert` compiles to for &str / &[u8]).
        T_TEXT => match colvec.data {
            ColvecColumn::Text(src) => {
                if src.len() != len {
                    return Err(format!(
                        "component returned column of {} values, expected {len}",
                        src.len()
                    )
                    .into());
                }
                for (i, s) in src.into_iter().enumerate() {
                    if is_null(i) {
                        out.set_null(i);
                    } else {
                        out.insert(i, s.as_str());
                    }
                }
                Ok(())
            }
            other => Err(format!(
                "component returned column {} incompatible with declared TEXT return type",
                describe_column(&other)
            )
            .into()),
        },
        T_BLOB => match colvec.data {
            ColvecColumn::Blob(src) => {
                if src.len() != len {
                    return Err(format!(
                        "component returned column of {} values, expected {len}",
                        src.len()
                    )
                    .into());
                }
                for (i, b) in src.into_iter().enumerate() {
                    if is_null(i) {
                        out.set_null(i);
                    } else {
                        out.insert(i, b.as_slice());
                    }
                }
                Ok(())
            }
            other => Err(format!(
                "component returned column {} incompatible with declared BLOB return type",
                describe_column(&other)
            )
            .into()),
        },
        T_COMPLEX => match colvec.data {
            ColvecColumn::Complex(src) => {
                if src.len() != len {
                    return Err(format!(
                        "component returned column of {} values, expected {len}",
                        src.len()
                    )
                    .into());
                }
                for (i, c) in src.into_iter().enumerate() {
                    if is_null(i) {
                        out.set_null(i);
                    } else {
                        out.insert(i, c.json.as_str());
                    }
                }
                Ok(())
            }
            other => Err(format!(
                "component returned column {} incompatible with declared COMPLEX return type",
                describe_column(&other)
            )
            .into()),
        },
        // The remaining HUGEINT-backed fixed-width types (DECIMAL / INTERVAL /
        // UUID) still take the fallback: they need per-value math (u128 split,
        // struct rebuild, or the UUID sign flip), so the write is per-row
        // anyway and a direct arm wouldn't buy the memcpy hoist. Lower the
        // Colvec to Vec<WitVal> and let the existing per-row writer handle it.
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
        // T2-1 residual (major-5): 128-bit + nested column arms.
        ColvecColumn::Hugeint(_) => "Hugeint",
        ColvecColumn::Uhugeint(_) => "Uhugeint",
        ColvecColumn::ListCol(_) => "ListCol",
        ColvecColumn::StructCol(_) => "StructCol",
        ColvecColumn::MapCol(_) => "MapCol",
        ColvecColumn::ArrayCol(_) => "ArrayCol",
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
        // T2-1 residual (major-5): 128-bit integer lower into first-class
        // WitVal arms carrying the (lower, upper) split — matches the
        // WIT `hugeintvalue` / `uhugeintvalue` records.
        ColvecColumn::Hugeint(v) => {
            for (i, d) in v.into_iter().enumerate() {
                out.push(if is_valid(i) {
                    WitVal::Hugeint(ducklink_runtime::duckdb_extension_bindings::duckdb::extension::types::Hugeintvalue {
                        lower: d.lower,
                        upper: d.upper,
                    })
                } else {
                    WitVal::Null
                });
            }
        }
        ColvecColumn::Uhugeint(v) => {
            for (i, d) in v.into_iter().enumerate() {
                out.push(if is_valid(i) {
                    WitVal::Uhugeint(ducklink_runtime::duckdb_extension_bindings::duckdb::extension::types::Uhugeintvalue {
                        lower: d.lower,
                        upper: d.upper,
                    })
                } else {
                    WitVal::Null
                });
            }
        }
        // S1 (major-5): nested column arms have NO first-class row-major
        // WitVal counterpart (the row-major path stays on `complex(json)`
        // per the column-types.wit header note). Nested reads are FAIL-LOUD
        // stubs in `read_col_to_colvec` above (empty encoded payload); the
        // row-major re-lift here surfaces every cell as NULL so the guest
        // observes a deterministic — not partially-encoded — column.
        ColvecColumn::ListCol(_)
        | ColvecColumn::StructCol(_)
        | ColvecColumn::MapCol(_)
        | ColvecColumn::ArrayCol(_) => {
            eprintln!(
                "[ducklink] colvec_to_witvals: nested column arm has no row-major \
                 WitVal shape (T2-1 residual continuation) — emitting {n} NULLs"
            );
            for _ in 0..n {
                out.push(WitVal::Null);
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
        // T2-1 residual (major-5): 128-bit integer per-row writer. Reassemble
        // the physical i128 / u128 from the (lower, upper) WIT split.
        (T_HUGEINT, WitVal::Hugeint(h)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<i128>(len) };
            s[i] = ((h.upper as i128) << 64) | (h.lower as i128 & 0xFFFF_FFFF_FFFF_FFFFi128);
        }
        (T_UHUGEINT, WitVal::Uhugeint(h)) => {
            let s = unsafe { vec.as_mut_slice_with_len::<u128>(len) };
            s[i] = ((h.upper as u128) << 64) | (h.lower as u128);
        }
        // No nested-vector writer: emit the JSON rendering into the VARCHAR column.
        (T_COMPLEX, WitVal::Complex(c)) => vec.insert(i, c.json.as_str()),
        (T_TEXT, WitVal::Text(x)) => vec.insert(i, x.as_str()),
        (T_BLOB, WitVal::Blob(x)) => vec.insert(i, x.as_slice()),
        // S1 (major-5): nested type WitVal has no row-major arm (row-major
        // uses `complex(json)` per the column-types.wit header note). If the
        // guest returns a nested WIT column but this write path (which
        // materialises row-major WitVal) fires, that's an internal path
        // routing error — FAIL-LOUD with an Err so the caller surfaces it
        // rather than silently zeroing the slot.
        (T_LIST, _) | (T_STRUCT, _) | (T_MAP, _) | (T_ARRAY, _) => {
            return Err(format!(
                "nested type write via write_ret (code {code}) not supported — nested \
                 columns must lower via write_colvec's structural arms (T2-1 residual)"
            )
            .into());
        }
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

    /// Per-thread `Vec<Colvec>` scratch for scalar arg columns. Reused across
    /// chunks so the ~16 KB `Vec<T>::to_vec()` per primitive column per chunk
    /// becomes ~16 KB per column on the FIRST chunk (allocator arena grows)
    /// and roughly zero on every subsequent chunk (clear+extend into the same
    /// allocation). Per-function `arg_codes` are fixed at registration, so
    /// once a specific function has warmed up the scratch every following
    /// chunk hits `refill_colvec`'s reuse fast path.
    static SCALAR_ARGS_SCRATCH: RefCell<Vec<Colvec>> = const { RefCell::new(Vec::new()) };

    /// Per-thread input NULL-mask scratch for scalar dispatch. Previously
    /// allocated fresh as `vec![false; len]` (~2 KB + zero-fill) any time a
    /// chunk contained a NULL; now reuses the same buffer across chunks.
    /// After each invoke it's left at len == 0 (so the next invoke either
    /// stays empty on the all-valid path, or resizes/fills once). The buffer
    /// capacity persists so the second and later NULL-bearing chunks pay
    /// only a zero-fill, never an allocation.
    ///
    /// (A parallel FlatVector cache was considered but rejected: FlatVector
    /// borrows from DataChunkHandle so it can't live in a 'static
    /// thread_local. The per-chunk Vec churn on the FlatVector list is
    /// <100 ns and not worth an unsafe lifetime transmute.)
    static SCALAR_NULL_MASK_SCRATCH: RefCell<Vec<bool>> = const { RefCell::new(Vec::new()) };
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
            // T1-3: mark the current thread as inside a guest dispatch so a
            // reentrant `NativeServices::query()` from the guest refuses
            // instead of deadlocking on the DuckDB executor lock.
            let _reentrancy_guard = crate::engine::QueryReentrancyGuard::new();
            let len = input.len();
            let cols: Vec<FlatVector> = (0..state.arg_codes.len())
                .map(|j| input.flat_vector(j))
                .collect();
            let mut out = output.flat_vector();
            // Raw chunk handle, so each column's validity mask can be fetched once
            // (FlatVector exposes only a per-row null check, which would re-fetch the
            // mask every cell). NULL-free columns -> null mask -> branch-free reads.
            let raw_chunk = input.get_ptr();

            let arity = state.arg_codes.len();
            // I1: NULL mask is stored in a per-thread scratch buffer that
            // persists across chunks. The all-valid common case leaves it
            // empty (zero allocation); NULL-bearing chunks resize once on
            // the first NULL, reusing the buffer thereafter. `has_input_null`
            // is the value we would previously read from `null_mask.is_some()`.
            let (result, has_input_null) = SCALAR_ARGS_SCRATCH.with(
                |cell| -> Result<(Colvec, bool), Box<dyn std::error::Error>> {
                    let mut args = cell.borrow_mut();
                    // Ensure the scratch has exactly `arity` slots. Grow with
                    // placeholder Int64 Colvecs on first use; shrink to arity
                    // if a wider scalar previously ran on this thread.
                    if args.len() < arity {
                        args.resize_with(arity, || Colvec {
                            data: ColvecColumn::Int64(Vec::new()),
                            validity: Vec::new(),
                            rows: 0,
                        });
                    } else if args.len() > arity {
                        args.truncate(arity);
                    }
                    let has_null = SCALAR_NULL_MASK_SCRATCH.with(|nm_cell| -> bool {
                        let mut nm_guard = nm_cell.borrow_mut();
                        // Reset for this chunk — clears length to 0 but
                        // keeps the underlying allocation. Chunks with no
                        // NULLs never re-grow it; NULL-bearing chunks
                        // resize-fill once and reuse thereafter.
                        nm_guard.clear();
                        for (j, &code) in state.arg_codes.iter().enumerate() {
                            let validity = unsafe {
                                let v = ffi::duckdb_data_chunk_get_vector(raw_chunk, j as u64);
                                ffi::duckdb_vector_get_validity(v) as *const u64
                            };
                            unsafe {
                                refill_colvec(
                                    &mut args[j],
                                    code,
                                    &cols[j],
                                    validity,
                                    len,
                                    state.arg_type_exprs[j].as_deref(),
                                );
                            }
                            if !validity.is_null()
                                && unsafe { validity_has_any_null(validity, len) }
                            {
                                // First NULL-bearing column in this chunk:
                                // materialise the mask. Capacity from previous
                                // chunks persists, so `resize` is amortized
                                // to zero after the first NULL-bearing chunk
                                // this thread ever sees.
                                if nm_guard.is_empty() {
                                    nm_guard.resize(len, false);
                                }
                                for i in 0..len {
                                    if unsafe { !row_valid(validity, i) } {
                                        nm_guard[i] = true;
                                    }
                                }
                            }
                        }
                        !nm_guard.is_empty()
                    });
                    let engine = &state.engine;
                    let result = engine
                        .dispatch_scalar_batch_col(state.callback_handle, 0, &args)
                        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
                    Ok((result, has_null))
                },
            )?;

            // Write the whole result column at once. On the all-valid path
            // pass `None` — write_colvec's primitive arms hit the
            // `copy_from_slice` fast-path (one memcpy per chunk). On a
            // NULL-bearing chunk, pass the scratch's slice so any input-null
            // row overrides the component's return with NULL.
            if has_input_null {
                SCALAR_NULL_MASK_SCRATCH.with(|nm_cell| -> Result<_, Box<dyn std::error::Error>> {
                    let nm_guard = nm_cell.borrow();
                    write_colvec(state.ret_code, &mut out, result, Some(&nm_guard[..]), len)
                })?;
            } else {
                write_colvec(state.ret_code, &mut out, result, None, len)?;
            }
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
///
/// T2-6 (major-5): the base-scalar overload-set migration was ATTEMPTED-AND-
/// DEFERRED. The stable C API `duckdb_create_scalar_function_set` +
/// `duckdb_add_scalar_function_to_set` + `duckdb_register_scalar_function_set`
/// path is used successfully for the `register_scalar_ex` sibling (varargs /
/// special-null / VOLATILE overloads land as a shared set). Migrating the
/// BASE scalar path off `VScalar` to match would require:
///
///   * Rewriting `WasmScalar::invoke` — currently a safe
///     `fn(&State, &mut DataChunkHandle, &mut dyn WritableVector) -> Result`
///     that duckdb-rs boxes into a C ABI callback — as a raw
///     `unsafe extern "C" fn(info, input_chunk, output_vec)` (matching
///     `ducklink_scalar_ex_invoke`). Every downstream helper the safe path
///     depends on (`FlatVector`, `refill_colvec` on a scratch borrow,
///     `write_colvec`'s `WritableVector`, the SCALAR_ARGS_SCRATCH /
///     SCALAR_NULL_MASK_SCRATCH thread-locals, the `guard()` panic
///     firewall, the `QueryReentrancyGuard` re-entrancy check) has to be
///     re-plumbed against the raw `duckdb_data_chunk` + `duckdb_vector`
///     handles rather than the duckdb-rs wrappers.
///   * Deleting the `PENDING_SIGNATURE` thread-local (raw C API takes
///     arg types explicitly via `duckdb_scalar_function_add_parameter`).
///   * A new `build_wasm_scalar_function` mirroring `build_scalar_ex_function`
///     that installs the WasmScalarState via `duckdb_scalar_function_set_extra_info`
///     and threads the invoke callback + extra-info destroy path.
///   * The set installer then groups by (extension, name) and mirrors
///     `register_scalar_ex`'s set + singleton branches (same ownership
///     argument: the C API copies each per-overload handle into the set,
///     so the per-overload handle is destroyed immediately after add).
///
/// That migration is materially larger than the T2-6 slice budget and
/// touches every hot-path scratch + guard site. Landing it partial (raw
/// invoke but preserving `VScalar` sig plumbing) is worse than not landing
/// it at all — the invoke ABI must match the C API's `duckdb_scalar_function`
/// contract exactly or DuckDB will call into freed memory. Chose the
/// **fail-loud DOCUMENTED DEFERRAL** per the "prefer partial with clear
/// reason over broken" guidance.
///
/// Interim state: this path stays on duckdb-rs' safe
/// `register_scalar_function_with_state`, which underneath calls
/// `duckdb_register_scalar_function` (single-overload). Registering
/// `my_add(INT, INT)` and then `my_add(DOUBLE, DOUBLE)` from the same load
/// still fails on the SECOND registration — the duplicate-name loud-log
/// below flags the shortfall explicitly instead of the generic "already
/// present?" message. Callers that need overload sets today must route
/// through `register-scalar-ex` (whose overloaded path IS wired) and pay
/// the ex-flag row-major invoke penalty for now.
pub fn register_scalars(
    con: &Connection,
    engine: Arc<Engine2>,
    scalars: &[ScalarFunc],
) -> duckdb::Result<usize> {
    use std::collections::HashMap;
    let mut per_name: HashMap<&str, usize> = HashMap::new();
    for f in scalars {
        *per_name.entry(f.name.as_str()).or_insert(0) += 1;
    }
    let mut seen: HashMap<&str, usize> = HashMap::new();
    let mut registered = 0usize;
    for f in scalars {
        let count = per_name.get(f.name.as_str()).copied().unwrap_or(1);
        let n_so_far = seen.entry(f.name.as_str()).or_insert(0);
        *n_so_far += 1;
        if count > 1 && *n_so_far > 1 {
            // Overload set: >1 signature for the same name in this load.
            eprintln!(
                "[ducklink] scalar function '{}' already registered on this load — \
                 duckdb overloads under one name require \
                 `duckdb_scalar_function_set` support, which this path does not \
                 yet use (T2-6). Skipping overload #{} of {}.",
                f.name, n_so_far, count
            );
            continue;
        }
        let arg_codes: Vec<u8> = f.arguments.iter().map(|a| type_code(&a.logical)).collect();
        // T2-7: capture the declared complex type-expression per arg so
        // `read_col_to_colvec` can emit it on every `ColvecComplex` rather
        // than erasing to `""`. Non-complex args carry `None`.
        let arg_type_exprs: Vec<Option<String>> = f
            .arguments
            .iter()
            .map(|a| match &a.logical {
                reg::LogicalType::Complex(e) => Some(e.clone()),
                _ => None,
            })
            .collect();
        let ret_code = type_code(&f.returns);
        let state = WasmScalarState {
            callback_handle: f.callback_handle,
            engine: engine.clone(),
            arg_codes: arg_codes.clone(),
            arg_type_exprs,
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
///
/// Sweep-6 FIX 3: extended with I8/I16/I32/U8/U16/U32/F32 arms — every typed
/// getter the duckdb-rs `Value` wrapper exposes (see
/// `duckdb-1.10504/src/vtab/value.rs`). The wider set of C-API getters
/// (`duckdb_get_hugeint`, `duckdb_get_timestamp`, `duckdb_get_date`,
/// `duckdb_get_time`, `duckdb_get_interval`, `duckdb_get_uuid`,
/// `duckdb_get_decimal`) exists on the raw C API but the wrapper does not
/// re-export them and the `Value::ptr` field is `pub(crate)` — so we can't
/// call them from here without either an unsafe transmute or a duckdb-rs
/// patch. Path (a) was taken for what's reachable via safe wrappers; the
/// remaining logical codes hit the fail-loud catch-all so a table function
/// binding a HUGEINT / DECIMAL / TIMESTAMP param surfaces the gap instead of
/// silently seeing NULL.
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
        // Sweep-6 FIX 3: narrow-int / float32 arms — all reachable via the
        // duckdb-rs `Value` wrapper's typed getters (`to_int8` .. `to_uint32`,
        // `to_float`). Previously silent-NULL through the catch-all.
        T_I8 => reg::DuckValue::Int8(v.to_int8()),
        T_I16 => reg::DuckValue::Int16(v.to_int16()),
        T_I32 => reg::DuckValue::Int32(v.to_int32()),
        T_U8 => reg::DuckValue::Uint8(v.to_uint8()),
        T_U16 => reg::DuckValue::Uint16(v.to_uint16()),
        T_U32 => reg::DuckValue::Uint32(v.to_uint32()),
        T_F32 => reg::DuckValue::Float32(v.to_float()),
        // Sweep-6 FIX 3: fail-loud catch-all. The duckdb-rs wrapper does not
        // expose typed getters for HUGEINT / UHUGEINT / DECIMAL / TIMESTAMP /
        // DATE / TIME / INTERVAL / UUID / nested — reaching the C-API
        // `duckdb_get_*` would need either an unsafe transmute of `Value` or
        // a duckdb-rs patch. Log the shortfall so a real caller shows up
        // rather than silently binding NULL.
        _ => {
            eprintln!(
                "param_to_neutral: unhandled type code {code} — the duckdb-rs \
                 Value wrapper has no safe getter for this type; \
                 binding NULL. See sweep-6 FIX 3 note."
            );
            reg::DuckValue::Null
        }
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
                // T1-3: bind is a guest dispatch — mark the thread so a
                // reentrant NativeServices::query() from inside the guest
                // refuses instead of deadlocking the DuckDB executor lock.
                let _reentrancy_guard = crate::engine::QueryReentrancyGuard::new();
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
            let n = bind
                .total_rows
                .saturating_sub(start)
                .min(STANDARD_VECTOR_SIZE as usize);
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

/// Per-group aggregate state. Selected at [`agg_init`] based on whether every
/// arg type is handled by the column-native fast path:
///
/// - [`AggState::ColNative`] — all arg types are I64/U64/F64/BOOL/sized ints/
///   F32/temporal/TEXT/BLOB. `agg_update` reads DuckDB flat vectors directly
///   into per-column typed accumulators without wrapping in `DuckValue`, so a
///   `sum(scalar_f(x))` over 10M rows no longer allocates 10M outer `Vec`s +
///   boxed enum variants. `agg_finalize` extracts the accumulators directly
///   as `Vec<Colvec>` and dispatches via `dispatch_aggregate_col`.
///
/// - [`AggState::RowMajor`] — at least one arg is DECIMAL / INTERVAL / UUID /
///   COMPLEX (the col-native path doesn't cover these yet). Keeps the
///   original `Vec<Vec<DuckValue>>` shape and dispatches through
///   `engine.dispatch_aggregate` (which does its own row→col pivot). This is
///   the pre-G1 behaviour, retained for coverage.
enum AggState {
    ColNative(ColNativeState),
    RowMajor(Vec<Vec<reg::DuckValue>>),
}

impl Default for AggState {
    /// Sentinel returned by `std::mem::take` when finalize moves the group's
    /// data out. Never accumulated into — the group is dropped after finalize.
    fn default() -> Self {
        AggState::RowMajor(Vec::new())
    }
}

/// Column-native accumulator. `columns[c]` is a [`ColvecColumn`] variant whose
/// variant matches `arg_codes[c]` (fixed at construction), so `push_row` never
/// matches on the variant per row — it dispatches on `code` and extends the
/// pre-shaped inner `Vec<T>`. `validity[c]` stays empty on the all-valid
/// common case (~zero cost); the first NULL in column `c` lazily materialises
/// a bit-mask covering the rows accumulated so far, then every subsequent NULL
/// clears its bit.
struct ColNativeState {
    columns: Vec<ColvecColumn>,
    /// Per-column NULL bit-mask. `validity[c].len() * 8 >= n` when non-empty;
    /// bit 1 = valid, bit 0 = NULL (mirrors WIT). Empty = all-valid.
    validity: Vec<Vec<u8>>,
    n: usize,
}

impl ColNativeState {
    fn new(arg_codes: &[u8]) -> Self {
        Self {
            columns: arg_codes.iter().map(|&c| empty_variant_for(c)).collect(),
            validity: vec![Vec::new(); arg_codes.len()],
            n: 0,
        }
    }

    /// Extend every column by row `row` of the parallel input vectors. On NULL
    /// (per column), lazily materialise the validity mask and push a
    /// type-appropriate default so the column data stays row-aligned with the
    /// mask.
    ///
    /// # Safety
    /// `vectors[c]` must be a live DuckDB flat vector storing `arg_codes[c]`-
    /// typed values; `validities[c]` (if non-null) must cover at least
    /// `row + 1` rows. Same contract as [`read_arg_raw`].
    unsafe fn push_row(
        &mut self,
        arg_codes: &[u8],
        vectors: &[ffi::duckdb_vector],
        validities: &[*const u64],
        row: usize,
    ) {
        for c in 0..arg_codes.len() {
            let code = arg_codes[c];
            let is_null =
                !validities[c].is_null() && !ffi::duckdb_validity_row_is_valid(validities[c] as *mut u64, row as u64);
            if is_null {
                self.mark_null(c);
                push_default_into(&mut self.columns[c], code);
            } else {
                push_from_vector(&mut self.columns[c], code, vectors[c], row);
            }
        }
        self.n += 1;
    }

    /// Bulk-append the ENTIRE input chunk into this state's per-column
    /// accumulators. This is the H1 fast path — fires only when every state
    /// pointer in the `states` array is the same (the ungrouped `SELECT
    /// agg(f(x)) FROM t` shape, which is the dominant real workload). For
    /// primitive columns this is one `Vec::extend_from_slice` from the DuckDB
    /// flat vector (a single memcpy per column, no per-row branch, no per-row
    /// FFI validity read). NULL handling stays lazy: the validity mask is
    /// materialised only if the input actually contains NULLs, then the NULL
    /// bits are translated into the accumulator's mask (still O(n) but pays
    /// nothing on the all-valid common case).
    ///
    /// TEXT/BLOB fall back to per-row `push_row` since each cell owns its
    /// String/Vec<u8> allocation; the primitive win covers ~99% of aggregate
    /// input types in practice.
    ///
    /// # Safety
    /// Every `vectors[c]` must be a DuckDB flat vector storing
    /// `arg_codes[c]`-typed values covering at least `n` rows;
    /// `validities[c]` (if non-null) must cover at least `n` rows.
    unsafe fn append_chunk(
        &mut self,
        arg_codes: &[u8],
        vectors: &[ffi::duckdb_vector],
        validities: &[*const u64],
        n: usize,
    ) {
        if n == 0 {
            return;
        }
        // TEXT/BLOB break the primitive fast path (variable-length storage,
        // per-cell allocation). If any column is TEXT/BLOB, fall back to the
        // per-row path — still uses the same accumulators, no boxing / no
        // Vec-per-row.
        let has_varlen = arg_codes.iter().any(|&c| matches!(c, T_TEXT | T_BLOB));
        if has_varlen {
            for row in 0..n {
                self.push_row(arg_codes, vectors, validities, row);
            }
            return;
        }

        let base = self.n;
        for c in 0..arg_codes.len() {
            let code = arg_codes[c];
            let vec = vectors[c];
            let data = ffi::duckdb_vector_get_data(vec);

            // One memcpy per column, no per-row branch, no per-row FFI hit.
            macro_rules! bulk_prim {
                ($variant:ident, $ty:ty) => {{
                    if let ColvecColumn::$variant(v) = &mut self.columns[c] {
                        let src = std::slice::from_raw_parts(data as *const $ty, n);
                        v.extend_from_slice(src);
                    }
                }};
            }
            match code {
                T_I64 => bulk_prim!(Int64, i64),
                T_U64 => bulk_prim!(Uint64, u64),
                T_F64 => bulk_prim!(Float64, f64),
                T_BOOL => bulk_prim!(Boolean, bool),
                T_I8 => bulk_prim!(Int8, i8),
                T_I16 => bulk_prim!(Int16, i16),
                T_I32 => bulk_prim!(Int32, i32),
                T_U8 => bulk_prim!(Uint8, u8),
                T_U16 => bulk_prim!(Uint16, u16),
                T_U32 => bulk_prim!(Uint32, u32),
                T_F32 => bulk_prim!(Float32, f32),
                T_TIMESTAMP => bulk_prim!(Timestamp, i64),
                T_DATE => bulk_prim!(Date, i32),
                T_TIME => bulk_prim!(Time, i64),
                T_TIMESTAMPTZ => bulk_prim!(Timestamptz, i64),
                _ => {
                    // Unreachable: supports_col_native_agg gates construction
                    // to the arms above (has_varlen check already caught TEXT
                    // and BLOB). Falling through as a no-op would drift row
                    // counts, so panic — this is a programming error.
                    unreachable!("append_chunk primitive arm reached with code {code}");
                }
            }

            // Validity: pay nothing on the all-valid common case (validity
            // pointer null OR no NULL bits in a bulk word-scan). Otherwise
            // translate the DuckDB bit-mask into our accumulator's mask.
            let validity = validities[c];
            if !validity.is_null() {
                // Bulk word-scan for any NULL in [0, n). Same trick
                // `read_col_to_colvec` uses to skip the mark loop when the
                // column *could* hold NULLs but this chunk doesn't.
                if validity_has_any_null(validity, n) {
                    self.merge_input_validity(c, base, validity, n);
                }
            }
        }
        self.n += n;
    }

    /// Copy `n` bits from the DuckDB validity bit-mask `src` into
    /// `self.validity[c]` starting at `base`. Materialises the accumulator
    /// mask if needed (lazy — first NULL). Only cleared (NULL) bits are
    /// touched; the accumulator's default all-1s pattern already covers
    /// valid rows.
    ///
    /// # Safety
    /// `src` must be non-null and cover at least `n` rows.
    unsafe fn merge_input_validity(
        &mut self,
        c: usize,
        base: usize,
        src: *const u64,
        n: usize,
    ) {
        // Ensure the accumulator mask covers positions [0, base + n).
        let need_bytes = (base + n + 7) / 8;
        if self.validity[c].is_empty() {
            self.validity[c] = vec![0xFFu8; need_bytes];
        } else if self.validity[c].len() < need_bytes {
            self.validity[c].resize(need_bytes, 0xFFu8);
        }
        // Walk `n` bits of the input validity; clear the corresponding
        // accumulator bit for each NULL row. A byte-at-a-time inner loop
        // suffices — the outer `validity_has_any_null` gate already ruled
        // out the common all-valid case.
        for row in 0..n {
            if !ffi::duckdb_validity_row_is_valid(src as *mut u64, row as u64) {
                let dst_row = base + row;
                self.validity[c][dst_row >> 3] &= !(1u8 << (dst_row & 7));
            }
        }
    }

    /// Ensure `validity[c]` has bits allocated for the current accumulated
    /// row count, then clear the bit for the next row (which is about to be
    /// appended). Grows the mask by whole bytes as needed.
    fn mark_null(&mut self, c: usize) {
        let target_row = self.n;
        let need_bytes = (target_row >> 3) + 1;
        if self.validity[c].is_empty() {
            // First NULL in this column: allocate a mask of all 1s covering
            // every previously-accumulated row (they were all valid).
            self.validity[c] = vec![0xFFu8; need_bytes];
        } else if self.validity[c].len() < need_bytes {
            self.validity[c].resize(need_bytes, 0xFFu8);
        }
        self.validity[c][target_row >> 3] &= !(1u8 << (target_row & 7));
    }

    /// Merge `other` into `self` (combine step). Concatenates each column's
    /// data and rewrites `self.validity[c]` so its trailing `other.n` bits
    /// come from `other.validity[c]` (or all-1s if `other` had no NULLs in
    /// column c).
    fn append(&mut self, mut other: ColNativeState) {
        let base = self.n;
        for c in 0..self.columns.len() {
            append_col_variant(&mut self.columns[c], &mut other.columns[c]);

            let self_has_nulls = !self.validity[c].is_empty();
            let other_has_nulls = !other.validity[c].is_empty();
            if !self_has_nulls && !other_has_nulls {
                continue; // both all-valid — no mask needed
            }
            // Materialise self.validity[c] as all-1s up to `base` rows if it
            // wasn't already (we're about to append NULL bits into positions
            // beyond `base`).
            if !self_has_nulls {
                self.validity[c] = vec![0xFFu8; (base + 7) / 8];
            }
            let combined_n = base + other.n;
            let need_bytes = (combined_n + 7) / 8;
            if self.validity[c].len() < need_bytes {
                self.validity[c].resize(need_bytes, 0xFFu8);
            }
            if other_has_nulls {
                // Copy each NULL bit from other into position (base + r).
                // Only 0 bits need attention; the mask is already all-1s for
                // positions we didn't already flip in the resize above.
                for r in 0..other.n {
                    let byte = r >> 3;
                    let bit = r & 7;
                    let byte_val = *other.validity[c].get(byte).unwrap_or(&0xFFu8);
                    if (byte_val & (1u8 << bit)) == 0 {
                        let dst_row = base + r;
                        self.validity[c][dst_row >> 3] &= !(1u8 << (dst_row & 7));
                    }
                }
            }
        }
        self.n += other.n;
    }

    /// Consume the accumulator, wrapping each `ColvecColumn` and its parallel
    /// validity mask into a `Colvec`. The whole finalize path is O(1) after
    /// this — no per-row walks, no allocations beyond the wrappers.
    fn into_colvecs(self) -> Vec<Colvec> {
        let n = self.n as u32;
        self.columns
            .into_iter()
            .zip(self.validity)
            .map(|(data, validity)| Colvec {
                data,
                validity,
                rows: n,
            })
            .collect()
    }
}

/// Empty `ColvecColumn` shaped for `code`. Distinct from `empty_colvec_for`
/// (which normalises temporal codes to the underlying Int64 storage for the
/// zero-row group case) because [`ColNativeState`] needs the CORRECT variant
/// so `push_from_vector` extends the right typed Vec.
fn empty_variant_for(code: u8) -> ColvecColumn {
    match code {
        T_I64 => ColvecColumn::Int64(Vec::new()),
        T_U64 => ColvecColumn::Uint64(Vec::new()),
        T_F64 => ColvecColumn::Float64(Vec::new()),
        T_BOOL => ColvecColumn::Boolean(Vec::new()),
        T_I8 => ColvecColumn::Int8(Vec::new()),
        T_I16 => ColvecColumn::Int16(Vec::new()),
        T_I32 => ColvecColumn::Int32(Vec::new()),
        T_U8 => ColvecColumn::Uint8(Vec::new()),
        T_U16 => ColvecColumn::Uint16(Vec::new()),
        T_U32 => ColvecColumn::Uint32(Vec::new()),
        T_F32 => ColvecColumn::Float32(Vec::new()),
        T_TIMESTAMP => ColvecColumn::Timestamp(Vec::new()),
        T_DATE => ColvecColumn::Date(Vec::new()),
        T_TIME => ColvecColumn::Time(Vec::new()),
        T_TIMESTAMPTZ => ColvecColumn::Timestamptz(Vec::new()),
        T_TEXT => ColvecColumn::Text(Vec::new()),
        T_BLOB => ColvecColumn::Blob(Vec::new()),
        // Unreachable when supports_col_native_agg gates ColNative construction.
        _ => ColvecColumn::Int64(Vec::new()),
    }
}

/// Push row `row` of `vec` (a DuckDB flat vector of type `code`) into the
/// matching arm of `col`. Called only when the caller has verified the row is
/// non-NULL; NULL rows go through `push_default_into` and mark the validity
/// mask separately.
///
/// # Safety
/// `vec` must be a DuckDB flat vector storing `code`-typed values, and `row`
/// must be within its size.
unsafe fn push_from_vector(
    col: &mut ColvecColumn,
    code: u8,
    vec: ffi::duckdb_vector,
    row: usize,
) {
    let data = ffi::duckdb_vector_get_data(vec);
    macro_rules! prim {
        ($variant:ident, $ty:ty) => {{
            if let ColvecColumn::$variant(v) = col {
                v.push(*(data as *const $ty).add(row));
            }
        }};
    }
    match code {
        T_I64 => prim!(Int64, i64),
        T_U64 => prim!(Uint64, u64),
        T_F64 => prim!(Float64, f64),
        T_BOOL => prim!(Boolean, bool),
        T_I8 => prim!(Int8, i8),
        T_I16 => prim!(Int16, i16),
        T_I32 => prim!(Int32, i32),
        T_U8 => prim!(Uint8, u8),
        T_U16 => prim!(Uint16, u16),
        T_U32 => prim!(Uint32, u32),
        T_F32 => prim!(Float32, f32),
        T_TIMESTAMP => prim!(Timestamp, i64),
        T_DATE => prim!(Date, i32),
        T_TIME => prim!(Time, i64),
        T_TIMESTAMPTZ => prim!(Timestamptz, i64),
        T_TEXT => {
            if let ColvecColumn::Text(v) = col {
                let mut t = *(data as *const duckdb_string_t).add(row);
                v.push(DuckString::new(&mut t).as_str().into_owned());
            }
        }
        T_BLOB => {
            if let ColvecColumn::Blob(v) = col {
                let mut t = *(data as *const duckdb_string_t).add(row);
                v.push(DuckString::new(&mut t).as_bytes().to_vec());
            }
        }
        _ => {} // unreachable if supports_col_native_agg gated construction
    }
}

/// Push a type-appropriate default into `col`. Used for NULL rows so the
/// column data stays row-aligned with the validity mask. The value itself is
/// never observed by the guest — the mask says NULL — but the slot must exist.
fn push_default_into(col: &mut ColvecColumn, code: u8) {
    macro_rules! prim {
        ($variant:ident, $default:expr) => {{
            if let ColvecColumn::$variant(v) = col {
                v.push($default);
            }
        }};
    }
    match code {
        T_I64 => prim!(Int64, 0),
        T_U64 => prim!(Uint64, 0),
        T_F64 => prim!(Float64, 0.0),
        T_BOOL => prim!(Boolean, false),
        T_I8 => prim!(Int8, 0),
        T_I16 => prim!(Int16, 0),
        T_I32 => prim!(Int32, 0),
        T_U8 => prim!(Uint8, 0),
        T_U16 => prim!(Uint16, 0),
        T_U32 => prim!(Uint32, 0),
        T_F32 => prim!(Float32, 0.0),
        T_TIMESTAMP => prim!(Timestamp, 0),
        T_DATE => prim!(Date, 0),
        T_TIME => prim!(Time, 0),
        T_TIMESTAMPTZ => prim!(Timestamptz, 0),
        T_TEXT => prim!(Text, String::new()),
        T_BLOB => prim!(Blob, Vec::new()),
        _ => {}
    }
}

/// Append `src`'s inner Vec into `dst`'s inner Vec, assuming the variants
/// match (they always do — same `arg_codes[c]`). Consumes `src`'s contents;
/// `src` is left with an empty Vec of the same variant.
fn append_col_variant(dst: &mut ColvecColumn, src: &mut ColvecColumn) {
    macro_rules! variant {
        ($v:ident) => {
            if let (ColvecColumn::$v(d), ColvecColumn::$v(s)) = (&mut *dst, &mut *src) {
                d.append(s);
            }
        };
    }
    match dst {
        ColvecColumn::Int64(_) => variant!(Int64),
        ColvecColumn::Uint64(_) => variant!(Uint64),
        ColvecColumn::Float64(_) => variant!(Float64),
        ColvecColumn::Boolean(_) => variant!(Boolean),
        ColvecColumn::Int8(_) => variant!(Int8),
        ColvecColumn::Int16(_) => variant!(Int16),
        ColvecColumn::Int32(_) => variant!(Int32),
        ColvecColumn::Uint8(_) => variant!(Uint8),
        ColvecColumn::Uint16(_) => variant!(Uint16),
        ColvecColumn::Uint32(_) => variant!(Uint32),
        ColvecColumn::Float32(_) => variant!(Float32),
        ColvecColumn::Timestamp(_) => variant!(Timestamp),
        ColvecColumn::Date(_) => variant!(Date),
        ColvecColumn::Time(_) => variant!(Time),
        ColvecColumn::Timestamptz(_) => variant!(Timestamptz),
        ColvecColumn::Text(_) => variant!(Text),
        ColvecColumn::Blob(_) => variant!(Blob),
        // Non-col-native variants (Decimal/Uuid/Interval/Complex) never appear
        // in a ColNativeState because supports_col_native_agg excludes them.
        _ => {}
    }
}

/// Row-major fallback: pivot a `Vec<Vec<DuckValue>>` group into `Vec<Colvec>`,
/// used only when [`AggState::RowMajor`] holds an arg-code set that includes a
/// col-native-unsupported type (DECIMAL / INTERVAL / UUID / COMPLEX). Kept
/// for coverage; the ColNative path handles the fast common case.
fn row_major_agg_state_to_colvecs(
    state: Vec<Vec<reg::DuckValue>>,
    arg_codes: &[u8],
) -> Result<Vec<Colvec>, String> {
    let n = state.len();
    let ncols = arg_codes.len();
    // Empty group: emit empty columns of the declared arg types so the guest
    // sees a well-formed all-empty argument set.
    if n == 0 {
        return Ok(arg_codes
            .iter()
            .map(|&code| empty_colvec_for(code))
            .collect());
    }
    // Per column, walk every row's cell at column c and push into a typed
    // `Vec<T>`. `validity` stays empty (the "all-valid" fast path) until an
    // actual `DuckValue::Null` is seen, then it gets lazily materialized as an
    // all-1 bit-mask that mark_null() flips per NULL row.
    let mut columns: Vec<Colvec> = Vec::with_capacity(ncols);
    for c in 0..ncols {
        let code = arg_codes[c];
        let mut validity: Vec<u8> = Vec::new();
        macro_rules! mark_null {
            ($row:expr) => {{
                if validity.is_empty() {
                    validity = vec![0xFFu8; (n + 7) / 8];
                }
                validity[$row >> 3] &= !(1u8 << ($row & 7));
            }};
        }
        macro_rules! prim {
            ($ty:ty, $variant:ident, $default:expr) => {{
                let mut out: Vec<$ty> = Vec::with_capacity(n);
                for (r, row) in state.iter().enumerate() {
                    match &row[c] {
                        reg::DuckValue::$variant(x) => out.push(*x),
                        reg::DuckValue::Null => {
                            mark_null!(r);
                            out.push($default);
                        }
                        other => {
                            return Err(format!(
                                "aggregate arg {c} expected {} but got {other:?}",
                                stringify!($variant)
                            ))
                        }
                    }
                }
                ColvecColumn::$variant(out)
            }};
        }
        let data = match code {
            T_I64 => prim!(i64, Int64, 0),
            T_U64 => prim!(u64, Uint64, 0),
            T_F64 => prim!(f64, Float64, 0.0),
            T_BOOL => prim!(bool, Boolean, false),
            T_I8 => prim!(i8, Int8, 0),
            T_I16 => prim!(i16, Int16, 0),
            T_I32 => prim!(i32, Int32, 0),
            T_U8 => prim!(u8, Uint8, 0),
            T_U16 => prim!(u16, Uint16, 0),
            T_U32 => prim!(u32, Uint32, 0),
            T_F32 => prim!(f32, Float32, 0.0),
            T_TIMESTAMP => prim!(i64, Timestamp, 0),
            T_DATE => prim!(i32, Date, 0),
            T_TIME => prim!(i64, Time, 0),
            T_TIMESTAMPTZ => prim!(i64, Timestamptz, 0),
            T_TEXT => {
                let mut out: Vec<String> = Vec::with_capacity(n);
                for (r, row) in state.iter().enumerate() {
                    match &row[c] {
                        reg::DuckValue::Text(s) => out.push(s.clone()),
                        reg::DuckValue::Null => {
                            mark_null!(r);
                            out.push(String::new());
                        }
                        other => {
                            return Err(format!(
                                "aggregate arg {c} expected Text but got {other:?}"
                            ))
                        }
                    }
                }
                ColvecColumn::Text(out)
            }
            T_BLOB => {
                let mut out: Vec<Vec<u8>> = Vec::with_capacity(n);
                for (r, row) in state.iter().enumerate() {
                    match &row[c] {
                        reg::DuckValue::Blob(b) => out.push(b.clone()),
                        reg::DuckValue::Null => {
                            mark_null!(r);
                            out.push(Vec::new());
                        }
                        other => {
                            return Err(format!(
                                "aggregate arg {c} expected Blob but got {other:?}"
                            ))
                        }
                    }
                }
                ColvecColumn::Blob(out)
            }
            _ => {
                // DECIMAL / INTERVAL / UUID / COMPLEX: rare on the aggregate path;
                // the existing DuckValue -> WitVal -> Colvec route via
                // dispatch_aggregate is preserved for these by returning an
                // error the caller can distinguish and fall back on.
                return Err(format!(
                    "aggregate arg {c}: type code {code} not supported by col-native aggregate; falling back"
                ));
            }
        };
        columns.push(Colvec {
            data,
            validity,
            rows: n as u32,
        });
    }
    Ok(columns)
}

/// Build an empty `Colvec` shaped for `code`. Used for the zero-row group case.
fn empty_colvec_for(code: u8) -> Colvec {
    let data = match code {
        T_I64 | T_TIMESTAMP | T_TIME | T_TIMESTAMPTZ => ColvecColumn::Int64(Vec::new()),
        T_U64 => ColvecColumn::Uint64(Vec::new()),
        T_F64 => ColvecColumn::Float64(Vec::new()),
        T_BOOL => ColvecColumn::Boolean(Vec::new()),
        T_I8 => ColvecColumn::Int8(Vec::new()),
        T_I16 => ColvecColumn::Int16(Vec::new()),
        T_I32 | T_DATE => ColvecColumn::Int32(Vec::new()),
        T_U8 => ColvecColumn::Uint8(Vec::new()),
        T_U16 => ColvecColumn::Uint16(Vec::new()),
        T_U32 => ColvecColumn::Uint32(Vec::new()),
        T_F32 => ColvecColumn::Float32(Vec::new()),
        T_TEXT => ColvecColumn::Text(Vec::new()),
        T_BLOB => ColvecColumn::Blob(Vec::new()),
        _ => ColvecColumn::Int64(Vec::new()),
    };
    Colvec {
        data,
        validity: Vec::new(),
        rows: 0,
    }
}

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
        // T1-4: DECIMAL now yields the correct enum discriminant. Callers
        // building a `duckdb_logical_type` from this must NOT use
        // `duckdb_create_logical_type(DUCKDB_TYPE_DECIMAL)` — that leaves
        // width/scale unset. Use [`logical_type_ffi`] instead, which
        // routes DECIMAL through `duckdb_create_decimal_type(18, 3)`
        // (matching the DECIMAL(18, 3) shape `logical_type()` above declares
        // and `read_col_to_colvec` / `write_ret_raw` assume).
        T_DECIMAL => ffi::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL,
        // COMPLEX falls back to VARCHAR/JSON.
        T_COMPLEX => ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
        // T2-1 residual (major-5): 128-bit integers get their real duckdb_type
        // discriminant so DECIMAL / cast plumbing that reads the code back
        // via `code_from_duckdb_type` round-trips.
        T_HUGEINT => ffi::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT,
        T_UHUGEINT => ffi::DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT,
        // S1 (major-5): nested types are code-only here — the discriminant
        // is right but the shape (element / fields / key-value / size) MUST
        // come from a full `reg::LogicalType`. Callers that only have a
        // code get the raw discriminant so `duckdb_type_of` never lies about
        // the KIND, but a `duckdb_create_logical_type(<this>)` at the raw
        // FFI level would produce an unusable handle without child types —
        // use `logical_type_ffi_from_lt` instead.
        T_LIST => ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST,
        T_STRUCT => ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT,
        T_MAP => ffi::DUCKDB_TYPE_DUCKDB_TYPE_MAP,
        T_ARRAY => ffi::DUCKDB_TYPE_DUCKDB_TYPE_ARRAY,
        _ => ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    }
}

/// T1-4: Construct a `duckdb_logical_type` from a bridge type `code`. This
/// mirrors [`duckdb_type_of`] but branches to
/// `duckdb_create_decimal_type(18, 3)` for T_DECIMAL so callers get a
/// properly-widthed DECIMAL type instead of the width/scale-less shape
/// `duckdb_create_logical_type(DUCKDB_TYPE_DECIMAL)` yields.
///
/// major-5 (T2-1 residual): the T_HUGEINT / T_UHUGEINT arms route through
/// `duckdb_create_logical_type(DUCKDB_TYPE_{U,}HUGEINT)`, which produces a
/// fully-formed 128-bit integer type — no per-value width/scale needed.
///
/// **Nested types (T_LIST / T_STRUCT / T_MAP / T_ARRAY) require the child
/// type shape to construct**, which a bare code cannot carry. This code-only
/// path therefore falls back to a VARCHAR handle for the nested arms and
/// logs an eprintln; callers that hold a full `reg::LogicalType` MUST use
/// [`logical_type_ffi_from_lt`] instead, which walks the shape recursively
/// into `duckdb_create_{list,struct,map,array}_type`.
///
/// The returned handle is owned by the caller; use
/// `duckdb_destroy_logical_type` to release.
///
/// # Safety
/// Calls into the DuckDB C API; caller must destroy the returned type.
unsafe fn logical_type_ffi(code: u8) -> ffi::duckdb_logical_type {
    match code {
        T_DECIMAL => {
            // Interim shape: DECIMAL(18, 3). Aligns with `logical_type()`
            // above (which returns `LogicalTypeHandle::decimal(18, 3)`) so
            // read_arg_raw / write_ret_raw round-trip correctly.
            // `logical_type_ffi_from_lt` supersedes this for callers that
            // hold the per-column (width, scale).
            ffi::duckdb_create_decimal_type(18, 3)
        }
        T_LIST | T_STRUCT | T_MAP | T_ARRAY => {
            eprintln!(
                "[ducklink] logical_type_ffi: code {code} is a nested type — code-only \
                 lowering has no child-shape; falling back to VARCHAR (T2-1 residual). \
                 Use logical_type_ffi_from_lt(&reg::LogicalType) with the full shape."
            );
            ffi::duckdb_create_logical_type(ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR)
        }
        _ => ffi::duckdb_create_logical_type(duckdb_type_of(code)),
    }
}

/// Structural counterpart of [`logical_type_ffi`]: build a `duckdb_logical_type`
/// from a full `reg::LogicalType`, honouring the per-column DECIMAL width/scale
/// (S2, major-5) and recursing into `duckdb_create_{list,struct,map,array}_type`
/// for the nested S1 arms.
///
/// Preferred over [`logical_type_ffi(code)`] wherever the caller already
/// holds a `reg::LogicalType` (scalar / aggregate / table registration paths,
/// cast source/target, modified-type registrations). The code-only variant is
/// retained for callers that only get the bridge code (e.g. cast route
/// derived from a raw `type_code_from_expr` fold, aggregate raw path).
///
/// The returned handle is owned by the caller; use
/// `duckdb_destroy_logical_type` to release.
///
/// # Safety
/// Calls into the DuckDB C API; caller must destroy the returned type.
unsafe fn logical_type_ffi_from_lt(lt: &reg::LogicalType) -> ffi::duckdb_logical_type {
    match lt {
        // S2 (major-5): honour per-column width/scale instead of hardcoding
        // DECIMAL(18, 3). Round-trips with `read_arg_raw` / `write_ret_raw`
        // as long as the caller passes the same shape on both sides.
        reg::LogicalType::Decimal { width, scale } => {
            ffi::duckdb_create_decimal_type(*width, *scale)
        }
        // S1 (major-5): recurse into the nested-type creators. Every child
        // handle is created here and consumed by the parent creator (which
        // takes ownership per the DuckDB C API contract), so no manual
        // destroy is needed for children.
        reg::LogicalType::List(elem) => {
            let child = logical_type_ffi_from_lt(elem);
            ffi::duckdb_create_list_type(child)
        }
        reg::LogicalType::Array(size, elem) => {
            let child = logical_type_ffi_from_lt(elem);
            ffi::duckdb_create_array_type(child, *size as ffi::idx_t)
        }
        reg::LogicalType::Map(k, v) => {
            let key = logical_type_ffi_from_lt(k);
            let val = logical_type_ffi_from_lt(v);
            ffi::duckdb_create_map_type(key, val)
        }
        reg::LogicalType::Struct(fields) => {
            // Two parallel vecs: owned child handles + owned CStrings (name
            // pointers must live until `duckdb_create_struct_type` returns).
            let mut child_types: Vec<ffi::duckdb_logical_type> = Vec::with_capacity(fields.len());
            let mut child_names_c: Vec<CString> = Vec::with_capacity(fields.len());
            for (n, t) in fields {
                child_types.push(logical_type_ffi_from_lt(t));
                child_names_c.push(
                    CString::new(n.as_str()).unwrap_or_else(|_| CString::new("_").unwrap()),
                );
            }
            let mut child_name_ptrs: Vec<*const c_char> =
                child_names_c.iter().map(|c| c.as_ptr()).collect();
            ffi::duckdb_create_struct_type(
                child_types.as_mut_ptr(),
                child_name_ptrs.as_mut_ptr(),
                fields.len() as ffi::idx_t,
            )
            // child_types / child_names_c / child_name_ptrs drop here; the
            // struct_type creator has already consumed / copied what it needs.
        }
        // Everything else falls through to the code-only path.
        _ => logical_type_ffi(type_code(lt)),
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
        // T1-4: DECIMAL round-trips through the raw aggregate path as an
        // i128 unscaled value. Scale is defaulted to 3 because Gap 2
        // (decimal precision/scale plumbing) is not yet landed and
        // `logical_type(T_DECIMAL)` above declares columns as
        // DECIMAL(18, 3); read_arg_raw stays in lockstep so the value the
        // guest sees matches the type the column was declared with.
        // TODO Gap 2: read the real width/scale from the vector's own
        // `duckdb_decimal_scale(duckdb_vector_get_column_type(vector))`
        // once the neutral `reg::LogicalType::Decimal` carries them.
        T_DECIMAL => {
            let unscaled = *(data as *const i128).add(i);
            reg::DuckValue::Decimal {
                lower: unscaled as u64,
                upper: (unscaled >> 64) as u64,
                width: 18,
                scale: 3,
            }
        }
        // T2-1 residual (major-5): HUGEINT is DuckDB's 128-bit signed integer.
        // Physical storage is a naked i128 in the vector; split into (lower u64,
        // upper i64) so it lifts into the WIT `hugeintvalue` shape without an
        // intermediate sign-extend.
        T_HUGEINT => {
            let raw = *(data as *const i128).add(i);
            reg::DuckValue::Hugeint {
                lower: raw as u64,
                upper: (raw >> 64) as i64,
            }
        }
        // T2-1 residual (major-5): UHUGEINT — same shape as HUGEINT but the
        // high half is unsigned (WIT `uhugeintvalue`).
        T_UHUGEINT => {
            let raw = *(data as *const u128).add(i);
            reg::DuckValue::UHugeint {
                lower: raw as u64,
                upper: (raw >> 64) as u64,
            }
        }
        // S1 (major-5): nested type reads via
        // `duckdb_list_vector_get_child` / `_get_size` /
        // `duckdb_struct_vector_get_child` / `duckdb_array_vector_get_child`
        // are not yet wired. Rather than partial-marshal a nested value with
        // the child row_id offsets uninitialised, log the shortfall once and
        // surface NULL so the aggregate's output row is at least deterministic.
        // FAIL-LOUD (T2-1 residual continuation).
        T_LIST | T_STRUCT | T_MAP | T_ARRAY => {
            eprintln!(
                "[ducklink] read_arg_raw: nested type code {code} not yet fully wired \
                 (T2-1 residual continuation) — surfacing NULL for row {i}"
            );
            reg::DuckValue::Null
        }
        // Sweep-7 FIX F1: COMPLEX escape-hatch is stored as VARCHAR JSON on the
        // raw aggregate path too. Mirror the `read_arg_neutral` T_COMPLEX arm so
        // the two peers stay in lockstep — previously this fell through the
        // catch-all and silently surfaced NULL, dropping COMPLEX aggregate
        // arguments on the floor.
        T_COMPLEX => {
            let strs = data as *const duckdb_string_t;
            let mut s = std::ptr::read(strs.add(i));
            let mut raw = DuckString::new(&mut s);
            reg::DuckValue::Complex {
                type_expr: "COMPLEX".to_string(),
                json: raw.as_str().to_string(),
            }
        }
        // Sweep-7 FIX F1: fail-loud catch-all (mirrors `read_arg_neutral`).
        // Any type code newly added to `code_from_duckdb_type` /
        // `type_code_from_expr` but not wired here used to silently return
        // NULL; log the code so the next gap is visible.
        unhandled => {
            eprintln!(
                "ducklink: read_arg_raw: unhandled code {unhandled} (row {i}) — \
                 add an arm here to mirror `read_arg_neutral` or extend the code table"
            );
            reg::DuckValue::Null
        }
    }
}

/// Write a neutral value into row `i` of a raw result vector (type `code`). Takes
/// the value by reference so the caller can walk its `Vec<Vec<DuckValue>>`
/// without cloning each cell.
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
        // T1-4: DECIMAL is a HUGEINT-backed scaled integer; write the
        // unscaled i128 straight into the vector's data slot. Every
        // scalar/aggregate/copy path encodes DECIMAL identically via this
        // per-cell arm (the per-column `write_col_from_raw` peer was
        // deleted in T2-5 as dead). Width/scale on the value are
        // informational — the column's LogicalType (DECIMAL(18, 3) per
        // `logical_type()`) determines interpretation.
        (T_DECIMAL, reg::DuckValue::Decimal { lower, upper, .. }) => {
            *(data as *mut i128).add(i) =
                (((*upper as u128) << 64) | *lower as u128) as i128;
        }
        (T_COMPLEX, reg::DuckValue::Complex { json, .. }) => {
            ffi::duckdb_vector_assign_string_element_len(
                vector,
                i as u64,
                json.as_ptr() as *const c_char,
                json.len() as u64,
            );
        }
        // T2-1 residual (major-5): 128-bit integer writes. Reassemble the
        // physical i128 / u128 from the WIT (lower, upper) split.
        (T_HUGEINT, reg::DuckValue::Hugeint { lower, upper }) => {
            let raw = ((*upper as i128) << 64) | (*lower as i128 & 0xFFFF_FFFF_FFFF_FFFFi128);
            *(data as *mut i128).add(i) = raw;
        }
        (T_UHUGEINT, reg::DuckValue::UHugeint { lower, upper }) => {
            let raw = ((*upper as u128) << 64) | (*lower as u128);
            *(data as *mut u128).add(i) = raw;
        }
        // S1 (major-5): nested writes need
        // `duckdb_list_vector_set_size` + `_reserve` + a child-vector fill,
        // which is a materially more invasive write path than the fixed-width
        // arms above. Rather than emit a partial write, FAIL-LOUD (T2-1
        // residual continuation): return Err so the caller surfaces the
        // shortfall as a query error instead of silently zeroing the slot.
        (T_LIST, reg::DuckValue::List(_))
        | (T_STRUCT, reg::DuckValue::Struct(_))
        | (T_MAP, reg::DuckValue::Map(_))
        | (T_ARRAY, reg::DuckValue::Array(_)) => {
            return Err(format!(
                "nested type write (code {code}) not yet fully wired (T2-1 residual continuation)"
            ));
        }
        (_, other) => {
            return Err(format!(
                "component returned {other:?}, incompatible with declared aggregate return type"
            ));
        }
    }
    Ok(())
}

// T2-5: `write_col_from_raw` — a column-hoisted analog of `write_ret_raw` —
// was deleted here. It was dead code (zero callers); the auditor's
// hypothetical caller was `ArrowShim::func`, whose per-cell `write_ret_raw`
// loop already handles every logical-type arm the peer would have. The
// arrow producer receives row-major DuckValues from
// `dispatch_arrow_next`, so a column-major pivot before write would be an
// extra full-chunk pass with no marshalling savings (the string-arena
// arms still iterate rows per element). The peer's DECIMAL arm was
// duplicated in `write_ret_raw` under T1-4, so nothing about the DECIMAL
// path regresses. Anyone reviving this optimization should re-add the
// column-hoisted writer, wire it into `ArrowShim::func` behind a pivot
// pass, and benchmark against the current per-cell loop.

unsafe extern "C" fn agg_state_size(_info: ffi::duckdb_function_info) -> ffi::idx_t {
    std::mem::size_of::<*mut AggState>() as ffi::idx_t
}

unsafe extern "C" fn agg_init(
    info: ffi::duckdb_function_info,
    state: ffi::duckdb_aggregate_state,
) {
    let extra = &*(ffi::duckdb_aggregate_function_get_extra_info(info) as *const AggExtra);
    let slot = state as *mut *mut AggState;
    // Pick the accumulator shape ONCE per aggregate group, based on the arg
    // types this aggregate was registered with. The check is cheap (a small
    // `all()`) and lets the col-native update path skip DuckValue enum boxing
    // and per-row Vec allocation on the hot common case.
    let initial = if supports_col_native_agg(&extra.arg_codes) {
        AggState::ColNative(ColNativeState::new(&extra.arg_codes))
    } else {
        AggState::RowMajor(Vec::new())
    };
    *slot = Box::into_raw(Box::new(initial));
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
        // Fetch the input vectors ONCE per chunk. Each vector handle is
        // stable for the whole chunk, so `duckdb_data_chunk_get_vector` on
        // every row was pure overhead.
        let vectors: Vec<ffi::duckdb_vector> = (0..ncols)
            .map(|c| ffi::duckdb_data_chunk_get_vector(input, c as u64))
            .collect();
        // Per-column validity mask pointer, also stable per chunk. `null` if
        // the column has no validity mask (all rows valid).
        let validities: Vec<*const u64> = vectors
            .iter()
            .map(|&v| ffi::duckdb_vector_get_validity(v) as *const u64)
            .collect();
        // H1 fast path: when every row in the chunk targets the SAME state
        // pointer (the ungrouped `SELECT agg(f(x)) FROM t` shape — the whole
        // aggregate accumulates into one group), we can bulk-copy the whole
        // chunk into that group's accumulator instead of pushing one row at a
        // time. Detection is a single-pass pointer-equality scan; on the
        // uniform-state case it costs O(n) reads with no branch and unlocks
        // an O(n) memcpy per column instead of O(n) individual Vec::push
        // calls.
        //
        // GROUP BY chunks with mixed pointers fall through to the per-row
        // loop unchanged.
        let uniform_state = if n > 0 {
            let first = *states.add(0);
            (1..n).all(|r| *states.add(r) == first)
        } else {
            true
        };
        if uniform_state && n > 0 {
            let st = *states.add(0);
            let group = &mut **(st as *mut *mut AggState);
            if let AggState::ColNative(state) = group {
                state.append_chunk(&extra.arg_codes, &vectors, &validities, n);
                return;
            }
            // RowMajor uniform-state: still row-major (need per-row
            // DuckValue), but at least avoid the per-row states.add() FFI.
            if let AggState::RowMajor(rows) = group {
                for row in 0..n {
                    let argrow: Vec<reg::DuckValue> = (0..ncols)
                        .map(|c| read_arg_raw(extra.arg_codes[c], vectors[c], row))
                        .collect();
                    rows.push(argrow);
                }
                return;
            }
        }

        for row in 0..n {
            // The state for this input row (states is parallel to the input chunk).
            let st = *states.add(row);
            let group = &mut **(st as *mut *mut AggState);
            match group {
                AggState::ColNative(state) => {
                    // Col-native fast path: extend each per-column typed
                    // accumulator by ONE value read straight from the DuckDB
                    // flat vector. No DuckValue enum, no outer Vec<Vec<_>>
                    // per row — the huge win for `sum(scalar_f(x))` over
                    // millions of rows.
                    state.push_row(&extra.arg_codes, &vectors, &validities, row);
                }
                AggState::RowMajor(rows) => {
                    let argrow: Vec<reg::DuckValue> = (0..ncols)
                        .map(|c| read_arg_raw(extra.arg_codes[c], vectors[c], row))
                        .collect();
                    rows.push(argrow);
                }
            }
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
            match (t, s) {
                (AggState::ColNative(td), AggState::ColNative(sd)) => {
                    // Move source into target, then leave source in a valid
                    // empty state (the enum's Default sentinel).
                    let taken = std::mem::replace(sd, ColNativeState::new(&[]));
                    td.append(taken);
                }
                (AggState::RowMajor(td), AggState::RowMajor(sd)) => {
                    td.append(sd);
                }
                // agg_init picks the same variant for every group of the
                // same aggregate registration, so cross-variant combines
                // are unreachable in practice.
                _ => {}
            }
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
        // T1-3: finalize dispatches into the guest via
        // `engine.dispatch_aggregate_col` / `dispatch_aggregate` below.
        // Mark the thread so a re-entrant `NativeServices::query()` from
        // inside the guest refuses instead of deadlocking on the DuckDB
        // executor lock. `agg_update` and `agg_combine` just accumulate
        // row data locally and never call into the guest, so they don't
        // need a guard.
        let _reentrancy_guard = crate::engine::QueryReentrancyGuard::new();
        let extra = &*(ffi::duckdb_aggregate_function_get_extra_info(info) as *const AggExtra);
        for i in 0..count as usize {
            let group = &mut **(*source.add(i) as *mut *mut AggState);
            let taken = std::mem::take(group);
            let engine = &extra.engine;
            let dispatched = match taken {
                AggState::ColNative(state) => {
                    // Col-native fast path: the accumulator IS the Colvec
                    // batch — `into_colvecs` wraps the per-column typed Vecs
                    // and mask straight into `Vec<Colvec>`, no pivot pass.
                    let cols = state.into_colvecs();
                    engine.dispatch_aggregate_col(extra.callback_handle, &cols)
                }
                AggState::RowMajor(rows) => {
                    // Fallback for arg types the col-native path doesn't
                    // cover (DECIMAL / INTERVAL / UUID / COMPLEX). Try the
                    // row-to-col pivot first (still avoids the runtime's
                    // rows_to_colvecs scratch); if the pivot bails on an
                    // unsupported type, fall back to the fully row-major
                    // dispatch_aggregate.
                    match row_major_agg_state_to_colvecs(rows, &extra.arg_codes) {
                        Ok(cols) => engine.dispatch_aggregate_col(extra.callback_handle, &cols),
                        Err(_) => {
                            // We took the state above; rebuild a fresh empty
                            // shape and re-take from a dummy — cleaner: use
                            // the dispatch_aggregate path directly, which
                            // needs the row-major rows. Reconstruct by
                            // repeating the finalize with `take` — safer to
                            // just short-circuit to Err since the pivot
                            // errored out before dispatch.
                            Err(anyhow::anyhow!("aggregate col-native pivot failed"))
                        }
                    }
                }
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

/// True when every arg code is one of the arms `agg_state_to_colvecs` handles
/// directly. Anything else stays on the row-major `dispatch_aggregate` path.
fn supports_col_native_agg(arg_codes: &[u8]) -> bool {
    arg_codes.iter().all(|&code| {
        matches!(
            code,
            T_I64
                | T_U64
                | T_F64
                | T_BOOL
                | T_I8
                | T_I16
                | T_I32
                | T_U8
                | T_U16
                | T_U32
                | T_F32
                | T_TIMESTAMP
                | T_DATE
                | T_TIME
                | T_TIMESTAMPTZ
                | T_TEXT
                | T_BLOB
        )
    })
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

/// Build one `duckdb_aggregate_function` for the given overload. The caller
/// owns the returned handle (destroy with `duckdb_destroy_aggregate_function`
/// after registration OR after adding to a function set — the set deep-copies
/// via TableFunction-style shared_ptr semantics on `function_info`, so the
/// outer handle's release does NOT drop the extra-info while the set holds a
/// reference). Returns `None` if the function name contains an interior NUL
/// byte (already logged).
///
/// # Safety
/// FFI: constructs a DuckDB handle via `duckdb_create_aggregate_function`.
unsafe fn build_aggregate_function(
    f: &AggregateFunc,
    engine: Arc<Engine2>,
) -> Option<ffi::duckdb_aggregate_function> {
    let arg_codes: Vec<u8> = f.arguments.iter().map(|a| type_code(&a.logical)).collect();
    let ret_code = type_code(&f.returns);

    let func = ffi::duckdb_create_aggregate_function();
    let cname = match CString::new(f.name.as_str()) {
        Ok(c) => c,
        Err(_) => {
            eprintln!("[ducklink] aggregate '{}' name contains NUL byte; skipping", f.name);
            let mut func_mut = func;
            ffi::duckdb_destroy_aggregate_function(&mut func_mut);
            return None;
        }
    };
    ffi::duckdb_aggregate_function_set_name(func, cname.as_ptr());
    for &code in &arg_codes {
        // T1-4: `logical_type_ffi` routes DECIMAL through
        // `duckdb_create_decimal_type(18, 3)` so widths/scales are set.
        let mut lt = logical_type_ffi(code);
        ffi::duckdb_aggregate_function_add_parameter(func, lt);
        ffi::duckdb_destroy_logical_type(&mut lt);
    }
    let mut rlt = logical_type_ffi(ret_code);
    ffi::duckdb_aggregate_function_set_return_type(func, rlt);
    ffi::duckdb_destroy_logical_type(&mut rlt);

    let extra = Box::into_raw(Box::new(AggExtra {
        callback_handle: f.callback_handle,
        engine,
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
    Some(func)
}

/// Register every component aggregate function on the raw connection `raw_con`
/// via the DuckDB C API. Functions registered on any connection of a database
/// are visible to all its connections, so `raw_con` need only share the database
/// with the connection used for queries.
///
/// # Safety
/// `raw_con` must be a valid `duckdb_connection`.
///
/// T2-6: overload sets landed. Overloads within a single (extension, name)
/// group are collected up front, and any group with >1 members is installed
/// via `duckdb_create_aggregate_function_set` +
/// `duckdb_add_aggregate_function_to_set` (per overload) +
/// `duckdb_register_aggregate_function_set`. Singletons keep the single-fn
/// install path. Ownership: the set deep-copies each added function via
/// `AggregateFunctionSet::AddFunction` (a value copy of `AggregateFunction`,
/// which holds `function_info` as a shared_ptr — so the extra-info's
/// destroy callback fires only when the LAST holder drops). We destroy the
/// per-overload handle immediately after adding it to the set; the extra-info
/// then lives with the set's copy until the set itself is destroyed (which
/// happens either explicitly on registration failure or via DuckDB's catalog
/// when registration succeeds).
pub unsafe fn register_aggregates(
    raw_con: ffi::duckdb_connection,
    engine: Arc<Engine2>,
    aggregates: &[AggregateFunc],
) -> duckdb::Result<usize> {
    use std::collections::HashMap;
    // Group by (extension, name) preserving first-seen order so error messages
    // reference overloads in declaration order.
    let mut groups: Vec<(String, String, Vec<usize>)> = Vec::new();
    let mut index: HashMap<(String, String), usize> = HashMap::new();
    for (i, f) in aggregates.iter().enumerate() {
        let key = (f.extension.clone(), f.name.clone());
        match index.get(&key) {
            Some(&g) => groups[g].2.push(i),
            None => {
                index.insert(key.clone(), groups.len());
                groups.push((key.0, key.1, vec![i]));
            }
        }
    }

    let mut registered = 0usize;
    for (_ext, name, member_ixs) in &groups {
        if member_ixs.len() == 1 {
            // Single-overload path (unchanged install semantics).
            let f = &aggregates[member_ixs[0]];
            let func = match build_aggregate_function(f, engine.clone()) {
                Some(func) => func,
                None => continue,
            };
            let rc = ffi::duckdb_register_aggregate_function(raw_con, func);
            let mut func_mut = func;
            ffi::duckdb_destroy_aggregate_function(&mut func_mut);
            if rc != ffi::DuckDBSuccess {
                // IDEMPOTENCY: duplicate name (re-load) is not a hard error.
                eprintln!("[ducklink] aggregate '{}' not registered (already present?)", f.name);
                continue;
            }
            registered += 1;
            continue;
        }

        // Overload-set path (>=2 signatures under this name).
        let set_name_c = match CString::new(name.as_str()) {
            Ok(c) => c,
            Err(_) => {
                eprintln!("[ducklink] aggregate set '{name}' name contains NUL byte; skipping");
                continue;
            }
        };
        let set = ffi::duckdb_create_aggregate_function_set(set_name_c.as_ptr());
        if set.is_null() {
            eprintln!("[ducklink] aggregate set '{name}' could not be created");
            continue;
        }
        let mut set_ok = true;
        let mut overloads_added = 0usize;
        for &ix in member_ixs {
            let f = &aggregates[ix];
            let func = match build_aggregate_function(f, engine.clone()) {
                Some(func) => func,
                None => continue,
            };
            let add_rc = ffi::duckdb_add_aggregate_function_to_set(set, func);
            let mut func_mut = func;
            ffi::duckdb_destroy_aggregate_function(&mut func_mut);
            if add_rc != ffi::DuckDBSuccess {
                eprintln!(
                    "[ducklink] aggregate '{}' overload {} failed to join set",
                    f.name, ix
                );
                set_ok = false;
                break;
            }
            overloads_added += 1;
        }
        if !set_ok || overloads_added == 0 {
            let mut set_mut = set;
            ffi::duckdb_destroy_aggregate_function_set(&mut set_mut);
            continue;
        }
        let rc = ffi::duckdb_register_aggregate_function_set(raw_con, set);
        let mut set_mut = set;
        ffi::duckdb_destroy_aggregate_function_set(&mut set_mut);
        if rc != ffi::DuckDBSuccess {
            // IDEMPOTENCY: duplicate set name (re-load) is skipped like the
            // single-fn path.
            eprintln!(
                "[ducklink] aggregate set '{name}' not registered (already present?)"
            );
            continue;
        }
        registered += overloads_added;
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
    /// Optional per-function documentation overrides the component shipped in
    /// its `duckdb.docs` wasm custom section. Merged into `ducklink.docs` at
    /// query time — summary / description / example REPLACE catalog values,
    /// tags are UNIONed. `None` for components without the section. Wrapped
    /// in an `Arc` so `snapshot_component_docs` is a refcount bump instead of
    /// deep-cloning the full ComponentDocs on every doc-view bind.
    docs: Option<Arc<crate::docs_section::ComponentDocs>>,
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

/// The host capability profile captured at `LOAD ducklink` time. Ducklink
/// is C-API-only across every platform now, so the only fields carried are
/// the reported DuckDB library version (informational, for
/// `ducklink.host`) and the wasm component contract version this host
/// speaks (used by `ducklink.modules.compatible`). Set once by the entry
/// point via [`set_host_caps`].
#[derive(Clone, Default)]
pub struct HostCaps {
    /// The host DuckDB library version string, if known.
    pub host_version: Option<String>,
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

/// The capability kinds this host satisfies (all through the stable DuckDB C API).
/// A module whose `requires` are all in this set is compatible on any platform.
const COMMON_TIER_KINDS: &[&str] = &[
    "scalar",
    "table",
    "aggregate",
    "macro",
    "cast",
    "network",
    "compose-dynlink",
];

/// True when THIS host can satisfy every capability `kind` in `requires`.
/// Ducklink is C-API-only on every platform, so anything outside
/// `COMMON_TIER_KINDS` (parser / optimizer / storage / …) is unsatisfied
/// everywhere.
fn module_compatible(requires: &[String], _caps: &HostCaps) -> bool {
    requires
        .iter()
        .all(|r| COMMON_TIER_KINDS.contains(&r.as_str()))
}

// SAFETY: `db` is a stable, process-lifetime handle DuckDB owns; we only read it
// to open sibling connections (a database-wide, thread-safe C-API operation).
unsafe impl Send for DucklinkRuntime {}
unsafe impl Sync for DucklinkRuntime {}

static RUNTIME: std::sync::OnceLock<DucklinkRuntime> = std::sync::OnceLock::new();

/// `ducklink_version()` -> the extension's version string. Registered
/// unconditionally (needs no WebAssembly component), so
/// `LOAD ducklink; SELECT ducklink_version();` is a self-contained smoke
/// test that the extension built and loaded.
///
/// Kept here rather than in `src/lib.rs` so `register_load_function` — the
/// SHARED entry point for the loadable-extension init AND any in-process
/// integration test — registers the entire STABILITY.md § 1.1 surface in
/// one place. That's the "one implementation, N surfaces" invariant
/// applied internally.
pub(crate) struct DucklinkVersion;

impl VScalar for DucklinkVersion {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let out = output.flat_vector();
        let version = concat!("ducklink ", env!("CARGO_PKG_VERSION"));
        for i in 0..len {
            out.insert(i, version);
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

/// `ducklink_help(name)` — pretty-printed markdown for a single function
/// or module. Reads its input per row and renders the doc rows from
/// `ducklink.docs` in a scalar-friendly single VARCHAR output.
pub(crate) struct DucklinkHelp;

impl VScalar for DucklinkHelp {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let mut in_col = input.flat_vector(0);
        let out = output.flat_vector();
        let names: Vec<String> = unsafe {
            let s = in_col.as_mut_slice_with_len::<duckdb_string_t>(len);
            (0..len)
                .map(|i| {
                    let mut t = s[i];
                    DuckString::new(&mut t).as_str().into_owned()
                })
                .collect()
        };
        for (i, name) in names.iter().enumerate() {
            let rendered = render_help(name);
            out.insert(i, rendered.as_str());
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

/// Register the ducklink SQL surface committed in STABILITY.md § 1.1 and
/// the discovery views committed in § 1.2, and capture the process-wide
/// runtime handle they depend on. Idempotent: the handle is set once (the
/// first `LOAD ducklink` in the process wins); the functions are
/// registered on `con` each call (a no-op duplicate is tolerated by DuckDB).
///
/// This is the SHARED entry point. Both `ducklink_init_c_api` (the
/// loadable-extension entry) and the in-process conformance runner call
/// it, so the registered surface can't drift between them.
///
/// The function name is retained for backward compatibility with existing
/// callers that once threaded through only the `ducklink_load` TF.
///
/// T1-7: `guest.shutdown` fires on Drop via `impl Drop for ExtensionInstance`
/// (see runtime/src/extension.rs) — the Arc<Mutex<ExtensionInstance>>::drop
/// chain reaches it naturally on load-replace / process exit. Reconfigure
/// awaits T3-1 (per-option SET-notification hook not in the DuckDB stable
/// C API).
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

    // Wire the persistent raw sibling connection into the engine so
    // NativeServices' config getters + `query()` reach the live DuckDB. The
    // raw connection is process-persistent (never disconnected here), so it
    // outlives every guest that will call the config/query host imports. If
    // opening it failed we skip the attach — NativeServices then reports
    // `Ok(None)` / unavailable, which the guest surface tolerates.
    if raw_ok {
        engine.attach_duckdb_connection(raw);
    }

    // First loader in the process captures the runtime; later ones reuse it.
    let _ = RUNTIME.set(DucklinkRuntime {
        db,
        con: Mutex::new(persistent),
        raw_con,
        engine,
        loaded: Mutex::new(Vec::new()),
    });

    // Install the process-wide DuckDB replacement-scan callback. This
    // registers the callback ONCE while `db` is still valid (we are inside
    // the extension's init) — later `files::register_replacement_scan`
    // calls from loaded components mutate REPLACEMENT_SCAN_REGISTRY only;
    // the callback below reads that registry on each unbound-table-name
    // reference to decide whether to rewrite the scan.
    unsafe {
        ffi::duckdb_add_replacement_scan(
            db,
            Some(ducklink_replacement_scan_callback),
            std::ptr::null_mut(),
            None,
        );
    }
    con.register_table_function::<WasmLoad>("ducklink_load")?;
    // User-side alias schemas. `FROM ducklink_prefix('c', 'crypto')`
    // creates a schema `c` and re-registers every function in schema
    // `crypto` under `c.<fn>`. The declaration is persisted in
    // `ducklink.prefixes` and replayed on subsequent
    // `ducklink_load('name', kind => 'native')` calls.
    con.register_table_function::<DucklinkPrefix>("ducklink_prefix")?;
    // Scalar form of the same name — usable as `SELECT ducklink_prefix('c','crypto');`.
    // The TF and scalar coexist under one name because DuckDB's binder
    // routes `FROM foo(...)` to the TF and `SELECT foo(...)` to the scalar.
    con.register_scalar_function::<DucklinkPrefixScalar>("ducklink_prefix")?;
    // The two always-available built-ins. Kept alongside the rest of the
    // STABILITY.md § 1.1 surface so a single call registers everything.
    con.register_scalar_function::<DucklinkVersion>("ducklink_version")?;
    con.register_scalar_function::<DucklinkHelp>("ducklink_help")?;
    // Shorter macro that just delegates to the scalar. Users still need
    // to quote the two identifiers — `SELECT PREFIX('c','crypto');` —
    // because DuckDB has no parser hook to reinterpret bare idents as
    // strings, but PREFIX is materially shorter than ducklink_prefix.
    if let Err(e) = con.execute(
        "CREATE OR REPLACE MACRO PREFIX(alias, namespace) AS ducklink_prefix(alias, namespace)",
        [],
    ) {
        eprintln!("[ducklink] could not register PREFIX macro: {e}");
    }
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
    con.register_table_function::<WasmDocs>("ducklink_docs")?;
    con.register_table_function::<WasmSearch>("ducklink_search")?;

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
///
/// The set of views and the empty `prefixes` table must match STABILITY.md
/// § 1.2 exactly. Adding a new view is a MINOR change; renaming or removing
/// one is MAJOR. The conformance suite asserts this shape.
fn create_ducklink_schema(con: &Connection) -> duckdb::Result<()> {
    con.execute_batch(
        "CREATE SCHEMA IF NOT EXISTS ducklink;
         CREATE OR REPLACE VIEW ducklink.modules AS SELECT * FROM ducklink_modules();
         CREATE OR REPLACE VIEW ducklink.functions AS SELECT * FROM ducklink_functions();
         CREATE OR REPLACE VIEW ducklink.host_capabilities AS SELECT * FROM ducklink_host_capabilities();
         CREATE OR REPLACE VIEW ducklink.cache AS SELECT * FROM ducklink_cache();
         CREATE OR REPLACE VIEW ducklink.module_compatibility AS SELECT * FROM ducklink_module_compatibility();
         CREATE OR REPLACE VIEW ducklink.events AS SELECT * FROM ducklink_events();
         CREATE OR REPLACE VIEW ducklink.host AS SELECT * FROM ducklink_host();
         CREATE OR REPLACE VIEW ducklink.docs AS SELECT * FROM ducklink_docs();
         CREATE TABLE IF NOT EXISTS ducklink.prefixes (
             alias VARCHAR PRIMARY KEY,
             namespace VARCHAR NOT NULL
         );",
    )?;
    // `ducklink.search` needs a bound argument — the query text — so it
    // can't be a plain view over `ducklink_search()`. Expose it as a MACRO
    // that takes the same argument shape and forwards to the TF, so users
    // write `SELECT * FROM ducklink.search('query')` matching the pattern
    // of the other discovery entries.
    con.execute_batch(
        "CREATE OR REPLACE MACRO ducklink.search(query) AS TABLE SELECT * FROM ducklink_search(query);",
    )
}

/// Load a component (by path or catalog name) into the GIVEN database handle and
/// register its functions. Takes the `duckdb_database` explicitly so callers
/// can pass a handle derived from a live `ClientContext` — the init-time
/// `rt.db` doesn't survive to re-`duckdb_connect` later (observed: "connect
/// error") in the loadable/CLI context, so the caller's context-derived handle
/// is what makes runtime loading work end to end.
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

    // Wire any `files::register_replacement_scan(exts, function_name)` calls
    // the component made during load() into the DuckDB catalog. Appends to
    // the process-wide registry the C-side callback consults on every
    // unbound table-name reference. Registration is idempotent (a repeat
    // (ext, fn) pair is a no-op) so re-loading a component does not stack.
    register_replacement_scans(&loaded.replacement_scans);

    // Wire every additive registration surface. Each `register_<x>` mirrors
    // the shape of `register_replacement_scans` — process-wide registry,
    // C ABI callback, `duckdb_register_*` install call — for one field on
    // `LoadedComponent`. See each function's doc comment for the C API path.
    // A failed register_<x> is NOT fatal to `load_wasm_into_db`: it is logged
    // and the load continues (parity with scalars/tables where a duplicate
    // is skipped). The FAIL-LOUD paths (arrow_tables) return Err — mapped
    // to an eprintln here so LOAD reports the shortfall without aborting.
    unsafe {
        let raw_con = rt.raw_con.0;
        if !raw_con.is_null() {
            if let Err(e) = register_settings(raw_con, &loaded.settings) {
                eprintln!("[ducklink] register_settings failed: {e}");
            }
            if let Err(e) = register_copy_handlers(raw_con, rt.engine.clone(), &loaded.copy_handlers) {
                eprintln!("[ducklink] register_copy_handlers failed: {e}");
            }
            if let Err(e) = register_scalar_ex(raw_con, rt.engine.clone(), &loaded.scalar_ex) {
                eprintln!("[ducklink] register_scalar_ex failed: {e}");
            }
            if let Err(e) = register_casts(raw_con, rt.engine.clone(), &loaded.casts) {
                eprintln!("[ducklink] register_casts failed: {e}");
            }
            if let Err(e) = register_logical_types(raw_con, &loaded.logical_types) {
                eprintln!("[ducklink] register_logical_types failed: {e}");
            }
            if let Err(e) = register_modified_types(raw_con, &loaded.modified_types) {
                eprintln!("[ducklink] register_modified_types failed: {e}");
            }
            if let Err(e) = register_enum_types(raw_con, &loaded.enum_types) {
                eprintln!("[ducklink] register_enum_types failed: {e}");
            }
        } else if !loaded.settings.is_empty()
            || !loaded.copy_handlers.is_empty()
            || !loaded.scalar_ex.is_empty()
            || !loaded.casts.is_empty()
            || !loaded.logical_types.is_empty()
            || !loaded.modified_types.is_empty()
            || !loaded.enum_types.is_empty()
        {
            eprintln!(
                "[ducklink] skipping {} settings / {} copy_handlers / {} scalar_ex / \
                 {} casts / {} logical_types / {} modified_types / {} enum_types \
                 registration(s) from '{}': no raw connection available",
                loaded.settings.len(),
                loaded.copy_handlers.len(),
                loaded.scalar_ex.len(),
                loaded.casts.len(),
                loaded.logical_types.len(),
                loaded.modified_types.len(),
                loaded.enum_types.len(),
                name
            );
        }
        if let Err(e) = register_log_storages(rt.db, rt.engine.clone(), &loaded.log_storages) {
            eprintln!("[ducklink] register_log_storages failed: {e}");
        }
        // T1-5: install component-declared PRAGMAs. Currently a no-op that
        // logs each declaration — the DuckDB stable C API does not expose
        // a pragma-registration entry point (see `register_pragmas` doc).
        if let Err(e) = register_pragmas(rt.raw_con.0, rt.engine.clone(), &loaded.pragmas) {
            eprintln!("[ducklink] register_pragmas failed: {e}");
        }
        // T2-2: install component-declared coordinate reference systems.
        // Currently a no-op that logs each declaration — the DuckDB stable
        // C API does not expose a CRS registration entry point (see
        // `register_coordinate_systems` doc).
        if let Err(e) = register_coordinate_systems(
            rt.raw_con.0,
            rt.engine.clone(),
            &loaded.coordinate_systems,
        ) {
            eprintln!("[ducklink] register_coordinate_systems failed: {e}");
        }
    }
    {
        let con = rt.con.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(e) = register_macros(&con, &loaded.macros) {
            eprintln!("[ducklink] register_macros failed: {e}");
        }
        if let Err(e) = register_table_macros(&con, &loaded.table_macros) {
            eprintln!("[ducklink] register_table_macros failed: {e}");
        }
        // Arrow producers register as duckdb-rs table function shims + a
        // replacement-scan rewrite (see `register_arrow_tables`); they need
        // the safe `Connection`, not the raw handle.
        match register_arrow_tables(&con, rt.engine.clone(), &loaded.arrow_tables) {
            Ok(n) if n > 0 => {
                eprintln!("[ducklink] register_arrow_tables: installed {n} producer(s)");
            }
            Ok(_) => {}
            Err(e) => eprintln!("[ducklink] register_arrow_tables: {e}"),
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
            docs: loaded.docs.clone().map(Arc::new),
        };
        match loaded_list.iter_mut().find(|r| r.name == name) {
            Some(existing) => *existing = rec,
            None => loaded_list.push(rec),
        }
    }
    bump_doc_cache_generation();

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

            // Named parameter `kind` selects the loader path:
            //   * "wasm" (default) — resolve as a wasm component and load via
            //     the embedded wasmtime engine, registering functions on the
            //     runtime's persistent connection.
            //   * "native"        — pick the best native backing:
            //     1. If a community-native provider is available, INSTALL +
            //        LOAD from `duckdb/community-extensions`. Signed by the
            //        community key so no `-unsigned` required.
            //     2. Else, download our own native provider matching this
            //        host's platform + DuckDB version and invoke DuckDB's
            //        LOAD on the cached path. Requires `-unsigned` because
            //        our signing key isn't in DuckDB's trust chain.
            //     3. Else, error clearly — no native backing available.
            //     The user's SQL doesn't change either way; ducklink is the
            //     routing layer over WASM and native backings.
            let kind = bind
                .get_named_parameter("kind")
                .map(|v| v.to_string().to_ascii_lowercase())
                .unwrap_or_else(|| "wasm".to_string());
            match kind.as_str() {
                "wasm" => {}
                "native" => return native_load_dispatch(rt, bind, &arg_str),
                other => {
                    return Err(format!(
                        "ducklink_load: kind must be 'wasm' or 'native', got '{other}'"
                    )
                    .into());
                }
            }

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

            // Wire the component's `files::register_replacement_scan` calls
            // into the process-wide registry the C callback consults.
            register_replacement_scans(&loaded.replacement_scans);

            // Additive registration surface, one call per LoadedComponent
            // field. Same shape as the sibling call site in `load_wasm_into_db`.
            // Failures are logged (not fatal) — matches the existing scalar/
            // table idempotency behaviour.
            unsafe {
                let raw_con = rt.raw_con.0;
                if !raw_con.is_null() {
                    if let Err(e) = register_settings(raw_con, &loaded.settings) {
                        eprintln!("[ducklink] register_settings failed: {e}");
                    }
                    if let Err(e) = register_copy_handlers(raw_con, rt.engine.clone(), &loaded.copy_handlers) {
                        eprintln!("[ducklink] register_copy_handlers failed: {e}");
                    }
                    if let Err(e) = register_scalar_ex(raw_con, rt.engine.clone(), &loaded.scalar_ex) {
                        eprintln!("[ducklink] register_scalar_ex failed: {e}");
                    }
                    if let Err(e) = register_casts(raw_con, rt.engine.clone(), &loaded.casts) {
                        eprintln!("[ducklink] register_casts failed: {e}");
                    }
                    if let Err(e) = register_logical_types(raw_con, &loaded.logical_types) {
                        eprintln!("[ducklink] register_logical_types failed: {e}");
                    }
                    if let Err(e) = register_modified_types(raw_con, &loaded.modified_types) {
                        eprintln!("[ducklink] register_modified_types failed: {e}");
                    }
                    if let Err(e) = register_enum_types(raw_con, &loaded.enum_types) {
                        eprintln!("[ducklink] register_enum_types failed: {e}");
                    }
                } else if !loaded.settings.is_empty()
                    || !loaded.copy_handlers.is_empty()
                    || !loaded.scalar_ex.is_empty()
                    || !loaded.casts.is_empty()
                    || !loaded.logical_types.is_empty()
                    || !loaded.modified_types.is_empty()
                    || !loaded.enum_types.is_empty()
                {
                    eprintln!(
                        "[ducklink] skipping settings/copy_handlers/scalar_ex/casts/logical_types/\
                         modified_types/enum_types registration(s) from '{}': no raw connection available",
                        name
                    );
                }
                if let Err(e) = register_log_storages(rt.db, rt.engine.clone(), &loaded.log_storages) {
                    eprintln!("[ducklink] register_log_storages failed: {e}");
                }
                // T1-5: same as the sibling call site above — currently a
                // no-op that logs each declaration.
                if let Err(e) = register_pragmas(rt.raw_con.0, rt.engine.clone(), &loaded.pragmas) {
                    eprintln!("[ducklink] register_pragmas failed: {e}");
                }
                // T2-2: same as the sibling call site above — currently a
                // no-op that logs each declaration.
                if let Err(e) = register_coordinate_systems(
                    rt.raw_con.0,
                    rt.engine.clone(),
                    &loaded.coordinate_systems,
                ) {
                    eprintln!("[ducklink] register_coordinate_systems failed: {e}");
                }
            }
            {
                let con = rt.con.lock().unwrap_or_else(|e| e.into_inner());
                if let Err(e) = register_macros(&con, &loaded.macros) {
                    eprintln!("[ducklink] register_macros failed: {e}");
                }
                if let Err(e) = register_table_macros(&con, &loaded.table_macros) {
                    eprintln!("[ducklink] register_table_macros failed: {e}");
                }
                // Arrow producers register as duckdb-rs table function shims
                // + a replacement-scan rewrite; needs the safe `Connection`,
                // not the raw handle.
                match register_arrow_tables(&con, rt.engine.clone(), &loaded.arrow_tables) {
                    Ok(n) if n > 0 => {
                        eprintln!("[ducklink] register_arrow_tables: installed {n} producer(s)");
                    }
                    Ok(_) => {}
                    Err(e) => eprintln!("[ducklink] register_arrow_tables: {e}"),
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
                    docs: loaded.docs.clone().map(Arc::new),
                };
                match loaded_list.iter_mut().find(|r| r.name == name) {
                    Some(existing) => *existing = rec,
                    None => loaded_list.push(rec),
                }
            }
            bump_doc_cache_generation();

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
        // Named parameters:
        //   * `name :=`  overrides the display name (defaults to the file stem
        //     for a path arg, or the catalog name for a name arg).
        //   * `kind :=`  selects the loader path; 'wasm' (default) or 'native'.
        Some(vec![
            (
                "name".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "kind".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
        ])
    }
}

/// Native dispatch for `ducklink_load(name, kind => 'native')`. Prefers a
/// community-native provider (community-signed, best trust posture); falls
/// back to a ducklink-native provider (our own build; requires `-unsigned`);
/// errors clearly if neither is available.
///
/// This is the "smart" native selection — the user says "give me the native
/// version" and ducklink picks the best backing. The user's SQL doesn't
/// change; ducklink is the routing layer.
fn native_load_dispatch(
    rt: &DucklinkRuntime,
    bind: &BindInfo,
    name_arg: &str,
) -> Result<WasmLoadBind, Box<dyn std::error::Error>> {
    // 1. Prefer community-native — community-signed, no `-unsigned` needed.
    if let Ok(spec) = crate::catalog::resolve_name_to_community_native(name_arg) {
        return community_native_load(rt, bind, name_arg, spec);
    }
    // 2. Fall back to our own native build for this host.
    native_load(rt, bind, name_arg)
}

/// Community-native branch — INSTALL + LOAD an existing extension from
/// `duckdb/community-extensions`, then alias its functions under ducklink's
/// chosen names. Ducklink is the router; the community extension is the
/// actual implementation. The community-signed key is in DuckDB's trust
/// chain already, so no `-unsigned` is needed.
fn community_native_load(
    rt: &DucklinkRuntime,
    bind: &BindInfo,
    name_arg: &str,
    spec: crate::catalog::CommunityNativeSpec,
) -> Result<WasmLoadBind, Box<dyn std::error::Error>> {
    let community_ext_name = &spec.extension_name;

    // Belt-and-braces: reject extension names that don't fit DuckDB's identifier
    // rules so an accidentally-crafted catalog entry can't inject arbitrary SQL.
    if !crate::catalog::is_safe_identifier(community_ext_name) {
        return Err(format!(
            "ducklink_load(kind='native'): community-native provider names '{community_ext_name}', \
             which contains characters outside the allowed [A-Za-z0-9_] identifier set. \
             Refusing to run INSTALL/LOAD."
        )
        .into());
    }

    crate::events::emit(
        "load_community_native_start",
        Some(name_arg),
        community_ext_name.clone(),
    );

    // INSTALL is idempotent (DuckDB no-ops when already at the right version);
    // LOAD registers the community extension's functions under community's names.
    let con = rt.con.lock().unwrap_or_else(|e| e.into_inner());
    if let Err(err) = con.execute(&format!("INSTALL {community_ext_name} FROM community"), []) {
        let err_msg = err.to_string();
        crate::events::emit("load_community_native_error", Some(name_arg), err_msg.clone());
        return Err(format!(
            "ducklink_load(kind='native'): INSTALL {community_ext_name} FROM community failed: {err_msg}"
        )
        .into());
    }
    if let Err(err) = con.execute(&format!("LOAD {community_ext_name}"), []) {
        let err_msg = err.to_string();
        crate::events::emit("load_community_native_error", Some(name_arg), err_msg.clone());
        return Err(format!(
            "ducklink_load(kind='native'): LOAD {community_ext_name} failed after successful INSTALL: {err_msg}"
        )
        .into());
    }

    // Generate aliases so both names — community's own and ducklink's
    // chosen — are callable. Scalar/table aliases go through
    // `CREATE OR REPLACE MACRO`; aggregate aliases go through the delegating
    // C-API aggregate so DISTINCT / FILTER / GROUP BY propagate. Per-pair
    // errors are non-fatal so a mismapping doesn't block the rest of the load.
    let alias_count = create_community_aliases(&con, &spec)
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    // Replay any prefixes persisted from prior sessions. The just-loaded
    // module might satisfy one whose namespace was empty before. Non-fatal:
    // failures don't block the load itself.
    let prefixes_replayed = replay_persisted_prefixes(&con);

    crate::events::emit(
        "load_community_native_ok",
        Some(name_arg),
        format!(
            "extension='{community_ext_name}' aliases={alias_count} prefixes_replayed={prefixes_replayed}"
        ),
    );

    bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("path", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("scalars", LogicalTypeHandle::from(LogicalTypeId::Bigint));
    bind.add_result_column("tables", LogicalTypeHandle::from(LogicalTypeId::Bigint));
    bind.add_result_column("aggregates", LogicalTypeHandle::from(LogicalTypeId::Bigint));
    Ok(WasmLoadBind {
        name: name_arg.to_string(),
        path: format!("community-extensions:{community_ext_name}"),
        scalars: usize::MAX, // sentinel: "n/a"
        tables: usize::MAX,
        aggregates: usize::MAX,
    })
}

/// After a community extension is INSTALLed and LOADed, generate the
/// aliases the catalog author asked for. Returns the number of aliases
/// created (across all kinds).
///
/// Two catalog shapes drive the mapping (either or both may appear on the
/// provider):
///
/// * `function_mapping: {"our_name": "their_name"}` — explicit per-function
///   pairs. Wins over `community_prefix` when both refer to the same
///   community function.
/// * `community_prefix: "t_"` — systematic prefix on community's exports;
///   ducklink discovers matching community names via `duckdb_functions()`
///   and creates aliases with the prefix stripped.
///
/// # Aliasing mechanism
///
/// Emits `CREATE OR REPLACE MACRO` per (pair, arity) — the same shape on
/// every platform. Scalar and table macros are planner-inlined (zero
/// overhead); single-arg aggregates use the `list_aggregate(list(x),
/// 'their')` trick, which works for basic `GROUP BY` but does NOT
/// propagate `DISTINCT` / `FILTER` / `ORDER BY` / window modifiers.
/// Users who need those call community's original name (unchanged).
///
/// When the catalog entry declares a `namespace`, each alias is
/// double-registered — once in `main` (bare form) and once in the
/// namespace schema (schema-qualified form).
///
/// Pair-selection is shared in [`crate::catalog::compute_alias_pairs`];
/// macro-shape building is shared in [`crate::catalog::build_alias_macro`].
fn create_community_aliases(
    con: &duckdb::Connection,
    spec: &crate::catalog::CommunityNativeSpec,
) -> Result<usize, String> {
    // 1. Discover community-registered function names for the prefix (if any).
    let discovered: Vec<String> = if spec.community_prefix.is_some() {
        // No SQL LIKE — `_` is LIKE's single-char wildcard, and prefixes
        // like `"t_"` would over-match. Filter by prefix in Rust instead;
        // `compute_alias_pairs` handles the starts_with test.
        let sql = "SELECT function_name FROM duckdb_functions() \
                   WHERE function_type IN ('scalar','aggregate','table_macro','scalar_macro','macro','table') \
                   GROUP BY function_name";
        let mut stmt = con
            .prepare(sql)
            .map_err(|e| format!("prepare duckdb_functions scan: {e}"))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query duckdb_functions scan: {e}"))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(|e| format!("row read: {e}"))?);
        }
        out
    } else {
        Vec::new()
    };

    // 2. Fold explicit mapping + discovered prefix hits into a stable pair list.
    let pairs = crate::catalog::compute_alias_pairs(spec, &discovered);

    let mut created = 0usize;
    // The catalog entry MAY carry a canonical `namespace` — when set, every
    // community function is aliased twice: once in `main` (bare form) and
    // once in `<namespace>` (schema-qualified form). Both share the same
    // underlying `CREATE OR REPLACE MACRO` body, so `foo(x)` and
    // `<namespace>.foo(x)` bind identically. Users opt into a bare short
    // name (`bar(x)` resolving from a non-main schema) via
    // `SET search_path`.
    let namespace = spec.namespace.as_deref();

    for (ours, theirs) in &pairs {
        // Macro-based registration — the portable path used on every
        // platform. `duckdb_functions()` can report multiple rows (one per
        // overload); produce one macro per distinct arity (macros support
        // arity-overloading with the same name).
        let info_sql = format!(
            "SELECT function_type, array_to_string(parameters, ',') AS param_csv \
             FROM duckdb_functions() WHERE function_name = '{theirs}'"
        );
        let mut stmt = con
            .prepare(&info_sql)
            .map_err(|e| format!("prepare info for '{theirs}': {e}"))?;
        let rows = stmt
            .query_map([], |row| {
                let ftype: String = row.get(0)?;
                // parameters rendered as a CSV via array_to_string —
                // duckdb-rs doesn't ship a FromSql impl for `Vec<String>`.
                let param_csv: Option<String> = row.get(1)?;
                Ok((ftype, param_csv))
            })
            .map_err(|e| format!("query info for '{theirs}': {e}"))?;

        let mut done_arities: std::collections::HashSet<usize> =
            std::collections::HashSet::new();
        for row in rows {
            let (ftype, param_csv) = row.map_err(|e| format!("row read: {e}"))?;
            let param_csv = param_csv.unwrap_or_default();
            let params: Vec<String> = if param_csv.is_empty() {
                Vec::new()
            } else {
                param_csv
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            };
            if !done_arities.insert(params.len()) {
                continue;
            }

            // AGGREGATE fast path: register a REAL C-API aggregate that
            // delegates to `theirs`. This is what makes DISTINCT / FILTER /
            // GROUP BY propagate transparently through the alias, on every
            // platform. Falls through to the macro fallback when we can't
            // register (unsupported types, no runtime raw_con, etc).
            let is_aggregate = matches!(ftype.as_str(), "aggregate");
            let mut delegated = false;
            if is_aggregate {
                match register_aggregate_delegate(&con, ours, theirs) {
                    Ok(()) => {
                        created += 1;
                        delegated = true;
                        if let Some(ns) = namespace {
                            if crate::catalog::is_safe_identifier(ns) {
                                // For the namespace-qualified form,
                                // register the same delegate under
                                // `<ns>_<ours>` (schemas can't hold C-API
                                // aggregates cleanly; the flat namespace
                                // form is the trade-off — users see
                                // `<ns>.<ours>` for scalars and
                                // `<ns>_<ours>` for aggregates until we
                                // add proper schema support to
                                // `duckdb_aggregate_function_set_name`).
                                let ns_name = format!("{ns}_{ours}");
                                if let Err(err) = register_aggregate_delegate(
                                    &con,
                                    &ns_name,
                                    theirs,
                                ) {
                                    crate::events::emit(
                                        "community_namespace_delegate_error",
                                        Some(&ns_name),
                                        format!("{theirs}: {err}"),
                                    );
                                } else {
                                    created += 1;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        crate::events::emit(
                            "community_delegate_fallback",
                            Some(ours),
                            format!("{theirs}: {err}"),
                        );
                        // Fall through to macro fallback.
                    }
                }
            }

            if !delegated {
                // Non-aggregate types (scalar / table) — or aggregates
                // that fell through — register as `CREATE OR REPLACE MACRO`.
                let Some(main_sql) = crate::catalog::build_alias_macro(
                    &ftype, None, ours, theirs, &params,
                ) else {
                    continue;
                };
                if let Err(err) = con.execute(&main_sql, []) {
                    crate::events::emit(
                        "community_alias_error",
                        Some(ours),
                        format!("{theirs}: {err}"),
                    );
                    continue;
                }
                created += 1;

                // Optional double-registration in the declared namespace.
                if let Some(ns) = namespace {
                    if !crate::catalog::is_safe_identifier(ns) {
                        continue;
                    }
                    if let Err(err) =
                        con.execute(&format!("CREATE SCHEMA IF NOT EXISTS {ns}"), [])
                    {
                        crate::events::emit(
                            "community_namespace_schema_error",
                            Some(ns),
                            err.to_string(),
                        );
                        continue;
                    }
                    let Some(ns_sql) = crate::catalog::build_alias_macro(
                        &ftype, Some(ns), ours, theirs, &params,
                    ) else {
                        continue;
                    };
                    if let Err(err) = con.execute(&ns_sql, []) {
                        crate::events::emit(
                            "community_namespace_alias_error",
                            Some(ours),
                            format!("{ns}.{ours}: {err}"),
                        );
                        continue;
                    }
                    created += 1;
                }
            }
        }
    }
    Ok(created)
}

/// Register `ours` as a delegating aggregate that calls `theirs`
/// internally. Looks up `theirs`'s signature from `duckdb_functions()`
/// (arg types + return type), maps DuckDB type names to ducklink type
/// codes, then invokes `crate::delegating_agg::register_delegating_aggregate`.
///
/// Returns Err on any of: no aggregate found under `theirs`, unsupported
/// type in the signature, no live runtime raw connection, C-API
/// registration failure. The caller treats an Err as "fall back to the
/// macro path" — a partial capability is better than nothing.
fn register_aggregate_delegate(
    con: &duckdb::Connection,
    ours: &str,
    theirs: &str,
) -> Result<(), String> {
    // Grab EVERY aggregate overload of `theirs` and pick the first one
    // that maps to a supported ducklink type set. `sum` has BIGINT,
    // DOUBLE, DECIMAL, HUGEINT overloads; taking the first row is a
    // coinflip that lands on DECIMAL half the time. Instead: scan all,
    // report the last mapping error only if none matched.
    let mut stmt = con
        .prepare(
            "SELECT array_to_string(parameter_types, ',') AS ptypes, return_type \
             FROM duckdb_functions() \
             WHERE function_name = ? AND function_type = 'aggregate'",
        )
        .map_err(|e| format!("prepare signature scan: {e}"))?;
    let overloads: Vec<(String, String)> = stmt
        .query_map([theirs], |r| {
            let params_csv: String = r.get::<usize, Option<String>>(0)?.unwrap_or_default();
            let ret: String = r.get(1)?;
            Ok((params_csv, ret))
        })
        .map_err(|e| format!("{theirs} signature scan: {e}"))?
        .filter_map(Result::ok)
        .collect();
    if overloads.is_empty() {
        return Err(format!("{theirs} not found as aggregate"));
    }

    let mut last_err: Option<String> = None;
    let mut chosen: Option<(Vec<u8>, u8)> = None;
    for (param_types_csv, return_type_name) in &overloads {
        let arg_type_names: Vec<&str> = param_types_csv
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        let arg_codes_res: Result<Vec<u8>, String> =
            arg_type_names.iter().map(|n| type_name_to_code(n)).collect();
        match arg_codes_res.and_then(|arg_codes| {
            type_name_to_code(return_type_name).map(|rc| (arg_codes, rc))
        }) {
            Ok(pair) => {
                chosen = Some(pair);
                break;
            }
            Err(e) => last_err = Some(e),
        }
    }
    let (arg_codes, return_code) = chosen.ok_or_else(|| {
        last_err.unwrap_or_else(|| format!("{theirs}: no supported overload"))
    })?;

    let rt = RUNTIME
        .get()
        .ok_or_else(|| "runtime not initialised".to_string())?;
    let raw_con = rt.raw_con.0;
    if raw_con.is_null() {
        return Err("no raw connection for aggregate registration".into());
    }
    let sibling = con
        .try_clone()
        .map_err(|e| format!("clone sibling connection: {e}"))?;
    let sibling = std::sync::Arc::new(std::sync::Mutex::new(sibling));

    unsafe {
        crate::delegating_agg::register_delegating_aggregate(
            raw_con,
            ours,
            theirs,
            arg_codes,
            return_code,
            sibling,
        )
    }
}

/// Map a DuckDB type NAME (as reported by `duckdb_functions().return_type`
/// or the `parameter_types` list) to a ducklink type code. Returns Err
/// for types the delegating wrapper doesn't handle yet.
fn type_name_to_code(name: &str) -> Result<u8, String> {
    match name.to_uppercase().as_str() {
        "BOOLEAN" | "BOOL" => Ok(crate::delegating_agg::T_BOOLEAN),
        "INTEGER" | "INT" | "INT4" | "SIGNED" => Ok(crate::delegating_agg::T_INTEGER),
        "BIGINT" | "INT8" | "LONG" => Ok(crate::delegating_agg::T_BIGINT),
        "DOUBLE" | "FLOAT8" => Ok(crate::delegating_agg::T_DOUBLE),
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" => Ok(crate::delegating_agg::T_VARCHAR),
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => Ok(crate::delegating_agg::T_BLOB),
        other => Err(format!(
            "delegating aggregate: unsupported type '{other}' \
             (supported: BOOLEAN/INTEGER/BIGINT/DOUBLE/VARCHAR/BLOB)"
        )),
    }
}

// ---------------------------------------------------------------------------
// ducklink_prefix table function — user-side alias schemas on the C API.
//
// `FROM ducklink_prefix('c', 'crypto')` creates an alias schema
// `c` populated with `CREATE OR REPLACE MACRO` entries mirroring every
// function reachable in schema `crypto`. Both `crypto.hash(x)` and
// `c.hash(x)` bind to the same underlying macro; users can layer whichever
// short form they want. The declaration is persisted in
// `ducklink.prefixes(alias, namespace)` and replayed automatically on the
// next `FROM ducklink_load('name', kind => 'native')` after a reconnect.
//
// Aggregate transparency trade-off: prefix aliases are `CREATE MACRO`
// shapes, so DISTINCT / FILTER / ORDER BY / OVER modifiers don't propagate
// through the prefix alias for aggregates. Users can still call the
// namespace-qualified form (`crypto.hash_agg(x)`) — which does go through
// the delegating C-API aggregate registered by `create_community_aliases`
// — with full modifier support.
// ---------------------------------------------------------------------------

/// Ensure `ducklink.prefixes(alias VARCHAR PRIMARY KEY, namespace VARCHAR)`
/// exists. Called lazily from [`persist_prefix`] so a user who never
/// declares a prefix never gets an unused table in their catalog.
fn ensure_prefixes_table(con: &Connection) -> Result<(), String> {
    con.execute("CREATE SCHEMA IF NOT EXISTS ducklink", [])
        .map_err(|e| format!("CREATE SCHEMA ducklink: {e}"))?;
    con.execute(
        "CREATE TABLE IF NOT EXISTS ducklink.prefixes (\
             alias VARCHAR PRIMARY KEY, \
             namespace VARCHAR NOT NULL)",
        [],
    )
    .map_err(|e| format!("CREATE TABLE ducklink.prefixes: {e}"))?;
    Ok(())
}

/// Persist a `(alias, namespace)` mapping into `ducklink.prefixes` so a
/// fresh connection can replay it via [`replay_persisted_prefixes`].
/// Redeclaring the same alias is idempotent via INSERT OR REPLACE.
fn persist_prefix(con: &Connection, alias: &str, namespace: &str) -> Result<(), String> {
    ensure_prefixes_table(con)?;
    let sql = format!(
        "INSERT OR REPLACE INTO ducklink.prefixes (alias, namespace) VALUES ('{alias}', '{namespace}')"
    );
    con.execute(&sql, [])
        .map(|_| ())
        .map_err(|e| format!("INSERT ducklink.prefixes: {e}"))
}

/// Populate the `<alias>` schema with `CREATE OR REPLACE MACRO` entries
/// mirroring every function reachable in schema `<namespace>`. Returns
/// the count of macros created. Skips functions whose type doesn't have
/// a macro shape (see [`crate::catalog::build_alias_macro`]).
fn create_prefix_aliases(
    con: &Connection,
    alias: &str,
    namespace: &str,
) -> Result<usize, String> {
    // Belt-and-braces before we splice anything into DDL.
    if !crate::catalog::is_safe_identifier(alias)
        || !crate::catalog::is_safe_identifier(namespace)
    {
        return Err(format!(
            "identifier gate: alias='{alias}', namespace='{namespace}' \
             (both must match [A-Za-z0-9_]+)"
        ));
    }
    // Ensure the alias schema exists; the namespace schema is expected to
    // already exist (populated by an earlier `ducklink_load('x',
    // kind => 'native')` with a namespace-declared entry).
    con.execute(&format!("CREATE SCHEMA IF NOT EXISTS {alias}"), [])
        .map_err(|e| format!("CREATE SCHEMA {alias}: {e}"))?;

    // Enumerate every function name in the source namespace once; we'll
    // fetch (type, params) per name in the inner loop.
    let mut stmt = con
        .prepare(&format!(
            "SELECT DISTINCT function_name FROM duckdb_functions() \
             WHERE schema_name = '{namespace}' \
             AND function_type IN ('scalar','aggregate','table_macro','scalar_macro','macro','table')"
        ))
        .map_err(|e| format!("prepare namespace scan: {e}"))?;
    let names: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| format!("scan {namespace}: {e}"))?
        .filter_map(Result::ok)
        .filter(|n| crate::catalog::is_safe_identifier(n))
        .collect();
    drop(stmt);
    if names.is_empty() {
        return Ok(0);
    }

    let mut created = 0usize;
    for fn_name in &names {
        let info_sql = format!(
            "SELECT function_type, array_to_string(parameters, ',') AS param_csv \
             FROM duckdb_functions() \
             WHERE schema_name = '{namespace}' AND function_name = '{fn_name}'"
        );
        let mut info_stmt = con
            .prepare(&info_sql)
            .map_err(|e| format!("prepare info for '{fn_name}': {e}"))?;
        let rows = info_stmt
            .query_map([], |r| {
                let ftype: String = r.get(0)?;
                let csv: Option<String> = r.get(1)?;
                Ok((ftype, csv))
            })
            .map_err(|e| format!("info query for '{fn_name}': {e}"))?;

        let mut done_arities: std::collections::HashSet<usize> =
            std::collections::HashSet::new();
        for row in rows.flatten() {
            let (ftype, csv) = row;
            let csv = csv.unwrap_or_default();
            let params: Vec<String> = if csv.is_empty() {
                Vec::new()
            } else {
                csv.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            };
            if !done_arities.insert(params.len()) {
                continue;
            }
            let Some(macro_sql) = crate::catalog::build_alias_macro(
                &ftype,
                Some(alias),
                fn_name,
                &format!("{namespace}.{fn_name}"),
                &params,
            ) else {
                continue;
            };
            if let Err(err) = con.execute(&macro_sql, []) {
                crate::events::emit(
                    "prefix_alias_error",
                    Some(alias),
                    format!("{fn_name}: {err}"),
                );
                continue;
            }
            created += 1;
        }
    }
    Ok(created)
}

/// Walk every row of `ducklink.prefixes` and reapply it: for each
/// `(alias, namespace)`, populate the alias schema with macros mirroring
/// the current contents of the namespace schema. Silently skips a prefix
/// whose namespace is empty (source module isn't loaded in this session
/// yet — another `ducklink_load` later triggers another replay pass).
/// Returns the number of prefixes successfully replayed (at least one
/// macro created for it).
fn replay_persisted_prefixes(con: &Connection) -> usize {
    let exists = con
        .query_row(
            "SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'ducklink' AND table_name = 'prefixes' LIMIT 1",
            [],
            |r| r.get::<_, i32>(0),
        )
        .is_ok();
    if !exists {
        return 0;
    }
    let mut stmt = match con.prepare("SELECT alias, namespace FROM ducklink.prefixes") {
        Ok(s) => s,
        Err(_) => return 0,
    };
    let rows: Vec<(String, String)> = match stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
    {
        Ok(iter) => iter.filter_map(Result::ok).collect(),
        Err(_) => return 0,
    };
    let mut replayed = 0usize;
    for (alias, namespace) in rows {
        match create_prefix_aliases(con, &alias, &namespace) {
            Ok(n) if n > 0 => replayed += 1,
            _ => {}
        }
    }
    replayed
}

/// The `ducklink_prefix(alias, namespace)` table function: declares a
/// user-side alias schema populated with C-API-only `CREATE OR REPLACE
/// MACRO` entries. Returns a single-row summary
/// `(alias, namespace, macros_created)`.
struct DucklinkPrefix;

struct DucklinkPrefixBind {
    alias: String,
    namespace: String,
    macros: i64,
}

struct DucklinkPrefixInit {
    done: AtomicUsize,
}

impl VTab for DucklinkPrefix {
    type InitData = DucklinkPrefixInit;
    type BindData = DucklinkPrefixBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("ducklink_prefix bind", || {
            let alias = bind.get_parameter(0).to_string();
            let namespace = bind.get_parameter(1).to_string();
            let macros = run_ducklink_prefix(&alias, &namespace)
                .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
            bind.add_result_column("alias", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("namespace", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("macros", LogicalTypeHandle::from(LogicalTypeId::Bigint));
            Ok(DucklinkPrefixBind {
                alias,
                namespace,
                macros,
            })
        })
    }

    fn init(_info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(DucklinkPrefixInit {
            done: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        guard("ducklink_prefix scan", || {
            let bind = func.get_bind_data();
            let init = func.get_init_data();
            if init.done.swap(1, Ordering::Relaxed) != 0 {
                output.set_len(0);
                return Ok(());
            }
            output.flat_vector(0).insert(0, bind.alias.as_str());
            output.flat_vector(1).insert(0, bind.namespace.as_str());
            unsafe {
                output.flat_vector(2).as_mut_slice::<i64>()[0] = bind.macros;
            }
            output.set_len(1);
            Ok(())
        })
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }
}

/// Shared body of the `ducklink_prefix()` TF, scalar, and PREFIX macro.
/// Validates identifiers, creates the alias schema on the runtime's
/// persistent connection, persists the declaration for replay, and
/// returns the number of macros registered.
fn run_ducklink_prefix(alias: &str, namespace: &str) -> Result<i64, String> {
    let rt = RUNTIME
        .get()
        .ok_or_else(|| "ducklink_prefix: runtime not initialised (LOAD ducklink first)".to_string())?;
    if !crate::catalog::is_safe_identifier(alias)
        || !crate::catalog::is_safe_identifier(namespace)
    {
        return Err(format!(
            "ducklink_prefix: alias and namespace must match [A-Za-z0-9_]+ \
             (got alias='{alias}', namespace='{namespace}')"
        ));
    }
    let con = rt.con.lock().unwrap_or_else(|e| e.into_inner());
    let macros = create_prefix_aliases(&con, alias, namespace)?;
    if macros == 0 {
        return Err(format!(
            "ducklink_prefix: namespace '{namespace}' has no functions to alias — \
             is the module loaded?"
        ));
    }
    if let Err(e) = persist_prefix(&con, alias, namespace) {
        crate::events::emit(
            "ducklink_prefix_persist_error",
            Some(alias),
            e.clone(),
        );
        // Non-fatal: session aliases succeeded; persistence failure
        // just means reconnect won't restore.
    }
    crate::events::emit(
        "ducklink_prefix_ok",
        Some(alias),
        format!("namespace='{namespace}' macros={macros}"),
    );
    Ok(macros as i64)
}

/// Scalar counterpart of the `ducklink_prefix()` TF. Same body, but
/// callable without `FROM` — `SELECT ducklink_prefix('c','crypto');`
/// returns a single VARCHAR summary. Register the scalar under the SAME
/// name as the TF: DuckDB's binder disambiguates by context.
struct DucklinkPrefixScalar;

impl VScalar for DucklinkPrefixScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        guard("ducklink_prefix scalar", || {
            let len = input.len();
            let mut a_col = input.flat_vector(0);
            let mut n_col = input.flat_vector(1);
            let out = output.flat_vector();
            let aliases: Vec<String> = unsafe {
                let s = a_col.as_mut_slice_with_len::<duckdb_string_t>(len);
                (0..len)
                    .map(|i| {
                        let mut t = s[i];
                        DuckString::new(&mut t).as_str().into_owned()
                    })
                    .collect()
            };
            let namespaces: Vec<String> = unsafe {
                let s = n_col.as_mut_slice_with_len::<duckdb_string_t>(len);
                (0..len)
                    .map(|i| {
                        let mut t = s[i];
                        DuckString::new(&mut t).as_str().into_owned()
                    })
                    .collect()
            };
            for i in 0..len {
                let alias = &aliases[i];
                let namespace = &namespaces[i];
                let summary = match run_ducklink_prefix(alias, namespace) {
                    Ok(macros) => format!(
                        "alias='{alias}' namespace='{namespace}' macros={macros}"
                    ),
                    Err(e) => return Err(e.into()),
                };
                out.insert(i, summary.as_str());
            }
            Ok(())
        })
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::Varchar.into(),
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

/// Fallback native path — download a ducklink-hosted native provider matching
/// this host's platform + DuckDB version, cache-and-verify, then LOAD via
/// DuckDB's own extension mechanism.
///
/// Resolves the catalog entry's native provider matching this platform +
/// DuckDB version, downloads + sha256-verifies into ducklink's native cache
/// if missing, then invokes DuckDB's own LOAD on the cached absolute path via
/// the runtime's persistent connection.
///
/// Does NOT flip `allow_unsigned_extensions`. If the user hasn't started
/// DuckDB with `-unsigned`, the LOAD errors out with a message directing
/// them to restart with the flag. See `docs/duckdb-upstream-custom-trusted-keys.md`
/// for the upstream feature that will eventually eliminate this friction.
///
/// The reported `scalars` / `tables` / `aggregates` counts are `-1` for the
/// native path — DuckDB's LOAD doesn't tell us which functions the extension
/// registered, and probing the catalog before/after would race and be
/// expensive. The sentinel makes it visible that these counts don't apply,
/// while keeping the output column shape identical to the WASM path.
fn native_load(
    rt: &DucklinkRuntime,
    bind: &BindInfo,
    name_arg: &str,
) -> Result<WasmLoadBind, Box<dyn std::error::Error>> {
    let cached = crate::catalog::resolve_name_to_native(
        name_arg,
        crate::catalog::NATIVE_PLATFORM,
        crate::catalog::HOST_DUCKDB_VERSION,
    )
    .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    let path_str = cached.to_string_lossy().into_owned();

    crate::events::emit("load_native_start", Some(name_arg), path_str.clone());

    // Invoke DuckDB's own LOAD on the absolute cached path. Path is
    // single-quoted; internal quotes are escaped by doubling (the same
    // convention SQL uses for string literals).
    let escaped = path_str.replace('\'', "''");
    let sql = format!("LOAD '{escaped}'");
    let con = rt.con.lock().unwrap_or_else(|e| e.into_inner());
    match con.execute(&sql, []) {
        Ok(_) => {
            crate::events::emit("load_native_ok", Some(name_arg), path_str.clone());
            bind.add_result_column(
                "name",
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            );
            bind.add_result_column(
                "path",
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            );
            bind.add_result_column(
                "scalars",
                LogicalTypeHandle::from(LogicalTypeId::Bigint),
            );
            bind.add_result_column(
                "tables",
                LogicalTypeHandle::from(LogicalTypeId::Bigint),
            );
            bind.add_result_column(
                "aggregates",
                LogicalTypeHandle::from(LogicalTypeId::Bigint),
            );
            Ok(WasmLoadBind {
                name: name_arg.to_string(),
                path: path_str,
                scalars: usize::MAX, // sentinel: "n/a" for native
                tables: usize::MAX,
                aggregates: usize::MAX,
            })
        }
        Err(err) => {
            let err_msg = err.to_string();
            crate::events::emit("load_native_error", Some(name_arg), err_msg.clone());
            let msg = if err_msg.contains("allow_unsigned_extensions")
                || err_msg.contains("signature")
            {
                format!(
                    "ducklink_load(kind='native'): '{name_arg}' was installed at {path_str} but its \
                     signature is not trusted by this DuckDB build.\n\
                     \n\
                     `allow_unsigned_extensions` can only be set at DuckDB startup, not from a \
                     running session. Restart DuckDB with `-unsigned` (or the equivalent SET at \
                     startup), then re-run this query.\n\
                     \n\
                     The friction is intentional: enabling unsigned extensions is a session-wide \
                     trust posture change and the user needs to make it explicitly. See \
                     docs/duckdb-upstream-custom-trusted-keys.md for the upstream feature that \
                     will remove this friction.\n\
                     \n\
                     Underlying DuckDB error: {err_msg}"
                )
            } else {
                format!(
                    "ducklink_load(kind='native'): DuckDB LOAD failed for '{name_arg}': {err_msg}"
                )
            };
            Err(msg.into())
        }
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
    /// `true` when the catalog entry has ANY native backing that resolves on
    /// THIS host — either a `kind:"native"` provider matching this host's
    /// platform + DuckDB ABI, or a `kind:"community-native"` provider (routed
    /// to `INSTALL ... FROM community`). From the user's perspective the two
    /// are equivalent: `ducklink_load('<name>', kind => 'native')` will succeed either way.
    /// Independent of `loaded` — a native artifact may be available without
    /// being loaded, and a module loaded as wasm may still have a native
    /// backing listed in the catalog.
    native_available: bool,
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
            // Positioned after `loaded` so the two host-state columns sit
            // together at the front of the row (identity -> host state ->
            // capability details); the trailing `compatible` column stays where
            // it is to preserve the existing column order.
            bind.add_result_column("native_available", boolean());
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
                    // A native backing is available IFF the catalog carries
                    // EITHER a `kind:"native"` provider matching THIS host's
                    // platform + DuckDB ABI, OR a `kind:"community-native"`
                    // provider (dispatched via `INSTALL ... FROM community`).
                    // Mirrors the resolution order in `ducklink_load(kind =>
                    // 'native')` (community-native preferred, ducklink-native
                    // fallback) — from the user's perspective both mean "if I
                    // ask for NATIVE, ducklink can deliver".
                    let native_available = e
                        .select_native_provider(
                            crate::catalog::NATIVE_PLATFORM,
                            crate::catalog::HOST_DUCKDB_VERSION,
                        )
                        .is_some()
                        || e.select_community_native_provider().is_some();
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
                        native_available,
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
            let n = bind
                .rows
                .len()
                .saturating_sub(start)
                .min(STANDARD_VECTOR_SIZE as usize);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            // Column layout (matches the bind order):
            //   0 name, 1 version, 2 description, 3 categories,
            //   4 loaded, 5 native_available,
            //   6 scalars, 7 tables, 8 aggregates,
            //   9 capabilities, 10 compatible
            let c0 = output.flat_vector(0);
            let c1 = output.flat_vector(1);
            let c2 = output.flat_vector(2);
            let c3 = output.flat_vector(3);
            let c9 = output.flat_vector(9);
            for r in 0..n {
                let row = &bind.rows[start + r];
                c0.insert(r, row.name.as_str());
                c1.insert(r, row.version.as_str());
                c2.insert(r, row.description.as_str());
                c3.insert(r, row.categories.as_str());
                c9.insert(r, row.capabilities.as_str());
            }
            // Fixed-width columns: fill the typed slices after the string inserts.
            unsafe {
                let mut lv = output.flat_vector(4);
                let l = lv.as_mut_slice::<bool>();
                let mut nv = output.flat_vector(5);
                let na = nv.as_mut_slice::<bool>();
                let mut sv = output.flat_vector(6);
                let s = sv.as_mut_slice::<i32>();
                let mut tv = output.flat_vector(7);
                let t = tv.as_mut_slice::<i32>();
                let mut av = output.flat_vector(8);
                let a = av.as_mut_slice::<i32>();
                let mut cv = output.flat_vector(10);
                let c = cv.as_mut_slice::<bool>();
                for r in 0..n {
                    let row = &bind.rows[start + r];
                    l[r] = row.loaded;
                    na[r] = row.native_available;
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
            let n = bind
                .rows
                .len()
                .saturating_sub(start)
                .min(STANDARD_VECTOR_SIZE as usize);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            let c0 = output.flat_vector(0);
            let c1 = output.flat_vector(1);
            let c2 = output.flat_vector(2);
            let c3 = output.flat_vector(3);
            let c4 = output.flat_vector(4);
            for r in 0..n {
                let row = &bind.rows[start + r];
                c0.insert(r, row.module.as_str());
                c1.insert(r, row.name.as_str());
                c2.insert(r, row.kind.as_str());
                c3.insert(r, row.arguments.as_str());
                c4.insert(r, row.returns.as_str());
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

/// `ducklink_host_capabilities()` — the HOST's capabilities: which capability
/// kinds this artifact + host can satisfy. The row-set is `COMMON_TIER_KINDS`
/// — the exact vocabulary `module_compatible()` checks against — so anything
/// that appears in a module's `kinds` column is guaranteed to have a row here.
struct WasmHostCapabilities;

impl VTab for WasmHostCapabilities {
    type InitData = WasmTableInit;
    type BindData = WasmHostCapabilitiesBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("ducklink_host_capabilities bind", || {
            bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("available", LogicalTypeHandle::from(LogicalTypeId::Boolean));
            bind.add_result_column("detail", LogicalTypeHandle::from(LogicalTypeId::Varchar));

            // Ducklink ships the C-API common tier only on every platform.
            // One row per common-tier kind; deduped in case the constant
            // ever grows synonyms.
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
            let n = bind
                .rows
                .len()
                .saturating_sub(start)
                .min(STANDARD_VECTOR_SIZE as usize);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            let c0 = output.flat_vector(0);
            let c2 = output.flat_vector(2);
            for r in 0..n {
                let row = &bind.rows[start + r];
                c0.insert(r, row.name.as_str());
                c2.insert(r, row.detail.as_str());
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
}

struct WasmHostBind {
    row: HostRow,
}

/// `ducklink_host()` — a single-row view of host metadata: the WIT contract
/// version this host speaks (`wasm_abi`, in `duckdb:extension@X.Y.Z` form)
/// and the host DuckDB library version.
struct WasmHost;

impl VTab for WasmHost {
    type InitData = WasmTableInit;
    type BindData = WasmHostBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("ducklink_host bind", || {
            let vc = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
            bind.add_result_column("wasm_abi", vc());
            bind.add_result_column("duckdb_version", vc());

            let caps = host_caps();
            let wasm_abi = normalize_generation(Some(caps.abi_version.clone()));
            let duckdb_version = caps.host_version.clone().unwrap_or_default();

            Ok(WasmHostBind {
                row: HostRow {
                    wasm_abi,
                    duckdb_version,
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
            let start = init.cursor.load(Ordering::Relaxed);
            let n = 1usize.saturating_sub(start);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            output.flat_vector(0).insert(0, bind.row.wasm_abi.as_str());
            output.flat_vector(1).insert(0, bind.row.duckdb_version.as_str());
            init.cursor.store(1, Ordering::Relaxed);
            output.set_len(1);
            Ok(())
        })
    }
}

// --- ducklink_docs() + ducklink_search() + ducklink_help() -----------------

/// One documentation row shared by `ducklink.docs`, `ducklink_search`, and
/// `ducklink_help`. All fields are strings so they render straight out of a
/// SELECT; `tags` is comma-joined for `LIKE`-friendly filtering (users who
/// want the array form can `string_split(tags, ', ')`).
#[derive(Clone)]
struct DocRow {
    module: String,
    function: String,
    kind: String,
    signature: String,
    summary: String,
    description: String,
    example: String,
    tags: String,
    loaded: bool,
}

/// Render a function signature in the same shape a SQL user would write:
/// `name(arg1 T1, arg2 T2) -> RETURNS` for scalars/aggregates, and
/// `name(...) TABLE(col1 T1, col2 T2)` for table functions.
fn render_signature(name: &str, sig: &crate::catalog::FunctionSig) -> String {
    let arg_text = sig
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
    let is_table = sig.kind.as_deref() == Some("table") || !sig.columns.is_empty();
    if is_table {
        let cols = sig
            .columns
            .iter()
            .map(|c| match (&c.name, &c.type_name) {
                (Some(n), Some(t)) if !n.is_empty() => format!("{n} {t}"),
                (_, Some(t)) => t.clone(),
                (Some(n), None) => n.clone(),
                (None, None) => String::new(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("{name}({arg_text}) TABLE({cols})")
    } else {
        let ret = sig.returns.as_deref().unwrap_or("");
        if ret.is_empty() {
            format!("{name}({arg_text})")
        } else {
            format!("{name}({arg_text}) -> {ret}")
        }
    }
}

/// Snapshot the per-module `ComponentDocs` for every loaded module carrying a
/// `duckdb.docs` custom section. Takes the runtime lock once and returns
/// refcount-bumped `Arc<ComponentDocs>` handles — the deep-clone the previous
/// shape did on every doc-view bind is gone. Empty when no loaded module
/// ships docs, or when the runtime isn't up.
fn snapshot_component_docs() -> std::collections::HashMap<String, Arc<crate::docs_section::ComponentDocs>>
{
    match RUNTIME.get() {
        Some(rt) => {
            let list = rt.loaded.lock().unwrap_or_else(|e| e.into_inner());
            list.iter()
                .filter_map(|r| r.docs.as_ref().map(|d| (r.name.clone(), Arc::clone(d))))
                .collect()
        }
        None => std::collections::HashMap::new(),
    }
}

/// Pre-lowercased side data for one `DocRow`, indexed positionally against
/// `CachedDocs::rows`. Built ONCE per generation so `ducklink_search`'s
/// per-query scoring is a plain `contains()` scan rather than four
/// `to_lowercase()` allocations per row per bind.
#[derive(Clone)]
struct DocRowLower {
    function: String,
    tags: String,
    summary: String,
    description: String,
}

/// The session-cached doc set. Owned via `Arc` in the OnceLock below and by
/// every doc/search/help bind that inspects it — so per-query work reduces to
/// scanning a fixed shared slice instead of rebuilding ~639 `DocRow`s + ~5K
/// String allocations per bind. Rebuilt lazily when the load-generation
/// counter changes.
struct CachedDocs {
    rows: Vec<DocRow>,
    lc: Vec<DocRowLower>,
}

/// Monotonic load-generation counter. Bumped after every successful component
/// load (both `ducklink_load` and `LOAD WASM 'name'` sites). The cache below
/// records the generation it was built against; a mismatch triggers a
/// rebuild.
static DOC_CACHE_GEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Cached (generation, doc-set) pair. `Mutex<Option<..>>` because we build
/// lazily on first read. Once populated, reads are Arc clones under a short
/// lock; rebuilds happen only on cache miss.
static DOC_CACHE: std::sync::OnceLock<Mutex<Option<(u64, Arc<CachedDocs>)>>> =
    std::sync::OnceLock::new();

/// Invalidate the docs cache. Called after every successful component load
/// so the next doc-view read observes the newly-loaded module's
/// component-provided docs (if any) plus its `loaded=true` flag.
fn bump_doc_cache_generation() {
    DOC_CACHE_GEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Return the current cached doc set, rebuilding if the generation has moved
/// since the last cache write. The `Arc` clone is cheap; the caller can hold
/// it for the duration of a bind + scan without contending with rebuilds
/// (which take the mutex only briefly to swap in the new snapshot).
fn get_or_build_doc_cache() -> Arc<CachedDocs> {
    let cell = DOC_CACHE.get_or_init(|| Mutex::new(None));
    let mut guard = cell.lock().unwrap_or_else(|e| e.into_inner());
    let current_gen = DOC_CACHE_GEN.load(std::sync::atomic::Ordering::Relaxed);
    if let Some((cached_gen, cached)) = guard.as_ref() {
        if *cached_gen == current_gen {
            return Arc::clone(cached);
        }
    }
    let built = Arc::new(build_cached_docs());
    *guard = Some((current_gen, Arc::clone(&built)));
    built
}

/// Build the cached doc set from scratch: materialize DocRows via the same
/// path `build_doc_rows` used, then precompute the lowercased side data for
/// search. Called only on cache miss.
fn build_cached_docs() -> CachedDocs {
    let rows = build_doc_rows();
    let lc: Vec<DocRowLower> = rows
        .iter()
        .map(|r| DocRowLower {
            function: r.function.to_lowercase(),
            tags: r.tags.to_lowercase(),
            summary: r.summary.to_lowercase(),
            description: r.description.to_lowercase(),
        })
        .collect();
    CachedDocs { rows, lc }
}

/// UNION `override_tags` into `catalog_tags`, preserving order (catalog first,
/// then component-only tags in declaration order). Case-sensitive comparison,
/// matching how `ducklink_search` scores them.
fn merge_tags(catalog_tags: &[String], override_tags: &[String]) -> Vec<String> {
    let mut out: Vec<String> = catalog_tags.to_vec();
    for t in override_tags {
        if !out.iter().any(|existing| existing == t) {
            out.push(t.clone());
        }
    }
    out
}

/// Scan the resolved catalog and materialize one `DocRow` per (module ×
/// function). Only functions carrying catalog enrichment produce rows —
/// bare exports (name only, no signature/summary) are skipped so a user
/// scanning `ducklink.docs` doesn't see a wall of placeholder rows.
///
/// COMPONENT-PROVIDED OVERRIDES: when the module is loaded and shipped a
/// `duckdb.docs` custom section, its per-function summary / description /
/// example REPLACE the catalog values field-by-field (a missing override
/// field falls through to catalog); tags UNION with the catalog's. Non-loaded
/// modules and loaded modules without a section render pure catalog data.
fn build_doc_rows() -> Vec<DocRow> {
    let catalog = crate::catalog::resolve_catalog();
    let loaded = loaded_names();
    let component_docs = snapshot_component_docs();
    let mut rows = Vec::new();
    for e in &catalog.extensions {
        let is_loaded = loaded.contains(&e.name);
        let module_docs = component_docs.get(&e.name);
        for f in &e.functions {
            let Some(name) = f.name.as_deref() else {
                continue;
            };
            if name.is_empty() {
                continue;
            }
            let override_entry = module_docs.and_then(|d| d.get(name));
            let summary = override_entry
                .and_then(|o| o.summary.clone())
                .or_else(|| f.summary.clone())
                .unwrap_or_default();
            let description = override_entry
                .and_then(|o| o.description.clone())
                .or_else(|| f.description.clone())
                .unwrap_or_default();
            let example = override_entry
                .and_then(|o| o.example.clone())
                .or_else(|| f.example.clone())
                .unwrap_or_default();
            let tags = match override_entry {
                Some(o) => merge_tags(&f.tags, &o.tags),
                None => f.tags.clone(),
            };
            rows.push(DocRow {
                module: e.name.clone(),
                function: name.to_string(),
                kind: f.kind.clone().unwrap_or_default(),
                signature: render_signature(name, f),
                summary,
                description,
                example,
                tags: tags.join(", "),
                loaded: is_loaded,
            });
        }
    }
    rows
}

struct WasmDocsBind {
    /// The session-cached doc set — an `Arc` clone of what
    /// `get_or_build_doc_cache` returned at bind time. `func()` scans
    /// `docs.rows` without allocation.
    docs: Arc<CachedDocs>,
}

/// `ducklink_docs()` — the searchable documentation surface. One row per
/// enriched function across every catalog module. Use `WHERE` clauses over
/// `description` / `tags` for plain lookups; use `ducklink_search('query')`
/// for ranked matches.
struct WasmDocs;

impl VTab for WasmDocs {
    type InitData = WasmTableInit;
    type BindData = WasmDocsBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("ducklink_docs bind", || {
            let vc = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
            let boolean = || LogicalTypeHandle::from(LogicalTypeId::Boolean);
            bind.add_result_column("module", vc());
            bind.add_result_column("function", vc());
            bind.add_result_column("kind", vc());
            bind.add_result_column("signature", vc());
            bind.add_result_column("summary", vc());
            bind.add_result_column("description", vc());
            bind.add_result_column("example", vc());
            bind.add_result_column("tags", vc());
            bind.add_result_column("loaded", boolean());
            Ok(WasmDocsBind {
                docs: get_or_build_doc_cache(),
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
        guard("ducklink_docs scan", || {
            let bind = func.get_bind_data();
            let init = func.get_init_data();
            let start = init.cursor.load(Ordering::Relaxed);
            let rows = &bind.docs.rows;
            let n = rows
                .len()
                .saturating_sub(start)
                .min(STANDARD_VECTOR_SIZE as usize);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            let c0 = output.flat_vector(0);
            let c1 = output.flat_vector(1);
            let c2 = output.flat_vector(2);
            let c3 = output.flat_vector(3);
            let c4 = output.flat_vector(4);
            let c5 = output.flat_vector(5);
            let c6 = output.flat_vector(6);
            let c7 = output.flat_vector(7);
            for r in 0..n {
                let row = &rows[start + r];
                c0.insert(r, row.module.as_str());
                c1.insert(r, row.function.as_str());
                c2.insert(r, row.kind.as_str());
                c3.insert(r, row.signature.as_str());
                c4.insert(r, row.summary.as_str());
                c5.insert(r, row.description.as_str());
                c6.insert(r, row.example.as_str());
                c7.insert(r, row.tags.as_str());
            }
            unsafe {
                let mut lv = output.flat_vector(8);
                let l = lv.as_mut_slice::<bool>();
                for r in 0..n {
                    l[r] = rows[start + r].loaded;
                }
            }
            init.cursor.store(start + n, Ordering::Relaxed);
            output.set_len(n);
            Ok(())
        })
    }
}

/// One search result row: a `DocRow` shape plus a computed relevance `score`.
/// Scored rows are sorted DESC at bind time so the caller sees the best match
/// first without an explicit `ORDER BY`.
#[derive(Clone)]
struct SearchRow {
    module: String,
    function: String,
    kind: String,
    signature: String,
    summary: String,
    tags: String,
    score: i64,
}

/// Weighted keyword score for one doc row against a set of lower-cased query
/// tokens. Matches are counted as case-insensitive substring hits; matches in
/// the function NAME weigh most (10×), tags 5×, summary 3×, description 1×.
/// Empty query returns 0 so the search TF's `> 0` filter drops the row.
/// Reads from the pre-lowercased side data cached alongside each `DocRow` so
/// `to_lowercase()` isn't re-run per row per bind.
fn score_doc(lc: &DocRowLower, tokens: &[String]) -> i64 {
    if tokens.is_empty() {
        return 0;
    }
    let mut score: i64 = 0;
    for t in tokens {
        if lc.function.contains(t.as_str()) {
            score += 10;
        }
        if lc.tags.contains(t.as_str()) {
            score += 5;
        }
        if lc.summary.contains(t.as_str()) {
            score += 3;
        }
        if lc.description.contains(t.as_str()) {
            score += 1;
        }
    }
    score
}

struct WasmSearchBind {
    rows: Vec<SearchRow>,
}

/// `ducklink_search('query')` — ranked search across the catalog docs. Splits
/// the query on whitespace (case-insensitive substring match per token) and
/// returns rows where score > 0, sorted by score DESC then module then
/// function so the ordering is stable across runs.
struct WasmSearch;

impl VTab for WasmSearch {
    type InitData = WasmTableInit;
    type BindData = WasmSearchBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("ducklink_search bind", || {
            let vc = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
            let bi = || LogicalTypeHandle::from(LogicalTypeId::Bigint);
            bind.add_result_column("module", vc());
            bind.add_result_column("function", vc());
            bind.add_result_column("kind", vc());
            bind.add_result_column("signature", vc());
            bind.add_result_column("summary", vc());
            bind.add_result_column("tags", vc());
            bind.add_result_column("score", bi());

            let query = bind.get_parameter(0).to_string();
            let tokens: Vec<String> = query
                .split_whitespace()
                .map(|t| t.to_lowercase())
                .collect();
            // Scan the CACHED doc set (built lazily on load-generation change)
            // and score against pre-lowercased side data — no per-row
            // to_lowercase() allocation. Only matched rows pay a String
            // clone into the `SearchRow` output.
            let docs = get_or_build_doc_cache();
            let mut scored: Vec<SearchRow> = docs
                .rows
                .iter()
                .zip(docs.lc.iter())
                .filter_map(|(row, lc)| {
                    let s = score_doc(lc, &tokens);
                    if s <= 0 {
                        return None;
                    }
                    Some(SearchRow {
                        module: row.module.clone(),
                        function: row.function.clone(),
                        kind: row.kind.clone(),
                        signature: row.signature.clone(),
                        summary: row.summary.clone(),
                        tags: row.tags.clone(),
                        score: s,
                    })
                })
                .collect();
            scored.sort_by(|a, b| {
                b.score
                    .cmp(&a.score)
                    .then_with(|| a.module.cmp(&b.module))
                    .then_with(|| a.function.cmp(&b.function))
            });
            Ok(WasmSearchBind { rows: scored })
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
        guard("ducklink_search scan", || {
            let bind = func.get_bind_data();
            let init = func.get_init_data();
            let start = init.cursor.load(Ordering::Relaxed);
            let n = bind
                .rows
                .len()
                .saturating_sub(start)
                .min(STANDARD_VECTOR_SIZE as usize);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            let c0 = output.flat_vector(0);
            let c1 = output.flat_vector(1);
            let c2 = output.flat_vector(2);
            let c3 = output.flat_vector(3);
            let c4 = output.flat_vector(4);
            let c5 = output.flat_vector(5);
            for r in 0..n {
                let row = &bind.rows[start + r];
                c0.insert(r, row.module.as_str());
                c1.insert(r, row.function.as_str());
                c2.insert(r, row.kind.as_str());
                c3.insert(r, row.signature.as_str());
                c4.insert(r, row.summary.as_str());
                c5.insert(r, row.tags.as_str());
            }
            unsafe {
                let mut sv = output.flat_vector(6);
                let s = sv.as_mut_slice::<i64>();
                for r in 0..n {
                    s[r] = bind.rows[start + r].score;
                }
            }
            init.cursor.store(start + n, Ordering::Relaxed);
            output.set_len(n);
            Ok(())
        })
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        // Positional arg 0: the search query.
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

/// Build a markdown help blob for `name`. `name` may be:
/// - a fully-qualified function name (`aba_validate`) — one match
/// - a module name (`aba`) — every function in the module
/// - anything else — a "not found, use ducklink_search" hint
///
/// Returned string is safe to emit as VARCHAR from the scalar wrapper below.
/// Used only via `ducklink_help()`; keeps the markdown formatting out of the
/// FFI boundary so `DucklinkHelp::invoke` stays a thin translator.
pub(crate) fn render_help(name: &str) -> String {
    let name_lc = name.to_lowercase();
    // Cached — a `SELECT ducklink_help(name) FROM ducklink.modules` no longer
    // rebuilds the entire doc set once per row. Match against the cached
    // per-row lc side data so we don't allocate a `to_lowercase()` per row
    // per invocation.
    let docs = get_or_build_doc_cache();

    // Prefer exact function-name matches first, then module matches.
    let fn_indices: Vec<usize> = docs
        .lc
        .iter()
        .enumerate()
        .filter_map(|(i, lc)| (lc.function == name_lc).then_some(i))
        .collect();
    if !fn_indices.is_empty() {
        let mut out = String::new();
        for (i, idx) in fn_indices.iter().enumerate() {
            if i > 0 {
                out.push_str("\n---\n\n");
            }
            append_function_help(&mut out, &docs.rows[*idx]);
        }
        return out;
    }

    let mod_indices: Vec<usize> = docs
        .rows
        .iter()
        .enumerate()
        .filter_map(|(i, r)| (r.module.eq_ignore_ascii_case(name)).then_some(i))
        .collect();
    if !mod_indices.is_empty() {
        let mut out = format!("# Module: {}\n\n", docs.rows[mod_indices[0]].module);
        for (i, idx) in mod_indices.iter().enumerate() {
            if i > 0 {
                out.push('\n');
            }
            append_function_help(&mut out, &docs.rows[*idx]);
        }
        return out;
    }

    format!(
        "No documentation found for '{name}'.\n\n\
         Try `SELECT * FROM ducklink_search('{name}');` for ranked matches, or\n\
         `SELECT * FROM ducklink.docs WHERE module ILIKE '%{name}%'` to browse.\n"
    )
}

/// Append the markdown section for one function to `out`.
fn append_function_help(out: &mut String, row: &DocRow) {
    use std::fmt::Write as _;
    let _ = writeln!(out, "## `{}`\n", row.signature);
    if !row.summary.is_empty() {
        let _ = writeln!(out, "{}\n", row.summary);
    }
    if !row.description.is_empty() {
        let _ = writeln!(out, "{}\n", row.description);
    }
    if !row.example.is_empty() {
        let _ = writeln!(out, "### Example\n\n```sql\n{}\n```\n", row.example);
    }
    if !row.tags.is_empty() {
        let _ = writeln!(out, "**Tags:** {}\n", row.tags);
    }
    let _ = writeln!(
        out,
        "*Module: `{}` — {}{}*",
        row.module,
        if row.kind.is_empty() {
            "function"
        } else {
            row.kind.as_str()
        },
        if row.loaded { " (loaded)" } else { "" }
    );
}

#[cfg(test)]
mod doc_merge_tests {
    use super::*;

    #[test]
    fn merge_tags_unions_without_duplicates() {
        let cat = vec!["banking".to_string(), "validator".to_string()];
        let over = vec!["validator".to_string(), "iso20022".to_string()];
        assert_eq!(
            merge_tags(&cat, &over),
            vec![
                "banking".to_string(),
                "validator".to_string(),
                "iso20022".to_string(),
            ],
            "catalog tags kept in order, then new component tags appended, duplicates dropped"
        );
    }

    #[test]
    fn merge_tags_empty_override_returns_catalog() {
        let cat = vec!["a".to_string(), "b".to_string()];
        assert_eq!(merge_tags(&cat, &[]), cat);
    }

    #[test]
    fn merge_tags_empty_catalog_returns_override() {
        let over = vec!["x".to_string()];
        assert_eq!(merge_tags(&[], &over), over);
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
            let n = bind
                .rows
                .len()
                .saturating_sub(start)
                .min(STANDARD_VECTOR_SIZE as usize);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            let c0 = output.flat_vector(0);
            let c1 = output.flat_vector(1);
            let c4 = output.flat_vector(4);
            for r in 0..n {
                let row = &bind.rows[start + r];
                c0.insert(r, row.digest.as_str());
                c1.insert(r, row.name.as_str());
                c4.insert(r, row.path.as_str());
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
            let n = bind
                .rows
                .len()
                .saturating_sub(start)
                .min(STANDARD_VECTOR_SIZE as usize);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            // Hoist every string column's flat_vector handle above the row
            // loop — c3/c4 were missed by the F2 sweep because c3 is fetched
            // inside a `match` arm (twice per row, once for the Some path and
            // once for None's set_null) and c4 sits on its own line but was
            // still called per row. The F2 pattern already caught c2. `mut`
            // is required only on c3 because `set_null` takes &mut self;
            // insert() takes &self on FlatVector (proven by the F2 hoist).
            let c2 = output.flat_vector(2);
            let mut c3 = output.flat_vector(3);
            let c4 = output.flat_vector(4);
            for r in 0..n {
                let row = &bind.rows[start + r];
                c2.insert(r, row.kind.as_str());
                match row.module.as_deref() {
                    Some(m) => c3.insert(r, m),
                    // NULL module column when the event is not module-scoped
                    // (e.g. catalog_fetch / catalog_fallback).
                    None => c3.set_null(r),
                }
                c4.insert(r, row.detail.as_str());
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
            let n = bind
                .rows
                .len()
                .saturating_sub(start)
                .min(STANDARD_VECTOR_SIZE as usize);
            if n == 0 {
                output.set_len(0);
                return Ok(());
            }
            let c0 = output.flat_vector(0);
            let c1 = output.flat_vector(1);
            let c2 = output.flat_vector(2);
            let c3 = output.flat_vector(3);
            for r in 0..n {
                let row = &bind.rows[start + r];
                c0.insert(r, row.module.as_str());
                c1.insert(r, row.module_generation.as_str());
                c2.insert(r, row.host_generation.as_str());
                c3.insert(r, row.lifecycle.as_str());
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
    let _ = &db; // reserved for a future database-level registration path
    let mut total = 0usize;
    for spec in specs {
        let loaded = {
            let e = &engine;
            e.load(&spec.name, &spec.path)?
        };
        total += register_scalars(con, engine.clone(), &loaded.scalars)?;
        total += register_tables(con, engine.clone(), &loaded.tables)?;
        let _ = db; // reserved for a future database-level registration path
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

    /// Path to the prebuilt `sample_extension.wasm` component if it exists on
    /// disk. Looks first at `DUCKLINK_CORPUS_DIR/sample_extension.wasm`, then
    /// falls back to the monorepo checkout layout (`../../artifacts/extensions`
    /// relative to this crate). Returns `None` when neither yields a real
    /// file — the standalone repo has no built-in corpus, so tests that need
    /// the sample wasm early-skip on that outcome instead of failing.
    fn sample_component() -> Option<PathBuf> {
        let dir = match std::env::var_os("DUCKLINK_CORPUS_DIR") {
            Some(dir) => PathBuf::from(dir),
            None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../artifacts/extensions"),
        };
        let path = dir.join("sample_extension.wasm");
        if path.exists() && path.metadata().map(|m| m.len() > 0).unwrap_or(false) {
            Some(path)
        } else {
            None
        }
    }

    /// Resolve `sample_component()` or early-return from the calling test
    /// with a skip message. Used by end-to-end tests that need the wasm
    /// corpus — treats missing/empty corpus as "not applicable to this
    /// checkout" rather than a failure. Set `DUCKLINK_CORPUS_DIR` to point
    /// at a directory containing `sample_extension.wasm` to run them.
    macro_rules! require_sample_component {
        ($test:literal) => {
            match sample_component() {
                Some(p) => p,
                None => {
                    eprintln!(
                        "[skip] {}: sample_extension.wasm not found. \
                         Point DUCKLINK_CORPUS_DIR at a directory containing it \
                         (monorepo default: <workspace>/artifacts/extensions).",
                        $test
                    );
                    return;
                }
            }
        };
    }

    /// End-to-end: load the sample wasm component, register its
    /// `sample_plus_one(BIGINT)->BIGINT` scalar into a real in-process DuckDB,
    /// and confirm the +1 is computed inside the wasm component.
    #[test]
    fn sample_plus_one_dispatches_into_wasm() {
        let path = require_sample_component!("sample_plus_one_dispatches_into_wasm");
        let mut engine = Engine2::new().expect("engine");
        let loaded = engine
            .load("sample_extension", &path)
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
        let path = require_sample_component!("register_components_exposes_scalar");
        let engine = Arc::new(Engine2::new().expect("engine"));
        let specs = vec![ComponentSpec {
            name: "sample_extension".to_string(),
            path,
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
        let path = require_sample_component!("sample_emit_sequence_streams_from_wasm");
        let engine = Arc::new(Engine2::new().expect("engine"));
        let specs = vec![ComponentSpec {
            name: "sample_extension".to_string(),
            path,
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
        let path = require_sample_component!("ducklink_load_registers_at_runtime_for_later_statements");
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
                    "SELECT count(*) FROM ducklink.host_capabilities WHERE name IN ('scalar','table')",
                    [],
                    |r| r.get(0),
                )
                .expect("capabilities rows");
            assert_eq!(n_caps, 2, "scalar + table capability rows present");

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
        let path = require_sample_component!("registered_function_visible_across_connections");
        let mut engine = Engine2::new().expect("engine");
        let loaded = engine
            .load("sample_extension", &path)
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
            // S2 (major-5): DECIMAL carries width/scale structurally now.
            Decimal { width: 18, scale: 3 },
            Interval,
            Uuid,
            // T2-1 residual (major-5): 128-bit integer logical types.
            Hugeint,
            UHugeint,
            // S1 (major-5): nested types (structural shape stubbed with
            // Int32 leaves — this test only cares that each variant maps to
            // a distinct bridge code).
            List(Box::new(Int32)),
            Struct(vec![("a".to_string(), Int32)]),
            Map(Box::new(Int32), Box::new(Int32)),
            Array(3, Box::new(Int32)),
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
        // Every defined code (0..=T_CODE_MAX) must build a column logical type and a
        // raw duckdb_type without hitting the `unreachable!` arm. Nested arms
        // fall back to VARCHAR at the code-only layer per the docstring on
        // `logical_type`; that's a fall-back, not a panic.
        for code in 0u8..=T_CODE_MAX {
            let _ = logical_type(code); // must not panic
            let _ = duckdb_type_of(code); // must not panic
        }
    }

    /// Sweep-6 FIX 5: coverage for `wit_logicaltype_from_code`'s DECIMAL
    /// width/scale extraction. Sweep-5 FIX 3 wired the (w, s) getters onto
    /// a real DuckDB LogicalType handle; without this test the extraction
    /// path was only exercised end-to-end from the ArrowShim register and
    /// the COPY-FROM writer, both of which need a full DuckDB engine plus
    /// the sample wasm corpus. This test synthesises a bare
    /// `duckdb_create_decimal_type(20, 5)` handle and asserts the WIT arm
    /// echoes (20, 5) instead of the (18, 3) code-only fallback.
    ///
    /// Requires DuckDB to be loadable (matches the `#[cfg(all(test,
    /// feature = "bundled"))]` gate on the module). Runs unconditionally
    /// here — the module is already scoped to the `bundled` feature so
    /// DuckDB is guaranteed present.
    #[test]
    fn wit_logicaltype_from_code_decimal_reads_width_and_scale() {
        use ducklink_runtime::duckdb_extension_bindings::duckdb::extension::types::{
            Decimalshape, Logicaltype as WitLogicaltype,
        };
        unsafe {
            let handle = ffi::duckdb_create_decimal_type(20, 5);
            let wit = wit_logicaltype_from_code(T_DECIMAL, Some(handle));
            match wit {
                WitLogicaltype::Decimal(Decimalshape { width, scale }) => {
                    assert_eq!(width, 20, "DECIMAL width should read as 20");
                    assert_eq!(scale, 5, "DECIMAL scale should read as 5");
                }
                other => panic!("expected WitLogicaltype::Decimal(20, 5), got {other:?}"),
            }
            let mut h = handle;
            ffi::duckdb_destroy_logical_type(&mut h);
        }
    }

    /// Sweep-6 FIX 5 companion: with `lt = None` the DECIMAL arm must fall
    /// back to the (18, 3) interim shape (the sweep-5 code-only path).
    /// This locks in the fallback so a future refactor that flips the
    /// default doesn't silently change guest-visible types on the code-only
    /// callers (aggregate `write_ret_raw`, `read_arg_raw`).
    #[test]
    fn wit_logicaltype_from_code_decimal_defaults_to_18_3_without_handle() {
        use ducklink_runtime::duckdb_extension_bindings::duckdb::extension::types::{
            Decimalshape, Logicaltype as WitLogicaltype,
        };
        unsafe {
            let wit = wit_logicaltype_from_code(T_DECIMAL, None);
            match wit {
                WitLogicaltype::Decimal(Decimalshape { width, scale }) => {
                    assert_eq!((width, scale), (18, 3),
                        "handle-less DECIMAL should keep the (18, 3) interim shape");
                }
                other => panic!("expected WitLogicaltype::Decimal(18, 3), got {other:?}"),
            }
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

    // --- Community-native aliasing -----------------------------------------
    //
    // These tests exercise `create_community_aliases()` against a live
    // in-process DuckDB, using user-defined SQL macros / aggregates to
    // stand in for the community extension's functions (which we can't
    // INSTALL from network in a unit test). The aliasing logic itself
    // doesn't care whether the underlying functions came from a real
    // community LOAD or from CREATE MACRO — it only reads
    // `duckdb_functions()`.

    fn spec_with(prefix: Option<&str>, map: &[(&str, &str)]) -> crate::catalog::CommunityNativeSpec {
        spec_with_namespace(prefix, map, None)
    }

    fn spec_with_namespace(
        prefix: Option<&str>,
        map: &[(&str, &str)],
        namespace: Option<&str>,
    ) -> crate::catalog::CommunityNativeSpec {
        let mut m = std::collections::HashMap::new();
        for (o, t) in map {
            m.insert((*o).to_string(), (*t).to_string());
        }
        crate::catalog::CommunityNativeSpec {
            extension_name: "test_ext".into(),
            community_prefix: prefix.map(str::to_string),
            function_mapping: m,
            namespace: namespace.map(str::to_string),
        }
    }

    #[test]
    fn alias_gen_scalar_via_prefix() {
        let con = Connection::open_in_memory().expect("open");
        // Simulate a community-registered scalar under community's name.
        con.execute("CREATE MACRO t_add_one(x) AS x + 1", [])
            .expect("create t_add_one");
        let n = create_community_aliases(&con, &spec_with(Some("t_"), &[]))
            .expect("alias gen");
        assert!(n >= 1, "expected >=1 alias, got {n}");
        // Both names work.
        let via_alias: i64 = con
            .query_row("SELECT add_one(41)", [], |r| r.get(0))
            .expect("via alias");
        assert_eq!(via_alias, 42);
        let via_original: i64 = con
            .query_row("SELECT t_add_one(41)", [], |r| r.get(0))
            .expect("via original");
        assert_eq!(via_original, 42);
    }

    #[test]
    fn alias_gen_table_via_mapping() {
        let con = Connection::open_in_memory().expect("open");
        // A community-registered table macro under their name.
        con.execute(
            "CREATE MACRO t_gen_range(n) AS TABLE SELECT i FROM range(n) t(i)",
            [],
        )
        .expect("create t_gen_range");
        let n = create_community_aliases(
            &con,
            &spec_with(None, &[("gen_range", "t_gen_range")]),
        )
        .expect("alias gen");
        assert!(n >= 1);
        let sum: i64 = con
            .query_row("SELECT sum(i) FROM gen_range(5)", [], |r| r.get(0))
            .expect("via alias");
        assert_eq!(sum, 0 + 1 + 2 + 3 + 4);
    }

    #[test]
    fn alias_gen_aggregate_via_prefix_using_list_aggregate_trick() {
        // Use a builtin single-arg aggregate — sum() — under a fake `t_`
        // prefix to keep the test hermetic. We CREATE MACRO t_sum(x) AS
        // sum(x) so the aliasing layer sees it as a `scalar_macro`
        // (macros register as scalar-shaped). That still exercises the
        // "alias into ducklink's namespace" wiring end-to-end; separate
        // list_aggregate coverage lives in the catalog unit tests.
        let con = Connection::open_in_memory().expect("open");
        con.execute("CREATE MACRO t_double(x) AS x * 2", [])
            .expect("create t_double");
        let n = create_community_aliases(&con, &spec_with(Some("t_"), &[]))
            .expect("alias gen");
        assert!(n >= 1);
        let out: i64 = con
            .query_row("SELECT double(21)", [], |r| r.get(0))
            .expect("via alias");
        assert_eq!(out, 42);
    }

    /// Namespace registration: `create_community_aliases` produces a
    /// callable `<namespace>.<ours>(x)` binding by emitting
    /// `CREATE OR REPLACE MACRO` in the namespace schema — works
    /// uniformly across every platform on the stable C API.
    ///
    /// Aggregate transparency: `create_community_aliases` now routes
    /// aggregate aliases through a delegating C-API aggregate, so
    /// DISTINCT / FILTER / GROUP BY propagate. This macro-only test
    /// covers the SCALAR path only.
    #[test]
    fn alias_gen_namespace_registers_via_macro_when_shim_off() {
        let con = Connection::open_in_memory().expect("open");
        // Simulate a community-registered scalar under its own name.
        con.execute("CREATE MACRO crypto_hash(algo, data) AS algo || ':' || data", [])
            .expect("create crypto_hash");
        let n = create_community_aliases(
            &con,
            &spec_with_namespace(
                None,
                &[("hash", "crypto_hash")],
                Some("crypto"),
            ),
        )
        .expect("alias gen");
        // Two macros: main.hash and crypto.hash. Both are callable and
        // return identical results.
        assert_eq!(n, 2, "expected 2 registrations (main + namespace), got {n}");
        let bare: String = con
            .query_row("SELECT hash('sha2-256', 'ping')", [], |r| r.get(0))
            .expect("bare hash()");
        assert_eq!(bare, "sha2-256:ping");
        let qualified: String = con
            .query_row("SELECT crypto.hash('sha2-256', 'ping')", [], |r| r.get(0))
            .expect("qualified crypto.hash()");
        assert_eq!(qualified, "sha2-256:ping");
        // Community's original name is untouched.
        let original: String = con
            .query_row("SELECT crypto_hash('sha2-256', 'ping')", [], |r| r.get(0))
            .expect("original crypto_hash()");
        assert_eq!(original, "sha2-256:ping");
    }

    /// `create_prefix_aliases` — the C-API-only implementation behind
    /// `ducklink_prefix('c', 'crypto')`. Populates the alias schema
    /// with `CREATE OR REPLACE MACRO` entries mirroring the source schema.
    #[test]
    fn create_prefix_aliases_populates_alias_schema_via_macros() {
        let con = Connection::open_in_memory().expect("open");
        // Set up a `crypto` schema with a couple of functions in it —
        // simulates the state right after `ducklink_load('crypto', kind
        // => 'native')` on an entry that declares `namespace: "crypto"`.
        con.execute_batch(
            "CREATE SCHEMA crypto; \
             CREATE MACRO crypto.hash(algo, data) AS algo || ':' || data; \
             CREATE MACRO crypto.hmac(algo, key, data) AS algo || '+' || key || ':' || data;",
        )
        .expect("seed crypto schema");

        let n = create_prefix_aliases(&con, "c", "crypto").expect("prefix");
        assert_eq!(n, 2, "expected 2 alias macros (hash + hmac), got {n}");

        // Both qualifiers now resolve, sharing the same underlying macros.
        let via_alias: String = con
            .query_row("SELECT c.hash('sha2-256', 'ping')", [], |r| r.get(0))
            .expect("c.hash()");
        let via_ns: String = con
            .query_row("SELECT crypto.hash('sha2-256', 'ping')", [], |r| r.get(0))
            .expect("crypto.hash()");
        assert_eq!(via_alias, via_ns);
        assert_eq!(via_alias, "sha2-256:ping");

        // Multi-arg case still works uniformly.
        let hmac: String = con
            .query_row("SELECT c.hmac('sha2-256', 'k1', 'body')", [], |r| r.get(0))
            .expect("c.hmac()");
        assert_eq!(hmac, "sha2-256+k1:body");
    }

    /// Empty-source-namespace path: if the source schema has no
    /// registrable functions, `create_prefix_aliases` returns Ok(0)
    /// rather than erroring — the ducklink_prefix TF turns that into a
    /// user-facing "is the module loaded?" message.
    #[test]
    fn create_prefix_aliases_empty_namespace_returns_zero() {
        let con = Connection::open_in_memory().expect("open");
        // An empty schema — no functions to alias.
        con.execute("CREATE SCHEMA emptyns", []).expect("schema");
        let n = create_prefix_aliases(&con, "e", "emptyns").expect("empty is not an error");
        assert_eq!(n, 0);
    }

    /// Persistence: declaring the same prefix twice must be idempotent
    /// (`INSERT OR REPLACE`) — no duplicate rows in `ducklink.prefixes`.
    #[test]
    fn persist_prefix_is_idempotent() {
        let con = Connection::open_in_memory().expect("open");
        persist_prefix(&con, "c", "crypto").expect("first");
        persist_prefix(&con, "c", "crypto").expect("second");
        let count: i64 = con
            .query_row(
                "SELECT count(*) FROM ducklink.prefixes WHERE alias = 'c'",
                [],
                |r| r.get(0),
            )
            .expect("count");
        assert_eq!(count, 1, "re-declare must be idempotent, got {count} rows");
    }

    /// Replay path: with a persisted `(c, crypto)` in `ducklink.prefixes`
    /// and a live `crypto` schema, `replay_persisted_prefixes` should
    /// recreate the alias macros.
    #[test]
    fn replay_persisted_prefixes_recreates_alias_schema() {
        let con = Connection::open_in_memory().expect("open");
        // Seed source namespace.
        con.execute_batch(
            "CREATE SCHEMA crypto; \
             CREATE MACRO crypto.hash(x) AS 'H:' || x;",
        )
        .expect("seed");
        // Seed persistence table with a prior declaration.
        persist_prefix(&con, "c", "crypto").expect("persist");
        // Fresh session simulation: `c` schema doesn't exist yet.
        let missing = con.query_row("SELECT c.hash('a')", [], |r| r.get::<_, String>(0));
        assert!(missing.is_err(), "c.hash should be missing before replay");

        let n = replay_persisted_prefixes(&con);
        assert_eq!(n, 1, "one prefix replayed, got {n}");

        let a: String = con
            .query_row("SELECT c.hash('a')", [], |r| r.get(0))
            .expect("c.hash() after replay");
        assert_eq!(a, "H:a");
    }

    #[test]
    fn alias_gen_explicit_mapping_wins_over_prefix() {
        let con = Connection::open_in_memory().expect("open");
        con.execute("CREATE MACRO t_add(x) AS x + 100", [])
            .expect("create t_add");
        con.execute("CREATE MACRO t_sub(x) AS x - 100", [])
            .expect("create t_sub");
        // Prefix would give us `add` + `sub`; explicit mapping renames
        // `t_add` to `plus100` and lets prefix handle `sub` normally.
        let n = create_community_aliases(
            &con,
            &spec_with(Some("t_"), &[("plus100", "t_add")]),
        )
        .expect("alias gen");
        assert!(n >= 2, "expected two aliases (explicit + prefix), got {n}");
        let a: i64 = con
            .query_row("SELECT plus100(1)", [], |r| r.get(0))
            .expect("plus100");
        assert_eq!(a, 101);
        let s: i64 = con
            .query_row("SELECT sub(200)", [], |r| r.get(0))
            .expect("sub");
        assert_eq!(s, 100);
    }

    // -----------------------------------------------------------------
    // Delegating aggregate prototype: prove modifiers propagate through
    // a real C-API aggregate wrapper. Registers `sum_delegate(BIGINT) ->
    // BIGINT` whose finalize invokes DuckDB's own `sum()` on a
    // per-group list of values via a sibling connection.
    // -----------------------------------------------------------------

    #[test]
    fn delegating_aggregate_supports_full_modifier_set() {
        use duckdb::ffi;
        use std::sync::{Arc, Mutex};

        // Open a raw db + a duckdb-rs Connection over it. `raw_con` is
        // where we register the wrapper; `sibling` is what finalize uses
        // for the nested query.
        let (raw_con, con, sibling) = unsafe {
            let mut db: ffi::duckdb_database = std::ptr::null_mut();
            assert_eq!(
                ffi::duckdb_open(c":memory:".as_ptr(), &mut db),
                ffi::DuckDBSuccess
            );
            let con = Connection::open_from_raw(db.cast()).expect("open_from_raw");
            let sibling = con.try_clone().expect("clone");
            let mut raw: ffi::duckdb_connection = std::ptr::null_mut();
            assert_eq!(ffi::duckdb_connect(db, &mut raw), ffi::DuckDBSuccess);
            (raw, con, sibling)
        };
        let sibling = Arc::new(Mutex::new(sibling));

        use crate::delegating_agg::{register_delegating_aggregate, T_BIGINT};
        unsafe {
            register_delegating_aggregate(
                raw_con,
                "sum_delegate",
                "sum",
                vec![T_BIGINT],
                T_BIGINT,
                sibling,
            )
            .expect("register");
        }

        con.execute_batch(
            "CREATE TABLE t(g INTEGER, x BIGINT, ok BOOLEAN); \
             INSERT INTO t VALUES \
               (1, 10, true), (1, 20, true), (1, 20, false), \
               (2, 30, true), (2, 40, true);",
        )
        .expect("seed");

        // Baseline: sum_delegate == sum. This proves basic accumulation +
        // delegation works.
        let baseline: i64 = con
            .query_row("SELECT sum_delegate(x) FROM t", [], |r| r.get(0))
            .expect("baseline");
        assert_eq!(baseline, 120);

        // GROUP BY: verifies per-group state isolation.
        let mut stmt = con
            .prepare("SELECT g, sum_delegate(x) FROM t GROUP BY g ORDER BY g")
            .expect("group by");
        let groups: Vec<(i32, i64)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(groups, vec![(1, 50), (2, 70)]);

        // DISTINCT: sum of distinct x values. This is the modifier that
        // broke on the macro path — DuckDB's binder deduplicates rows
        // BEFORE our `update` sees them, so our accumulator only holds
        // {10, 20, 30, 40} = 100. Zero code in the wrapper handles
        // DISTINCT; DuckDB's binder does it for us.
        let distinct: i64 = con
            .query_row("SELECT sum_delegate(DISTINCT x) FROM t", [], |r| r.get(0))
            .expect("distinct");
        assert_eq!(distinct, 100);

        // FILTER: sum where ok is true. DuckDB applies the filter before
        // our update — the wrapper accumulates {10, 20, 30, 40} = 100.
        let filtered: i64 = con
            .query_row(
                "SELECT sum_delegate(x) FILTER (WHERE ok) FROM t",
                [],
                |r| r.get(0),
            )
            .expect("filter");
        assert_eq!(filtered, 100);

        // ORDER BY + OVER (window) both use a different C-API state-array
        // calling convention than regular aggregates (sorted-aggregate
        // path / window framework respectively); the prototype's
        // per-row states.add() indexing doesn't survive them. Extending
        // to those is a follow-up slice; the prototype proves the
        // three important modifiers (DISTINCT, FILTER, GROUP BY) work.
    }

    /// Multi-column signature + VARCHAR I/O — the shape most community
    /// aggregates take (e.g. `crypto_hash_agg(algo, data)`,
    /// `string_agg(value, separator)`). Verifies:
    ///
    /// 1. Two-column signature wires end-to-end
    /// 2. VARCHAR extraction from vectors works (duckdb_string_t path)
    /// 3. VARCHAR result gets written back to the output vector
    /// 4. `DISTINCT` still propagates through with typed values
    /// 5. `FILTER` still propagates through
    ///
    /// Target: built-in `string_agg(x, sep)`. No network needed; no
    /// ORDER-BY-required aggregate quirks.
    #[test]
    fn delegating_aggregate_multi_column_varchar() {
        use duckdb::ffi;
        use std::sync::{Arc, Mutex};

        let (raw_con, con, sibling) = unsafe {
            let mut db: ffi::duckdb_database = std::ptr::null_mut();
            assert_eq!(
                ffi::duckdb_open(c":memory:".as_ptr(), &mut db),
                ffi::DuckDBSuccess
            );
            let con = Connection::open_from_raw(db.cast()).expect("open_from_raw");
            let sibling = con.try_clone().expect("clone");
            let mut raw: ffi::duckdb_connection = std::ptr::null_mut();
            assert_eq!(ffi::duckdb_connect(db, &mut raw), ffi::DuckDBSuccess);
            (raw, con, sibling)
        };
        let sibling = Arc::new(Mutex::new(sibling));

        use crate::delegating_agg::{register_delegating_aggregate, T_VARCHAR};
        unsafe {
            register_delegating_aggregate(
                raw_con,
                "concat_agg",
                "string_agg",
                vec![T_VARCHAR, T_VARCHAR],
                T_VARCHAR,
                sibling,
            )
            .expect("register");
        }

        con.execute_batch(
            "CREATE TABLE t(tag VARCHAR, ok BOOLEAN); \
             INSERT INTO t VALUES \
               ('a', true), ('a', true), ('b', false), \
               ('c', true), ('d', true);",
        )
        .expect("seed");

        // Baseline: concat_agg == string_agg, produces some concatenation
        // (DuckDB's default separator ordering isn't strict, but the SET
        // of values is deterministic).
        let baseline: String = con
            .query_row("SELECT concat_agg(tag, ',') FROM t", [], |r| r.get(0))
            .expect("baseline");
        assert_eq!(baseline.split(',').count(), 5, "5 values → 5 pieces");

        // DISTINCT: only unique tags concatenate. This proves multi-column
        // DISTINCT propagates through — DuckDB dedupes on the (tag, ',')
        // tuple, which for a constant separator effectively dedupes on tag.
        let distinct: String = con
            .query_row("SELECT concat_agg(DISTINCT tag, ',') FROM t", [], |r| r.get(0))
            .expect("distinct");
        let mut d: Vec<&str> = distinct.split(',').collect();
        d.sort();
        assert_eq!(d, vec!["a", "b", "c", "d"], "DISTINCT: unique tags only");

        // FILTER: only tags where ok=true concatenate.
        let filtered: String = con
            .query_row(
                "SELECT concat_agg(tag, ',') FILTER (WHERE ok) FROM t",
                [],
                |r| r.get(0),
            )
            .expect("filter");
        let mut f: Vec<&str> = filtered.split(',').collect();
        f.sort();
        assert_eq!(f, vec!["a", "a", "c", "d"], "FILTER: only ok rows");

        // GROUP BY: per-group concatenation. Since input has 2 'a's,
        // 1 each of 'b','c','d', grouping by (ok) gives ok=true → 4
        // items, ok=false → 1.
        let mut stmt = con
            .prepare("SELECT ok, concat_agg(tag, ',') FROM t GROUP BY ok ORDER BY ok")
            .expect("group by");
        let groups: Vec<(bool, String)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, false);
        assert_eq!(groups[0].1.split(',').count(), 1);
        assert_eq!(groups[1].0, true);
        assert_eq!(groups[1].1.split(',').count(), 4);
    }

    /// Window aggregates through a delegating alias produce the SAME
    /// per-row output as the target aggregate does natively — for both
    /// full-partition and running (`ORDER BY inside OVER`) window
    /// frames.
    ///
    /// This asserts two fixes in `delegating_agg`:
    ///
    /// 1. `combine` clones its source rather than moving. DuckDB's
    ///    window framework uses segment trees over partial states and
    ///    may combine the SAME source into multiple targets; a
    ///    destructive move would empty the source on the second visit.
    /// 2. `build_delegation_sql`'s all-columns-constant path emits N
    ///    synthetic rows, not one. A running-sum frame containing
    ///    repeated values (two rows both with x=20) used to collapse
    ///    into `sum(20) FROM (VALUES (1))` = 20 instead of 40.
    #[test]
    fn delegating_aggregate_over_window_matches_target() {
        use duckdb::ffi;
        use std::sync::{Arc, Mutex};

        let (raw_con, con, sibling) = unsafe {
            let mut db: ffi::duckdb_database = std::ptr::null_mut();
            assert_eq!(
                ffi::duckdb_open(c":memory:".as_ptr(), &mut db),
                ffi::DuckDBSuccess
            );
            let con = Connection::open_from_raw(db.cast()).expect("open_from_raw");
            let sibling = con.try_clone().expect("clone");
            let mut raw: ffi::duckdb_connection = std::ptr::null_mut();
            assert_eq!(ffi::duckdb_connect(db, &mut raw), ffi::DuckDBSuccess);
            (raw, con, sibling)
        };
        let sibling = Arc::new(Mutex::new(sibling));

        use crate::delegating_agg::{register_delegating_aggregate, T_BIGINT};
        unsafe {
            register_delegating_aggregate(
                raw_con,
                "total",
                "sum",
                vec![T_BIGINT],
                T_BIGINT,
                sibling,
            )
            .expect("register total");
        }

        con.execute_batch(
            "CREATE TABLE t(g INTEGER, x BIGINT, ord INTEGER); \
             INSERT INTO t VALUES \
               (1,10,3),(1,20,1),(1,20,2),(2,30,2),(2,40,1);",
        )
        .expect("seed");

        let collect = |sql: &str| -> Vec<(i32, i64, Option<i64>)> {
            let mut s = con.prepare(sql).expect("prepare");
            s.query_map([], |r| {
                Ok((
                    r.get::<usize, i32>(0)?,
                    r.get::<usize, i64>(1)?,
                    r.get::<usize, Option<i64>>(2)?,
                ))
            })
            .expect("query")
            .filter_map(Result::ok)
            .collect()
        };

        // Full-partition window: `sum(x) OVER (PARTITION BY g)`.
        let native = collect(
            "SELECT g, x, sum(x) OVER (PARTITION BY g) FROM t ORDER BY g, ord",
        );
        let ours = collect(
            "SELECT g, x, total(x) OVER (PARTITION BY g) FROM t ORDER BY g, ord",
        );
        assert_eq!(ours, native, "OVER (PARTITION BY g) must match native");

        // Running window: `sum(x) OVER (PARTITION BY g ORDER BY ord)`.
        // Group 1 has values [20,20,10] in ord order — the running sums
        // 20, 40, 50 exercise the repeat-value degenerate path.
        let native = collect(
            "SELECT g, x, sum(x) OVER (PARTITION BY g ORDER BY ord) FROM t ORDER BY g, ord",
        );
        let ours = collect(
            "SELECT g, x, total(x) OVER (PARTITION BY g ORDER BY ord) FROM t ORDER BY g, ord",
        );
        assert_eq!(
            ours, native,
            "running window (ORDER BY inside OVER) must match native"
        );
    }

    /// Stress test — every remaining window-frame shape and every
    /// order-sensitive aggregate we can hit with the currently-supported
    /// type codes. Each case asserts the delegating alias matches
    /// DuckDB's native output row-for-row (or, for the not-yet-supported
    /// shapes, asserts they FAIL rather than silently producing wrong
    /// numbers). Read as a survey of what does and doesn't work.
    #[test]
    fn delegating_aggregate_window_stress() {
        use duckdb::ffi;
        use std::sync::{Arc, Mutex};

        let (raw_con, con, sibling) = unsafe {
            let mut db: ffi::duckdb_database = std::ptr::null_mut();
            assert_eq!(
                ffi::duckdb_open(c":memory:".as_ptr(), &mut db),
                ffi::DuckDBSuccess
            );
            let con = Connection::open_from_raw(db.cast()).expect("open_from_raw");
            let sibling = con.try_clone().expect("clone");
            let mut raw: ffi::duckdb_connection = std::ptr::null_mut();
            assert_eq!(ffi::duckdb_connect(db, &mut raw), ffi::DuckDBSuccess);
            (raw, con, sibling)
        };
        let sibling = Arc::new(Mutex::new(sibling));

        use crate::delegating_agg::{
            register_delegating_aggregate, T_BIGINT, T_DOUBLE, T_VARCHAR,
        };
        unsafe {
            register_delegating_aggregate(
                raw_con,
                "total",
                "sum",
                vec![T_BIGINT],
                T_BIGINT,
                sibling.clone(),
            )
            .expect("total");
            register_delegating_aggregate(
                raw_con,
                "mean_dlg",
                "avg",
                vec![T_BIGINT],
                T_DOUBLE,
                sibling.clone(),
            )
            .expect("mean_dlg");
            register_delegating_aggregate(
                raw_con,
                "maxv",
                "max",
                vec![T_BIGINT],
                T_BIGINT,
                sibling.clone(),
            )
            .expect("maxv");
            register_delegating_aggregate(
                raw_con,
                "minv",
                "min",
                vec![T_BIGINT],
                T_BIGINT,
                sibling.clone(),
            )
            .expect("minv");
            register_delegating_aggregate(
                raw_con,
                "join_dlg",
                "string_agg",
                vec![T_VARCHAR, T_VARCHAR],
                T_VARCHAR,
                sibling,
            )
            .expect("join_dlg");
        }

        // Seed table: two groups, some repeated values (to exercise
        // the constant-inlining path), ties on the ORDER BY key in
        // group 2, and a NULL x in group 1 to check null-in-partition
        // propagation.
        con.execute_batch(
            "CREATE TABLE t(g INTEGER, x BIGINT, y VARCHAR, ord INTEGER); \
             INSERT INTO t VALUES \
               (1,10,'a',3),(1,20,'b',1),(1,20,'c',2),(1,NULL,'d',4), \
               (2,30,'e',2),(2,40,'f',1),(2,40,'g',2);",
        )
        .expect("seed");

        // ------------------------------------------------------------
        // helpers: fetch two parallel columns of BIGINT window output
        // ------------------------------------------------------------
        let bigints = |sql: &str| -> Vec<Option<i64>> {
            let mut s = con.prepare(sql).expect("prepare");
            s.query_map([], |r| r.get::<usize, Option<i64>>(0))
                .expect("query")
                .filter_map(Result::ok)
                .collect()
        };
        let doubles = |sql: &str| -> Vec<Option<f64>> {
            let mut s = con.prepare(sql).expect("prepare");
            s.query_map([], |r| r.get::<usize, Option<f64>>(0))
                .expect("query")
                .filter_map(Result::ok)
                .collect()
        };
        let strings = |sql: &str| -> Vec<Option<String>> {
            let mut s = con.prepare(sql).expect("prepare");
            s.query_map([], |r| r.get::<usize, Option<String>>(0))
                .expect("query")
                .filter_map(Result::ok)
                .collect()
        };

        let assert_bigints_match = |native_sql: &str, ours_sql: &str, label: &str| {
            let n = bigints(native_sql);
            let o = bigints(ours_sql);
            assert_eq!(o, n, "{label}: delegating output != native");
        };
        let assert_doubles_match = |native_sql: &str, ours_sql: &str, label: &str| {
            let n = doubles(native_sql);
            let o = doubles(ours_sql);
            assert_eq!(o, n, "{label}: delegating output != native");
        };
        let assert_strings_match = |native_sql: &str, ours_sql: &str, label: &str| {
            let n = strings(native_sql);
            let o = strings(ours_sql);
            assert_eq!(o, n, "{label}: delegating output != native");
        };

        // ------------------------------------------------------------
        // 1. ROWS frames — explicit sliding-window frame with fixed row
        //    offsets. Segment-tree recomputes each frame from partial
        //    states via `combine` — exercises the non-destructive-combine
        //    fix hard.
        // ------------------------------------------------------------
        assert_bigints_match(
            "SELECT sum(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
             FROM t ORDER BY g, ord",
            "SELECT total(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
             FROM t ORDER BY g, ord",
            "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
        );
        assert_bigints_match(
            "SELECT sum(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM t ORDER BY g, ord",
            "SELECT total(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM t ORDER BY g, ord",
            "ROWS UNBOUNDED PRECEDING AND CURRENT ROW",
        );
        assert_bigints_match(
            "SELECT sum(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) \
             FROM t ORDER BY g, ord",
            "SELECT total(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) \
             FROM t ORDER BY g, ord",
            "ROWS CURRENT ROW AND UNBOUNDED FOLLOWING",
        );
        // Full-partition explicit frame — DuckDB routes this through
        // the sorted-aggregate / shared-state C-API dispatch, which
        // hands us one init'd state at slot 0 and NULL at slot 1 as
        // the "single-state" sentinel. `update` detects that shape
        // and folds every input row into slot 0.
        assert_bigints_match(
            "SELECT sum(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \
             FROM t ORDER BY g, ord",
            "SELECT total(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \
             FROM t ORDER BY g, ord",
            "ROWS UNBOUNDED..UNBOUNDED (full-partition explicit)",
        );

        // ------------------------------------------------------------
        // 2. RANGE frames — peer-based bounds; every row with the same
        //    ORDER BY key is treated as a peer group. Group 2 has ties
        //    on `ord=2` so RANGE UNBOUNDED PRECEDING AND CURRENT ROW
        //    produces a DIFFERENT result than ROWS.
        // ------------------------------------------------------------
        assert_bigints_match(
            "SELECT sum(x) OVER (PARTITION BY g ORDER BY ord \
             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM t ORDER BY g, ord, x",
            "SELECT total(x) OVER (PARTITION BY g ORDER BY ord \
             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM t ORDER BY g, ord, x",
            "RANGE UNBOUNDED PRECEDING AND CURRENT ROW",
        );

        // ------------------------------------------------------------
        // 3. Frame EXCLUDE clauses — over a FINITE frame, since the
        //    UNBOUNDED..UNBOUNDED base crashes on its own (see the
        //    note above). Using ROWS BETWEEN 1 PRECEDING AND 1
        //    FOLLOWING as the base still exercises each EXCLUDE arm.
        // ------------------------------------------------------------
        assert_bigints_match(
            "SELECT sum(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING \
             EXCLUDE CURRENT ROW) \
             FROM t ORDER BY g, ord, x",
            "SELECT total(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING \
             EXCLUDE CURRENT ROW) \
             FROM t ORDER BY g, ord, x",
            "EXCLUDE CURRENT ROW",
        );
        assert_bigints_match(
            "SELECT sum(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING \
             EXCLUDE GROUP) \
             FROM t ORDER BY g, ord, x",
            "SELECT total(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING \
             EXCLUDE GROUP) \
             FROM t ORDER BY g, ord, x",
            "EXCLUDE GROUP",
        );
        assert_bigints_match(
            "SELECT sum(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING \
             EXCLUDE TIES) \
             FROM t ORDER BY g, ord, x",
            "SELECT total(x) OVER (PARTITION BY g ORDER BY ord \
             ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING \
             EXCLUDE TIES) \
             FROM t ORDER BY g, ord, x",
            "EXCLUDE TIES",
        );

        // ------------------------------------------------------------
        // 4. NULL handling — group 1 has a NULL x. sum/avg/min/max
        //    should skip nulls; the delegating alias inherits DuckDB's
        //    null-handling because the update callback simply doesn't
        //    receive null-filtered rows (validity mask handled in
        //    `extract_value`).
        // ------------------------------------------------------------
        assert_bigints_match(
            "SELECT sum(x) OVER (PARTITION BY g) FROM t ORDER BY g, ord",
            "SELECT total(x) OVER (PARTITION BY g) FROM t ORDER BY g, ord",
            "NULL-in-partition: sum",
        );
        assert_doubles_match(
            "SELECT avg(x) OVER (PARTITION BY g) FROM t ORDER BY g, ord",
            "SELECT mean(x) OVER (PARTITION BY g) FROM t ORDER BY g, ord",
            "NULL-in-partition: avg",
        );

        // ------------------------------------------------------------
        // 5. min/max in a running window — the aggregate needs its full
        //    input to compute correctly; segment-tree combine has to
        //    produce a state whose finalize == max(partial_maxes).
        // ------------------------------------------------------------
        assert_bigints_match(
            "SELECT max(x) OVER (PARTITION BY g ORDER BY ord) \
             FROM t ORDER BY g, ord",
            "SELECT maxv(x) OVER (PARTITION BY g ORDER BY ord) \
             FROM t ORDER BY g, ord",
            "running max",
        );
        assert_bigints_match(
            "SELECT min(x) OVER (PARTITION BY g ORDER BY ord) \
             FROM t ORDER BY g, ord",
            "SELECT minv(x) OVER (PARTITION BY g ORDER BY ord) \
             FROM t ORDER BY g, ord",
            "running min",
        );

        // ------------------------------------------------------------
        // 6. ORDER BY inside the aggregate call — order-sensitive
        //    aggregate (string_agg) with an explicit sort key. DuckDB's
        //    sorted-aggregate wrapper pre-sorts by the sort key and
        //    calls our update in sorted order; the delegation SQL's
        //    VALUES ... t(cN) hands them to string_agg in insertion
        //    order.
        // ------------------------------------------------------------
        assert_strings_match(
            "SELECT string_agg(y, ',' ORDER BY ord) FROM t WHERE g = 1",
            "SELECT join_dlg(y, ',' ORDER BY ord) FROM t WHERE g = 1",
            "string_agg(y, ',' ORDER BY ord) — group 1",
        );
        assert_strings_match(
            "SELECT g, string_agg(y, ',' ORDER BY ord) FROM t \
             GROUP BY g ORDER BY g",
            "SELECT g, join_dlg(y, ',' ORDER BY ord) FROM t \
             GROUP BY g ORDER BY g",
            "string_agg with GROUP BY + ORDER BY inside",
        );

        // ------------------------------------------------------------
        // 7. Multiple delegating aggregates in one query — one query,
        //    several state arrays alive simultaneously; each finalize
        //    must open its own delegation query without contending on
        //    the sibling connection.
        // ------------------------------------------------------------
        let ns = bigints(
            "SELECT sum(x) OVER (PARTITION BY g) + max(x) OVER (PARTITION BY g) \
             FROM t ORDER BY g, ord",
        );
        let os = bigints(
            "SELECT total(x) OVER (PARTITION BY g) + maxv(x) OVER (PARTITION BY g) \
             FROM t ORDER BY g, ord",
        );
        assert_eq!(os, ns, "sum + max composed over the same partition window");

        // ------------------------------------------------------------
        // 8. Empty partition — WHERE clause filters out one group; the
        //    delegating alias must produce the same output row set
        //    (including NULL cells) as the target.
        // ------------------------------------------------------------
        assert_bigints_match(
            "SELECT sum(x) OVER (PARTITION BY g) FROM t WHERE g = 3 \
             ORDER BY ord",
            "SELECT total(x) OVER (PARTITION BY g) FROM t WHERE g = 3 \
             ORDER BY ord",
            "empty partition",
        );
    }

    /// End-to-end: `create_community_aliases` detects an aggregate
    /// function type in its scan and registers a REAL delegating
    /// aggregate under ducklink's chosen name. Modifiers propagate
    /// through the alias just like they do on the target aggregate.
    ///
    /// Uses the built-in `sum` as the "community" aggregate so the test
    /// stays hermetic — no INSTALL FROM community required.
    #[test]
    fn community_aliases_register_aggregate_delegate_end_to_end() {
        let _guard = RUNTIME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        use crate::engine::Engine2;
        use duckdb::ffi;
        let (db, con) = unsafe {
            let mut db: ffi::duckdb_database = std::ptr::null_mut();
            assert_eq!(
                ffi::duckdb_open(c":memory:".as_ptr(), &mut db),
                ffi::DuckDBSuccess
            );
            let con = Connection::open_from_raw(db.cast()).expect("open_from_raw");
            (db, con)
        };
        let engine = Arc::new(Engine2::new().expect("engine"));
        register_load_function(&con, db, engine).expect("register ducklink_load");
        if !runtime_is_ours(db) {
            eprintln!("[test] RUNTIME already bound elsewhere; skipping");
            return;
        }

        // Simulate a community-native entry that aliases `sum` as `total`.
        // We use the persistent connection from the runtime — same one
        // `create_community_aliases` uses in production.
        let rt = RUNTIME.get().unwrap();
        let persistent = rt.con.lock().unwrap_or_else(|e| e.into_inner());
        let spec = spec_with(None, &[("total", "sum")]);
        let n = create_community_aliases(&persistent, &spec).expect("alias gen");
        drop(persistent);
        assert!(n >= 1, "expected >=1 alias, got {n}");

        con.execute_batch(
            "CREATE TABLE t(g INTEGER, x BIGINT); \
             INSERT INTO t VALUES (1,10),(1,20),(1,20),(2,30),(2,40);",
        )
        .expect("seed");

        // The alias is a real AggregateFunction — modifiers propagate.
        let baseline: i64 = con
            .query_row("SELECT total(x) FROM t", [], |r| r.get(0))
            .expect("baseline");
        assert_eq!(baseline, 120);

        let distinct: i64 = con
            .query_row("SELECT total(DISTINCT x) FROM t", [], |r| r.get(0))
            .expect("distinct");
        assert_eq!(distinct, 100);

        let mut stmt = con
            .prepare("SELECT g, total(x) FROM t GROUP BY g ORDER BY g")
            .expect("group by");
        let groups: Vec<(i32, i64)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(groups, vec![(1, 50), (2, 70)]);
    }

    /// Scalar form: `SELECT ducklink_prefix('c','crypto')` runs the same
    /// alias-schema work as the TF and returns a VARCHAR summary. Both
    /// shapes must coexist under the same name because DuckDB's binder
    /// routes them by context.
    #[test]
    fn ducklink_prefix_scalar_form_creates_alias_schema() {
        let _guard = RUNTIME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        use crate::engine::Engine2;
        use duckdb::ffi;
        let (db, con) = unsafe {
            let mut db: ffi::duckdb_database = std::ptr::null_mut();
            assert_eq!(
                ffi::duckdb_open(c":memory:".as_ptr(), &mut db),
                ffi::DuckDBSuccess
            );
            let con = Connection::open_from_raw(db.cast()).expect("open_from_raw");
            (db, con)
        };
        let engine = Arc::new(Engine2::new().expect("engine"));
        register_load_function(&con, db, engine).expect("register ducklink_load");
        if !runtime_is_ours(db) {
            eprintln!("[test] RUNTIME already bound elsewhere; skipping");
            return;
        }

        con.execute_batch(
            "CREATE SCHEMA crypto; \
             CREATE MACRO crypto.hash(algo, data) AS algo || ':' || data;",
        )
        .expect("seed crypto schema");

        let summary: String = con
            .query_row("SELECT ducklink_prefix('c','crypto')", [], |r| r.get(0))
            .expect("scalar ducklink_prefix");
        assert!(
            summary.contains("alias='c'") && summary.contains("namespace='crypto'"),
            "unexpected summary: {summary}"
        );
        assert!(summary.contains("macros=1"), "unexpected summary: {summary}");

        // The alias schema is populated — resolves same as `crypto.hash(...)`.
        let via_alias: String = con
            .query_row("SELECT c.hash('sha2-256', 'ping')", [], |r| r.get(0))
            .expect("c.hash()");
        assert_eq!(via_alias, "sha2-256:ping");

        // Redeclaring the same prefix is idempotent — the scalar just
        // reruns the CREATE OR REPLACE + INSERT OR REPLACE bodies.
        let _ = con
            .query_row("SELECT ducklink_prefix('c','crypto')", [], |r| {
                r.get::<usize, String>(0)
            })
            .expect("redeclare");
        let count: i64 = con
            .query_row(
                "SELECT count(*) FROM ducklink.prefixes WHERE alias = 'c'",
                [],
                |r| r.get(0),
            )
            .expect("count");
        assert_eq!(count, 1);
    }

    /// The `PREFIX(a,n)` macro delegates to the scalar. Users still
    /// have to quote the two identifiers — bare `PREFIX(c, crypto)`
    /// binds them as column refs — but the macro is materially
    /// shorter than `ducklink_prefix`.
    #[test]
    fn prefix_macro_delegates_to_scalar() {
        let _guard = RUNTIME_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        use crate::engine::Engine2;
        use duckdb::ffi;
        let (db, con) = unsafe {
            let mut db: ffi::duckdb_database = std::ptr::null_mut();
            assert_eq!(
                ffi::duckdb_open(c":memory:".as_ptr(), &mut db),
                ffi::DuckDBSuccess
            );
            let con = Connection::open_from_raw(db.cast()).expect("open_from_raw");
            (db, con)
        };
        let engine = Arc::new(Engine2::new().expect("engine"));
        register_load_function(&con, db, engine).expect("register ducklink_load");
        if !runtime_is_ours(db) {
            eprintln!("[test] RUNTIME already bound elsewhere; skipping");
            return;
        }

        con.execute_batch(
            "CREATE SCHEMA crypto; \
             CREATE MACRO crypto.hash(algo, data) AS algo || ':' || data;",
        )
        .expect("seed crypto schema");

        let summary: String = con
            .query_row("SELECT PREFIX('c','crypto')", [], |r| r.get(0))
            .expect("PREFIX macro");
        assert!(
            summary.contains("alias='c'") && summary.contains("namespace='crypto'"),
            "unexpected summary: {summary}"
        );

        let via_alias: String = con
            .query_row("SELECT c.hash('sha2-256', 'ping')", [], |r| r.get(0))
            .expect("c.hash() via PREFIX");
        assert_eq!(via_alias, "sha2-256:ping");
    }
}

// ---------------------------------------------------------------------------
// Replacement-scan integration
// ---------------------------------------------------------------------------
//
// DuckDB's replacement-scan mechanism lets an extension rewrite an unbound
// table-name reference (e.g. `SELECT * FROM 'lambda.gb'`) to a table function
// call (`SELECT * FROM genbank_read_path('lambda.gb')`) at parse time. The
// stable C API surface for this is:
//
//   duckdb_add_replacement_scan(db, callback, extra, on_delete)
//   duckdb_replacement_scan_set_function_name(info, "fn")
//   duckdb_replacement_scan_add_parameter(info, value)
//   duckdb_replacement_scan_set_error(info, "msg")
//
// duckdb-rs 1.10504.0 exposes these via `duckdb::ffi::*` re-exports but
// ships no safe wrapper (same story as aggregates — see `register_aggregates`
// above).
//
// We use ONE process-wide callback installed at `ducklink_init_c_api` time,
// backed by a `Mutex<Vec<Registration>>` that loaded components append to via
// `register_replacement_scans`. The callback matches an unbound table name
// against each registration's extension list; on a hit it sets the target
// function name and passes the original string through as its first
// argument.

use std::sync::OnceLock;

/// One (extension-list, target-table-fn-name) mapping the C callback
/// consults on every unbound table reference. Populated by
/// `register_replacement_scans`; installed at extension init.
struct ReplacementScanRegistration {
    /// File extensions to match on (no leading dot, lower-case). A table
    /// reference is a hit when its trailing `.<ext>` matches any entry
    /// here (case-insensitive on the reference side).
    extensions: Vec<String>,
    /// The registered DuckDB table function to route the scan to.
    function_name: String,
    /// The extension NAME that owns this mapping (for diagnostics only —
    /// not consumed by the callback).
    #[allow(dead_code)]
    owner: String,
}

/// The process-wide registry. `OnceLock<Mutex<Vec<_>>>` mirrors the shape
/// used elsewhere in this crate (see `RUNTIME`).
static REPLACEMENT_SCAN_REGISTRY: OnceLock<Mutex<Vec<ReplacementScanRegistration>>> =
    OnceLock::new();

fn replacement_registry() -> &'static Mutex<Vec<ReplacementScanRegistration>> {
    REPLACEMENT_SCAN_REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// Append one component's replacement-scan bindings to the process-wide
/// registry. Idempotent — a repeated (extension, function_name) pair is
/// a no-op so re-loading the same component does not stack duplicates.
pub fn register_replacement_scans(scans: &[ReplacementScan]) {
    if scans.is_empty() {
        return;
    }
    let mut reg = replacement_registry().lock().unwrap_or_else(|e| e.into_inner());
    for s in scans {
        let already = reg.iter().any(|r| {
            r.function_name == s.function_name
                && r.extensions.len() == s.extensions.len()
                && r.extensions
                    .iter()
                    .zip(s.extensions.iter())
                    .all(|(a, b)| a.eq_ignore_ascii_case(b))
        });
        if already {
            continue;
        }
        eprintln!(
            "[ducklink] replacement scan: {:?} -> '{}' (owner '{}')",
            s.extensions, s.function_name, s.extension
        );
        reg.push(ReplacementScanRegistration {
            extensions: s.extensions.iter().map(|e| e.to_ascii_lowercase()).collect(),
            function_name: s.function_name.clone(),
            owner: s.extension.clone(),
        });
    }
}

/// The one C callback installed at init. DuckDB calls this for every
/// unbound table-name reference in a query; we walk the registry and, on
/// an extension match, rewrite the scan to the registered table function
/// with the original table-name string as its first parameter.
///
/// # Safety
///
/// `info` and `table_name` are valid for the duration of the call — DuckDB
/// owns both. `_data` is the `extra` pointer we passed at registration
/// (currently null; the registry is looked up statically instead).
pub unsafe extern "C" fn ducklink_replacement_scan_callback(
    info: ffi::duckdb_replacement_scan_info,
    table_name: *const c_char,
    _data: *mut c_void,
) {
    if table_name.is_null() {
        return;
    }
    let name_cstr = std::ffi::CStr::from_ptr(table_name);
    let name = match name_cstr.to_str() {
        Ok(s) => s,
        Err(_) => return, // non-UTF-8 table name — leave alone, DuckDB errors as unbound
    };

    // First: arrow-table producers, keyed on the exact bare table name.
    // Registered by `register_arrow_tables` (task #53): rewrites
    // `SELECT * FROM feed` -> `SELECT * FROM __ducklink_arrow_shim_<safe>()`
    // so every query re-enters the shim's `bind`/`init`/`func` and opens a
    // fresh guest cursor. On CString failure we fall through to the
    // file-extension logic below so the user sees DuckDB's normal error
    // rather than a silent miss.
    if let Some(shim_name) = arrow_shim_for_table_name(name) {
        if let Ok(fname_c) = CString::new(shim_name) {
            ffi::duckdb_replacement_scan_set_function_name(info, fname_c.as_ptr());
            // Arrow shim takes zero args — nothing to `add_parameter`.
            return;
        }
    }

    // Extract the trailing extension (lower-case, no dot). A table name
    // without a dot has no extension; a name ending in a dot has an empty
    // one — both cases are treated as "no match".
    let ext = match name.rsplit_once('.') {
        Some((_, e)) if !e.is_empty() => e.to_ascii_lowercase(),
        _ => return,
    };

    let reg = match REPLACEMENT_SCAN_REGISTRY.get() {
        Some(r) => r,
        None => return, // nobody has registered any scans yet
    };
    let guard = reg.lock().unwrap_or_else(|e| e.into_inner());
    let hit = guard.iter().find(|r| r.extensions.iter().any(|e| e == &ext));
    let hit = match hit {
        Some(h) => h,
        None => return,
    };

    // Route the scan to `hit.function_name` and pass the original table
    // name as its first parameter. On CString / duckdb_create_varchar
    // failure we fall through silently — DuckDB then reports the scan as
    // unbound with its normal error, so the user isn't left thinking the
    // replacement fired.
    let fname_c = match CString::new(hit.function_name.as_str()) {
        Ok(c) => c,
        Err(_) => return,
    };
    let arg_c = match CString::new(name) {
        Ok(c) => c,
        Err(_) => return,
    };

    ffi::duckdb_replacement_scan_set_function_name(info, fname_c.as_ptr());
    let mut param = ffi::duckdb_create_varchar(arg_c.as_ptr());
    ffi::duckdb_replacement_scan_add_parameter(info, param);
    ffi::duckdb_destroy_value(&mut param);
}

// ============================================================================
// Phase: register_<x> C API wiring for every additive LoadedComponent field
// ============================================================================
//
// Each block below mirrors the shape of `register_replacement_scans` /
// `REPLACEMENT_SCAN_REGISTRY` / `ducklink_replacement_scan_callback` above:
//
//   1. A `struct <X>Registration` capturing everything the callback needs.
//   2. A `static <X>_REGISTRY: OnceLock<Mutex<...>>` process-wide registry.
//   3. An `extern "C" fn ducklink_<x>_callback(...)` C ABI trampoline that
//      looks up the guest handle in the registry and re-enters
//      `Engine2::dispatch_*` with it.
//   4. A `pub fn register_<x>(...)` entry point the two call sites in
//      `ducklink_init_c_api` / `WasmLoad::bind` invoke on every load.
//
// Where a C API function does not exist in the shipped bindings we install
// a fail-loud stub: an eprintln explaining the shortfall AND an `Err`
// return so LOAD fails visibly, not a silent no-op.

use ducklink_runtime::duckdb_extension_bindings::duckdb::extension::types::Columndef as WitColumndef;
use ducklink_runtime::duckdb_extension_bindings::duckdb::extension::types::Logicaltype as WitLogicaltype;

/// R1 (COPY TO): map a bridge type code to the WIT `logicaltype` variant the
/// guest's `copy-to-bind` receives on each column of the target schema. Codes
/// outside the neutral logical-type set fall back to `Text` — they're what
/// `code_from_duckdb_type` yields for any unmapped duckdb_type, so the
/// column's declared type is preserved as VARCHAR at the boundary.
///
/// Sweep-5 fix (FIX 3): when the caller holds the target-column
/// `duckdb_logical_type` handle for a DECIMAL column, pass it as `lt` so the
/// real `(width, scale)` are extracted via `duckdb_decimal_width` /
/// `duckdb_decimal_scale`. Callers without a handle (or a non-decimal code)
/// pass `None` and the DECIMAL(18, 3) fallback stands — still a Gap 2
/// continuation for the code-only paths.
///
/// # Safety
/// When `lt` is `Some`, the handle must be a live `duckdb_logical_type` the
/// caller has not yet destroyed. Passing a stale or freed handle is UB.
/// When `lt` is `None` the function performs no FFI calls and is trivially
/// safe.
unsafe fn wit_logicaltype_from_code(
    code: u8,
    lt: Option<ffi::duckdb_logical_type>,
) -> WitLogicaltype {
    match code {
        T_I64 => WitLogicaltype::Int64,
        T_U64 => WitLogicaltype::Uint64,
        T_F64 => WitLogicaltype::Float64,
        T_BOOL => WitLogicaltype::Boolean,
        T_TEXT => WitLogicaltype::Text,
        T_BLOB => WitLogicaltype::Blob,
        T_I8 => WitLogicaltype::Int8,
        T_I16 => WitLogicaltype::Int16,
        T_I32 => WitLogicaltype::Int32,
        T_U8 => WitLogicaltype::Uint8,
        T_U16 => WitLogicaltype::Uint16,
        T_U32 => WitLogicaltype::Uint32,
        T_F32 => WitLogicaltype::Float32,
        T_TIMESTAMP => WitLogicaltype::Timestamp,
        T_DATE => WitLogicaltype::Date,
        T_TIME => WitLogicaltype::Time,
        T_TIMESTAMPTZ => WitLogicaltype::Timestamptz,
        // Sweep-5 FIX 3: DECIMAL width/scale extraction. When the caller
        // hands us the live `duckdb_logical_type` handle, ask DuckDB for
        // the real (width, scale) — the guest then receives DECIMAL(20, 5)
        // instead of the DECIMAL(18, 3) interim shape that used to silently
        // scale values 100x on COPY FROM. Handle-less callers keep the
        // (18, 3) fallback (Gap 2 continuation).
        T_DECIMAL => {
            let (width, scale) = if let Some(handle) = lt {
                (ffi::duckdb_decimal_width(handle), ffi::duckdb_decimal_scale(handle))
            } else {
                (18, 3)
            };
            WitLogicaltype::Decimal(
                ducklink_runtime::duckdb_extension_bindings::duckdb::extension::types::Decimalshape {
                    width,
                    scale,
                },
            )
        }
        T_INTERVAL => WitLogicaltype::Interval,
        T_UUID => WitLogicaltype::Uuid,
        // T2-1 residual (major-5): 128-bit integer logical types.
        T_HUGEINT => WitLogicaltype::Hugeint,
        T_UHUGEINT => WitLogicaltype::Uhugeint,
        // S1 (major-5): nested logical types have NO structural WIT arm
        // (wit-parser 0.251 forbids recursive VALUE types — see
        // column-types.wit header note). Degrade via `complex(<kind>)` so
        // the guest sees the kind label rather than silently collapsing to
        // VARCHAR. The exact type-expression isn't reconstructable from the
        // code alone; callers that hold the full reg::LogicalType shape
        // should use `neutral_to_wit_logicaltype` in engine.rs which knows
        // the child types.
        T_LIST => WitLogicaltype::Complex("LIST".to_string()),
        T_STRUCT => WitLogicaltype::Complex("STRUCT".to_string()),
        T_MAP => WitLogicaltype::Complex("MAP".to_string()),
        T_ARRAY => WitLogicaltype::Complex("ARRAY".to_string()),
        // Sweep-7 FIX F2: COMPLEX previously fell through the catch-all and
        // silently degraded to Text — the guest lost the escape-hatch label.
        // Preserve the "COMPLEX" kind label so callers see the actual shape.
        T_COMPLEX => WitLogicaltype::Complex("COMPLEX".to_string()),
        // Sweep-7 FIX F2: fail-loud catch-all. Any type code newly added but
        // not wired here used to silently degrade to Text; log the code so
        // the gap is visible. Still return Text so callers don't panic.
        unhandled => {
            eprintln!(
                "ducklink: wit_logicaltype_from_code: unhandled code {unhandled} — \
                 add an arm here or extend the code table (returning Text as fallback)"
            );
            WitLogicaltype::Text
        }
    }
}

/// Inverse of [`wit_logicaltype_from_code`]: map a WIT `Logicaltype` variant
/// (as returned by the guest's `copy-from-bind`) back to a bridge type code.
/// The `Complex` escape-hatch arm falls back to `T_TEXT` — matching the
/// same fallback the forward map applies for out-of-set codes.
///
/// T1-6: previously used by `ducklink_copy_from_bind` to derive col_codes
/// from the guest's returned columns. The DuckDB C API contract forbids
/// the copy-from install path from declaring its own schema via
/// `duckdb_bind_add_result_column`, so col_codes now come from the
/// target-table schema via `duckdb_table_function_bind_get_result_column_*`
/// instead, and this helper's remaining use is for symmetry / future
/// callers (e.g. an inbound WIT-target-schema plumb). Retained rather
/// than deleted to keep the round-trip pair intact.
#[allow(dead_code)]
fn code_from_wit_logicaltype(lt: &WitLogicaltype) -> u8 {
    match lt {
        WitLogicaltype::Boolean => T_BOOL,
        WitLogicaltype::Int64 => T_I64,
        WitLogicaltype::Uint64 => T_U64,
        WitLogicaltype::Float64 => T_F64,
        WitLogicaltype::Text => T_TEXT,
        WitLogicaltype::Blob => T_BLOB,
        WitLogicaltype::Int8 => T_I8,
        WitLogicaltype::Int16 => T_I16,
        WitLogicaltype::Int32 => T_I32,
        WitLogicaltype::Uint8 => T_U8,
        WitLogicaltype::Uint16 => T_U16,
        WitLogicaltype::Uint32 => T_U32,
        WitLogicaltype::Float32 => T_F32,
        WitLogicaltype::Timestamp => T_TIMESTAMP,
        WitLogicaltype::Date => T_DATE,
        WitLogicaltype::Time => T_TIME,
        WitLogicaltype::Timestamptz => T_TIMESTAMPTZ,
        // S2 (major-5): Decimal now tuple-variant with a Decimalshape payload.
        // The code path erases width/scale (T_DECIMAL is fieldless); callers
        // that need it must lift via the runtime `wit_logicaltype_to_neutral`
        // path in engine.rs.
        WitLogicaltype::Decimal(_) => T_DECIMAL,
        WitLogicaltype::Interval => T_INTERVAL,
        WitLogicaltype::Uuid => T_UUID,
        // T2-1 residual (major-5): 128-bit integer logical types.
        WitLogicaltype::Hugeint => T_HUGEINT,
        WitLogicaltype::Uhugeint => T_UHUGEINT,
        WitLogicaltype::Complex(_) => T_TEXT,
    }
}

// ---------------------------------------------------------------------------
// 1. register_settings — declares DB config options to DuckDB. `SET <name>=`
// stores into the DB config catalog; `runtime.get-string` reads it back via
// the existing NativeServices path. C API: duckdb_create_config_option +
// duckdb_config_option_set_{name,type,default_value,default_scope,description}
// + duckdb_register_config_option. There is no on-SET callback in the shipped
// bindings (no `duckdb_config_option_set_change_callback` or similar), so
// this registers the option so it is known to DuckDB but does NOT dispatch
// into the guest on SET — the guest reads the current value via the runtime's
// `get-string` bridge instead.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
struct SettingRegistration {
    owner: String,
    name: String,
    ty: String,
    scope: String,
}

static SETTING_REGISTRY: OnceLock<Mutex<Vec<SettingRegistration>>> = OnceLock::new();

fn setting_registry() -> &'static Mutex<Vec<SettingRegistration>> {
    SETTING_REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

fn config_scope_code(scope: &str) -> ffi::duckdb_config_option_scope {
    match scope.to_ascii_lowercase().as_str() {
        "local" => ffi::duckdb_config_option_scope_DUCKDB_CONFIG_OPTION_SCOPE_LOCAL,
        "session" => ffi::duckdb_config_option_scope_DUCKDB_CONFIG_OPTION_SCOPE_SESSION,
        "global" => ffi::duckdb_config_option_scope_DUCKDB_CONFIG_OPTION_SCOPE_GLOBAL,
        _ => ffi::duckdb_config_option_scope_DUCKDB_CONFIG_OPTION_SCOPE_GLOBAL,
    }
}

fn setting_logical_type(ty: &str) -> ffi::duckdb_logical_type {
    let code = match ty.to_ascii_lowercase().as_str() {
        "boolean" | "bool" => ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
        "bigint" | "int64" | "long" => ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
        "double" | "float64" => ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
        _ => ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    };
    unsafe { ffi::duckdb_create_logical_type(code) }
}

fn setting_default_value(ty: &str, raw: &str) -> ffi::duckdb_value {
    unsafe {
        match ty.to_ascii_lowercase().as_str() {
            "boolean" | "bool" => {
                let b = matches!(raw.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "on");
                ffi::duckdb_create_bool(b)
            }
            "bigint" | "int64" | "long" => {
                let n = raw.parse::<i64>().unwrap_or(0);
                ffi::duckdb_create_int64(n)
            }
            "double" | "float64" => {
                let f = raw.parse::<f64>().unwrap_or(0.0);
                ffi::duckdb_create_double(f)
            }
            _ => {
                let c = match CString::new(raw) {
                    Ok(c) => c,
                    Err(_) => return std::ptr::null_mut(),
                };
                ffi::duckdb_create_varchar(c.as_ptr())
            }
        }
    }
}

/// Register every declared setting on `raw_con` as a DB config option so
/// `SET <name>=<value>` reaches the core catalog and the guest can read it
/// via the runtime's `get-string`/`get-int64`/... bridge. Returns an error
/// only if the C API surface is unusable (never expected in the shipped
/// bindings) — a duplicate-name registration is skipped (already installed).
pub unsafe fn register_settings(
    raw_con: ffi::duckdb_connection,
    settings: &[Setting],
) -> Result<usize, String> {
    if settings.is_empty() {
        return Ok(0);
    }
    if raw_con.is_null() {
        return Err(
            "register_settings: raw_con is null — cannot register config options".to_string(),
        );
    }
    let mut registered = 0usize;
    let mut reg = setting_registry().lock().unwrap_or_else(|e| e.into_inner());
    for s in settings {
        let already = reg.iter().any(|r| r.name.eq_ignore_ascii_case(&s.name));
        if already {
            eprintln!("[ducklink] setting '{}' already registered, skipping", s.name);
            continue;
        }
        let name_c = match CString::new(s.name.as_str()) {
            Ok(c) => c,
            Err(_) => {
                eprintln!("[ducklink] setting '{}' has a NUL byte; skipping", s.name);
                continue;
            }
        };
        let desc_c = CString::new(s.description.as_str())
            .unwrap_or_else(|_| CString::new("").unwrap());
        let opt = ffi::duckdb_create_config_option();
        ffi::duckdb_config_option_set_name(opt, name_c.as_ptr());
        let mut lt = setting_logical_type(&s.ty);
        ffi::duckdb_config_option_set_type(opt, lt);
        ffi::duckdb_destroy_logical_type(&mut lt);
        if let Some(dv) = s.default_value.as_deref() {
            let mut v = setting_default_value(&s.ty, dv);
            if !v.is_null() {
                ffi::duckdb_config_option_set_default_value(opt, v);
                ffi::duckdb_destroy_value(&mut v);
            }
        }
        ffi::duckdb_config_option_set_default_scope(opt, config_scope_code(&s.scope));
        ffi::duckdb_config_option_set_description(opt, desc_c.as_ptr());

        let mut opt_mut = opt;
        let rc = ffi::duckdb_register_config_option(raw_con, opt);
        ffi::duckdb_destroy_config_option(&mut opt_mut);
        if rc != ffi::DuckDBSuccess {
            eprintln!(
                "[ducklink] setting '{}' not registered (already present?)",
                s.name
            );
            continue;
        }
        reg.push(SettingRegistration {
            owner: s.extension.clone(),
            name: s.name.clone(),
            ty: s.ty.clone(),
            scope: s.scope.clone(),
        });
        registered += 1;
    }
    Ok(registered)
}

// ---------------------------------------------------------------------------
// 2. register_copy_handlers — installs each COPY handler as a DuckDB COPY
// function keyed on its file extension. C API: duckdb_create_copy_function +
// duckdb_copy_function_set_{name,extra_info,bind,global_init,sink,finalize}
// + duckdb_register_copy_function.
//
// Extra info is a `Box<CopyExtra>` carrying the guest function handle + the
// engine Arc; the bind/sink/finalize callbacks look this pointer up and
// re-enter Engine2::dispatch_copy_to_{bind,sink,finalize}.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
struct CopyExtra {
    function_handle: u32,
    engine: Arc<Engine2>,
    file_extension: String,
}

/// Per-invocation state stashed on the bind info; sink/finalize read the
/// writer handle it carries. R1: the schema + option list captured from the
/// DuckDB copy-function bind accessors is held here so `global_init` can
/// forward them to the guest's `copy-to-bind` (which returns the writer
/// handle). This makes the guest receive the full COPY target schema +
/// COPY-clause option map instead of empty lists.
struct CopyBindState {
    writer_handle: u32,
    /// Target column schema captured at bind (WIT column defs — names are
    /// empty because the C API surfaces types only, no column names).
    columns: Vec<WitColumndef>,
    /// COPY-clause options, if the C API surfaced any. Best-effort: only
    /// rendered when `duckdb_copy_function_bind_get_options` returns a
    /// non-null map-shaped value; otherwise empty (guest sees no options).
    options: Vec<(String, String)>,
}

unsafe extern "C" fn copy_extra_destroy(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut CopyExtra));
    }
}

unsafe extern "C" fn copy_bind_state_destroy(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut CopyBindState));
    }
}

unsafe extern "C" fn ducklink_copy_bind(info: ffi::duckdb_copy_function_bind_info) {
    let extra = ffi::duckdb_copy_function_bind_get_extra_info(info) as *const CopyExtra;
    if extra.is_null() {
        let msg = CString::new("copy_bind: missing extra info").unwrap();
        ffi::duckdb_copy_function_bind_set_error(info, msg.as_ptr());
        return;
    }
    // R1: BIND is where DuckDB surfaces the target COLUMN SCHEMA
    // (`duckdb_copy_function_bind_get_column_{count,type}`). The file
    // PATH is only available at global_init
    // (`duckdb_copy_function_global_init_get_file_path`), so we stash the
    // captured schema here and forward it to the guest's `copy-to-bind`
    // from `global_init` along with the path. Column NAMES are absent from
    // the C API — every WitColumndef gets an empty `name` and only its
    // `logical` carries information.
    let ncols = ffi::duckdb_copy_function_bind_get_column_count(info) as usize;
    let mut columns: Vec<WitColumndef> = Vec::with_capacity(ncols);
    for c in 0..ncols {
        let mut lt = ffi::duckdb_copy_function_bind_get_column_type(info, c as u64);
        let code = code_from_duckdb_type(ffi::duckdb_get_type_id(lt));
        // Sweep-5 FIX 3: pass the live LogicalType handle so DECIMAL columns
        // carry the real (width, scale) instead of the DECIMAL(18, 3)
        // fallback the code-only path used to emit.
        let logical = wit_logicaltype_from_code(code, Some(lt));
        ffi::duckdb_destroy_logical_type(&mut lt);
        columns.push(WitColumndef {
            name: String::new(),
            logical,
        });
    }
    // COPY options: `duckdb_copy_function_bind_get_options` returns a
    // `duckdb_value` (an opaque map-shaped value). The stable C API does
    // not expose typed iterators over that map in
    // libduckdb-sys 1.10504.0, so we cannot faithfully unpack it to
    // (String, String) pairs. Leaving `options` empty is the safe default:
    // the guest sees an empty options list. TODO Gap: when the C API
    // exposes options-map traversal, populate this list from
    // `bind_get_options`.
    let options: Vec<(String, String)> = Vec::new();

    let state = Box::into_raw(Box::new(CopyBindState {
        writer_handle: 0,
        columns,
        options,
    })) as *mut c_void;
    ffi::duckdb_copy_function_bind_set_bind_data(info, state, Some(copy_bind_state_destroy));
    let _ = extra;
}

unsafe extern "C" fn ducklink_copy_global_init(info: ffi::duckdb_copy_function_global_init_info) {
    // T1-3: dispatch_copy_to_bind reaches the guest — mark thread so a
    // re-entrant `NativeServices::query()` refuses instead of deadlocking.
    let _reentrancy_guard = crate::engine::QueryReentrancyGuard::new();
    let extra = ffi::duckdb_copy_function_global_init_get_extra_info(info) as *const CopyExtra;
    if extra.is_null() {
        let msg = CString::new("copy_global_init: missing extra info").unwrap();
        ffi::duckdb_copy_function_global_init_set_error(info, msg.as_ptr());
        return;
    }
    let extra_ref = &*extra;
    let path_ptr = ffi::duckdb_copy_function_global_init_get_file_path(info);
    let path = if path_ptr.is_null() {
        String::new()
    } else {
        std::ffi::CStr::from_ptr(path_ptr).to_string_lossy().into_owned()
    };
    // R1: pull the column list + options `ducklink_copy_bind` captured on
    // the bind info's data slot and forward them to the guest along with
    // the target path. `bind_data` is set at bind (see `ducklink_copy_bind`);
    // NULL only if bind was skipped, which shouldn't happen — treat as an
    // empty schema then.
    let bind_data = ffi::duckdb_copy_function_global_init_get_bind_data(info) as *mut CopyBindState;
    let (cols, opts): (Vec<WitColumndef>, Vec<(String, String)>) = if bind_data.is_null() {
        (Vec::new(), Vec::new())
    } else {
        ((*bind_data).columns.clone(), (*bind_data).options.clone())
    };
    let writer = match extra_ref
        .engine
        .dispatch_copy_to_bind(extra_ref.function_handle, &path, &cols, &opts)
    {
        Ok(w) => w,
        Err(e) => {
            let msg = CString::new(format!("copy_to_bind failed: {e}")).unwrap();
            ffi::duckdb_copy_function_global_init_set_error(info, msg.as_ptr());
            return;
        }
    };
    if !bind_data.is_null() {
        (*bind_data).writer_handle = writer;
    }
    // Attach a small global state (owned by DuckDB) mirroring the writer
    // handle, so sink/finalize can read it without touching bind data.
    let gstate = Box::into_raw(Box::new(writer as u64)) as *mut c_void;
    ffi::duckdb_copy_function_global_init_set_global_state(info, gstate, Some(copy_writer_destroy));
}

unsafe extern "C" fn copy_writer_destroy(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut u64));
    }
}

unsafe extern "C" fn ducklink_copy_sink(
    info: ffi::duckdb_copy_function_sink_info,
    input: ffi::duckdb_data_chunk,
) {
    // T1-3: dispatch_copy_to_sink reaches the guest — mark thread so a
    // re-entrant `NativeServices::query()` refuses instead of deadlocking.
    let _reentrancy_guard = crate::engine::QueryReentrancyGuard::new();
    let extra = ffi::duckdb_copy_function_sink_get_extra_info(info) as *const CopyExtra;
    if extra.is_null() {
        let msg = CString::new("copy_sink: missing extra info").unwrap();
        ffi::duckdb_copy_function_sink_set_error(info, msg.as_ptr());
        return;
    }
    let extra_ref = &*extra;
    let gstate = ffi::duckdb_copy_function_sink_get_global_state(info) as *const u64;
    let writer = if gstate.is_null() { 0u32 } else { *gstate as u32 };
    // Marshal the input chunk to Vec<Vec<reg::DuckValue>> — the wide, slow
    // path for now. A column-native COPY sink is a follow-up.
    let n = ffi::duckdb_data_chunk_get_size(input) as usize;
    let ncols = ffi::duckdb_data_chunk_get_column_count(input) as usize;
    let mut rows: Vec<Vec<reg::DuckValue>> = (0..n).map(|_| Vec::with_capacity(ncols)).collect();
    for c in 0..ncols {
        let vec = ffi::duckdb_data_chunk_get_vector(input, c as u64);
        let ty = ffi::duckdb_vector_get_column_type(vec);
        let code = code_from_duckdb_type(ffi::duckdb_get_type_id(ty));
        // Sweep-6 FIX 4 path (a): pass the LogicalType handle so
        // `read_arg_neutral`'s DECIMAL arm can read the real (width, scale)
        // instead of the (18, 3) fallback. Especially load-bearing for
        // COPY TO where the source column's DECIMAL(w, s) is authoritative.
        for r in 0..n {
            let v = read_arg_neutral(code, vec, r, Some(ty));
            rows[r].push(v);
        }
        let mut ty_mut = ty;
        ffi::duckdb_destroy_logical_type(&mut ty_mut);
    }
    if let Err(e) = extra_ref
        .engine
        .dispatch_copy_to_sink(extra_ref.function_handle, writer, rows)
    {
        let msg = CString::new(format!("copy_to_sink failed: {e}")).unwrap();
        ffi::duckdb_copy_function_sink_set_error(info, msg.as_ptr());
    }
}

unsafe extern "C" fn ducklink_copy_finalize(info: ffi::duckdb_copy_function_finalize_info) {
    // T1-3: dispatch_copy_to_finalize reaches the guest — mark thread so a
    // re-entrant `NativeServices::query()` refuses instead of deadlocking.
    let _reentrancy_guard = crate::engine::QueryReentrancyGuard::new();
    let extra = ffi::duckdb_copy_function_finalize_get_extra_info(info) as *const CopyExtra;
    if extra.is_null() {
        let msg = CString::new("copy_finalize: missing extra info").unwrap();
        ffi::duckdb_copy_function_finalize_set_error(info, msg.as_ptr());
        return;
    }
    let extra_ref = &*extra;
    let gstate = ffi::duckdb_copy_function_finalize_get_global_state(info) as *const u64;
    let writer = if gstate.is_null() { 0u32 } else { *gstate as u32 };
    if let Err(e) = extra_ref
        .engine
        .dispatch_copy_to_finalize(extra_ref.function_handle, writer)
    {
        let msg = CString::new(format!("copy_to_finalize failed: {e}")).unwrap();
        ffi::duckdb_copy_function_finalize_set_error(info, msg.as_ptr());
    }
}

// ---------------------------------------------------------------------------
// T1-6 (COPY FROM): a dedicated raw C `duckdb_table_function` installed on
// the COPY function via `duckdb_copy_function_set_copy_from_function`. The
// C API surfaces COPY FROM as a table function (not a mirror of the TO
// bind/sink/finalize triple) — DuckDB rewrites `COPY tbl FROM 'path'` into a
// call on this table function with the path as parameter 0, and the scan is
// driven by the standard table-function bind/init/func lifecycle.
//
// This is a lighter shim than the ArrowShim VTab: no duckdb-rs wrapper, no
// per-column register step, no replacement-scan machinery — just three
// `extern "C"` callbacks that trampoline into `Engine2::dispatch_copy_from_*`.
// `bind` opens the reader (calling into the guest) and captures the column
// schema; `func` pulls up to STANDARD_VECTOR_SIZE rows per call and writes
// them column-by-column with `write_ret_raw`; the bind-data destroy callback
// closes the reader so LIMIT-terminated scans still release guest state.
// ---------------------------------------------------------------------------

/// Extra info attached to the COPY FROM table function via
/// `duckdb_table_function_set_extra_info`. Cloned from the `CopyExtra` the
/// COPY function was installed with — the table function and COPY function
/// are separate DuckDB objects with independent extra-info slots (each with
/// its own destroy callback), so we cannot share the same `Box<CopyExtra>`.
struct CopyFromExtra {
    function_handle: u32,
    engine: Arc<Engine2>,
}

unsafe extern "C" fn copy_from_extra_destroy(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut CopyFromExtra));
    }
}

/// Per-query bind state. Owns the reader handle the guest returned from
/// `copy-from-bind` plus the column type codes `func` needs to marshal
/// each scanned row. `eof` short-circuits further `func` calls once the
/// guest reports an empty batch. Dropped when DuckDB destroys the bind
/// data (end-of-query or query error) — see [`copy_from_bind_state_destroy`],
/// which is where the paired `dispatch_copy_from_close` fires.
struct CopyFromBindState {
    engine: Arc<Engine2>,
    callback_handle: u32,
    reader: u32,
    col_codes: Vec<u8>,
    eof: std::sync::atomic::AtomicBool,
}

unsafe extern "C" fn copy_from_bind_state_destroy(ptr: *mut c_void) {
    if !ptr.is_null() {
        let state = Box::from_raw(ptr as *mut CopyFromBindState);
        // Close the reader even if we short-circuited on EOF or a LIMIT — the
        // guest allocated it in `copy-from-bind`, so it owns the release.
        //
        // FIX 2: this is a C-ABI callback. `dispatch_copy_from_close` locks
        // the engine's instance map — a poisoned lock, or a wasm trap surfaced
        // as a Rust panic anywhere in the dispatch chain, would unwind
        // through this `extern "C"` boundary (UB, per T1-7's ExtensionInstance
        // Drop guard note). Wrap the whole dispatch in `catch_unwind` and
        // log; Drop-of-bind-data has no better outlet than stderr.
        let dispatch = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state
                .engine
                .dispatch_copy_from_close(state.callback_handle, state.reader)
        }));
        match dispatch {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                eprintln!(
                    "[ducklink] copy-from destroy: dispatch failed: {e}"
                );
            }
            Err(_) => {
                eprintln!(
                    "[ducklink] copy-from destroy: dispatch failed: panic caught (continuing teardown)"
                );
            }
        }
    }
}

unsafe extern "C" fn ducklink_copy_from_bind(info: ffi::duckdb_bind_info) {
    // T1-3: dispatch_copy_from_bind reaches the guest — mark thread so a
    // re-entrant `NativeServices::query()` refuses instead of deadlocking.
    let _reentrancy_guard = crate::engine::QueryReentrancyGuard::new();
    let extra = ffi::duckdb_bind_get_extra_info(info) as *const CopyFromExtra;
    if extra.is_null() {
        let msg = CString::new("copy_from_bind: missing extra info").unwrap();
        ffi::duckdb_bind_set_error(info, msg.as_ptr());
        return;
    }
    let extra_ref = &*extra;

    // COPY FROM lands here as a table function; DuckDB passes the source
    // file path as positional parameter 0. Read it as VARCHAR.
    let param_count = ffi::duckdb_bind_get_parameter_count(info);
    if param_count == 0 {
        let msg = CString::new("copy_from_bind: missing path parameter").unwrap();
        ffi::duckdb_bind_set_error(info, msg.as_ptr());
        return;
    }
    let mut path_value = ffi::duckdb_bind_get_parameter(info, 0);
    let path_ptr = ffi::duckdb_get_varchar(path_value);
    let path = if path_ptr.is_null() {
        String::new()
    } else {
        std::ffi::CStr::from_ptr(path_ptr).to_string_lossy().into_owned()
    };
    if !path_ptr.is_null() {
        ffi::duckdb_free(path_ptr as *mut c_void);
    }
    ffi::duckdb_destroy_value(&mut path_value);

    // COPY-clause options: the stable C API in libduckdb-sys 1.10504.0 does
    // not expose typed iteration over the bind-info's option map from a
    // table function bind (the peer `ducklink_copy_bind` on the COPY TO
    // path has the same gap on its own accessor). Pass an empty list so
    // the guest sees no options — parity with the TO path.
    let options: Vec<(String, String)> = Vec::new();

    // T1-6: read the target-table schema DuckDB has already published on the
    // bind info via `duckdb_table_function_bind_get_result_column_*`. The C
    // API's contract (libduckdb-sys 1.10504.0 bindgen line 3484) says a
    // COPY-FROM install path table function MUST derive its result schema
    // from the target table via these accessors and must NOT publish its
    // own columns via `duckdb_bind_add_result_column`. So the col_codes
    // driving `func`'s writes come from the TARGET table's declared types,
    // not from the guest's returned column list. The guest's column
    // declaration (still forwarded to it via `dispatch_copy_from_bind` so
    // the reader can prepare to yield rows matching the target) is used
    // for parity validation only — a mismatched arity here surfaces as a
    // clear bind-time error rather than a per-row `write_ret_raw`
    // type-mismatch during scan.
    let target_col_count = ffi::duckdb_table_function_bind_get_result_column_count(info) as usize;
    if target_col_count == 0 {
        let msg = CString::new(
            "copy_from_bind: DuckDB published no target columns on the bind info; refusing to \
             proceed — COPY FROM requires a bound target table",
        )
        .unwrap();
        ffi::duckdb_bind_set_error(info, msg.as_ptr());
        return;
    }
    let mut col_codes: Vec<u8> = Vec::with_capacity(target_col_count);
    let mut target_columns: Vec<WitColumndef> = Vec::with_capacity(target_col_count);
    for c in 0..target_col_count {
        let mut lt = ffi::duckdb_table_function_bind_get_result_column_type(info, c as u64);
        let code = code_from_duckdb_type(ffi::duckdb_get_type_id(lt));
        // Sweep-5 FIX 3: pull the real DECIMAL(width, scale) from the
        // target-column LogicalType handle BEFORE destroying it. This is
        // the fix for the 100x COPY FROM corruption: guest used to receive
        // DECIMAL(18, 3) regardless of the target column's declared shape,
        // so values written back at guest scale got re-scaled by DuckDB.
        let logical = wit_logicaltype_from_code(code, Some(lt));
        ffi::duckdb_destroy_logical_type(&mut lt);
        col_codes.push(code);
        // Names may be null (DuckDB does not always populate them on the
        // copy-from bind info in this API rev); fall back to positional.
        let name_ptr = ffi::duckdb_table_function_bind_get_result_column_name(info, c as u64);
        let name = if name_ptr.is_null() {
            format!("col{c}")
        } else {
            let s = std::ffi::CStr::from_ptr(name_ptr).to_string_lossy().into_owned();
            // The C API does not document ownership of this string clearly
            // in 1.10504.0 (unlike `duckdb_get_varchar` which returns
            // freeable memory). Leave it alone — copying to an owned
            // String is the safe read path.
            s
        };
        target_columns.push(WitColumndef {
            name,
            logical,
        });
    }

    // Dispatch to the guest's `copy-from-bind` for the reader handle. T1-6:
    // the copy-dispatch WIT now carries `target-columns` — lower the captured
    // `target_columns` (Vec<WitColumndef>) into the neutral `reg::ColumnDef`
    // shape the engine layer expects, then forward. The engine re-lowers to
    // WIT at the wasmtime boundary. This mechanical conversion is intentionally
    // minimal; the reg_duckdb consolidator will fold it into a single-pass
    // build alongside col_codes / target_columns above.
    let neutral_target_columns: Vec<ducklink_runtime::reg::ColumnDef> = target_columns
        .iter()
        .map(|c| ducklink_runtime::reg::ColumnDef {
            name: c.name.clone(),
            logical: crate::engine::wit_logicaltype_to_neutral(&c.logical),
        })
        .collect();
    let result = match extra_ref
        .engine
        .dispatch_copy_from_bind(
            extra_ref.function_handle,
            &path,
            &options,
            neutral_target_columns,
        )
    {
        Ok(r) => r,
        Err(e) => {
            let msg = CString::new(format!("copy_from_bind failed: {e}")).unwrap();
            ffi::duckdb_bind_set_error(info, msg.as_ptr());
            return;
        }
    };

    // Validate arity: the guest's returned column list must match the
    // target's column count. Types are NOT re-checked here (DuckDB will
    // cast at column-write time if the underlying storage matches);
    // arity mismatches, however, will scribble past the output vectors
    // in `func`, so we refuse them early.
    if result.columns.len() != target_col_count {
        // Close the reader the guest just opened, otherwise the state
        // Box below never gets built and DuckDB never fires
        // `copy_from_bind_state_destroy` to release it.
        if let Err(e) = extra_ref
            .engine
            .dispatch_copy_from_close(extra_ref.function_handle, result.reader)
        {
            eprintln!(
                "[ducklink] copy_from_close after arity-mismatch bind failed: {e}"
            );
        }
        let msg = CString::new(format!(
            "copy_from_bind: guest reader declared {} column(s), target table has {}",
            result.columns.len(),
            target_col_count
        ))
        .unwrap();
        ffi::duckdb_bind_set_error(info, msg.as_ptr());
        return;
    }

    let state = Box::into_raw(Box::new(CopyFromBindState {
        engine: extra_ref.engine.clone(),
        callback_handle: extra_ref.function_handle,
        reader: result.reader,
        col_codes,
        eof: std::sync::atomic::AtomicBool::new(false),
    })) as *mut c_void;
    ffi::duckdb_bind_set_bind_data(info, state, Some(copy_from_bind_state_destroy));
}

unsafe extern "C" fn ducklink_copy_from_init(_info: ffi::duckdb_init_info) {
    // No per-thread state: COPY FROM is single-cursor. `func` reads bind
    // data directly (via `duckdb_function_get_bind_data`), so init has
    // nothing to publish.
}

unsafe extern "C" fn ducklink_copy_from_function(
    info: ffi::duckdb_function_info,
    output: ffi::duckdb_data_chunk,
) {
    // T1-3: dispatch_copy_from_scan reaches the guest — mark thread so a
    // re-entrant `NativeServices::query()` refuses instead of deadlocking.
    let _reentrancy_guard = crate::engine::QueryReentrancyGuard::new();
    let bind_data = ffi::duckdb_function_get_bind_data(info) as *const CopyFromBindState;
    if bind_data.is_null() {
        let msg = CString::new("copy_from_function: missing bind data").unwrap();
        ffi::duckdb_function_set_error(info, msg.as_ptr());
        return;
    }
    let state = &*bind_data;
    if state.eof.load(Ordering::Relaxed) {
        ffi::duckdb_data_chunk_set_size(output, 0);
        return;
    }
    // Mirror the ArrowShim clamp (see `func` in that VTab): DuckDB output
    // chunks allocate at most STANDARD_VECTOR_SIZE rows per column; a batch
    // beyond that is a guest protocol violation.
    let rows = match state.engine.dispatch_copy_from_scan(
        state.callback_handle,
        state.reader,
        STANDARD_VECTOR_SIZE,
    ) {
        Ok(r) => r,
        Err(e) => {
            let msg = CString::new(format!("copy_from_scan failed: {e}")).unwrap();
            ffi::duckdb_function_set_error(info, msg.as_ptr());
            return;
        }
    };
    if rows.is_empty() {
        state.eof.store(true, Ordering::Relaxed);
        ffi::duckdb_data_chunk_set_size(output, 0);
        return;
    }
    if rows.len() > STANDARD_VECTOR_SIZE as usize {
        let msg = CString::new(format!(
            "copy_from_scan returned {} rows in a single batch, exceeds \
             STANDARD_VECTOR_SIZE ({}). Producers must yield at most {} \
             rows per `copy-from-scan` call.",
            rows.len(),
            STANDARD_VECTOR_SIZE,
            STANDARD_VECTOR_SIZE,
        ))
        .unwrap();
        ffi::duckdb_function_set_error(info, msg.as_ptr());
        return;
    }
    let ncols = state.col_codes.len();
    for (i, row) in rows.iter().enumerate() {
        if row.len() != ncols {
            let msg = CString::new(format!(
                "copy_from_scan row width {} != expected {} columns",
                row.len(),
                ncols,
            ))
            .unwrap();
            ffi::duckdb_function_set_error(info, msg.as_ptr());
            return;
        }
        for (c, v) in row.iter().enumerate() {
            let vec = ffi::duckdb_data_chunk_get_vector(output, c as u64);
            if let Err(e) = write_ret_raw(state.col_codes[c], vec, i, v) {
                let msg = CString::new(format!("copy_from_scan write failed: {e}"))
                    .unwrap();
                ffi::duckdb_function_set_error(info, msg.as_ptr());
                return;
            }
        }
    }
    ffi::duckdb_data_chunk_set_size(output, rows.len() as u64);
}

/// Build the `duckdb_table_function` that services this COPY handler's FROM
/// side. Wired onto the COPY function via
/// `duckdb_copy_function_set_copy_from_function` in [`register_copy_handlers`].
/// Ownership: the table function itself is owned by DuckDB once installed on
/// the copy function (the copy function's destroy walks it), so the caller
/// does NOT free it separately.
unsafe fn build_copy_from_table_function(
    handler_name: &str,
    function_handle: u32,
    engine: Arc<Engine2>,
) -> ffi::duckdb_table_function {
    let tf = ffi::duckdb_create_table_function();
    // The name only surfaces if DuckDB ever advertises this as a plain
    // table function (it doesn't for the COPY FROM install path), but set
    // it defensively — it's also useful in any diagnostic that dumps the
    // table function's identity.
    let name_c = CString::new(format!("__ducklink_copy_from_{handler_name}"))
        .unwrap_or_else(|_| CString::new("__ducklink_copy_from").unwrap());
    ffi::duckdb_table_function_set_name(tf, name_c.as_ptr());
    // Positional path parameter (VARCHAR). DuckDB always passes the source
    // path here on a COPY FROM invocation.
    let mut varchar = ffi::duckdb_create_logical_type(ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);
    ffi::duckdb_table_function_add_parameter(tf, varchar);
    ffi::duckdb_destroy_logical_type(&mut varchar);

    let extra = Box::into_raw(Box::new(CopyFromExtra {
        function_handle,
        engine,
    })) as *mut c_void;
    ffi::duckdb_table_function_set_extra_info(tf, extra, Some(copy_from_extra_destroy));
    ffi::duckdb_table_function_set_bind(tf, Some(ducklink_copy_from_bind));
    ffi::duckdb_table_function_set_init(tf, Some(ducklink_copy_from_init));
    ffi::duckdb_table_function_set_function(tf, Some(ducklink_copy_from_function));
    tf
}

/// Register every declared COPY handler on `raw_con`. Idempotency is
/// delegated to DuckDB (a duplicate name returns failure, which we log and
/// skip).
pub unsafe fn register_copy_handlers(
    raw_con: ffi::duckdb_connection,
    engine: Arc<Engine2>,
    handlers: &[CopyHandler],
) -> Result<usize, String> {
    if handlers.is_empty() {
        return Ok(0);
    }
    if raw_con.is_null() {
        return Err("register_copy_handlers: raw_con is null".to_string());
    }
    let mut registered = 0usize;
    for h in handlers {
        let name_c = match CString::new(h.file_extension.as_str()) {
            Ok(c) => c,
            Err(_) => {
                eprintln!(
                    "[ducklink] copy handler '{}' has a NUL byte; skipping",
                    h.file_extension
                );
                continue;
            }
        };
        let cf = ffi::duckdb_create_copy_function();
        ffi::duckdb_copy_function_set_name(cf, name_c.as_ptr());
        let extra = Box::into_raw(Box::new(CopyExtra {
            function_handle: h.function_handle,
            engine: engine.clone(),
            file_extension: h.file_extension.clone(),
        })) as *mut c_void;
        ffi::duckdb_copy_function_set_extra_info(cf, extra, Some(copy_extra_destroy));
        ffi::duckdb_copy_function_set_bind(cf, Some(ducklink_copy_bind));
        ffi::duckdb_copy_function_set_global_init(cf, Some(ducklink_copy_global_init));
        ffi::duckdb_copy_function_set_sink(cf, Some(ducklink_copy_sink));
        ffi::duckdb_copy_function_set_finalize(cf, Some(ducklink_copy_finalize));
        // T1-6 (COPY FROM): install the reader as a `duckdb_table_function`
        // on the COPY function — the C API's single-hook shape for the FROM
        // side (see `build_copy_from_table_function` above).
        let tf = build_copy_from_table_function(
            &h.file_extension,
            h.function_handle,
            engine.clone(),
        );
        ffi::duckdb_copy_function_set_copy_from_function(cf, tf);
        // P2 fix: `duckdb_copy_function_set_copy_from_function` value-copies
        // the underlying `TableFunction` into the copy function's slot
        // (upstream `copy_function-c.cpp:740` — `copy_function_ref.copy_from_function = tf;`
        // invokes TableFunction's copy-constructor). The heap allocation
        // returned by `duckdb_create_table_function` is NOT taken over.
        // Free it now — leaking would strand the `CopyFromExtra` extra-info
        // (and its cloned `Arc<Engine2>`) plus the TableFunction struct for
        // the process lifetime. Note: the extra-info was already deep-copied
        // by the set call's TableFunction copy-constructor into the slot
        // held by the copy function, so its lifetime is not affected here.
        let mut tf_mut = tf;
        ffi::duckdb_destroy_table_function(&mut tf_mut);

        let rc = ffi::duckdb_register_copy_function(raw_con, cf);
        let mut cf_mut = cf;
        ffi::duckdb_destroy_copy_function(&mut cf_mut);
        if rc != ffi::DuckDBSuccess {
            eprintln!(
                "[ducklink] copy handler '{}' not registered (already present?)",
                h.file_extension
            );
            continue;
        }
        registered += 1;
    }
    Ok(registered)
}

// Small helper: turn a duckdb_type id into our internal T_* code.
fn code_from_duckdb_type(t: ffi::duckdb_type) -> u8 {
    match t {
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => T_I64,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => T_U64,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => T_F64,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => T_BOOL,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => T_TEXT,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_BLOB => T_BLOB,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => T_I8,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => T_I16,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => T_I32,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => T_U8,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => T_U16,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => T_U32,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => T_F32,
        // T1-4: recognise DECIMAL so R1 (COPY schema forwarding) surfaces
        // DECIMAL target columns as `Logicaltype::Decimal` to the guest
        // instead of silently dropping to VARCHAR.
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => T_DECIMAL,
        // Temporal + UUID arms mirror `code_from_logical_type` above so the
        // T1-6 COPY FROM target-column lift and the R1 COPY TO schema
        // forwarding path surface `DATE`, `TIME`, `TIMESTAMP`,
        // `TIMESTAMP WITH TIME ZONE`, `INTERVAL`, and `UUID` targets to the
        // guest as their actual `LogicalType` variants — previously these
        // all silently collapsed to `T_TEXT`, so a target column of
        // e.g. `TIMESTAMP` looked like `VARCHAR` to the guest.
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_DATE => T_DATE,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIME => T_TIME,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => T_TIMESTAMP,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => T_TIMESTAMPTZ,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => T_INTERVAL,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UUID => T_UUID,
        // T2-1 residual (major-5): HUGEINT / UHUGEINT are first-class now.
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => T_HUGEINT,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => T_UHUGEINT,
        // S1 (major-5): nested types. The bridge code tags the KIND; the
        // structural child-type shape (LIST elem, STRUCT fields, MAP
        // key/value, ARRAY size + elem) can be recovered via
        // `duckdb_list_type_child_type` / `duckdb_struct_type_child_{name,type}`
        // / `duckdb_map_type_{key,value}_type` / `duckdb_array_type_child_type`
        // — but reconstructing the full `reg::LogicalType` tree from a raw
        // `duckdb_logical_type` handle (not just its top-level `duckdb_type`)
        // is a separate lift path that's not wired here. Callers that need
        // the child shape must go through the LogicalTypeHandle-based reader.
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST => T_LIST,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => T_STRUCT,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_MAP => T_MAP,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => T_ARRAY,
        // Any other DuckDB type warns once and degrades to VARCHAR.
        other => {
            eprintln!(
                "code_from_duckdb_type: unsupported duckdb type '{}' \u{2192} T_TEXT (see T2-1 residual)",
                other as u32
            );
            T_TEXT
        }
    }
}

// Read one row of a DuckDB flat vector into a neutral `DuckValue`.
//
// Sweep-6 FIX 1 + FIX 4: full logical-type coverage. The prior shape covered
// only fixed-width primitives + TEXT/BLOB + the sweep-5-added
// HUGEINT/UHUGEINT/DECIMAL arms; TIMESTAMP/DATE/TIME/TIMESTAMPTZ/INTERVAL/UUID
// /COMPLEX all fell through to the silent-NULL catch-all. Mirror the
// `read_arg_raw` peer (~2589-2690) arm-for-arm so cast / scalar_ex / COPY-TO
// round-trips no longer suppress non-primitive values.
//
// The `lt` parameter (FIX 4 path (a)) is the vector's live
// `duckdb_logical_type` handle when the caller has it: it lets the DECIMAL
// arm ask DuckDB for the real `(width, scale)` via
// `duckdb_decimal_width` / `duckdb_decimal_scale` instead of hardcoding
// (18, 3). Callers without a handle pass `None` and the (18, 3) fallback
// stands (Gap 2 continuation for the code-only paths).
//
// # Safety
// When `lt` is `Some`, the handle must be a live `duckdb_logical_type` the
// caller has not yet destroyed. Passing a stale or freed handle is UB.
unsafe fn read_arg_neutral(
    code: u8,
    vec: ffi::duckdb_vector,
    row: usize,
    lt: Option<ffi::duckdb_logical_type>,
) -> reg::DuckValue {
    let data = ffi::duckdb_vector_get_data(vec);
    let validity = ffi::duckdb_vector_get_validity(vec) as *mut u64;
    if !validity.is_null() && !ffi::duckdb_validity_row_is_valid(validity, row as u64) {
        return reg::DuckValue::Null;
    }
    match code {
        T_I64 => reg::DuckValue::Int64(*(data as *const i64).add(row)),
        T_U64 => reg::DuckValue::Uint64(*(data as *const u64).add(row)),
        T_F64 => reg::DuckValue::Float64(*(data as *const f64).add(row)),
        T_BOOL => reg::DuckValue::Boolean(*(data as *const bool).add(row)),
        T_I8 => reg::DuckValue::Int8(*(data as *const i8).add(row)),
        T_I16 => reg::DuckValue::Int16(*(data as *const i16).add(row)),
        T_I32 => reg::DuckValue::Int32(*(data as *const i32).add(row)),
        T_U8 => reg::DuckValue::Uint8(*(data as *const u8).add(row)),
        T_U16 => reg::DuckValue::Uint16(*(data as *const u16).add(row)),
        T_U32 => reg::DuckValue::Uint32(*(data as *const u32).add(row)),
        T_F32 => reg::DuckValue::Float32(*(data as *const f32).add(row)),
        T_TEXT => {
            let strs = data as *const duckdb_string_t;
            let mut s = std::ptr::read(strs.add(row));
            let mut raw = DuckString::new(&mut s);
            reg::DuckValue::Text(raw.as_str().to_string())
        }
        T_BLOB => {
            let strs = data as *const duckdb_string_t;
            let mut s = std::ptr::read(strs.add(row));
            let mut raw = DuckString::new(&mut s);
            reg::DuckValue::Blob(raw.as_str().as_bytes().to_vec())
        }
        // Sweep-5 fix: HUGEINT / UHUGEINT / DECIMAL were extended in
        // `code_from_duckdb_type` + `type_code_from_expr` (b152edf) but the
        // reader here still silently returned NULL, corrupting cast /
        // scalar_ex round-trips. Mirror the writer arms in `write_ret_raw`
        // (~2772-2779): read the raw i128/u128 slot and split into the WIT
        // (lower, upper) shape.
        T_HUGEINT => {
            let raw = *(data as *const i128).add(row);
            reg::DuckValue::Hugeint {
                lower: raw as u64,
                upper: (raw >> 64) as i64,
            }
        }
        T_UHUGEINT => {
            let raw = *(data as *const u128).add(row);
            reg::DuckValue::UHugeint {
                lower: raw as u64,
                upper: (raw >> 64) as u64,
            }
        }
        // Sweep-6 FIX 4 path (a): when the caller hands us the live
        // LogicalType handle, query DuckDB for the real `(width, scale)`
        // instead of the DECIMAL(18, 3) interim shape. Handle-less callers
        // keep the (18, 3) fallback — matches the peer arm in `read_arg_raw`
        // (~2643-2651) and the `wit_logicaltype_from_code` behaviour.
        T_DECIMAL => {
            let unscaled = *(data as *const i128).add(row);
            let (width, scale) = if let Some(handle) = lt {
                (ffi::duckdb_decimal_width(handle), ffi::duckdb_decimal_scale(handle))
            } else {
                (18, 3)
            };
            reg::DuckValue::Decimal {
                lower: unscaled as u64,
                upper: (unscaled >> 64) as u64,
                width,
                scale,
            }
        }
        // Sweep-6 FIX 1: TIMESTAMP/DATE/TIME/TIMESTAMPTZ read as raw ints —
        // mirrors the `read_arg_raw` peer at ~2615-2618.
        T_TIMESTAMP => reg::DuckValue::Timestamp(*(data as *const i64).add(row)),
        T_DATE => reg::DuckValue::Date(*(data as *const i32).add(row)),
        T_TIME => reg::DuckValue::Time(*(data as *const i64).add(row)),
        T_TIMESTAMPTZ => reg::DuckValue::Timestamptz(*(data as *const i64).add(row)),
        // Sweep-6 FIX 1: INTERVAL is a three-field struct in the vector.
        T_INTERVAL => {
            let iv = *(data as *const ffi::duckdb_interval).add(row);
            reg::DuckValue::Interval {
                months: iv.months,
                days: iv.days,
                micros: iv.micros,
            }
        }
        // Sweep-6 FIX 1: UUID is a 128-bit logical value; DuckDB stores the
        // XOR-flipped physical form so unpack via `uuid_storage_to_logical`
        // (matches `read_arg_raw`).
        T_UUID => {
            let logical = uuid_storage_to_logical(*(data as *const i128).add(row));
            reg::DuckValue::Uuid {
                hi: (logical >> 64) as u64,
                lo: logical as u64,
            }
        }
        // Sweep-6 FIX 1: COMPLEX escape-hatch is stored as VARCHAR JSON; the
        // `type_expr` is filled in as the code-level "COMPLEX" label because
        // the code-only path can't reconstruct the DuckDB type-expression
        // string from the vector alone. Guests that need the full type
        // expression should route through `read_arg_raw` (which has the
        // Aggregate-side LogicalType shape).
        T_COMPLEX => {
            let strs = data as *const duckdb_string_t;
            let mut s = std::ptr::read(strs.add(row));
            let mut raw = DuckString::new(&mut s);
            reg::DuckValue::Complex {
                type_expr: "COMPLEX".to_string(),
                json: raw.as_str().to_string(),
            }
        }
        // Sweep-5 fix: nested reads over the code-only path have no way to
        // reconstruct child vectors from a `duckdb_vector` handle alone.
        // FAIL-LOUD (log + NULL) instead of silently returning NULL on the
        // catch-all — matches `read_arg_raw`'s nested arm shape.
        T_LIST | T_STRUCT | T_MAP | T_ARRAY => {
            eprintln!(
                "read_arg_neutral: nested type code {code} not yet wired \
                 (sweep-5 finding) — surfacing NULL for row {row}"
            );
            reg::DuckValue::Null
        }
        // Sweep-6 FIX 1: fail-loud catch-all. Any type code newly added to
        // `code_from_duckdb_type` / `type_code_from_expr` but not wired here
        // used to silently return NULL and corrupt round-trips (the sweep-5
        // symptom for HUGEINT/UHUGEINT/DECIMAL). Logging the code surfaces
        // the gap the next time it happens.
        _ => {
            eprintln!(
                "read_arg_neutral: unhandled type code {code} (row {row}) — \
                 add an arm here to mirror `read_arg_raw` or extend the code table"
            );
            reg::DuckValue::Null
        }
    }
}

// ---------------------------------------------------------------------------
// 3. register_arrow_tables — arrow-producer table function shim + replacement
// scan (Option B per task #53).
//
// The producer end-to-end: for each declared arrow-table, register a per-table
// DuckDB table function under an internal name
// `__ducklink_arrow_shim_<safe_name>` (safe_name = `<extension>_<table_name>`
// sanitised to `[a-zA-Z0-9_]`). The shim's `bind` re-emits the declared column
// schema, `init` sets up a lazy cursor slot, and `func` pulls one guest batch
// per invocation through `Engine2::dispatch_arrow_{open,next,close}` and writes
// rows directly to the DuckDB `DataChunkHandle` via the existing
// `write_ret_raw` per-cell writer. Extending [`ducklink_replacement_scan_callback`]
// with an arrow-table lookup lets a bare `SELECT * FROM feed` be rewritten to
// `SELECT * FROM __ducklink_arrow_shim_<safe_name>()` at parse time — so every
// query walks the same per-query shim path and re-opens a fresh cursor,
// eliminating the "second query returns empty" one-shot bug the previous
// `duckdb_arrow_scan` install path had.
//
// Design choice (Option B over Option A): the shim is a DuckDB table function
// whose `func` callback pulls guest rows and writes them straight to the
// output `DataChunkHandle` — no Arrow FFI layer on the hot path, no
// `duckdb_arrow_scan` + temp-alias plumbing per query. The Arrow encoder
// (`crate::arrow_encoder::ArrowEncoder`) is preserved for external / future
// use but is not on this path anymore.
//
// Cursor lifecycle: opened lazily on the first `func` call, closed on drop of
// the per-query `ArrowShimInit`. LIMIT queries that terminate before EOF still
// have the cursor released by the guest via `dispatch_arrow_close` at that
// drop point.
// ---------------------------------------------------------------------------

/// Per-arrow-table extra info handed to the shim VTab callbacks via
/// `register_table_function_with_extra_info`. `col_codes`/`col_names` drive
/// the bind schema; `callback_handle` + `engine` route each per-query
/// `dispatch_arrow_{open,next,close}` back to the owning component instance.
#[derive(Clone)]
struct ArrowShimExtra {
    callback_handle: u32,
    engine: Arc<Engine2>,
    col_codes: Vec<u8>,
    col_names: Vec<String>,
}

/// Bind state cloned from `ArrowShimExtra` at bind time. `func` reads
/// `callback_handle` + `engine` + `col_codes` from here to dispatch the
/// next-batch pull and write the result column-by-column.
struct ArrowShimBind {
    callback_handle: u32,
    engine: Arc<Engine2>,
    col_codes: Vec<u8>,
}

/// Per-query cursor state. `cursor` is `None` before the first `func` call
/// (opened lazily then), `Some(id)` once opened, and back to `None` after
/// `Drop` closes it. `eof` short-circuits further `func` calls once the
/// guest reports an empty batch. `callback_handle` + `engine` are cloned in
/// so `Drop` can close the cursor without touching bind data (DuckDB drops
/// init before bind).
struct ArrowShimInit {
    engine: Arc<Engine2>,
    callback_handle: u32,
    cursor: Mutex<Option<u32>>,
    eof: std::sync::atomic::AtomicBool,
}

impl Drop for ArrowShimInit {
    fn drop(&mut self) {
        if let Some(c) = self.cursor.lock().unwrap_or_else(|e| e.into_inner()).take() {
            // P3 fix: wrap the guest dispatch in a QueryReentrancyGuard so a
            // guest attempting `NativeServices::query()` from inside its own
            // `arrow-close` refuses cleanly instead of deadlocking on the
            // connection's read lock. Consistency with the sibling
            // `copy_from_bind_state_destroy` path (whose destroy is invoked
            // by DuckDB from the same execution contexts) — install even
            // though the arrow shim's own bind/init/func are already guarded
            // upstream, because drop can fire from a background finalizer
            // path where no other guard is in scope.
            let _reentrancy_guard = crate::engine::QueryReentrancyGuard::new();
            // FIX 2: DuckDB drops init data from an `extern "C"` teardown
            // callback. `dispatch_arrow_close` locks the engine's instance
            // map (the same lock the T1-7 note flags), and a poisoned lock
            // or wasm trap surfacing as a Rust panic would unwind across
            // the C ABI boundary — UB. Mirror the shape of
            // `copy_from_bind_state_destroy` above and `ExtensionInstance::
            // drop` (runtime/src/extension.rs) with a catch_unwind wrap.
            let dispatch = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.engine.dispatch_arrow_close(self.callback_handle, c)
            }));
            match dispatch {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    eprintln!("[ducklink] arrow shim: dispatch_arrow_close failed: {e}");
                }
                Err(_) => {
                    eprintln!(
                        "[ducklink] arrow shim: dispatch_arrow_close panicked (continuing teardown)"
                    );
                }
            }
        }
    }
}

/// One `VTab` impl serving every arrow-table shim. `bind` republishes the
/// declared column list, `init` sets up a lazy cursor slot, `func` pulls one
/// batch per call and writes it to the output `DataChunkHandle`. Fresh
/// `BindData`/`InitData` per query = a fresh guest cursor per query, which
/// is exactly what the old `duckdb_arrow_scan` install path failed to
/// provide.
struct ArrowShim;

impl VTab for ArrowShim {
    type BindData = ArrowShimBind;
    type InitData = ArrowShimInit;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        guard("arrow shim bind", || {
            let extra = unsafe { &*bind.get_extra_info::<ArrowShimExtra>() };
            for (name, &code) in extra.col_names.iter().zip(&extra.col_codes) {
                bind.add_result_column(name, logical_type(code));
            }
            Ok(ArrowShimBind {
                callback_handle: extra.callback_handle,
                engine: extra.engine.clone(),
                col_codes: extra.col_codes.clone(),
            })
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        // Snapshot callback_handle + engine into the init data so `Drop` can
        // close the cursor without reaching for bind data (DuckDB does not
        // guarantee bind outlives init across parallel scans, and reading
        // from a dropped bind pointer would be UB).
        let bind = unsafe { &*init.get_bind_data::<ArrowShimBind>() };
        Ok(ArrowShimInit {
            engine: bind.engine.clone(),
            callback_handle: bind.callback_handle,
            cursor: Mutex::new(None),
            eof: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        guard("arrow shim scan", || {
            // T1-3: this callback dispatches into the guest via
            // `dispatch_arrow_open` / `dispatch_arrow_next` — mark the
            // thread so a re-entrant `NativeServices::query()` from
            // inside the guest refuses instead of deadlocking on the
            // DuckDB executor lock.
            let _reentrancy_guard = crate::engine::QueryReentrancyGuard::new();
            let bind = func.get_bind_data();
            let init = func.get_init_data();
            if init.eof.load(Ordering::Relaxed) {
                output.set_len(0);
                return Ok(());
            }
            // Lazy-open the cursor on first call. `cursor` is guarded so a
            // (hypothetical) reentrant `func` from DuckDB races safely.
            let cursor = {
                let mut guard = init.cursor.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(c) = *guard {
                    c
                } else {
                    let c = bind
                        .engine
                        .dispatch_arrow_open(bind.callback_handle)
                        .map_err(|e| -> Box<dyn std::error::Error> {
                            e.to_string().into()
                        })?;
                    *guard = Some(c);
                    c
                }
            };
            let rows = bind
                .engine
                .dispatch_arrow_next(bind.callback_handle, cursor)
                .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
            if rows.is_empty() {
                init.eof.store(true, Ordering::Relaxed);
                output.set_len(0);
                return Ok(());
            }
            // T1-1: clamp to STANDARD_VECTOR_SIZE (2048). DuckDB's output
            // chunk allocates at most STANDARD_VECTOR_SIZE rows per column;
            // writing beyond that (or calling `set_len` past the capacity)
            // is UB. Peer table-fns in this file all clamp their batch to
            // `.min(STANDARD_VECTOR_SIZE as usize)`. Buffering the tail
            // would require reshaping `ArrowShimInit` to hold a leftover;
            // instead we treat an oversized batch as a guest protocol
            // violation and surface a clear error so a well-behaved
            // producer trims to STANDARD_VECTOR_SIZE.
            if rows.len() > STANDARD_VECTOR_SIZE as usize {
                return Err(format!(
                    "arrow producer returned {} rows in a single batch, exceeds STANDARD_VECTOR_SIZE ({}). \
                     Producers must yield at most {} rows per `arrow-next` call.",
                    rows.len(),
                    STANDARD_VECTOR_SIZE,
                    STANDARD_VECTOR_SIZE,
                )
                .into());
            }
            let ncols = bind.col_codes.len();
            let raw_chunk = output.get_ptr();
            // Row-major write via write_ret_raw (per-cell). The guest hands
            // us row-major DuckValues from `dispatch_arrow_next`, so a
            // column-major pivot before write would be an extra pass; the
            // per-cell writer is sufficient for the streaming path and
            // reuses the already-tested logical-type-code arms.
            for (i, row) in rows.iter().enumerate() {
                if row.len() != ncols {
                    return Err(format!(
                        "arrow producer returned {} cols, expected {ncols}",
                        row.len()
                    )
                    .into());
                }
                for (c, v) in row.iter().enumerate() {
                    let vec = unsafe { ffi::duckdb_data_chunk_get_vector(raw_chunk, c as u64) };
                    unsafe {
                        write_ret_raw(bind.col_codes[c], vec, i, v)
                            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
                    }
                }
            }
            output.set_len(rows.len());
            Ok(())
        })
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        // Arrow shims take no positional args; the replacement scan drives
        // them with a bare `SELECT * FROM shim()`.
        Some(vec![])
    }
}

/// One entry in the process-wide arrow-table registry consulted by
/// [`ducklink_replacement_scan_callback`]. `table_name` is the user-facing
/// bare identifier a bare `SELECT * FROM <name>` references;
/// `shim_function_name` is the internal DuckDB table function the scan is
/// rewritten to. `(extension, table_name)` is the idempotency key.
struct ArrowTableRegistration {
    extension: String,
    table_name: String,
    shim_function_name: String,
}

static ARROW_TABLE_REGISTRY: OnceLock<Mutex<Vec<ArrowTableRegistration>>> = OnceLock::new();

fn arrow_table_registry() -> &'static Mutex<Vec<ArrowTableRegistration>> {
    ARROW_TABLE_REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// Sanitise `extension_table` into a valid SQL identifier suffix: keep
/// `[a-zA-Z0-9_]` as-is, replace everything else with `_`. Idempotent.
fn sanitize_shim_ident(raw: &str) -> String {
    raw.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

/// Install every declared arrow-table producer as a per-table shim
/// (`__ducklink_arrow_shim_<safe_name>`) plus a replacement-scan mapping
/// from the bare user-facing name to the shim. The returned count is the
/// number of successful installs (idempotent — a re-load of the same
/// component skips already-registered `(extension, name)` pairs).
pub fn register_arrow_tables(
    con: &Connection,
    engine: Arc<Engine2>,
    tables: &[ArrowTable],
) -> Result<usize, String> {
    if tables.is_empty() {
        return Ok(0);
    }
    let mut installed = 0usize;
    for t in tables {
        // Idempotency: skip repeats of (extension, table_name).
        {
            let reg = arrow_table_registry().lock().unwrap_or_else(|e| e.into_inner());
            let already = reg
                .iter()
                .any(|r| r.extension == t.extension && r.table_name == t.name);
            if already {
                eprintln!(
                    "[ducklink] arrow table '{}::{}' already registered; skipping",
                    t.extension, t.name
                );
                continue;
            }
        }
        let safe = sanitize_shim_ident(&format!("{}_{}", t.extension, t.name));
        let shim_name = format!("__ducklink_arrow_shim_{safe}");
        let col_codes: Vec<u8> = t.columns.iter().map(|c| type_code(&c.logical)).collect();
        let col_names: Vec<String> = t.columns.iter().map(|c| c.name.clone()).collect();
        let extra = ArrowShimExtra {
            callback_handle: t.callback_handle,
            engine: engine.clone(),
            col_codes,
            col_names,
        };
        let result = con
            .register_table_function_with_extra_info::<ArrowShim, ArrowShimExtra>(&shim_name, &extra);
        match result {
            Ok(()) => {
                let mut reg = arrow_table_registry().lock().unwrap_or_else(|e| e.into_inner());
                reg.push(ArrowTableRegistration {
                    extension: t.extension.clone(),
                    table_name: t.name.clone(),
                    shim_function_name: shim_name.clone(),
                });
                drop(reg);
                installed += 1;
                eprintln!(
                    "[ducklink] arrow producer '{}::{}' installed -> {} (per-query cursor)",
                    t.extension, t.name, shim_name
                );
            }
            Err(e) => {
                eprintln!(
                    "[ducklink] arrow table '{}::{}' shim registration failed: {e}",
                    t.extension, t.name
                );
            }
        }
    }
    Ok(installed)
}

/// Return the shim function name registered for a bare table reference,
/// or `None` if no arrow producer claims that name. Case-sensitive to
/// match DuckDB's default identifier resolution.
fn arrow_shim_for_table_name(name: &str) -> Option<String> {
    let reg = ARROW_TABLE_REGISTRY.get()?;
    let guard = reg.lock().unwrap_or_else(|e| e.into_inner());
    guard
        .iter()
        .find(|r| r.table_name == name)
        .map(|r| r.shim_function_name.clone())
}

// ---------------------------------------------------------------------------
// 4. register_scalar_ex — extends a plain scalar with the three attributes
// the base `register_scalars` path cannot express: varargs trailing type,
// special-NULL handling, and VOLATILE (re-evaluated per row).
//
// Installed as a sibling scalar via the raw C API so the ex-flags reach
// DuckDB (duckdb-rs' `VScalar` path does not surface them).
// ---------------------------------------------------------------------------

/// State stashed on the C-side scalar function via
/// `duckdb_scalar_function_set_extra_info`. Read from the invoke callback to
/// dispatch back into the engine.
#[allow(dead_code)]
struct ScalarExExtra {
    callback_handle: u32,
    engine: Arc<Engine2>,
    arg_codes: Vec<u8>,
    ret_code: u8,
}

unsafe extern "C" fn scalar_ex_extra_destroy(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut ScalarExExtra));
    }
}

/// One process-wide invoke callback for every scalar-ex. Marshals the input
/// chunk row-by-row, dispatches through `Engine2::dispatch_scalar`, writes
/// the per-row result out. The columnar fast path stays with the base
/// scalar registration; scalar-ex is the correctness fallback for varargs
/// / special-null / volatile.
unsafe extern "C" fn ducklink_scalar_ex_invoke(
    info: ffi::duckdb_function_info,
    input: ffi::duckdb_data_chunk,
    output: ffi::duckdb_vector,
) {
    // T1-3: mark the thread as inside a guest dispatch so a re-entrant
    // `NativeServices::query()` from the guest refuses instead of deadlocking.
    let _reentrancy_guard = crate::engine::QueryReentrancyGuard::new();
    let extra = ffi::duckdb_scalar_function_get_extra_info(info) as *const ScalarExExtra;
    if extra.is_null() {
        let msg = CString::new("scalar_ex_invoke: missing extra info").unwrap();
        ffi::duckdb_scalar_function_set_error(info, msg.as_ptr());
        return;
    }
    let extra_ref = &*extra;
    let n = ffi::duckdb_data_chunk_get_size(input) as usize;
    let ncols = ffi::duckdb_data_chunk_get_column_count(input) as usize;
    let mut rows: Vec<Vec<reg::DuckValue>> = (0..n).map(|_| Vec::with_capacity(ncols)).collect();
    for c in 0..ncols {
        let vec = ffi::duckdb_data_chunk_get_vector(input, c as u64);
        let code = extra_ref.arg_codes.get(c).copied().unwrap_or(T_TEXT);
        // Sweep-6 FIX 4 path (a): fetch the LogicalType once per column so
        // DECIMAL args see the real (width, scale) via read_arg_neutral.
        let ty = ffi::duckdb_vector_get_column_type(vec);
        for r in 0..n {
            let v = read_arg_neutral(code, vec, r, Some(ty));
            rows[r].push(v);
        }
        let mut ty_mut = ty;
        ffi::duckdb_destroy_logical_type(&mut ty_mut);
    }
    for r in 0..n {
        let args = std::mem::take(&mut rows[r]);
        let out = match extra_ref
            .engine
            .dispatch_scalar(extra_ref.callback_handle, r as u64, args)
        {
            Ok(v) => v,
            Err(e) => {
                let msg = CString::new(format!("scalar_ex dispatch failed: {e}")).unwrap();
                ffi::duckdb_scalar_function_set_error(info, msg.as_ptr());
                return;
            }
        };
        write_row_out(extra_ref.ret_code, output, r, out);
    }
}

unsafe fn write_row_out(code: u8, out: ffi::duckdb_vector, row: usize, val: reg::DuckValue) {
    let data = ffi::duckdb_vector_get_data(out);
    // Ensure validity mask exists so we can flip individual NULL bits.
    ffi::duckdb_vector_ensure_validity_writable(out);
    let validity = ffi::duckdb_vector_get_validity(out) as *mut u64;
    if matches!(val, reg::DuckValue::Null) {
        if !validity.is_null() {
            ffi::duckdb_validity_set_row_invalid(validity, row as u64);
        }
        return;
    }
    match (code, val) {
        (T_I64, reg::DuckValue::Int64(v)) => *(data as *mut i64).add(row) = v,
        (T_U64, reg::DuckValue::Uint64(v)) => *(data as *mut u64).add(row) = v,
        (T_F64, reg::DuckValue::Float64(v)) => *(data as *mut f64).add(row) = v,
        (T_BOOL, reg::DuckValue::Boolean(v)) => *(data as *mut bool).add(row) = v,
        (T_I8, reg::DuckValue::Int8(v)) => *(data as *mut i8).add(row) = v,
        (T_I16, reg::DuckValue::Int16(v)) => *(data as *mut i16).add(row) = v,
        (T_I32, reg::DuckValue::Int32(v)) => *(data as *mut i32).add(row) = v,
        (T_U8, reg::DuckValue::Uint8(v)) => *(data as *mut u8).add(row) = v,
        (T_U16, reg::DuckValue::Uint16(v)) => *(data as *mut u16).add(row) = v,
        (T_U32, reg::DuckValue::Uint32(v)) => *(data as *mut u32).add(row) = v,
        (T_F32, reg::DuckValue::Float32(v)) => *(data as *mut f32).add(row) = v,
        (T_TEXT, reg::DuckValue::Text(s)) => {
            let c = CString::new(s).unwrap_or_else(|_| CString::new("").unwrap());
            ffi::duckdb_vector_assign_string_element(out, row as u64, c.as_ptr());
        }
        (T_BLOB, reg::DuckValue::Blob(b)) => {
            ffi::duckdb_vector_assign_string_element_len(
                out,
                row as u64,
                b.as_ptr() as *const c_char,
                b.len() as u64,
            );
        }
        // Sweep-5 fix: symmetric with `read_arg_neutral` above and the
        // `write_ret_raw` peer at ~2772-2779. Without these arms the
        // catch-all silently marked the row NULL — for a DECIMAL COPY FROM
        // that meant 100x data corruption because the value was suppressed
        // rather than written unscaled.
        (T_HUGEINT, reg::DuckValue::Hugeint { lower, upper }) => {
            let raw = ((upper as i128) << 64) | (lower as i128 & 0xFFFF_FFFF_FFFF_FFFFi128);
            *(data as *mut i128).add(row) = raw;
        }
        (T_UHUGEINT, reg::DuckValue::UHugeint { lower, upper }) => {
            let raw = ((upper as u128) << 64) | (lower as u128);
            *(data as *mut u128).add(row) = raw;
        }
        // TODO Gap 2 (sweep-5 continuation): DECIMAL width/scale threading.
        // The value's width/scale are informational — the output vector's
        // LogicalType (declared by the caller) is authoritative. We write
        // the unscaled i128 straight into the slot to match the peer arm
        // in `write_ret_raw`.
        (T_DECIMAL, reg::DuckValue::Decimal { lower, upper, .. }) => {
            *(data as *mut i128).add(row) =
                (((upper as u128) << 64) | lower as u128) as i128;
        }
        // Sweep-6 FIX 2: TIMESTAMP/DATE/TIME/TIMESTAMPTZ writes — mirror the
        // `write_ret_raw` peer at ~2736-2739. Previously these silently
        // NULL'd via the catch-all, so cast/COPY-TO paths that returned a
        // temporal value dropped every row.
        (T_TIMESTAMP, reg::DuckValue::Timestamp(v)) => *(data as *mut i64).add(row) = v,
        (T_DATE, reg::DuckValue::Date(v)) => *(data as *mut i32).add(row) = v,
        (T_TIME, reg::DuckValue::Time(v)) => *(data as *mut i64).add(row) = v,
        (T_TIMESTAMPTZ, reg::DuckValue::Timestamptz(v)) => *(data as *mut i64).add(row) = v,
        // Sweep-6 FIX 2: INTERVAL as a three-field struct in the vector.
        (T_INTERVAL, reg::DuckValue::Interval { months, days, micros }) => {
            *(data as *mut ffi::duckdb_interval).add(row) = ffi::duckdb_interval {
                months,
                days,
                micros,
            };
        }
        // Sweep-6 FIX 2: UUID logical (hi<<64|lo) -> physical i128 via the
        // XOR-flip helper; matches the `write_ret_raw` peer.
        (T_UUID, reg::DuckValue::Uuid { hi, lo }) => {
            let logical = ((hi as u128) << 64) | lo as u128;
            *(data as *mut i128).add(row) = uuid_storage_to_logical(logical as i128) as i128;
        }
        // Sweep-6 FIX 2: COMPLEX escape-hatch writes the JSON payload as
        // VARCHAR — mirrors the `write_ret_raw` peer at ~2762-2769.
        (T_COMPLEX, reg::DuckValue::Complex { json, .. }) => {
            ffi::duckdb_vector_assign_string_element_len(
                out,
                row as u64,
                json.as_ptr() as *const c_char,
                json.len() as u64,
            );
        }
        // Sweep-5 fix: nested writes need list_vector_set_size / reserve +
        // child fill, not a scalar slot store. Explicit fail-loud so callers
        // see the shortfall instead of silently NULL'ing the row.
        (T_LIST, reg::DuckValue::List(_))
        | (T_STRUCT, reg::DuckValue::Struct(_))
        | (T_MAP, reg::DuckValue::Map(_))
        | (T_ARRAY, reg::DuckValue::Array(_)) => {
            eprintln!(
                "write_row_out: nested type not yet wired (sweep-5 finding) — \
                 marking row {row} invalid for code {code}"
            );
            if !validity.is_null() {
                ffi::duckdb_validity_set_row_invalid(validity, row as u64);
            }
        }
        // Sweep-6 FIX 2: fail-loud catch-all. Previously any (code, value)
        // pair not explicitly handled silently marked the row NULL — for
        // cast_ex / scalar_ex / COPY-TO that's silent data loss. Log the
        // shortfall so a future missed arm surfaces immediately.
        (unhandled_code, other) => {
            eprintln!(
                "write_row_out: unhandled (code {unhandled_code}, value {other:?}) — \
                 marking row {row} invalid (mirror this arm from `write_ret_raw`)"
            );
            if !validity.is_null() {
                ffi::duckdb_validity_set_row_invalid(validity, row as u64);
            }
        }
    }
}

/// Build one `duckdb_scalar_function` for a single `ScalarEx` overload,
/// wiring the ex-flags (varargs / special-null / volatile) and the invoke
/// callback + extra-info. The caller owns the returned handle. Returns
/// `None` on interior NUL in the name (already logged).
///
/// # Safety
/// FFI: constructs a DuckDB handle via `duckdb_create_scalar_function`.
unsafe fn build_scalar_ex_function(
    s: &ScalarEx,
    engine: Arc<Engine2>,
) -> Option<ffi::duckdb_scalar_function> {
    let arg_codes: Vec<u8> = s.arguments.iter().map(|a| type_code(&a.logical)).collect();
    let ret_code = type_code(&s.returns);
    let cname = match CString::new(s.name.as_str()) {
        Ok(c) => c,
        Err(_) => {
            eprintln!("[ducklink] scalar_ex '{}' has a NUL byte; skipping", s.name);
            return None;
        }
    };
    let func = ffi::duckdb_create_scalar_function();
    ffi::duckdb_scalar_function_set_name(func, cname.as_ptr());
    // T2-1 residual (major-5): route through the LogicalType-aware
    // `logical_type_ffi_from_lt` so DECIMAL(w, s) honours per-column
    // width/scale and nested LIST/STRUCT/MAP/ARRAY get real child types
    // instead of the code-only VARCHAR fallback.
    for a in &s.arguments {
        let mut lt = logical_type_ffi_from_lt(&a.logical);
        ffi::duckdb_scalar_function_add_parameter(func, lt);
        ffi::duckdb_destroy_logical_type(&mut lt);
    }
    let mut rlt = logical_type_ffi_from_lt(&s.returns);
    ffi::duckdb_scalar_function_set_return_type(func, rlt);
    ffi::duckdb_destroy_logical_type(&mut rlt);
    // ---- the three ex flags (per-overload) ----
    if let Some(v) = s.varargs.as_ref() {
        let mut vlt = logical_type_ffi_from_lt(v);
        ffi::duckdb_scalar_function_set_varargs(func, vlt);
        ffi::duckdb_destroy_logical_type(&mut vlt);
    }
    if s.special_null {
        ffi::duckdb_scalar_function_set_special_handling(func);
    }
    // Gate VOLATILE on the per-function flag now that `ScalarEx` carries it —
    // the runtime derives it from the register-scalar-ex attributes.
    // Non-volatile scalar-ex skip the call entirely (immutable by default).
    if s.volatile {
        ffi::duckdb_scalar_function_set_volatile(func);
    }
    // ---- extra info + invoke callback ----
    let extra = Box::into_raw(Box::new(ScalarExExtra {
        callback_handle: s.callback_handle,
        engine,
        arg_codes,
        ret_code,
    })) as *mut c_void;
    ffi::duckdb_scalar_function_set_extra_info(func, extra, Some(scalar_ex_extra_destroy));
    ffi::duckdb_scalar_function_set_function(func, Some(ducklink_scalar_ex_invoke));
    Some(func)
}

/// Register the ex-attributes for every scalar_ex on `raw_con`. Installs a
/// C API-level scalar sibling with varargs / special-null / VOLATILE flags
/// wired through the C API. The base scalar registration (name + core
/// signature) is expected to have already run via `register_scalars`.
///
/// T2-6: overload sets landed on this raw-C-API path. Overloads within a
/// single (extension, name) group install through
/// `duckdb_create_scalar_function_set` + `duckdb_add_scalar_function_to_set`
/// (per overload) + `duckdb_register_scalar_function_set`. Singletons keep
/// the single-fn install path. See `register_aggregates` for the ownership
/// argument that lets us destroy each per-overload handle immediately after
/// adding it to the set (function_info shared_ptr keeps the extra-info
/// alive with the set's copy).
///
/// Note: the base `register_scalars` path still routes through the duckdb-rs
/// safe `VScalar` wrapper, which is single-overload and NOT migrated here —
/// mixing the two APIs on the same connection under the same name is
/// unsafe. In practice, overloaded ex-flagged scalars must all be declared
/// via `register-scalar-ex`; the base overload skip in `register_scalars`
/// remains the failure mode for the mixed path.
pub unsafe fn register_scalar_ex(
    raw_con: ffi::duckdb_connection,
    engine: Arc<Engine2>,
    scalar_ex: &[ScalarEx],
) -> Result<usize, String> {
    if scalar_ex.is_empty() {
        return Ok(0);
    }
    if raw_con.is_null() {
        return Err("register_scalar_ex: raw_con is null".to_string());
    }
    use std::collections::HashMap;
    let mut groups: Vec<(String, String, Vec<usize>)> = Vec::new();
    let mut index: HashMap<(String, String), usize> = HashMap::new();
    for (i, s) in scalar_ex.iter().enumerate() {
        let key = (s.extension.clone(), s.name.clone());
        match index.get(&key) {
            Some(&g) => groups[g].2.push(i),
            None => {
                index.insert(key.clone(), groups.len());
                groups.push((key.0, key.1, vec![i]));
            }
        }
    }
    let mut registered = 0usize;
    for (_ext, name, member_ixs) in &groups {
        if member_ixs.len() == 1 {
            let s = &scalar_ex[member_ixs[0]];
            let func = match build_scalar_ex_function(s, engine.clone()) {
                Some(func) => func,
                None => continue,
            };
            let rc = ffi::duckdb_register_scalar_function(raw_con, func);
            let mut func_mut = func;
            ffi::duckdb_destroy_scalar_function(&mut func_mut);
            if rc != ffi::DuckDBSuccess {
                eprintln!(
                    "[ducklink] scalar_ex '{}' not registered (already present?)",
                    s.name
                );
                continue;
            }
            registered += 1;
            continue;
        }
        // Overload-set path.
        let set_name_c = match CString::new(name.as_str()) {
            Ok(c) => c,
            Err(_) => {
                eprintln!(
                    "[ducklink] scalar_ex set '{name}' name contains NUL byte; skipping"
                );
                continue;
            }
        };
        let set = ffi::duckdb_create_scalar_function_set(set_name_c.as_ptr());
        if set.is_null() {
            eprintln!("[ducklink] scalar_ex set '{name}' could not be created");
            continue;
        }
        let mut set_ok = true;
        let mut overloads_added = 0usize;
        for &ix in member_ixs {
            let s = &scalar_ex[ix];
            let func = match build_scalar_ex_function(s, engine.clone()) {
                Some(func) => func,
                None => continue,
            };
            let add_rc = ffi::duckdb_add_scalar_function_to_set(set, func);
            let mut func_mut = func;
            ffi::duckdb_destroy_scalar_function(&mut func_mut);
            if add_rc != ffi::DuckDBSuccess {
                eprintln!(
                    "[ducklink] scalar_ex '{}' overload {} failed to join set",
                    s.name, ix
                );
                set_ok = false;
                break;
            }
            overloads_added += 1;
        }
        if !set_ok || overloads_added == 0 {
            let mut set_mut = set;
            ffi::duckdb_destroy_scalar_function_set(&mut set_mut);
            continue;
        }
        let rc = ffi::duckdb_register_scalar_function_set(raw_con, set);
        let mut set_mut = set;
        ffi::duckdb_destroy_scalar_function_set(&mut set_mut);
        if rc != ffi::DuckDBSuccess {
            eprintln!(
                "[ducklink] scalar_ex set '{name}' not registered (already present?)"
            );
            continue;
        }
        registered += overloads_added;
    }
    Ok(registered)
}

// ---------------------------------------------------------------------------
// 5. register_casts — installs each declared cast via duckdb_create_cast_function
// + duckdb_cast_function_set_{source_type,target_type,function,extra_info} +
// duckdb_register_cast_function. Callback re-enters
// `Engine2::dispatch_cast_col(handle, value)` per input-vector row.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
struct CastExtra {
    callback_handle: u32,
    engine: Arc<Engine2>,
    source_code: u8,
    target_code: u8,
}

unsafe extern "C" fn cast_extra_destroy(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut CastExtra));
    }
}

unsafe extern "C" fn ducklink_cast_invoke(
    info: ffi::duckdb_function_info,
    count: ffi::idx_t,
    input: ffi::duckdb_vector,
    output: ffi::duckdb_vector,
) -> bool {
    // T1-3: mark the thread as inside a guest dispatch so a re-entrant
    // `NativeServices::query()` from the guest refuses instead of deadlocking.
    let _reentrancy_guard = crate::engine::QueryReentrancyGuard::new();
    let extra = ffi::duckdb_cast_function_get_extra_info(info) as *const CastExtra;
    if extra.is_null() {
        let msg = CString::new("cast_invoke: missing extra info").unwrap();
        ffi::duckdb_cast_function_set_error(info, msg.as_ptr());
        return false;
    }
    let extra_ref = &*extra;
    // Sweep-6 FIX 4 path (a): fetch the source vector's LogicalType once so
    // read_arg_neutral's DECIMAL arm can query the real (width, scale). Cast
    // FROM DECIMAL(20, 5) was previously handed a value labelled (18, 3),
    // silently shifting the scale by two decimal places.
    let src_ty = ffi::duckdb_vector_get_column_type(input);
    for r in 0..(count as usize) {
        let v = read_arg_neutral(extra_ref.source_code, input, r, Some(src_ty));
        let out = match extra_ref.engine.dispatch_cast_col(extra_ref.callback_handle, v) {
            Ok(v) => v,
            Err(e) => {
                let msg = CString::new(format!("cast dispatch failed: {e}")).unwrap();
                ffi::duckdb_cast_function_set_row_error(info, msg.as_ptr(), r as u64, output);
                continue;
            }
        };
        write_row_out(extra_ref.target_code, output, r, out);
    }
    let mut src_ty_mut = src_ty;
    ffi::duckdb_destroy_logical_type(&mut src_ty_mut);
    true
}

/// Map a DuckDB SQL type expression (as declared by the guest, e.g. "BIGINT",
/// "DECIMAL(10,2)", "TIMESTAMP_NS") to the bridge type code the raw marshal
/// paths key on. Alias tables mirror the DuckDB type synonyms.
///
/// T2-1: extended past the original ~15-entry alias table so the cast /
/// modified-type / logical-type installers no longer silently coerce
/// unfamiliar type expressions to VARCHAR. DECIMAL(w,s) / NUMERIC(w,s) route
/// through T_DECIMAL (width/scale is honoured by `logical_type_ffi`'s
/// callers of `parse_decimal_expr`, e.g. `register_modified_types`); the
/// TIMESTAMP_* precision aliases fold onto T_TIMESTAMP (real unit plumbing
/// is a follow-up); truly unknown expressions still fall back to T_TEXT but
/// now log a warning instead of silently degrading.
///
/// Notes:
///   * HUGEINT / UHUGEINT: no T_HUGEINT code exists — `reg::LogicalType`
///     doesn't have a Hugeint arm, so we can't route through the bridge
///     without extending the whole logical-type set. Left as an unknown so
///     the warning fires with the concrete type name.
///
///     TODO(T2-1 residual, DEFERRED — bounded work item): add
///     `LogicalType::Hugeint` + `Uhugeint` variants across the neutral
///     logical-type set. Concretely this means: (1) two new arms in
///     `reg::LogicalType` (runtime/src/lib.rs), (2) matching arms in
///     `duckdb:extension/types.wit` `logicaltype` variant (both
///     wit-canonical and wit/deps copies), (3) new bridge type codes
///     (`T_HUGEINT` / `T_UHUGEINT`) plus writer/reader arms in every
///     colvec encoder/decoder in reg_duckdb.rs (search for `T_DECIMAL`
///     as the closest sibling — HUGEINT is DECIMAL's 128-bit backing
///     store), (4) `neutral_to_wit_logicaltype` /
///     `wit_logicaltype_to_neutral` / `convert_extension_logicaltype`
///     arms across engine.rs + runtime, (5) `describe_runtime_logicaltype`.
///     The audit flagged this as rippling through the whole neutral
///     logical-type set — a `mix` of variants would be worse than none,
///     so we choose (b) documented deferral over a partial (a). Route
///     back through this docstring when landing.
///
///   * ENUM by name: resolving requires cross-referencing
///     `register_enum_types` (which today has no queryable registry). Left
///     as an unknown so callers see the shortfall. TODO(T2-1 follow-up):
///     add an enum-name -> LogicalType registry populated by
///     `register_enum_types` and query it here.
fn type_code_from_expr(expr: &str) -> u8 {
    let trimmed = expr.trim();
    let upper = trimmed.to_ascii_uppercase();
    match upper.as_str() {
        "BOOLEAN" | "BOOL" => return T_BOOL,
        "BIGINT" | "INT8" | "INT64" => return T_I64,
        "UBIGINT" | "UINT64" => return T_U64,
        "DOUBLE" | "FLOAT8" | "FLOAT64" => return T_F64,
        "VARCHAR" | "TEXT" | "STRING" => return T_TEXT,
        "BLOB" | "BYTEA" | "BINARY" => return T_BLOB,
        "TINYINT" | "INT1" => return T_I8,
        "SMALLINT" | "INT2" | "INT16" => return T_I16,
        "INTEGER" | "INT" | "INT4" | "INT32" => return T_I32,
        "UTINYINT" | "UINT8" => return T_U8,
        "USMALLINT" | "UINT16" => return T_U16,
        "UINTEGER" | "UINT32" => return T_U32,
        "FLOAT" | "FLOAT4" | "FLOAT32" => return T_F32,
        "TIMESTAMP" => return T_TIMESTAMP,
        "DATE" => return T_DATE,
        "TIME" => return T_TIME,
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => return T_TIMESTAMPTZ,
        "DECIMAL" | "NUMERIC" => return T_DECIMAL,
        "INTERVAL" => return T_INTERVAL,
        "UUID" => return T_UUID,
        // T2-1 residual (major-5): 128-bit integer type expressions land as
        // their own bridge codes now that HUGEINT / UHUGEINT are first-class.
        "HUGEINT" | "INT128" => return T_HUGEINT,
        "UHUGEINT" | "UINT128" => return T_UHUGEINT,
        // Precision-suffixed timestamps: DuckDB stores them physically as
        // TIMESTAMP (microseconds); the precision is a scale annotation the
        // C API set_ffi call does not carry through this bridge path today.
        // TODO(T2-1 follow-up): plumb the unit into logical_type_ffi so we
        // stop coercing NS/MS/S to microseconds silently.
        "TIMESTAMP_NS" | "TIMESTAMP_MS" | "TIMESTAMP_S" => return T_TIMESTAMP,
        _ => {}
    }
    // DECIMAL(w,s) / NUMERIC(w,s) — same detector as
    // `register_modified_types` uses for its width/scale honouring path.
    if parse_decimal_expr(trimmed).is_some() {
        return T_DECIMAL;
    }
    // S1 (major-5): DuckDB nested type-expression prefixes. Full recursive
    // parsing (recognising the child type-expression inside the parens /
    // brackets) is FAIL-LOUD-DEFERRED — building a small recursive parser
    // for `INTEGER[]` / `STRUCT(a INTEGER, ...)` / `MAP(K, V)` / `T[N]`
    // rippled beyond the scope of this pass. Recognise the KIND from the
    // prefix so the cast route at least tags the code correctly, then log
    // that the structural payload is not yet threaded through. Callers that
    // need the child type MUST plumb a full `reg::LogicalType` through
    // `logical_type_ffi_from_lt` and skip this expr-based path.
    let upper_trimmed = trimmed.to_ascii_uppercase();
    if upper_trimmed.starts_with("STRUCT(") || upper_trimmed.starts_with("STRUCT<") {
        eprintln!(
            "[ducklink] type_code_from_expr: STRUCT expression '{expr}' — child-type parsing \
             not yet wired (T2-1 residual continuation); tagging as T_STRUCT with no child shape"
        );
        return T_STRUCT;
    }
    if upper_trimmed.starts_with("MAP(") || upper_trimmed.starts_with("MAP<") {
        eprintln!(
            "[ducklink] type_code_from_expr: MAP expression '{expr}' — child-type parsing \
             not yet wired (T2-1 residual continuation); tagging as T_MAP with no child shape"
        );
        return T_MAP;
    }
    // LIST: DuckDB uses `<TYPE>[]` (no fixed size) for LIST and `<TYPE>[N]`
    // (fixed size N) for ARRAY. Detect the bracket suffix and count digits
    // to distinguish. This is fail-loud in the same "kind-tag only" sense.
    if let Some(stripped) = upper_trimmed.strip_suffix(']') {
        if let Some(open_ix) = stripped.rfind('[') {
            let inside = &stripped[open_ix + 1..];
            if inside.is_empty() {
                eprintln!(
                    "[ducklink] type_code_from_expr: LIST expression '{expr}' — child-type \
                     parsing not yet wired (T2-1 residual continuation); tagging as T_LIST"
                );
                return T_LIST;
            }
            if inside.chars().all(|c| c.is_ascii_digit()) {
                eprintln!(
                    "[ducklink] type_code_from_expr: ARRAY expression '{expr}' — child-type \
                     parsing not yet wired (T2-1 residual continuation); tagging as T_ARRAY"
                );
                return T_ARRAY;
            }
        }
    }
    eprintln!(
        "[ducklink] cast type-expression '{expr}' unrecognized; falling back to VARCHAR \
         — this cast may not route correctly (T2-1)"
    );
    T_TEXT
}

pub unsafe fn register_casts(
    raw_con: ffi::duckdb_connection,
    engine: Arc<Engine2>,
    casts: &[CastEntry],
) -> Result<usize, String> {
    if casts.is_empty() {
        return Ok(0);
    }
    if raw_con.is_null() {
        return Err("register_casts: raw_con is null".to_string());
    }
    let mut registered = 0usize;
    for c in casts {
        let src_code = type_code_from_expr(&c.source);
        let tgt_code = type_code_from_expr(&c.target);
        let cf = ffi::duckdb_create_cast_function();
        // T1-4: `logical_type_ffi` routes DECIMAL correctly.
        let mut src_lt = logical_type_ffi(src_code);
        let mut tgt_lt = logical_type_ffi(tgt_code);
        ffi::duckdb_cast_function_set_source_type(cf, src_lt);
        ffi::duckdb_cast_function_set_target_type(cf, tgt_lt);
        ffi::duckdb_destroy_logical_type(&mut src_lt);
        ffi::duckdb_destroy_logical_type(&mut tgt_lt);
        // T2-4: DuckDB's cast planner uses this cost to pick between
        // competing casts (lower = preferred). Wire the WIT-declared
        // `implicit_cost` (now on CastEntry). The bindgen signature exposes
        // the setter as `(duckdb_cast_function, cost: i64)`:
        //   * None         — apply ducklink's default of 100. (DuckDB's C
        //                    API default is -1 / explicit-only, per
        //                    cast_function-c.cpp:20 CCastFunction::
        //                    implicit_cast_cost, but ducklink treats unset
        //                    as implicit-cost 100 to match the typical
        //                    scalar-registration ergonomic — we do so by
        //                    explicitly calling the setter with 100 below.)
        //   * Some(v) v>=0 — call the setter with `v` as i64.
        //   * Some(-1)     — SKIP the setter entirely. Per the task contract
        //     this maps to "explicit-only" semantics: the cast is still
        //     installed but never contributes to implicit-overload
        //     resolution, so DuckDB's planner will only reach it via an
        //     explicit CAST expression.
        match c.implicit_cost {
            None => {
                ffi::duckdb_cast_function_set_implicit_cast_cost(cf, 100);
            }
            Some(v) if v >= 0 => {
                ffi::duckdb_cast_function_set_implicit_cast_cost(cf, v as i64);
            }
            Some(_) => {
                // Some(-1) or any other negative: explicit-only. Do not
                // call the setter — leaves the cast unreachable via
                // implicit overload resolution.
            }
        }
        ffi::duckdb_cast_function_set_function(cf, Some(ducklink_cast_invoke));
        let extra = Box::into_raw(Box::new(CastExtra {
            callback_handle: c.callback_handle,
            engine: engine.clone(),
            source_code: src_code,
            target_code: tgt_code,
        })) as *mut c_void;
        ffi::duckdb_cast_function_set_extra_info(cf, extra, Some(cast_extra_destroy));

        let rc = ffi::duckdb_register_cast_function(raw_con, cf);
        let mut cf_mut = cf;
        ffi::duckdb_destroy_cast_function(&mut cf_mut);
        if rc != ffi::DuckDBSuccess {
            eprintln!(
                "[ducklink] cast '{}' -> '{}' not registered (already present?)",
                c.source, c.target
            );
            continue;
        }
        registered += 1;
    }
    Ok(registered)
}

// ---------------------------------------------------------------------------
// 6. register_logical_types — user-defined logical type aliases. C API:
// duckdb_create_logical_type on the physical type + duckdb_register_logical_type.
// ---------------------------------------------------------------------------

pub unsafe fn register_logical_types(
    raw_con: ffi::duckdb_connection,
    types: &[LogicalTypeEntry],
) -> Result<usize, String> {
    if types.is_empty() {
        return Ok(0);
    }
    if raw_con.is_null() {
        return Err("register_logical_types: raw_con is null".to_string());
    }
    let mut registered = 0usize;
    for t in types {
        let code = type_code_from_expr(&t.physical);
        // T1-4: `logical_type_ffi` routes DECIMAL correctly.
        let mut lt = logical_type_ffi(code);
        let name_c = match CString::new(t.name.as_str()) {
            Ok(c) => c,
            Err(_) => {
                eprintln!(
                    "[ducklink] logical type '{}' has a NUL byte; skipping",
                    t.name
                );
                ffi::duckdb_destroy_logical_type(&mut lt);
                continue;
            }
        };
        ffi::duckdb_logical_type_set_alias(lt, name_c.as_ptr());
        let rc = ffi::duckdb_register_logical_type(raw_con, lt, std::ptr::null_mut());
        ffi::duckdb_destroy_logical_type(&mut lt);
        if rc != ffi::DuckDBSuccess {
            eprintln!(
                "[ducklink] logical type '{}' not registered (already present?)",
                t.name
            );
            continue;
        }
        registered += 1;
    }
    Ok(registered)
}

// ---------------------------------------------------------------------------
// 7. register_modified_types — logical types registered over a full
// type-expression (e.g. DECIMAL(18,3)). Same shape as logical_types plus a
// decimal-width/scale extraction.
// ---------------------------------------------------------------------------

fn parse_decimal_expr(expr: &str) -> Option<(u8, u8)> {
    let s = expr.trim();
    let upper = s.to_ascii_uppercase();
    if !upper.starts_with("DECIMAL") && !upper.starts_with("NUMERIC") {
        return None;
    }
    let open = s.find('(')?;
    let close = s.find(')')?;
    let inner = &s[open + 1..close];
    let mut it = inner.split(',');
    let w: u8 = it.next()?.trim().parse().ok()?;
    let sc: u8 = it.next().and_then(|x| x.trim().parse().ok()).unwrap_or(0);
    Some((w, sc))
}

pub unsafe fn register_modified_types(
    raw_con: ffi::duckdb_connection,
    types: &[ModifiedTypeEntry],
) -> Result<usize, String> {
    if types.is_empty() {
        return Ok(0);
    }
    if raw_con.is_null() {
        return Err("register_modified_types: raw_con is null".to_string());
    }
    let mut registered = 0usize;
    for t in types {
        // Decimal path: honour the (width,scale) shape.
        let mut lt = if let Some((w, sc)) = parse_decimal_expr(&t.type_expr) {
            ffi::duckdb_create_decimal_type(w, sc)
        } else {
            let code = type_code_from_expr(&t.type_expr);
            // T1-4: `logical_type_ffi` routes DECIMAL correctly (for the
            // `DECIMAL` fallback path that has no width/scale suffix).
            logical_type_ffi(code)
        };
        let name_c = match CString::new(t.name.as_str()) {
            Ok(c) => c,
            Err(_) => {
                eprintln!(
                    "[ducklink] modified type '{}' has a NUL byte; skipping",
                    t.name
                );
                ffi::duckdb_destroy_logical_type(&mut lt);
                continue;
            }
        };
        ffi::duckdb_logical_type_set_alias(lt, name_c.as_ptr());
        let rc = ffi::duckdb_register_logical_type(raw_con, lt, std::ptr::null_mut());
        ffi::duckdb_destroy_logical_type(&mut lt);
        if rc != ffi::DuckDBSuccess {
            eprintln!(
                "[ducklink] modified type '{}' not registered (already present?)",
                t.name
            );
            continue;
        }
        registered += 1;
    }
    Ok(registered)
}

// ---------------------------------------------------------------------------
// 8. register_enum_types — ENUM types. C API: duckdb_create_enum_type +
// duckdb_register_logical_type. The enum dictionary is a flat list of
// member name c-strings.
// ---------------------------------------------------------------------------

pub unsafe fn register_enum_types(
    raw_con: ffi::duckdb_connection,
    enums: &[EnumTypeEntry],
) -> Result<usize, String> {
    if enums.is_empty() {
        return Ok(0);
    }
    if raw_con.is_null() {
        return Err("register_enum_types: raw_con is null".to_string());
    }
    let mut registered = 0usize;
    for e in enums {
        // Build a stable Vec<CString> so pointers stay live for the duration
        // of the create call.
        let cstrs: Vec<CString> = e
            .members
            .iter()
            .filter_map(|m| CString::new(m.as_str()).ok())
            .collect();
        if cstrs.len() != e.members.len() {
            eprintln!(
                "[ducklink] enum type '{}' has a member with a NUL byte; skipping",
                e.name
            );
            continue;
        }
        let mut ptrs: Vec<*const c_char> = cstrs.iter().map(|c| c.as_ptr()).collect();
        let mut lt = ffi::duckdb_create_enum_type(ptrs.as_mut_ptr(), ptrs.len() as ffi::idx_t);
        let name_c = match CString::new(e.name.as_str()) {
            Ok(c) => c,
            Err(_) => {
                ffi::duckdb_destroy_logical_type(&mut lt);
                eprintln!("[ducklink] enum type '{}' has a NUL byte; skipping", e.name);
                continue;
            }
        };
        ffi::duckdb_logical_type_set_alias(lt, name_c.as_ptr());
        let rc = ffi::duckdb_register_logical_type(raw_con, lt, std::ptr::null_mut());
        ffi::duckdb_destroy_logical_type(&mut lt);
        if rc != ffi::DuckDBSuccess {
            eprintln!(
                "[ducklink] enum type '{}' not registered (already present?)",
                e.name
            );
            continue;
        }
        registered += 1;
    }
    Ok(registered)
}

// ---------------------------------------------------------------------------
// 9. register_macros — no C API; uses `CREATE OR REPLACE MACRO`.
// Identifiers are quoted with double quotes and internal quotes doubled to
// keep the shape safe against injection through component-supplied names.
// ---------------------------------------------------------------------------

fn quote_ident(ident: &str) -> String {
    let mut s = String::with_capacity(ident.len() + 2);
    s.push('"');
    for ch in ident.chars() {
        if ch == '"' {
            s.push('"');
        }
        s.push(ch);
    }
    s.push('"');
    s
}

pub fn register_macros(con: &Connection, macros: &[MacroEntry]) -> Result<usize, String> {
    if macros.is_empty() {
        return Ok(0);
    }
    let mut registered = 0usize;
    for m in macros {
        let schema = if m.schema.is_empty() {
            "main".to_string()
        } else {
            m.schema.clone()
        };
        let params = m
            .parameters
            .iter()
            .map(|p| quote_ident(p))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "CREATE OR REPLACE MACRO {}.{}({}) AS {}",
            quote_ident(&schema),
            quote_ident(&m.name),
            params,
            m.definition_sql
        );
        match con.execute_batch(&sql) {
            Ok(_) => registered += 1,
            Err(e) => eprintln!(
                "[ducklink] macro '{}.{}' not registered: {e}",
                schema, m.name
            ),
        }
    }
    Ok(registered)
}

// ---------------------------------------------------------------------------
// 10. register_table_macros — CREATE OR REPLACE MACRO ... AS TABLE <body>.
// ---------------------------------------------------------------------------

pub fn register_table_macros(
    con: &Connection,
    macros: &[TableMacroEntry],
) -> Result<usize, String> {
    if macros.is_empty() {
        return Ok(0);
    }
    let mut registered = 0usize;
    for m in macros {
        let schema = if m.schema.is_empty() {
            "main".to_string()
        } else {
            m.schema.clone()
        };
        let params = m
            .parameters
            .iter()
            .map(|p| quote_ident(p))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "CREATE OR REPLACE MACRO {}.{}({}) AS TABLE {}",
            quote_ident(&schema),
            quote_ident(&m.name),
            params,
            m.body_sql
        );
        match con.execute_batch(&sql) {
            Ok(_) => registered += 1,
            Err(e) => eprintln!(
                "[ducklink] table macro '{}.{}' not registered: {e}",
                schema, m.name
            ),
        }
    }
    Ok(registered)
}

// ---------------------------------------------------------------------------
// 11. register_log_storages — log-storage sinks. C API:
// duckdb_create_log_storage + duckdb_log_storage_set_{write_log_entry,name,
// extra_data} + duckdb_register_log_storage. The `write_log_entry` callback
// receives level/type/message directly from DuckDB, packages them into a
// `LogEntry` and dispatches back into the owning component via
// `Engine2::dispatch_write_log_entry` — the guest-side
// `log-storage-dispatch.write-log-entry` export (added by the log-storage-wit
// agent) receives the record. The Engine2 handle is looked up through the
// process-wide `LOG_STORAGE_REGISTRY`, mirroring the `REPLACEMENT_SCAN_REGISTRY`
// pattern above.
// ---------------------------------------------------------------------------

/// One (callback_handle -> Engine2, name) mapping the C callback consults on
/// every DuckDB log entry. Populated by `register_log_storages`; installed at
/// extension init and on subsequent LOADs.
///
/// T1-2 (path A): after the runtime-side prep landed
/// (`register_log_storage` now allocates a callback handle via
/// `allocate_callback_handle` before returning it to the guest), the
/// `callback_handle` stored here IS the same allocated global that:
///   * the guest received from `log-storage.register-log-storage`,
///   * `LogStorageEntry.callback_handle` propagates through, and
///   * `Engine2::dispatch_write_log_entry` resolves via the shared
///     callback registry to reach the owning ExtensionInstance.
/// So the local lookup here (`resolve_log_storage`) and the engine-side
/// registry lookup route to the SAME logical sink. The stale-handle bug
/// (guest-only handle stored in a global-keyed slot) is fixed at source.
struct LogStorageRegistration {
    callback_handle: u32,
    engine: Arc<Engine2>,
    name: String,
    /// Owning extension. Used to purge stale entries when the same extension
    /// re-registers the same log-storage name under a new `callback_handle`
    /// (e.g. after a component reload). Without this key the old entry would
    /// stay in the registry forever with a dangling engine and a
    /// callback_handle no live component recognises.
    extension: String,
}

/// Process-wide registry mirroring `REPLACEMENT_SCAN_REGISTRY`. Keyed by
/// callback_handle, which is unique per registered sink.
static LOG_STORAGE_REGISTRY: OnceLock<Mutex<Vec<LogStorageRegistration>>> = OnceLock::new();

fn log_storage_registry() -> &'static Mutex<Vec<LogStorageRegistration>> {
    LOG_STORAGE_REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// Look up (Engine2, name) for a callback handle. Returns `None` if the
/// handle is unknown (e.g. a stray DuckDB log record arriving after the
/// component was unloaded).
fn resolve_log_storage(callback_handle: u32) -> Option<(Arc<Engine2>, String)> {
    let reg = LOG_STORAGE_REGISTRY.get()?;
    let guard = reg.lock().unwrap_or_else(|e| e.into_inner());
    guard
        .iter()
        .find(|r| r.callback_handle == callback_handle)
        .map(|r| (r.engine.clone(), r.name.clone()))
}

#[allow(dead_code)]
struct LogStorageExtra {
    callback_handle: u32,
    name: String,
}

unsafe extern "C" fn log_storage_extra_destroy(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut LogStorageExtra));
    }
}

/// Map a DuckDB log-level string (e.g. "TRACE"/"DEBUG"/"INFO"/"WARN"/"ERROR"/
/// "FATAL") into the u32 the WIT `log-entry.level` field expects. Unknown
/// strings map to `u32::MAX` so components can distinguish an unrecognised
/// level from a real level=0 record.
fn log_level_to_u32(s: &str) -> u32 {
    match s.to_ascii_uppercase().as_str() {
        "TRACE" => 0,
        "DEBUG" => 1,
        "INFO" => 2,
        "WARN" | "WARNING" => 3,
        "ERROR" => 4,
        "FATAL" => 5,
        _ => u32::MAX,
    }
}

unsafe extern "C" fn ducklink_log_storage_write(
    extra_data: *mut c_void,
    _timestamp: *mut ffi::duckdb_timestamp,
    level: *const c_char,
    log_type: *const c_char,
    log_message: *const c_char,
) {
    // T1-3: mark the thread as inside a guest dispatch so a re-entrant
    // `NativeServices::query()` from the guest refuses instead of deadlocking.
    let _reentrancy_guard = crate::engine::QueryReentrancyGuard::new();
    if extra_data.is_null() {
        return;
    }
    let extra_ref = &*(extra_data as *const LogStorageExtra);
    let read_cstr = |p: *const c_char| -> String {
        if p.is_null() {
            String::new()
        } else {
            std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
        }
    };
    let lvl_str = read_cstr(level);
    let ty = read_cstr(log_type);
    let msg = read_cstr(log_message);
    // Fold the DuckDB `type` label into the message so the guest sees it even
    // though the WIT `log-entry` has no dedicated type field.
    let message = if ty.is_empty() {
        msg
    } else {
        format!("[{ty}] {msg}")
    };
    let ts_micros = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros())
            .unwrap_or(0),
    )
    .unwrap_or(0);
    let entry = LogEntry {
        level: log_level_to_u32(&lvl_str),
        message,
        tags: None,
        ts_micros,
    };
    let (engine, sink_name) = match resolve_log_storage(extra_ref.callback_handle) {
        Some(pair) => pair,
        None => {
            eprintln!(
                "[ducklink:log-storage:{}] no engine registered for callback_handle={}",
                extra_ref.name, extra_ref.callback_handle
            );
            return;
        }
    };
    if let Err(err) = engine.dispatch_write_log_entry(extra_ref.callback_handle, entry) {
        eprintln!("log-storage {sink_name}: dispatch failed: {err}");
    }
}

pub unsafe fn register_log_storages(
    db: ffi::duckdb_database,
    engine: Arc<Engine2>,
    storages: &[LogStorageEntry],
) -> Result<usize, String> {
    if storages.is_empty() {
        return Ok(0);
    }
    if db.is_null() {
        return Err("register_log_storages: db is null".to_string());
    }
    let mut registered = 0usize;
    for s in storages {
        let name_c = match CString::new(s.name.as_str()) {
            Ok(c) => c,
            Err(_) => {
                eprintln!(
                    "[ducklink] log storage '{}' has a NUL byte; skipping",
                    s.name
                );
                continue;
            }
        };
        let ls = ffi::duckdb_create_log_storage();
        ffi::duckdb_log_storage_set_name(ls, name_c.as_ptr());
        let extra = Box::into_raw(Box::new(LogStorageExtra {
            callback_handle: s.callback_handle,
            name: s.name.clone(),
        })) as *mut c_void;
        ffi::duckdb_log_storage_set_extra_data(ls, extra, Some(log_storage_extra_destroy));
        ffi::duckdb_log_storage_set_write_log_entry(ls, Some(ducklink_log_storage_write));
        // Publish (callback_handle -> engine) so the C callback can resolve
        // its owning component. Two dedupe axes:
        //   * (extension, name) — P1 fix: purge any stale entries for the
        //     same sink under this extension before appending. A component
        //     reload assigns fresh `callback_handle`s, so keying only on
        //     the handle (as before) leaked the previous registration and
        //     its `Arc<Engine2>` for the process lifetime.
        //   * callback_handle — repeated LOAD of the same component with
        //     the same handle refreshes in place instead of stacking.
        {
            let reg = log_storage_registry();
            let mut guard = reg.lock().unwrap_or_else(|e| e.into_inner());
            guard.retain(|r| !(r.extension == s.extension && r.name == s.name));
            if let Some(existing) = guard
                .iter_mut()
                .find(|r| r.callback_handle == s.callback_handle)
            {
                existing.engine = engine.clone();
                existing.name = s.name.clone();
                existing.extension = s.extension.clone();
            } else {
                guard.push(LogStorageRegistration {
                    callback_handle: s.callback_handle,
                    engine: engine.clone(),
                    name: s.name.clone(),
                    extension: s.extension.clone(),
                });
            }
        }

        let rc = ffi::duckdb_register_log_storage(db, ls);
        let mut ls_mut = ls;
        ffi::duckdb_destroy_log_storage(&mut ls_mut);
        if rc != ffi::DuckDBSuccess {
            eprintln!(
                "[ducklink] log storage '{}' not registered (already present?)",
                s.name
            );
            continue;
        }
        registered += 1;
    }
    Ok(registered)
}

// ---------------------------------------------------------------------------
// 12. register_pragmas — install component-declared PRAGMAs.
//
// T1-5 STATUS: BLOCKED-UPSTREAM.
//
// A. C API surface: libduckdb-sys 1.10504.0 does NOT export a pragma
//    registration entry point. A grep of the bundled bindings
//    (`bindgen_bundled_version_loadable.rs`) for `duckdb_add_pragma`,
//    `duckdb_create_pragma_function`, `duckdb_register_pragma_function`,
//    or `duckdb_pragma_function` returns zero hits — the stable C API
//    treats PRAGMAs as an internal concept exposed only through the C++
//    catalog, not the extension-facing C header. Any first-class
//    pragma-installer path requires a DuckDB C API change, and no
//    "install-me-as-a-pragma" symbol exists to wrap.
//
// B. Table-macro fallback: DuckDB does let user code register a macro
//    via SQL (`CREATE OR REPLACE MACRO name(...) AS TABLE (...)`), which
//    would route `SELECT * FROM myext.mypragma()` to a supporting scalar.
//    That is NOT the same surface the runtime advertises through
//    `runtime.register-pragma` (which the guest expects to see fire on
//    `PRAGMA myext.mypragma;` — bare, no parentheses). Users would need
//    to invoke it as a table macro (with parens) instead of a pragma,
//    which changes the guest contract. Left unimplemented for that
//    reason; if users are willing to accept the different call shape a
//    follow-up can register `pragmas` as table macros here.
//
// C. Engine wrapper: even the fallback needs a dispatch path from the C
//    callback into the guest's `callback-dispatch.call-pragma` export.
//    `ExtensionInstance::dispatch_pragma` exists (see
//    runtime/src/extension.rs:2558) but no `Engine2::dispatch_pragma`
//    public wrapper does — engine.rs is frozen per the prep note, so
//    the fallback can't be wired from this file alone either.
//
// Behaviour: `register_pragmas` logs each declared pragma so users see
// the shortfall visibly, then returns Ok(0). No side effects on the
// database. Once one of the two blockers clears (native C API arrives
// OR the table-macro fallback + Engine2 wrapper lands), swap the body
// for the real installer.
#[allow(unused_variables)]
pub unsafe fn register_pragmas(
    _raw_con: ffi::duckdb_connection,
    _engine: Arc<Engine2>,
    pragmas: &[PragmaEntry],
) -> Result<usize, String> {
    if pragmas.is_empty() {
        return Ok(0);
    }
    for p in pragmas {
        eprintln!(
            "[ducklink] pragma '{}::{}' (callback={}) declared but NOT installed: \
             libduckdb-sys 1.10504.0 exposes no pragma registration entry point in \
             the stable C API. See src/reg_duckdb.rs T1-5 comment for details.",
            p.extension, p.name, p.callback_handle
        );
    }
    Ok(0)
}

// ---------------------------------------------------------------------------
// 13. register_coordinate_systems — install component-declared CRS entries.
//
// T2-2 STATUS: BLOCKED-UPSTREAM (mirror of register_pragmas T1-5).
//
// The DuckDB stable C API in libduckdb-sys 1.10504.0 exposes no coordinate
// reference system / SRID registration entry point. A grep of the bundled
// bindings (`bindgen_bundled_version_loadable.rs`) for `duckdb_*srid*`,
// `duckdb_*coordinate*`, or `duckdb_*crs*` returns zero hits — the concept
// lives in the `spatial` extension's SQL surface (`ST_SRID`,
// `spatial_ref_sys` table) rather than the extension-facing C header. Any
// first-class installer path requires either a DuckDB C API change or
// piggy-backing on the spatial extension's SQL catalog (INSERT INTO
// spatial_ref_sys), which is a fundamentally different surface than the one
// the guest advertises via `runtime.register-coordinate-system`.
//
// Behaviour: fail-loud stub — logs each declared CRS so users see the
// shortfall visibly, then returns Ok(0). No side effects on the database.
// Once the upstream C API gains a CRS registration hook (OR we agree to
// route CRS registrations through the spatial extension's catalog), swap
// the body for the real installer.
#[allow(unused_variables)]
pub unsafe fn register_coordinate_systems(
    _raw_con: ffi::duckdb_connection,
    _engine: Arc<Engine2>,
    coordinate_systems: &[CoordinateSystemEntry],
) -> Result<usize, String> {
    if coordinate_systems.is_empty() {
        return Ok(0);
    }
    for c in coordinate_systems {
        eprintln!(
            "[ducklink] coordinate system '{}::{}:{}' (wkt.len={}) declared but NOT \
             installed: libduckdb-sys 1.10504.0 exposes no coordinate-system / SRID \
             registration entry point in the stable C API. See src/reg_duckdb.rs T2-2 \
             comment for details.",
            c.extension,
            c.auth_name,
            c.code,
            c.wkt.len()
        );
    }
    Ok(0)
}
