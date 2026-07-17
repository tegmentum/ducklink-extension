//! Advanced-tier native dispatch: the Rust side of the C++ shim
//! (`cpp/ducklink_*.cpp`) that binds DuckDB's INTERNAL C++ ABI.
//!
//! The common tier (scalar/table/aggregate) rides the stable C API and is wired
//! in [`crate::reg_duckdb`]. The advanced tier — PARSER, OPTIMIZER, and table
//! FILTER pushdown — has no stable C anchor, so a small C++ shim registers the
//! real DuckDB extension points and calls back into the embedded wasmtime engine
//! through the `extern "C"` bridge functions implemented here.
//!
//! The shim is reached through a process-global [`Advanced`] handle, set on the
//! first component registration. Its callbacks run inside DuckDB's parser /
//! optimizer / scan, lock the shared engine, and dispatch to the owning
//! component (resolved through the same callback registry the scalar path uses).

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use duckdb::ffi;

use ducklink_runtime::reg;

use crate::engine::{Engine2, LoadedComponent};
use crate::reg_duckdb::{type_code, write_col_from_raw};
use ducklink_runtime::duckdb_extension_bindings::duckdb::extension::types::Duckvalue as WitVal;

/// Process-global advanced-tier state. The C++ shim's bridge callbacks (invoked
/// from DuckDB's parser / optimizer / scan) reach the embedded engine and the
/// set of declared rule handles through this.
struct Advanced {
    engine: Arc<Engine2>,
    /// Every component-declared PARSER extension as (owning component, the
    /// component's guest dispatcher handle). RwLock because the parser bridge
    /// (`ducklink_parse`) reads this on every unrecognized-statement retry to
    /// offer the SQL to each registered component; writes only happen at
    /// component load. Multiple concurrent parses shouldn't serialize.
    parsers: RwLock<Vec<(String, u32)>>,
    /// Every component-declared OPTIMIZER rule as (owning component, handle).
    /// RwLock for symmetry with `parsers`; C++ shim reads this per query
    /// plan.
    optimizers: RwLock<Vec<(String, u32)>>,
    /// Filterable streaming table functions, keyed by the table's callback
    /// handle: the owning component + its full declared column types (used to
    /// derive the projected schema for writing chunks). RwLock because
    /// `ducklink_ts_open` (per scan) reads it, writes only happen at load.
    filterable: RwLock<HashMap<u32, (String, Vec<reg::LogicalType>)>>,
    /// Live streaming cursors, keyed by a bridge-local id (so component cursor
    /// ids can never collide across components). Stays a Mutex — every fill
    /// mutates via the reads-then-cursor-clones dance below, and open/close
    /// are the write paths.
    cursors: Mutex<HashMap<u32, CursorState>>,
    /// Most recent table-stream bridge error (valid until the next bridge call).
    ts_last_error: Mutex<Option<CString>>,
}

/// Per-cursor state for an open streaming scan.
///
/// `extension` is `Arc<str>` and `col_codes` is `Arc<[u8]>` so every
/// `ducklink_ts_fill` — which today runs 2048 rows per call, thousands of
/// times per scan — can capture them via one atomic refcount bump each,
/// instead of the `String::clone` + `Vec::clone` the pre-G3 shape paid on
/// every fill.
struct CursorState {
    extension: Arc<str>,
    /// The table function's callback handle (passed to next/close).
    handle: u32,
    /// The component-side cursor handle returned by open.
    component_cursor: u32,
    /// Type codes of the emitted (post-projection) columns, in emit order.
    col_codes: Arc<[u8]>,
}

/// Sentinel prefix the parser bridge returns for a `LOAD WASM '<arg>'` statement.
/// The C++ plan path strips this and calls `ducklink_load_wasm` with the live
/// `context.db`. Kept in lock-step with `DUCKLINK_LOAD_WASM_SENTINEL` in
/// cpp/ducklink_advanced.h.
const LOAD_WASM_SENTINEL: &str = "\u{1}ducklink:load-wasm\u{1}";

/// Sentinel prefix the parser bridge returns for a `LOAD NATIVE '<arg>'`
/// statement. The C++ plan path strips this and calls `ducklink_load_native`
/// with the live `context.db`. Kept in lock-step with
/// `DUCKLINK_LOAD_NATIVE_SENTINEL` in cpp/ducklink_advanced.h.
const LOAD_NATIVE_SENTINEL: &str = "\u{1}ducklink:load-native\u{1}";

/// Sentinel prefix the parser bridge returns for a `DUCKLINK PREFIX
/// <alias>: <namespace>;` statement. Payload is `{alias}\t{namespace}` —
/// the tab is illegal inside SQL identifiers, so it never collides with
/// a real name. The C++ plan path strips this and calls
/// [`ducklink_prefix`] with the parser's LIVE database. Kept in lock-step
/// with `DUCKLINK_PREFIX_SENTINEL` in cpp/ducklink_advanced.h.
const PREFIX_SENTINEL: &str = "\u{1}ducklink:prefix\u{1}";

static ADVANCED: OnceLock<Advanced> = OnceLock::new();
/// Bridge-local cursor id generator (0 is reserved for "open failed").
static CURSOR_SEQ: AtomicU32 = AtomicU32::new(1);

fn advanced_or_init(engine: &Arc<Engine2>) -> &'static Advanced {
    ADVANCED.get_or_init(|| Advanced {
        engine: engine.clone(),
        parsers: RwLock::new(Vec::new()),
        optimizers: RwLock::new(Vec::new()),
        filterable: RwLock::new(HashMap::new()),
        cursors: Mutex::new(HashMap::new()),
        ts_last_error: Mutex::new(None),
    })
}

/// Run an advanced-tier FFI bridge body, converting any panic into the
/// function's error sentinel so a Rust panic can NEVER unwind across the
/// C++/Rust boundary (which is undefined behavior). These functions are CALLED
/// FROM the C++ shim (inside DuckDB's parser / optimizer / scan), so an
/// unguarded panic — e.g. a poisoned engine/state mutex after an earlier
/// failure — would unwind into C++. A panic here is an internal bug; we log it
/// and degrade the single call to its error value rather than abort the process.
fn guard<T>(on_panic: T, body: impl FnOnce() -> T) -> T {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
        Ok(v) => v,
        Err(_) => {
            eprintln!("[ducklink] advanced-tier bridge call panicked; recovered at the FFI boundary");
            on_panic
        }
    }
}

/// Like [`guard`], but also records the panic in the table-stream last-error
/// slot so the C++ TableFunction raises a clean SQL error instead of silently
/// treating the panic as end-of-stream.
fn guard_ts<T>(on_panic: T, body: impl FnOnce() -> T) -> T {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
        Ok(v) => v,
        Err(_) => {
            ts_set_last_error("ducklink advanced-tier table-stream bridge panicked".to_string());
            on_panic
        }
    }
}

extern "C" {
    /// Install the component-driven ParserExtension on `db` (idempotent).
    fn ducklink_register_parser(db: *mut c_void) -> i32;
    /// Install the component-driven OptimizerExtension on `db` (idempotent).
    fn ducklink_register_optimizer(db: *mut c_void) -> i32;
    /// Register a streaming + filter-pushdown TableFunction on `db`.
    fn ducklink_register_filterable_table_function(
        db: *mut c_void,
        name: *const c_char,
        handle: u32,
        arg_type_codes: *const c_char,
        cols_spec: *const c_char,
    ) -> i32;
    /// Register `existing_name` in `source_schema` under `new_name` in
    /// `target_schema`. NULL schema args default to `main`. Returns 1/2/3
    /// for aggregate/scalar/table on success; -1..-5 on failure with
    /// `*out_err` set to a malloc'd message (free via `ducklink_adv_free`).
    /// See `cpp/ducklink_alias.cpp` for the full contract.
    fn ducklink_alias_function(
        conn: *mut c_void,
        source_schema: *const c_char,
        existing_name: *const c_char,
        target_schema: *const c_char,
        new_name: *const c_char,
        out_err: *mut *mut c_char,
    ) -> i32;
}

/// Kind of function ducklink aliased into its own namespace, reported by
/// `ducklink_alias_function`. Ordering matches the shim's return codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AliasKind {
    Aggregate,
    Scalar,
    Table,
}

/// Safe wrapper over the C++ `ducklink_alias_function` shim.
///
/// Registers `existing` (looked up in `source_schema`, or `main` when `None`)
/// under `new_name` in `target_schema` (or `main` when `None`). Both callable
/// names resolve to the SAME underlying `AggregateFunction` /
/// `ScalarFunction` / `TableFunction` in the catalog, so aggregates preserve
/// their DISTINCT / FILTER / ORDER BY / window support through the alias.
///
/// Missing target schemas are created (IGNORE_ON_CONFLICT) — callers don't
/// need to `CREATE SCHEMA` before invoking this.
///
/// # Safety
/// `raw_conn` must be a live `duckdb_connection`. Ducklink obtains it via the
/// same C API code path used by aggregate registration (see `RawConnHandle`
/// in reg_duckdb.rs) or by opening a sibling connection on the raw database
/// handle (see `advanced.rs` community-native branch).
pub unsafe fn catalog_alias(
    raw_conn: ffi::duckdb_connection,
    source_schema: Option<&str>,
    existing: &str,
    target_schema: Option<&str>,
    new_name: &str,
) -> Result<AliasKind, String> {
    if raw_conn.is_null() {
        return Err("null raw connection".to_string());
    }
    let c_src = source_schema
        .map(|s| CString::new(s).map_err(|e| format!("source schema contains NUL: {e}")))
        .transpose()?;
    let c_existing =
        CString::new(existing).map_err(|e| format!("existing name contains NUL: {e}"))?;
    let c_tgt = target_schema
        .map(|s| CString::new(s).map_err(|e| format!("target schema contains NUL: {e}")))
        .transpose()?;
    let c_new = CString::new(new_name).map_err(|e| format!("new name contains NUL: {e}"))?;
    let mut err_ptr: *mut c_char = std::ptr::null_mut();
    let rc = ducklink_alias_function(
        raw_conn as *mut c_void,
        c_src.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
        c_existing.as_ptr(),
        c_tgt.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
        c_new.as_ptr(),
        &mut err_ptr,
    );
    if rc > 0 {
        // Success — a stray *out_err (shouldn't happen) still needs freeing.
        if !err_ptr.is_null() {
            drop(CString::from_raw(err_ptr));
        }
        return match rc {
            1 => Ok(AliasKind::Aggregate),
            2 => Ok(AliasKind::Scalar),
            3 => Ok(AliasKind::Table),
            _ => Err(format!("unknown alias-kind code {rc}")),
        };
    }
    let msg = if err_ptr.is_null() {
        format!("catalog alias failed (rc={rc})")
    } else {
        let s = CStr::from_ptr(err_ptr).to_string_lossy().into_owned();
        drop(CString::from_raw(err_ptr));
        s
    };
    Err(msg)
}

/// Wire a freshly loaded component's advanced-tier declarations into DuckDB.
/// Idempotent across components: the global engine handle is set once; each
/// component's parser/optimizer/filterable-table handles are appended.
///
/// `db` is the `duckdb_database` the loader handed the extension; the C++ shim
/// casts it to the internal `DatabaseInstance` to reach `DBConfig`.
pub fn register(db: ffi::duckdb_database, engine: &Arc<Engine2>, loaded: &LoadedComponent) {
    let adv = advanced_or_init(engine);

    if !loaded.parsers.is_empty() {
        {
            let mut guard = adv.parsers.write().expect("advanced parsers lock poisoned");
            for parser in &loaded.parsers {
                guard.push((parser.extension.clone(), parser.callback_handle));
            }
        }
        let rc = unsafe { ducklink_register_parser(db.cast()) };
        if rc != 0 {
            eprintln!("[ducklink] failed to register parser extension (rc={rc})");
        }
    }

    if !loaded.optimizers.is_empty() {
        {
            let mut guard = adv.optimizers.write().expect("advanced optimizers lock poisoned");
            for rule in &loaded.optimizers {
                guard.push((rule.extension.clone(), rule.callback_handle));
            }
        }
        let rc = unsafe { ducklink_register_optimizer(db.cast()) };
        if rc != 0 {
            eprintln!("[ducklink] failed to register optimizer extension (rc={rc})");
        }
    }

    for table in &loaded.filterable_tables {
        let arg_codes: String = table
            .arguments
            .iter()
            .map(|a| type_code(&a.logical).to_string())
            .collect::<Vec<_>>()
            .join(",");
        let cols_spec: String = table
            .columns
            .iter()
            .map(|c| format!("{}\t{}", c.name, type_code(&c.logical)))
            .collect::<Vec<_>>()
            .join("\n");
        {
            let mut guard = adv.filterable.write().expect("advanced filterable lock poisoned");
            guard.insert(
                table.callback_handle,
                (
                    table.extension.clone(),
                    table.columns.iter().map(|c| c.logical.clone()).collect(),
                ),
            );
        }
        let (Ok(c_name), Ok(c_args), Ok(c_cols)) = (
            CString::new(table.name.as_str()),
            CString::new(arg_codes),
            CString::new(cols_spec),
        ) else {
            eprintln!("[ducklink] filterable table '{}' has an interior NUL", table.name);
            continue;
        };
        let rc = unsafe {
            ducklink_register_filterable_table_function(
                db.cast(),
                c_name.as_ptr(),
                table.callback_handle,
                c_args.as_ptr(),
                c_cols.as_ptr(),
            )
        };
        if rc != 0 {
            eprintln!(
                "[ducklink] failed to register filterable table '{}' (rc={rc})",
                table.name
            );
        }
    }
}

fn ts_set_last_error(msg: String) {
    if let Some(adv) = ADVANCED.get() {
        if let Ok(mut guard) = adv.ts_last_error.lock() {
            *guard = CString::new(msg).ok();
        }
    }
}

/// Build a neutral value from a tagged C operand (DUCKLINK_TS_VAL_*).
unsafe fn ts_value_to_neutral(v: &DucklinkTsValue) -> reg::DuckValue {
    match v.value_type {
        1 => reg::DuckValue::Boolean(v.i64 != 0), // BOOLEAN
        2 => reg::DuckValue::Int64(v.i64),        // INT64
        3 => reg::DuckValue::Float64(v.f64),      // FLOAT64
        4 => {
            if v.text.is_null() {
                reg::DuckValue::Text(String::new())
            } else {
                reg::DuckValue::Text(CStr::from_ptr(v.text).to_string_lossy().into_owned())
            }
        }
        _ => reg::DuckValue::Null,
    }
}

/// C-ABI mirror of `DucklinkTsValue` in cpp/ducklink_advanced.h.
#[repr(C)]
pub struct DucklinkTsValue {
    value_type: u8,
    i64: i64,
    f64: f64,
    text: *const c_char,
}

/// C-ABI mirror of `DucklinkTsFilter` in cpp/ducklink_advanced.h.
#[repr(C)]
pub struct DucklinkTsFilter {
    column: u32,
    op: u8,
    values: *const DucklinkTsValue,
    nvalues: u32,
}

/// TABLE FILTER PUSHDOWN bridge — open a streaming cursor (called from the C++
/// TableFunction's init). Returns a bridge-local cursor id, or 0 on error
/// (message in [`ducklink_ts_last_error`]).
///
/// # Safety
/// `args` / `projection` / `filters` must point to `nargs` / `nproj` / `nfilt`
/// valid elements (or be null when the count is 0), valid for the call.
#[no_mangle]
pub unsafe extern "C" fn ducklink_ts_open(
    handle: u32,
    args: *const DucklinkTsValue,
    nargs: u32,
    projection: *const u32,
    nproj: u32,
    filters: *const DucklinkTsFilter,
    nfilt: u32,
) -> u32 {
    guard_ts(0, || unsafe {
        ducklink_ts_open_impl(handle, args, nargs, projection, nproj, filters, nfilt)
    })
}

unsafe fn ducklink_ts_open_impl(
    handle: u32,
    args: *const DucklinkTsValue,
    nargs: u32,
    projection: *const u32,
    nproj: u32,
    filters: *const DucklinkTsFilter,
    nfilt: u32,
) -> u32 {
    let adv = match ADVANCED.get() {
        Some(a) => a,
        None => return 0,
    };
    let (extension, columns) = {
        let guard = adv.filterable.read().expect("advanced filterable lock poisoned");
        match guard.get(&handle) {
            Some((ext, cols)) => (ext.clone(), cols.clone()),
            None => {
                ts_set_last_error(format!("ducklink_ts_open: unknown table handle {handle}"));
                return 0;
            }
        }
    };

    let args_slice: &[DucklinkTsValue] = if args.is_null() || nargs == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(args, nargs as usize)
    };
    let arg_values: Vec<reg::DuckValue> = args_slice.iter().map(|a| ts_value_to_neutral(a)).collect();

    let projection: Vec<u32> = if projection.is_null() || nproj == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(projection, nproj as usize).to_vec()
    };

    let filters_slice: &[DucklinkTsFilter] = if filters.is_null() || nfilt == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(filters, nfilt as usize)
    };
    let filter_set: Vec<(u32, u8, Vec<reg::DuckValue>)> = filters_slice
        .iter()
        .map(|f| {
            let vals: &[DucklinkTsValue] = if f.values.is_null() || f.nvalues == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(f.values, f.nvalues as usize)
            };
            (
                f.column,
                f.op,
                vals.iter().map(|v| ts_value_to_neutral(v)).collect(),
            )
        })
        .collect();

    // Emitted (post-projection) column type codes, in emit order.
    let col_codes: Vec<u8> = if projection.is_empty() {
        columns.iter().map(type_code).collect()
    } else {
        projection
            .iter()
            .filter_map(|&i| columns.get(i as usize).map(type_code))
            .collect()
    };

    let component_cursor = match adv.engine.dispatch_table_open_filtered(
        &extension,
        handle,
        arg_values,
        projection,
        filter_set,
    ) {
        Ok(c) => c,
        Err(err) => {
            ts_set_last_error(format!("{err}"));
            return 0;
        }
    };

    let cursor_id = CURSOR_SEQ.fetch_add(1, Ordering::Relaxed);
    // Wrap the per-cursor identity fields as Arc so every subsequent
    // `ducklink_ts_fill` (thousands of calls per scan) captures them via a
    // refcount bump instead of a String / Vec clone.
    adv.cursors.lock().expect("cursors mutex poisoned").insert(
        cursor_id,
        CursorState {
            extension: Arc::from(extension),
            handle,
            component_cursor,
            col_codes: Arc::from(col_codes),
        },
    );
    cursor_id
}

/// Pull the next batch into `chunk` (a `duckdb_data_chunk`). Returns true if rows
/// were written, false at EOF (size set 0) or on error.
///
/// # Safety
/// `chunk` must be a valid `duckdb_data_chunk` with the emitted column schema.
#[no_mangle]
pub unsafe extern "C" fn ducklink_ts_fill(handle: u32, cursor: u32, chunk: *mut c_void) -> bool {
    guard_ts(false, || unsafe { ducklink_ts_fill_impl(handle, cursor, chunk) })
}

unsafe fn ducklink_ts_fill_impl(_handle: u32, cursor: u32, chunk: *mut c_void) -> bool {
    let adv = match ADVANCED.get() {
        Some(a) => a,
        None => return false,
    };
    let output = chunk as ffi::duckdb_data_chunk;
    if output.is_null() {
        ts_set_last_error("ducklink_ts_fill: null chunk".to_string());
        return false;
    }
    // G3: extension and col_codes are Arc-wrapped in CursorState, so clone()
    // here is one atomic refcount bump each — no String / Vec allocation on
    // the fill hot path.
    let (extension, handle, component_cursor, col_codes) = {
        let guard = adv.cursors.lock().expect("cursors mutex poisoned");
        match guard.get(&cursor) {
            Some(s) => (
                Arc::clone(&s.extension),
                s.handle,
                s.component_cursor,
                Arc::clone(&s.col_codes),
            ),
            None => {
                ts_set_last_error(format!("ducklink_ts_fill: unknown cursor {cursor}"));
                return false;
            }
        }
    };

    let rows = match adv
        .engine
        .dispatch_table_next(&extension, handle, component_cursor, 2048)
    {
        Ok(rows) => rows,
        Err(err) => {
            ts_set_last_error(format!("{err}"));
            return false;
        }
    };

    if rows.is_empty() {
        ffi::duckdb_data_chunk_set_size(output, 0);
        return false;
    }

    // Defensive bound at the component trust boundary: we ask for at most a
    // vector's worth of rows, but a misbehaving component could return more.
    // Writing past the chunk's vector capacity would be out-of-bounds, so reject
    // an over-long batch cleanly instead of corrupting memory.
    let capacity = ffi::duckdb_vector_size() as usize;
    if rows.len() > capacity {
        ts_set_last_error(format!(
            "ducklink_ts_fill: component returned {} rows, exceeding the chunk capacity {capacity}",
            rows.len()
        ));
        return false;
    }

    let ncols = col_codes.len();
    let n = rows.len();
    // Hoist the per-row column-count check out of the inner (col × row) loop —
    // shape is invariant per row, so `ncols * rows` checks is `ncols - 1`
    // extra passes when one is enough. Runs before any writes so a shape error
    // aborts cleanly.
    for (row, row_values) in rows.iter().enumerate() {
        if row_values.len() != ncols {
            ts_set_last_error(format!(
                "ducklink_ts_fill: row {row} has {} cols, expected {ncols}",
                row_values.len()
            ));
            return false;
        }
    }
    // Pivot row-major -> column-major ONCE per chunk. Every downstream column
    // then rides `write_col_from_raw`'s hoisted per-column write — the typed
    // data pointer and (code, WitVal) match happen once per column instead of
    // once per emitted cell. Cost: one Vec allocation per column at ~n * size_of::<WitVal>();
    // saves ~n * ncols FFI derefs + pattern matches.
    let mut columns: Vec<Vec<WitVal>> =
        (0..ncols).map(|_| Vec::with_capacity(n)).collect();
    for row in rows {
        for (c, v) in row.into_iter().enumerate() {
            columns[c].push(v);
        }
    }
    ffi::duckdb_data_chunk_set_size(output, n as ffi::idx_t);
    for (col_idx, &code) in col_codes.iter().enumerate() {
        let vector = ffi::duckdb_data_chunk_get_vector(output, col_idx as ffi::idx_t);
        if let Err(err) = write_col_from_raw(code, vector, &columns[col_idx], n) {
            ts_set_last_error(err);
            return false;
        }
    }
    true
}

/// Close + free a streaming cursor.
#[no_mangle]
pub extern "C" fn ducklink_ts_close(handle: u32, cursor: u32) {
    guard((), || ducklink_ts_close_impl(handle, cursor))
}

fn ducklink_ts_close_impl(_handle: u32, cursor: u32) {
    let adv = match ADVANCED.get() {
        Some(a) => a,
        None => return,
    };
    let state = adv
        .cursors
        .lock()
        .expect("cursors mutex poisoned")
        .remove(&cursor);
    if let Some(state) = state {
        let engine = &adv.engine;
        let _ = engine.dispatch_table_close(&state.extension, state.handle, state.component_cursor);
    }
}

/// Most recent table-stream bridge error (owned by Rust; valid until the next
/// bridge call). Empty C string when none.
#[no_mangle]
pub extern "C" fn ducklink_ts_last_error() -> *const c_char {
    const EMPTY: &[u8] = b"\0";
    guard(EMPTY.as_ptr() as *const c_char, ducklink_ts_last_error_impl)
}

fn ducklink_ts_last_error_impl() -> *const c_char {
    static EMPTY: &[u8] = b"\0";
    let adv = match ADVANCED.get() {
        Some(a) => a,
        None => return EMPTY.as_ptr() as *const c_char,
    };
    match adv.ts_last_error.lock() {
        Ok(guard) => match guard.as_ref() {
            Some(s) => s.as_ptr(),
            None => EMPTY.as_ptr() as *const c_char,
        },
        Err(_) => EMPTY.as_ptr() as *const c_char,
    }
}

/// A flattened plan node: (id, op-type, parent, params-json). Mirrors the tuple
/// `ducklink_runtime::ExtensionInstance::call_optimize` consumes; `params_json`
/// carries the whole neutral node object (so a rule can read e.g. the table
/// name). Same neutral shape the host's `plan_shape::flatten_plan_json` produces.
type FlatNode = (u32, String, Option<u32>, String);

/// Parse the core's flattened plan JSON
/// (`[{"id":N,"op":"X","parent":P,"table":"T"?}, ...]`) into the neutral node
/// tuples. Total and panic-free for any input: invalid JSON / unexpected shape
/// degrades to an empty list rather than erroring, so a bad plan never aborts
/// optimization.
fn flatten_plan_json(plan_json: &str) -> Vec<FlatNode> {
    let parsed: serde_json::Value = match serde_json::from_str(plan_json) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let mut nodes: Vec<FlatNode> = Vec::new();
    if let Some(arr) = parsed.as_array() {
        // Bound the materialized node count against an adversarial/huge plan.
        const MAX_NODES: usize = 1 << 16;
        for node in arr.iter().take(MAX_NODES) {
            let id = node.get("id").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
            let op = node
                .get("op")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let parent = node
                .get("parent")
                .and_then(|v| v.as_i64())
                .filter(|p| *p >= 0)
                .map(|p| p as u32);
            let params = node.to_string();
            nodes.push((id, op, parent, params));
        }
    }
    nodes
}

/// OPTIMIZER bridge (called from the C++ OptimizerExtension). Offer the flattened
/// `plan_json` + source `query` to each declared component rule; the first that
/// returns a `rewrite-query` directive wins. Returns a malloc'd rewrite-SQL C
/// string (freed via [`ducklink_adv_free`]) or NULL if no rule rewrote it.
///
/// # Safety
/// `plan_json` / `query` must be valid NUL-terminated C strings for the call.
#[no_mangle]
pub unsafe extern "C" fn ducklink_optimizer_try_rewrite(
    plan_json: *const c_char,
    query: *const c_char,
) -> *mut c_char {
    guard(std::ptr::null_mut(), || unsafe {
        ducklink_optimizer_try_rewrite_impl(plan_json, query)
    })
}

unsafe fn ducklink_optimizer_try_rewrite_impl(
    plan_json: *const c_char,
    query: *const c_char,
) -> *mut c_char {
    let adv = match ADVANCED.get() {
        Some(a) => a,
        None => return std::ptr::null_mut(),
    };
    if plan_json.is_null() {
        return std::ptr::null_mut();
    }
    let plan = match CStr::from_ptr(plan_json).to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let query = if query.is_null() {
        ""
    } else {
        CStr::from_ptr(query).to_str().unwrap_or("")
    };
    let handles = {
        let guard = adv
            .optimizers
            .read()
            .expect("advanced optimizers lock poisoned");
        if guard.is_empty() {
            return std::ptr::null_mut();
        }
        guard.clone()
    };
    let nodes = flatten_plan_json(plan);
    let engine = &adv.engine;
    for (extension, handle) in handles {
        match engine.dispatch_optimize(&extension, handle, nodes.clone(), query) {
            Ok(Some(rewrite)) => {
                return CString::new(rewrite)
                    .map(|c| c.into_raw())
                    .unwrap_or(std::ptr::null_mut());
            }
            Ok(None) => continue,
            Err(err) => {
                eprintln!("[ducklink] optimizer dispatch error: {err}");
                continue;
            }
        }
    }
    std::ptr::null_mut()
}

/// PARSER-OVERRIDE bridge (called from the C++ ParserExtension's
/// `parser_override` hook BEFORE DuckDB's built-in parser sees the query).
///
/// Runs [`rewrite_colon_syntax`] on `sql`. If a rewrite happened, returns a
/// malloc'd C string with the rewritten SQL (caller frees via
/// [`ducklink_adv_free`]); if nothing to rewrite, returns NULL so the shim
/// skips its parse-and-swap and lets the built-in parser see the original
/// unchanged.
///
/// # Safety
/// `sql` must be a valid NUL-terminated C string for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn ducklink_parser_rewrite_colon(sql: *const c_char) -> *mut c_char {
    guard(std::ptr::null_mut(), || unsafe {
        if sql.is_null() {
            return std::ptr::null_mut();
        }
        let s = match CStr::from_ptr(sql).to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        };
        match rewrite_colon_syntax(s) {
            Some(rewritten) => CString::new(rewritten)
                .map(|c| c.into_raw())
                .unwrap_or(std::ptr::null_mut()),
            None => std::ptr::null_mut(),
        }
    })
}

/// PARSER bridge (called from the C++ ParserExtension's parse_function). Offer
/// the rejected statement `sql` to each declared component parser; the first that
/// claims it wins. Returns a malloc'd rewrite-SQL C string (freed via
/// [`ducklink_adv_free`]) or NULL if none claim it.
///
/// # Safety
/// `sql` must be a valid NUL-terminated C string for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn ducklink_parser_try_rewrite(sql: *const c_char) -> *mut c_char {
    guard(std::ptr::null_mut(), || unsafe {
        ducklink_parser_try_rewrite_impl(sql)
    })
}

unsafe fn ducklink_parser_try_rewrite_impl(sql: *const c_char) -> *mut c_char {
    if sql.is_null() {
        return std::ptr::null_mut();
    }
    let query = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return std::ptr::null_mut(),
    };

    // `DUCKLINK LOAD '<name>' [WASM|NATIVE]` — the advanced-tier statement that
    // loads a component (WASM by default, native when explicit) at runtime from
    // SQL. DuckDB's built-in parser rejects it (the `DUCKLINK` keyword is ours),
    // so it reaches this ParserExtension. We return a SENTINEL rewrite
    // (a per-kind sentinel + the argument); the C++ plan path recognizes it and
    // dispatches to `ducklink_load_wasm` or `ducklink_load_native` with the
    // parser's LIVE `context.db`.
    //
    // Default kind is WASM because WASM is the safer trust posture (sandboxed;
    // no `allow_unsigned_extensions` change required). Users force native with
    // an explicit trailing `NATIVE` keyword, accepting the trust trade.
    if let Some((name, kind)) = parse_ducklink_load(&query) {
        let sentinel = match kind {
            LoadKind::Wasm => format!("{LOAD_WASM_SENTINEL}{name}"),
            LoadKind::Native => format!("{LOAD_NATIVE_SENTINEL}{name}"),
        };
        return CString::new(sentinel)
            .map(|c| c.into_raw())
            .unwrap_or(std::ptr::null_mut());
    }

    // `DUCKLINK PREFIX <alias>: <namespace>;` — declare a session-scoped
    // schema alias. The C++ plan path strips the sentinel and invokes
    // `ducklink_prefix` with the live db.
    if let Some((alias, namespace)) = parse_ducklink_prefix(&query) {
        let sentinel = format!("{PREFIX_SENTINEL}{alias}\t{namespace}");
        return CString::new(sentinel)
            .map(|c| c.into_raw())
            .unwrap_or(std::ptr::null_mut());
    }

    let adv = match ADVANCED.get() {
        Some(a) => a,
        None => return std::ptr::null_mut(),
    };
    let handles = {
        let guard = adv.parsers.read().expect("advanced parsers lock poisoned");
        if guard.is_empty() {
            return std::ptr::null_mut();
        }
        guard.clone()
    };
    let engine = &adv.engine;
    for (extension, handle) in handles {
        match engine.dispatch_parse(&extension, handle, &query) {
            Ok(Some(rewrite)) => {
                return CString::new(rewrite)
                    .map(|c| c.into_raw())
                    .unwrap_or(std::ptr::null_mut());
            }
            Ok(None) => continue,
            Err(err) => {
                eprintln!("[ducklink] parser dispatch error: {err}");
                continue;
            }
        }
    }
    std::ptr::null_mut()
}

/// `LOAD WASM` bridge — load a component into the LIVE database the parser is
/// executing against and register its functions. Called from the C++ parser
/// shim's plan path with the `duckdb_database` it wraps around the parser's
/// `ClientContext` (`context.db`). `path` is the quoted argument (a filesystem
/// path or a catalog name). On success writes a human-readable summary into
/// `*out_summary` (malloc'd, free via [`ducklink_adv_free`]) and returns 0; on
/// error writes the message into `*out_summary` and returns non-zero.
///
/// # Safety
/// `db` must be the valid live `duckdb_database`; `path` a valid C string;
/// `out_summary` a valid pointer to write one `*mut c_char` into.
#[no_mangle]
pub unsafe extern "C" fn ducklink_load_wasm(
    db: *mut c_void,
    path: *const c_char,
    out_summary: *mut *mut c_char,
) -> i32 {
    guard(2, || unsafe { ducklink_load_wasm_impl(db, path, out_summary) })
}

unsafe fn ducklink_load_wasm_impl(
    db: *mut c_void,
    path: *const c_char,
    out_summary: *mut *mut c_char,
) -> i32 {
    if path.is_null() || out_summary.is_null() {
        return 2;
    }
    let arg = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(_) => {
            *out_summary = CString::new("LOAD WASM: argument is not valid UTF-8")
                .map(|c| c.into_raw())
                .unwrap_or(std::ptr::null_mut());
            return 1;
        }
    };
    match crate::reg_duckdb::load_wasm_into_db(db.cast(), arg) {
        Ok((name, scalars, tables, aggregates)) => {
            let replayed = replay_persisted_prefixes(db.cast());
            let replay_note = if replayed > 0 {
                format!(", replayed {replayed} persisted prefix(es)")
            } else {
                String::new()
            };
            let msg = format!(
                "loaded '{name}': {scalars} scalar(s), {tables} table(s), {aggregates} aggregate(s){replay_note}"
            );
            *out_summary = CString::new(msg)
                .map(|c| c.into_raw())
                .unwrap_or(std::ptr::null_mut());
            0
        }
        Err(err) => {
            *out_summary = CString::new(format!("LOAD WASM failed: {err}"))
                .map(|c| c.into_raw())
                .unwrap_or(std::ptr::null_mut());
            1
        }
    }
}

/// Which loader path a `DUCKLINK LOAD` statement selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoadKind {
    Wasm,
    Native,
}

/// Case-insensitive prefix strip of a keyword. Returns the remainder AFTER the
/// keyword's characters (does not require trailing whitespace). Uses byte-
/// indexing so it's cheap; safe because keywords are ASCII.
fn strip_keyword<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    if s.len() >= kw.len() && s[..kw.len()].eq_ignore_ascii_case(kw) {
        Some(&s[kw.len()..])
    } else {
        None
    }
}

/// Recognize `DUCKLINK LOAD '<name>' [WASM|NATIVE]` (case-insensitive on the
/// keywords, optional trailing `;`/whitespace) and return the quoted argument
/// together with the selected loader kind. Default kind is [`LoadKind::Wasm`]
/// when the trailing keyword is omitted — WASM is the safer trust posture
/// (sandboxed, no signature-check gate), so users have to opt into `NATIVE`
/// explicitly.
///
/// Any statement that isn't exactly this shape returns `None`, so an
/// unrecognised statement falls through to the component parser path
/// unchanged.
fn parse_ducklink_load(sql: &str) -> Option<(String, LoadKind)> {
    let s = sql.trim().trim_end_matches(';').trim();

    // `DUCKLINK` keyword.
    let rest = strip_keyword(s, "DUCKLINK")?;
    if !rest.starts_with(char::is_whitespace) {
        return None;
    }
    let rest = rest.trim_start();

    // `LOAD` keyword.
    let rest = strip_keyword(rest, "LOAD")?;
    if !rest.starts_with(char::is_whitespace) {
        return None;
    }
    let rest = rest.trim_start();

    // Single-quoted name.
    let after_open = rest.strip_prefix('\'')?;
    let quote_end = after_open.find('\'')?;
    let name = &after_open[..quote_end];
    if name.is_empty() {
        return None;
    }
    let after_close = after_open[quote_end + 1..].trim_start();

    // Optional trailing `WASM` / `NATIVE` keyword; default WASM.
    let kind = if after_close.is_empty() {
        LoadKind::Wasm
    } else if let Some(after) = strip_keyword(after_close, "WASM") {
        if !after.trim_start().is_empty() {
            return None;
        }
        LoadKind::Wasm
    } else if let Some(after) = strip_keyword(after_close, "NATIVE") {
        if !after.trim_start().is_empty() {
            return None;
        }
        LoadKind::Native
    } else {
        return None;
    };

    Some((name.to_string(), kind))
}

/// Rewrite ducklink's SPARQL-flavored colon syntax (`c:hash(x)`) into
/// DuckDB's schema-qualified dot form (`c.hash(x)`).
///
/// Called by the C++ [`ParserExtension::parser_override`] hook (see
/// `cpp/ducklink_parser.cpp`) BEFORE DuckDB's built-in parser sees the
/// query. When we return `Some`, the shim hands the rewritten SQL to
/// `Parser::ParseQuery` and uses those statements; when we return `None`,
/// the built-in parser sees the original text unchanged.
///
/// Only rewrites when the shape is unambiguous: `<ident>+:<ident>+` where
/// the colon is (a) preceded by an identifier character in the source
/// text, (b) followed by identifier characters, (c) followed by an
/// optional whitespace run and then `(`. That last requirement is what
/// distinguishes our function-call syntax from `:name` bind parameters
/// (colon preceded by non-ident) and `::` casts (colon preceded by
/// colon).
///
/// Skips string literals (single-quoted with `''` escapes), quoted
/// identifiers (double-quoted with `""` escapes), and both comment
/// forms (`-- …\n` and `/* … */`) so a colon inside those never gets
/// rewritten. UTF-8 safe: non-ASCII bytes pass through verbatim since
/// none of the interesting anchors (`:` `'` `"` `-` `/` `(` ident chars)
/// are non-ASCII.
///
/// Deliberately conservative — the design decision was "ship colon as
/// sugar only if it can't step on DuckDB's existing syntax", so anything
/// that fails these gates is left alone.
pub(crate) fn rewrite_colon_syntax(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    let mut modified = false;

    while i < bytes.len() {
        // Skip over string/comment lexical units without touching their
        // contents — colons inside them are user data, not our syntax.
        if let Some(end) = skip_string_or_comment(bytes, i) {
            out.extend_from_slice(&bytes[i..end]);
            i = end;
            continue;
        }

        // `::` cast — copy both bytes and skip. Order matters: check this
        // BEFORE the colon-syntax detector so `foo::bar(x)` doesn't get
        // rewritten (the second `:` would have the first as a preceding
        // ident-adjacent char).
        if bytes[i] == b':' && i + 1 < bytes.len() && bytes[i + 1] == b':' {
            out.push(b':');
            out.push(b':');
            i += 2;
            continue;
        }

        if bytes[i] == b':' {
            // Left side: colon preceded by an identifier byte in the
            // OUTPUT (which mirrors the source at this point since we've
            // been copying byte-for-byte). Excludes `:name`, `IS :bind`,
            // etc.
            let prev_is_ident = out
                .last()
                .copied()
                .map(is_ident_byte)
                .unwrap_or(false);
            if prev_is_ident {
                // Right side: at least one ident byte, then optional
                // whitespace, then `(`.
                let mut j = i + 1;
                while j < bytes.len() && is_ident_byte(bytes[j]) {
                    j += 1;
                }
                if j > i + 1 {
                    let mut k = j;
                    while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                        k += 1;
                    }
                    if k < bytes.len() && bytes[k] == b'(' {
                        // Rewrite: emit `.` in place of the `:`. The rest
                        // of the ident + whitespace + `(` copy through on
                        // subsequent iterations unchanged.
                        out.push(b'.');
                        i += 1;
                        modified = true;
                        continue;
                    }
                }
            }
        }

        out.push(bytes[i]);
        i += 1;
    }

    if !modified {
        return None;
    }
    // Rewrites replace only ASCII bytes with ASCII bytes; non-ASCII bytes
    // pass through untouched. So the output is valid UTF-8 iff the input was.
    String::from_utf8(out).ok()
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// If `bytes[start..]` begins with a string literal, quoted identifier,
/// line comment, or block comment, return the byte index one past the end
/// of that lexical unit. Otherwise `None`. Handles PostgreSQL-style `''`
/// and `""` escapes inside the respective quoted forms.
fn skip_string_or_comment(bytes: &[u8], start: usize) -> Option<usize> {
    match bytes.get(start).copied()? {
        b'\'' => {
            let mut i = start + 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2; // '' escape stays inside the string
                    } else {
                        return Some(i + 1);
                    }
                } else {
                    i += 1;
                }
            }
            Some(i)
        }
        b'"' => {
            let mut i = start + 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                    } else {
                        return Some(i + 1);
                    }
                } else {
                    i += 1;
                }
            }
            Some(i)
        }
        b'-' if bytes.get(start + 1) == Some(&b'-') => {
            let mut i = start + 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            Some(i) // leave the newline to be processed normally
        }
        b'/' if bytes.get(start + 1) == Some(&b'*') => {
            let mut i = start + 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < bytes.len() {
                Some(i + 2)
            } else {
                Some(bytes.len())
            }
        }
        _ => None,
    }
}

/// Recognize `DUCKLINK PREFIX <alias>: <namespace>[;]` (case-insensitive on
/// the keywords, optional trailing whitespace / semicolon) and return the
/// two identifiers. Both must be plain SQL identifiers (`[A-Za-z0-9_]+`);
/// anything else means "not our statement" and returns `None` so the parser
/// hook falls through unchanged.
fn parse_ducklink_prefix(sql: &str) -> Option<(String, String)> {
    let s = sql.trim().trim_end_matches(';').trim();
    let rest = strip_keyword(s, "DUCKLINK")?;
    if !rest.starts_with(char::is_whitespace) {
        return None;
    }
    let rest = rest.trim_start();
    let rest = strip_keyword(rest, "PREFIX")?;
    if !rest.starts_with(char::is_whitespace) {
        return None;
    }
    let rest = rest.trim_start();

    // The alias is an identifier up to the `:` separator (allowing spaces
    // before the colon).
    let colon_idx = rest.find(':')?;
    let alias = rest[..colon_idx].trim();
    let after_colon = rest[colon_idx + 1..].trim();
    let namespace = after_colon;
    if alias.is_empty() || namespace.is_empty() {
        return None;
    }
    if !crate::catalog::is_safe_identifier(alias)
        || !crate::catalog::is_safe_identifier(namespace)
    {
        return None;
    }
    Some((alias.to_string(), namespace.to_string()))
}

/// The `duckdb_version` string the extension was compiled against. Used to
/// select the matching native provider from the catalog. Native
/// `.duckdb_extension` files are tightly coupled to a DuckDB version, so
/// exact-match is required. Re-exported from `crate::catalog` so the common
/// tier (which decides `ducklink.modules.native_available` at bind time) shares
/// the same version string as the advanced-tier `LOAD NATIVE` path.
use crate::catalog::HOST_DUCKDB_VERSION;

/// `LOAD NATIVE` bridge — install a native `.duckdb_extension` for the current
/// platform + DuckDB version (downloading + sha256-verifying if missing) and
/// then have DuckDB load it. On success writes a summary; on error writes a
/// human-readable message including remediation for the common
/// `allow_unsigned_extensions=false` case.
///
/// The extension is NOT automatically flipped to allow unsigned loads: this
/// crosses a session-global security posture change that the user must make
/// explicitly. On a signature-check failure the returned summary tells the
/// user exactly what to run (`SET allow_unsigned_extensions=true;`) and points
/// at the upstream trust-mechanism proposal.
///
/// # Safety
/// `db` must be the valid live `duckdb_database`; `name` a valid C string;
/// `out_summary` a valid pointer to write one `*mut c_char` into.
#[no_mangle]
pub unsafe extern "C" fn ducklink_load_native(
    db: *mut c_void,
    name: *const c_char,
    out_summary: *mut *mut c_char,
) -> i32 {
    guard(2, || unsafe { ducklink_load_native_impl(db, name, out_summary) })
}

unsafe fn ducklink_load_native_impl(
    db: *mut c_void,
    name: *const c_char,
    out_summary: *mut *mut c_char,
) -> i32 {
    if name.is_null() || out_summary.is_null() || db.is_null() {
        return 2;
    }
    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => {
            *out_summary = CString::new("LOAD NATIVE: argument is not valid UTF-8")
                .map(|c| c.into_raw())
                .unwrap_or(std::ptr::null_mut());
            return 1;
        }
    };

    // 1. Prefer a community-native provider — INSTALL + LOAD from
    //    `duckdb/community-extensions`. Signed by community's key so no
    //    `-unsigned` needed. This is the routing-layer story: ducklink
    //    delegates to an existing native implementation when one is
    //    published, rather than shipping a competing native build.
    use crate::catalog::{resolve_name_to_community_native, resolve_name_to_native, NATIVE_PLATFORM};
    if let Ok(spec) = resolve_name_to_community_native(name_str) {
        let community_ext = spec.extension_name.clone();
        // Belt-and-braces: identifier check on the extension name so a bad
        // catalog entry can't inject SQL into INSTALL / LOAD.
        if !crate::catalog::is_safe_identifier(&community_ext) {
            *out_summary = CString::new(format!(
                "DUCKLINK LOAD NATIVE: community-native provider for '{name_str}' names \
                 '{community_ext}', which contains characters outside [A-Za-z0-9_]. \
                 Refusing to run INSTALL / LOAD."
            ))
            .map(|c| c.into_raw())
            .unwrap_or(std::ptr::null_mut());
            return 1;
        }
        let install_sql = format!("INSTALL {community_ext} FROM community");
        let load_sql = format!("LOAD {community_ext}");
        if let Err(e) = load_via_duckdb_query(db.cast(), &install_sql) {
            *out_summary = CString::new(format!(
                "DUCKLINK LOAD NATIVE: INSTALL {community_ext} FROM community failed: {e}"
            ))
            .map(|c| c.into_raw())
            .unwrap_or(std::ptr::null_mut());
            return 1;
        }
        if let Err(e) = load_via_duckdb_query(db.cast(), &load_sql) {
            *out_summary = CString::new(format!(
                "DUCKLINK LOAD NATIVE: LOAD {community_ext} failed after INSTALL: {e}"
            ))
            .map(|c| c.into_raw())
            .unwrap_or(std::ptr::null_mut());
            return 1;
        }
        let alias_count = match create_community_aliases_advanced(db.cast(), &spec) {
            Ok(n) => n,
            Err(e) => {
                // Non-fatal for the LOAD itself: community's own names are
                // still callable. Surface the aliasing error in the summary so
                // catalog authors can fix mismappings without hiding them.
                crate::events::emit("community_alias_error", Some(name_str), e.clone());
                *out_summary = CString::new(format!(
                    "installed '{name_str}' via community-extensions:{community_ext}; \
                     alias generation failed: {e}"
                ))
                .map(|c| c.into_raw())
                .unwrap_or(std::ptr::null_mut());
                return 0;
            }
        };
        // Replay any prefixes persisted from prior sessions — the
        // just-loaded module might satisfy one whose namespace was empty
        // before. Non-fatal: on failure, the current session's community
        // aliases still work; the user just has to redeclare the prefix.
        let replayed = replay_persisted_prefixes(db.cast());
        crate::events::emit(
            "load_community_native_ok",
            Some(name_str),
            format!(
                "extension='{community_ext}' aliases={alias_count} prefixes_replayed={replayed}"
            ),
        );
        let replay_note = if replayed > 0 {
            format!(", replayed {replayed} persisted prefix(es)")
        } else {
            String::new()
        };
        *out_summary = CString::new(format!(
            "installed '{name_str}' via community-extensions:{community_ext} \
             ({alias_count} alias{}{replay_note})",
            if alias_count == 1 { "" } else { "es" }
        ))
        .map(|c| c.into_raw())
        .unwrap_or(std::ptr::null_mut());
        return 0;
    }

    // 2. Fall back to a ducklink-hosted native provider matching this
    //    host's platform + DuckDB version. Downloads + sha256-verifies
    //    against catalog digest before caching. LOAD via DuckDB's native
    //    machinery on the cached path.
    let path = match resolve_name_to_native(name_str, NATIVE_PLATFORM, HOST_DUCKDB_VERSION) {
        Ok(p) => p,
        Err(e) => {
            *out_summary = CString::new(format!("DUCKLINK LOAD NATIVE: {e}"))
                .map(|c| c.into_raw())
                .unwrap_or(std::ptr::null_mut());
            return 1;
        }
    };

    // 2. Have DuckDB do the actual LOAD. We open a sibling connection on the
    //    live database, then run `LOAD '<absolute-path>'`. DuckDB runs the
    //    extension's init function, wiring functions into the catalog visible
    //    to every connection.
    let path_str = path.to_string_lossy().into_owned();
    let load_result = load_via_duckdb(db.cast(), &path_str);

    match load_result {
        Ok(()) => {
            let replayed = replay_persisted_prefixes(db.cast());
            let replay_note = if replayed > 0 {
                format!(" (replayed {replayed} persisted prefix(es))")
            } else {
                String::new()
            };
            let msg = format!("installed '{name_str}' at {}{replay_note}", path_str);
            crate::events::emit(
                "load_native_ok",
                Some(name_str),
                format!("{path_str} prefixes_replayed={replayed}"),
            );
            *out_summary = CString::new(msg)
                .map(|c| c.into_raw())
                .unwrap_or(std::ptr::null_mut());
            0
        }
        Err(err) => {
            // Special-case the missing-signature error so the user sees an
            // actionable "here's what to do" message instead of DuckDB's raw
            // exception text. We do NOT flip allow_unsigned_extensions
            // ourselves — that's the user's explicit trust decision.
            let msg = if err.contains("allow_unsigned_extensions")
                || err.contains("signature")
            {
                format!(
                    "LOAD NATIVE: '{name_str}' was installed at {path_str} but its signature is \
                     not trusted by this DuckDB build.\n\
                     \n\
                     `allow_unsigned_extensions` can only be set at DuckDB startup, not from a \
                     running session. To load this extension, restart DuckDB with the -unsigned \
                     flag (or set it via the command-line/config), then:\n\
                     \n\
                     \tduckdb -unsigned\n\
                     \tLOAD 'path/to/ducklink.duckdb_extension';\n\
                     \tLOAD NATIVE '{name_str}';\n\
                     \n\
                     The friction is intentional: enabling unsigned extensions is a session-wide \
                     trust posture change and the user needs to make it explicitly. See \
                     docs/duckdb-upstream-custom-trusted-keys.md for the upstream feature that \
                     will remove this friction.\n\
                     \n\
                     Underlying DuckDB error: {err}"
                )
            } else {
                format!("LOAD NATIVE: DuckDB LOAD failed for '{name_str}': {err}")
            };
            crate::events::emit("load_native_error", Some(name_str), err);
            *out_summary = CString::new(msg)
                .map(|c| c.into_raw())
                .unwrap_or(std::ptr::null_mut());
            1
        }
    }
}

/// `DUCKLINK PREFIX <alias>: <namespace>;` — declare a schema alias.
///
/// Enumerates every function currently registered in the `<namespace>`
/// schema (in the default catalog, where community-native double-
/// registration lands via [`catalog_alias`]) and re-registers each of
/// them under the `<alias>` schema. The result: `alias.foo(x)` and
/// `namespace.foo(x)` bind to the same underlying function set.
///
/// # Safety
/// `db` must be the valid live `duckdb_database`; `alias` and `namespace`
/// valid C strings; `out_summary` a valid pointer to write one
/// `*mut c_char` into. Callers of the ABI free the summary via
/// [`ducklink_adv_free`].
#[no_mangle]
pub unsafe extern "C" fn ducklink_prefix(
    db: *mut c_void,
    alias: *const c_char,
    namespace: *const c_char,
    out_summary: *mut *mut c_char,
) -> i32 {
    guard(2, || unsafe { ducklink_prefix_impl(db, alias, namespace, out_summary) })
}

unsafe fn ducklink_prefix_impl(
    db: *mut c_void,
    alias_c: *const c_char,
    namespace_c: *const c_char,
    out_summary: *mut *mut c_char,
) -> i32 {
    if db.is_null() || alias_c.is_null() || namespace_c.is_null() || out_summary.is_null() {
        return 2;
    }
    let alias = match CStr::from_ptr(alias_c).to_str() {
        Ok(s) => s.to_string(),
        Err(_) => {
            *out_summary = CString::new("DUCKLINK PREFIX: alias is not valid UTF-8")
                .map(|c| c.into_raw())
                .unwrap_or(std::ptr::null_mut());
            return 1;
        }
    };
    let namespace = match CStr::from_ptr(namespace_c).to_str() {
        Ok(s) => s.to_string(),
        Err(_) => {
            *out_summary = CString::new("DUCKLINK PREFIX: namespace is not valid UTF-8")
                .map(|c| c.into_raw())
                .unwrap_or(std::ptr::null_mut());
            return 1;
        }
    };
    // Belt-and-braces identifier gate — the parser already validated both
    // as `[A-Za-z0-9_]+` before returning the sentinel, but re-check so a
    // caller reaching us through a different path can't inject SQL through
    // the `schema_name = '...'` splice below.
    if !crate::catalog::is_safe_identifier(&alias)
        || !crate::catalog::is_safe_identifier(&namespace)
    {
        *out_summary = CString::new(format!(
            "DUCKLINK PREFIX: alias / namespace must match [A-Za-z0-9_]+ \
             (got alias='{alias}', namespace='{namespace}')"
        ))
        .map(|c| c.into_raw())
        .unwrap_or(std::ptr::null_mut());
        return 1;
    }

    // Enumerate functions in the source schema. Community-native
    // double-registration (see reg_duckdb.rs::create_community_aliases +
    // advanced.rs::create_community_aliases_advanced) puts aliases in the
    // default catalog's `<namespace>` schema, so this is the schema
    // `duckdb_functions()` reports for them.
    let scan_sql = format!(
        "SELECT function_name FROM duckdb_functions() \
         WHERE schema_name = '{namespace}' \
         AND function_type IN ('scalar','aggregate','table_macro','scalar_macro','macro','table') \
         GROUP BY function_name"
    );
    let db_h: ffi::duckdb_database = db.cast();
    let rows = match query_rows_via_duckdb(db_h, &scan_sql) {
        Ok(r) => r,
        Err(e) => {
            *out_summary = CString::new(format!("DUCKLINK PREFIX: scan failed: {e}"))
                .map(|c| c.into_raw())
                .unwrap_or(std::ptr::null_mut());
            return 1;
        }
    };
    let fns: Vec<String> = rows
        .into_iter()
        .filter_map(|r| r.into_iter().next().flatten())
        .filter(|n| crate::catalog::is_safe_identifier(n))
        .collect();
    if fns.is_empty() {
        *out_summary = CString::new(format!(
            "DUCKLINK PREFIX: namespace '{namespace}' has no functions to alias — \
             is the module loaded?"
        ))
        .map(|c| c.into_raw())
        .unwrap_or(std::ptr::null_mut());
        return 1;
    }

    // Open a sibling raw connection for the alias pass.
    let mut raw: ffi::duckdb_connection = std::ptr::null_mut();
    if ffi::duckdb_connect(db_h, &mut raw) != ffi::DuckDBSuccess {
        *out_summary = CString::new("DUCKLINK PREFIX: duckdb_connect failed")
            .map(|c| c.into_raw())
            .unwrap_or(std::ptr::null_mut());
        return 1;
    }
    struct ConnGuard(ffi::duckdb_connection);
    impl Drop for ConnGuard {
        fn drop(&mut self) {
            unsafe { ffi::duckdb_disconnect(&mut self.0) };
        }
    }
    let _guard = ConnGuard(raw);

    let mut aliased = 0usize;
    let mut errors: Vec<String> = Vec::new();
    for fn_name in &fns {
        // Re-register `namespace.fn_name` under `alias.fn_name` — same name
        // in both schemas, just a different qualifier prefix.
        match catalog_alias(raw, Some(&namespace), fn_name, Some(&alias), fn_name) {
            Ok(_) => aliased += 1,
            Err(e) => {
                if errors.len() < 3 {
                    errors.push(format!("{fn_name}: {e}"));
                }
            }
        }
    }

    // Persist the mapping so reconnecting to the same database restores
    // this alias on the next `DUCKLINK LOAD`. Non-fatal: aliasing already
    // succeeded for this session; if persistence fails (e.g. read-only
    // catalog) we record it in the summary but the user's current-session
    // aliases still work.
    let persistence_note = match persist_prefix(db_h, &alias, &namespace) {
        Ok(()) => String::new(),
        Err(e) => format!(" (persist failed: {e})"),
    };

    let mut summary = format!(
        "DUCKLINK PREFIX {alias}: {namespace} — aliased {aliased} of {} function(s){persistence_note}",
        fns.len()
    );
    if !errors.is_empty() {
        summary.push_str("; errors: ");
        summary.push_str(&errors.join(", "));
    }
    *out_summary = CString::new(summary)
        .map(|c| c.into_raw())
        .unwrap_or(std::ptr::null_mut());
    0
}

/// Create the `ducklink` schema + `ducklink.prefixes` table if they don't
/// already exist. Called lazily from [`persist_prefix`] so a user who never
/// declares a `DUCKLINK PREFIX` never gets an unused table in their catalog.
///
/// # Safety
/// `db` must be a valid live `duckdb_database`.
unsafe fn ensure_prefixes_table(db: ffi::duckdb_database) -> Result<(), String> {
    load_via_duckdb_query(db, "CREATE SCHEMA IF NOT EXISTS ducklink")
        .map_err(|e| format!("CREATE SCHEMA ducklink: {e}"))?;
    load_via_duckdb_query(
        db,
        "CREATE TABLE IF NOT EXISTS ducklink.prefixes (\
             alias VARCHAR PRIMARY KEY, \
             namespace VARCHAR NOT NULL)",
    )
    .map_err(|e| format!("CREATE TABLE ducklink.prefixes: {e}"))?;
    Ok(())
}

/// Persist a `(alias, namespace)` mapping into `ducklink.prefixes` so a
/// fresh connection can replay it. Redeclaring the same alias
/// idempotently REPLACES the row (INSERT OR REPLACE). Both `alias` and
/// `namespace` must already be safe identifiers — callers gate on
/// [`crate::catalog::is_safe_identifier`] before splicing.
///
/// For :memory: databases the table lives with the rest of the in-memory
/// state and naturally dies at close — user redeclares on reconnect. For
/// file-backed databases the row survives, and any subsequent
/// `DUCKLINK LOAD` triggers [`replay_persisted_prefixes`].
///
/// # Safety
/// `db` must be a valid live `duckdb_database`.
unsafe fn persist_prefix(
    db: ffi::duckdb_database,
    alias: &str,
    namespace: &str,
) -> Result<(), String> {
    ensure_prefixes_table(db)?;
    // Identifiers already validated by the caller — safe to splice.
    let sql = format!(
        "INSERT OR REPLACE INTO ducklink.prefixes (alias, namespace) VALUES ('{alias}', '{namespace}')"
    );
    load_via_duckdb_query(db, &sql).map_err(|e| format!("INSERT ducklink.prefixes: {e}"))
}

/// Walk every persisted `(alias, namespace)` and reapply it (register
/// aliases in the alias schema for every function currently in the
/// namespace schema). Silent-skip a prefix whose namespace has no
/// currently-loaded functions — that just means the source module isn't
/// loaded yet in this session, and a later `DUCKLINK LOAD 'name' NATIVE`
/// call will trigger another replay pass.
///
/// Called at the end of `ducklink_load_wasm_impl` /
/// `ducklink_load_native_impl` so `DUCKLINK LOAD 'crypto' NATIVE` after
/// a reconnect automatically restores any `c: crypto` alias declared in
/// a prior session.
///
/// # Safety
/// `db` must be a valid live `duckdb_database`.
unsafe fn replay_persisted_prefixes(db: ffi::duckdb_database) -> usize {
    // If the table hasn't been created yet (no prefix ever declared),
    // nothing to replay. Check via information_schema so this stays a
    // clean no-op on a virgin database.
    let exists_check = query_rows_via_duckdb(
        db,
        "SELECT 1 FROM information_schema.tables \
         WHERE table_schema = 'ducklink' AND table_name = 'prefixes' LIMIT 1",
    );
    match exists_check {
        Ok(rows) if !rows.is_empty() => {}
        _ => return 0,
    }
    let rows = match query_rows_via_duckdb(db, "SELECT alias, namespace FROM ducklink.prefixes") {
        Ok(r) => r,
        Err(_) => return 0,
    };
    let mut replayed = 0usize;
    for row in rows {
        let alias = match row.first().and_then(|c| c.clone()) {
            Some(a) => a,
            None => continue,
        };
        let namespace = match row.get(1).and_then(|c| c.clone()) {
            Some(n) => n,
            None => continue,
        };
        if !crate::catalog::is_safe_identifier(&alias)
            || !crate::catalog::is_safe_identifier(&namespace)
        {
            continue;
        }
        // Reuse the impl but suppress persistence — we're already reading
        // FROM the table, no need to write back into it. Passing None for
        // the persistence hook is what does it.
        if apply_prefix_no_persist(db, &alias, &namespace).is_ok() {
            replayed += 1;
        }
    }
    replayed
}

/// The core work of aliasing every function in `namespace` under `alias`,
/// factored out of `ducklink_prefix_impl` so replay can share it without
/// re-persisting. Returns Ok(count) when at least the schema scan
/// succeeded, Err(msg) on hard C-API failure. A namespace with zero
/// currently-loaded functions returns Ok(0).
unsafe fn apply_prefix_no_persist(
    db: ffi::duckdb_database,
    alias: &str,
    namespace: &str,
) -> Result<usize, String> {
    let scan_sql = format!(
        "SELECT function_name FROM duckdb_functions() \
         WHERE schema_name = '{namespace}' \
         AND function_type IN ('scalar','aggregate','table_macro','scalar_macro','macro','table') \
         GROUP BY function_name"
    );
    let rows = query_rows_via_duckdb(db, &scan_sql)?;
    let fns: Vec<String> = rows
        .into_iter()
        .filter_map(|r| r.into_iter().next().flatten())
        .filter(|n| crate::catalog::is_safe_identifier(n))
        .collect();
    if fns.is_empty() {
        return Ok(0);
    }
    let mut raw: ffi::duckdb_connection = std::ptr::null_mut();
    if ffi::duckdb_connect(db, &mut raw) != ffi::DuckDBSuccess {
        return Err("duckdb_connect failed".to_string());
    }
    struct ConnGuard(ffi::duckdb_connection);
    impl Drop for ConnGuard {
        fn drop(&mut self) {
            unsafe { ffi::duckdb_disconnect(&mut self.0) };
        }
    }
    let _guard = ConnGuard(raw);
    let mut aliased = 0usize;
    for fn_name in &fns {
        if catalog_alias(raw, Some(namespace), fn_name, Some(alias), fn_name).is_ok() {
            aliased += 1;
        }
    }
    Ok(aliased)
}

/// Open a sibling connection on the live database and run
/// `LOAD '<absolute_path>';`. Returns the DuckDB error string on failure.
unsafe fn load_via_duckdb(db: ffi::duckdb_database, path: &str) -> Result<(), String> {
    // Escape single-quotes in the path (extremely unlikely but let's be right).
    let escaped = path.replace('\'', "''");
    let sql = format!("LOAD '{escaped}'");
    load_via_duckdb_query(db, &sql)
}

/// Open a sibling connection on the live database and run an arbitrary SQL
/// statement (used for the community-native `INSTALL <ext> FROM community` +
/// `LOAD <ext>` pair). Returns the DuckDB error string on failure.
///
/// # Safety
/// `db` must be a valid live `duckdb_database`. Callers are responsible for
/// validating any user-controlled data spliced into `sql` — see the identifier
/// check on `extension_name` in the community-native branch.
unsafe fn load_via_duckdb_query(
    db: ffi::duckdb_database,
    sql: &str,
) -> Result<(), String> {
    let c_sql = CString::new(sql).map_err(|e| format!("query contains NUL: {e}"))?;

    let mut conn: ffi::duckdb_connection = std::ptr::null_mut();
    if ffi::duckdb_connect(db, &mut conn) != ffi::DuckDBSuccess {
        return Err("duckdb_connect failed".to_string());
    }
    struct ConnGuard(ffi::duckdb_connection);
    impl Drop for ConnGuard {
        fn drop(&mut self) {
            unsafe { ffi::duckdb_disconnect(&mut self.0) };
        }
    }
    let _guard = ConnGuard(conn);

    let mut result: ffi::duckdb_result = std::mem::zeroed();
    let rc = ffi::duckdb_query(conn, c_sql.as_ptr(), &mut result);
    struct ResGuard<'a>(&'a mut ffi::duckdb_result);
    impl Drop for ResGuard<'_> {
        fn drop(&mut self) {
            unsafe { ffi::duckdb_destroy_result(self.0) };
        }
    }
    let mut result_guard = ResGuard(&mut result);

    if rc == ffi::DuckDBSuccess {
        return Ok(());
    }
    let err_ptr = ffi::duckdb_result_error(result_guard.0 as *mut _);
    let msg = if err_ptr.is_null() {
        "duckdb_query failed with no error message".to_string()
    } else {
        CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
    };
    Err(msg)
}

/// Advanced-tier flavour of the reg_duckdb `create_community_aliases`: after
/// `INSTALL / LOAD` succeeded on a community extension, discover its
/// functions and register `CREATE OR REPLACE MACRO` aliases under ducklink's
/// chosen names.
///
/// The SQL-building lives in [`crate::catalog::build_alias_macro`] and the
/// pair-selection in [`crate::catalog::compute_alias_pairs`]; this function
/// only wraps them in DuckDB's C API for the parser-hook code path (which
/// has a raw `duckdb_database` and no `duckdb::Connection`).
///
/// # Safety
/// `db` must be a valid live `duckdb_database`. All names composed into SQL
/// pass through [`crate::catalog::is_safe_identifier`] before splicing.
unsafe fn create_community_aliases_advanced(
    db: ffi::duckdb_database,
    spec: &crate::catalog::CommunityNativeSpec,
) -> Result<usize, String> {
    // 1. Discover community-registered function names for the prefix (if any).
    //    Prefix filter is applied in Rust (by compute_alias_pairs) — the SQL
    //    wildcard `_` in LIKE would over-match for prefixes like "t_".
    let discovered: Vec<String> = if spec.community_prefix.is_some() {
        let scan_sql = "SELECT function_name FROM duckdb_functions() \
                        WHERE function_type IN ('scalar','aggregate','table_macro','scalar_macro','macro','table') \
                        GROUP BY function_name";
        let rows = query_rows_via_duckdb(db, scan_sql)?;
        rows.into_iter()
            .filter_map(|r| r.into_iter().next().flatten())
            .collect()
    } else {
        Vec::new()
    };

    // 2. Fold explicit mapping + prefix hits into a stable pair list.
    let pairs = crate::catalog::compute_alias_pairs(spec, &discovered);
    if pairs.is_empty() {
        return Ok(0);
    }

    // 3. Open a sibling connection once for the whole alias pass — we use the
    //    C++ catalog-alias shim per pair (transparent for aggregates), and
    //    fall back to per-arity CREATE MACRO on kinds the shim can't handle.
    let mut conn: ffi::duckdb_connection = std::ptr::null_mut();
    if ffi::duckdb_connect(db, &mut conn) != ffi::DuckDBSuccess {
        return Err("duckdb_connect failed for alias pass".to_string());
    }
    struct ConnGuard(ffi::duckdb_connection);
    impl Drop for ConnGuard {
        fn drop(&mut self) {
            unsafe { ffi::duckdb_disconnect(&mut self.0) };
        }
    }
    let _guard = ConnGuard(conn);

    let mut created = 0usize;
    // Optional namespace mirror — see the reg_duckdb.rs sibling for the
    // full explanation. Same shape: double-register each alias in `main`
    // (backcompat) and in `<namespace>` (schema-qualified).
    let namespace = spec.namespace.as_deref();

    for (ours, theirs) in &pairs {
        // Try the C++ catalog-alias first — it gives real transparency:
        // aggregate DISTINCT/FILTER/ORDER BY/window all work through the
        // alias because the alias IS an AggregateFunctionCatalogEntry.
        match catalog_alias(conn, None, theirs, None, ours) {
            Ok(_kind) => {
                created += 1;
                if let Some(ns) = namespace {
                    if let Err(err) = catalog_alias(conn, None, theirs, Some(ns), ours) {
                        crate::events::emit(
                            "community_namespace_alias_error",
                            Some(ours.as_str()),
                            format!("{ns}.{ours}: {err}"),
                        );
                    } else {
                        created += 1;
                    }
                }
                continue;
            }
            Err(err) => {
                crate::events::emit(
                    "community_alias_shim_fallback",
                    Some(ours.as_str()),
                    format!("{theirs}: {err}"),
                );
                // Fall through to the macro fallback below.
            }
        }

        // Macro fallback (rare — only if the shim couldn't find the entry
        // or version-drifted). Same shapes as the loadable-only build uses,
        // with the documented aggregate caveat.
        let info_sql = format!(
            "SELECT function_type, array_to_string(parameters, ',') AS param_csv \
             FROM duckdb_functions() WHERE function_name = '{theirs}'"
        );
        let rows = query_rows_via_duckdb(db, &info_sql)?;
        let mut done_arities: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for row in rows {
            let ftype = match row.first().and_then(|x| x.clone()) {
                Some(t) => t,
                None => continue,
            };
            let param_csv = row.get(1).cloned().flatten().unwrap_or_default();
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
            let Some(macro_sql) =
                crate::catalog::build_alias_macro(&ftype, None, ours, theirs, &params)
            else {
                continue;
            };
            if let Err(err) = load_via_duckdb_query(db, &macro_sql) {
                crate::events::emit(
                    "community_alias_error",
                    Some(ours.as_str()),
                    format!("{theirs}: {err}"),
                );
                continue;
            }
            created += 1;
        }
    }
    Ok(created)
}

/// Open a sibling connection, run `sql`, materialize every row's columns as
/// `Option<String>` (NULL → `None`). All values are read via
/// `duckdb_value_varchar`, which casts every column type to VARCHAR. Suitable
/// for small metadata queries like `duckdb_functions()`; not for bulk data.
///
/// # Safety
/// `db` must be a valid live `duckdb_database`.
unsafe fn query_rows_via_duckdb(
    db: ffi::duckdb_database,
    sql: &str,
) -> Result<Vec<Vec<Option<String>>>, String> {
    let c_sql = CString::new(sql).map_err(|e| format!("query contains NUL: {e}"))?;

    let mut conn: ffi::duckdb_connection = std::ptr::null_mut();
    if ffi::duckdb_connect(db, &mut conn) != ffi::DuckDBSuccess {
        return Err("duckdb_connect failed".to_string());
    }
    struct ConnGuard(ffi::duckdb_connection);
    impl Drop for ConnGuard {
        fn drop(&mut self) {
            unsafe { ffi::duckdb_disconnect(&mut self.0) };
        }
    }
    let _guard = ConnGuard(conn);

    let mut result: ffi::duckdb_result = std::mem::zeroed();
    let rc = ffi::duckdb_query(conn, c_sql.as_ptr(), &mut result);
    struct ResGuard<'a>(&'a mut ffi::duckdb_result);
    impl Drop for ResGuard<'_> {
        fn drop(&mut self) {
            unsafe { ffi::duckdb_destroy_result(self.0) };
        }
    }
    let result_guard = ResGuard(&mut result);
    if rc != ffi::DuckDBSuccess {
        let err_ptr = ffi::duckdb_result_error(result_guard.0 as *mut _);
        let msg = if err_ptr.is_null() {
            "duckdb_query failed with no error message".to_string()
        } else {
            CStr::from_ptr(err_ptr).to_string_lossy().into_owned()
        };
        return Err(msg);
    }

    let col_count = ffi::duckdb_column_count(result_guard.0 as *mut _);
    let row_count = ffi::duckdb_row_count(result_guard.0 as *mut _);
    let mut rows: Vec<Vec<Option<String>>> = Vec::with_capacity(row_count as usize);
    for r in 0..row_count {
        let mut row_vals: Vec<Option<String>> = Vec::with_capacity(col_count as usize);
        for c in 0..col_count {
            let ptr = ffi::duckdb_value_varchar(result_guard.0 as *mut _, c, r);
            if ptr.is_null() {
                row_vals.push(None);
            } else {
                let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
                ffi::duckdb_free(ptr as *mut _);
                row_vals.push(Some(s));
            }
        }
        rows.push(row_vals);
    }
    Ok(rows)
}

/// Free a C string returned by the advanced-tier bridge functions.
///
/// # Safety
/// `ptr` must be NULL or a pointer previously returned by a `ducklink_*` bridge.
#[no_mangle]
pub unsafe extern "C" fn ducklink_adv_free(ptr: *mut c_char) {
    guard((), || unsafe {
        if !ptr.is_null() {
            drop(CString::from_raw(ptr));
        }
    })
}

#[cfg(test)]
mod colon_rewrite_tests {
    use super::rewrite_colon_syntax;

    /// The primary transformation: `<ident>:<ident>(...)` → `<ident>.<ident>(...)`.
    #[test]
    fn function_call_rewrites() {
        assert_eq!(
            rewrite_colon_syntax("SELECT c:hash(x) FROM t").as_deref(),
            Some("SELECT c.hash(x) FROM t")
        );
        assert_eq!(
            rewrite_colon_syntax("SELECT c:hash_agg('sha2-256', s ORDER BY s) FROM t").as_deref(),
            Some("SELECT c.hash_agg('sha2-256', s ORDER BY s) FROM t")
        );
    }

    /// Multiple occurrences in one statement.
    #[test]
    fn multiple_calls_all_rewrite() {
        assert_eq!(
            rewrite_colon_syntax("SELECT c:hash(x), math:sqrt(y) FROM t").as_deref(),
            Some("SELECT c.hash(x), math.sqrt(y) FROM t")
        );
    }

    /// Whitespace between the colon and the function is fine.
    #[test]
    fn whitespace_before_open_paren_ok() {
        assert_eq!(
            rewrite_colon_syntax("SELECT c:hash  (x)").as_deref(),
            Some("SELECT c.hash  (x)")
        );
    }

    /// `::` cast is untouched. This is the load-bearing collision check —
    /// getting this wrong would break every DuckDB CAST expression.
    #[test]
    fn double_colon_cast_untouched() {
        assert_eq!(rewrite_colon_syntax("SELECT x::TEXT FROM t"), None);
        assert_eq!(
            rewrite_colon_syntax("SELECT c:hash(x)::VARCHAR FROM t").as_deref(),
            Some("SELECT c.hash(x)::VARCHAR FROM t")
        );
        // Trailing :: after an ident (looks close to our pattern but the
        // second colon rules it out).
        assert_eq!(rewrite_colon_syntax("SELECT foo::bar"), None);
    }

    /// `:name` bind parameters MUST NOT be rewritten. The `:` there is
    /// preceded by non-identifier characters (whitespace, comma, `(`).
    #[test]
    fn bind_parameter_untouched() {
        assert_eq!(rewrite_colon_syntax("SELECT :name FROM t"), None);
        assert_eq!(rewrite_colon_syntax("SELECT * FROM t WHERE x = :x"), None);
        assert_eq!(
            rewrite_colon_syntax("EXECUTE q(:p1, :p2, :p3)"),
            None,
            "bind params inside function calls must not rewrite"
        );
    }

    /// Colon inside a single-quoted string literal is user data.
    #[test]
    fn string_literal_colon_untouched() {
        assert_eq!(
            rewrite_colon_syntax("SELECT 'c:hash(x)' FROM t"),
            None,
            "colon inside single-quoted string literal must not rewrite"
        );
        // A rewrite outside the string still works even if there's a
        // colon inside the string.
        assert_eq!(
            rewrite_colon_syntax("SELECT c:hash('c:x') FROM t").as_deref(),
            Some("SELECT c.hash('c:x') FROM t")
        );
    }

    /// `''` escape inside a single-quoted string keeps us in the string.
    #[test]
    fn escaped_single_quote_inside_string() {
        assert_eq!(
            rewrite_colon_syntax("SELECT 'a''c:hash(x)' FROM t"),
            None,
            "'' escape must not close the string prematurely"
        );
    }

    /// Colon inside a double-quoted identifier is user data.
    #[test]
    fn quoted_identifier_colon_untouched() {
        assert_eq!(
            rewrite_colon_syntax(r#"SELECT "weird:name" FROM t"#),
            None
        );
    }

    /// Line comment absorbs everything to end-of-line.
    #[test]
    fn line_comment_colon_untouched() {
        assert_eq!(
            rewrite_colon_syntax("SELECT 1 -- c:hash(x)\nFROM t"),
            None
        );
        // Rewrite AFTER the comment closes still works.
        assert_eq!(
            rewrite_colon_syntax("-- c:hash(x)\nSELECT c:hash(y)").as_deref(),
            Some("-- c:hash(x)\nSELECT c.hash(y)")
        );
    }

    /// Block comment absorbs across newlines.
    #[test]
    fn block_comment_colon_untouched() {
        assert_eq!(
            rewrite_colon_syntax("SELECT /* c:hash(x) */ 1"),
            None
        );
        assert_eq!(
            rewrite_colon_syntax("/* c:x\nc:y(z) */ SELECT c:hash(w)").as_deref(),
            Some("/* c:x\nc:y(z) */ SELECT c.hash(w)")
        );
    }

    /// A colon between idents but NOT followed by `(` — leave alone.
    /// Could be JSON path syntax someone adds to DuckDB later, or a
    /// user's alias-like construct we don't want to accidentally break.
    #[test]
    fn colon_between_idents_without_paren_untouched() {
        assert_eq!(rewrite_colon_syntax("SELECT c:hash FROM t"), None);
        assert_eq!(rewrite_colon_syntax("SELECT c:hash, x FROM t"), None);
        // With optional whitespace before the '('  we DO rewrite —
        // whitespace between name and `(` is legal SQL for function calls.
        assert_eq!(
            rewrite_colon_syntax("SELECT c:hash (x)").as_deref(),
            Some("SELECT c.hash (x)")
        );
    }

    /// Non-ASCII content passes through untouched.
    #[test]
    fn utf8_passthrough() {
        assert_eq!(
            rewrite_colon_syntax("SELECT 'naïve' AS x, c:hash(y) FROM t").as_deref(),
            Some("SELECT 'naïve' AS x, c.hash(y) FROM t")
        );
        assert_eq!(
            rewrite_colon_syntax("SELECT '日本語:test(x)' FROM t"),
            None
        );
    }

    /// No rewrite → returns None (so the shim can skip the parse-and-swap
    /// entirely and let DuckDB's parser see the original text).
    #[test]
    fn returns_none_when_nothing_to_rewrite() {
        assert_eq!(rewrite_colon_syntax("SELECT 1"), None);
        assert_eq!(rewrite_colon_syntax(""), None);
        assert_eq!(rewrite_colon_syntax(";"), None);
        assert_eq!(rewrite_colon_syntax("SELECT crypto.hash(x) FROM t"), None);
    }

    /// Unterminated string — don't emit garbage or panic. Leaving the
    /// original text unchanged is the right call: DuckDB's parser will
    /// report the syntax error on the original source.
    #[test]
    fn unterminated_string_or_comment_doesnt_panic() {
        // Should complete without panicking (the string absorbs to EOF,
        // no rewrite candidate outside it, so `None`).
        let _ = rewrite_colon_syntax("SELECT 'unterminated");
        let _ = rewrite_colon_syntax("SELECT /* unclosed comment ");
    }

    /// The specific pathological patterns the design flagged.
    #[test]
    fn design_flagged_patterns() {
        // JSON path-ish: no colons in DuckDB JSON syntax today, but
        // future-proof by not rewriting things that don't fit our shape.
        assert_eq!(rewrite_colon_syntax("SELECT x->'a:b'"), None);
        // Prepared statement with bind params.
        assert_eq!(
            rewrite_colon_syntax("PREPARE q AS SELECT * FROM t WHERE x = :p"),
            None
        );
        // Multi-arg aggregate with a bind param — no false positive on `:`.
        assert_eq!(
            rewrite_colon_syntax("SELECT crypto_hash_agg(:algo, s ORDER BY s) FROM t"),
            None
        );
    }
}
