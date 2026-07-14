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
    /// Register `existing_name` (scalar / aggregate / table) under `new_name`
    /// in the system catalog. Returns 1/2/3 for aggregate/scalar/table on
    /// success; -1..-5 on failure with `*out_err` set to a malloc'd message
    /// (free via `ducklink_adv_free`). See `cpp/ducklink_alias.cpp`.
    fn ducklink_alias_function(
        conn: *mut c_void,
        existing_name: *const c_char,
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

/// Safe wrapper over the C++ `ducklink_alias_function` shim: alias the
/// community-registered function `existing` under ducklink's chosen name
/// `new_name` in the system catalog. Returns `Ok(kind)` when the alias
/// landed, `Err(msg)` with the shim's message otherwise. Both names remain
/// callable — DuckDB's binder resolves each to the same underlying function
/// set, so aggregates keep their DISTINCT/FILTER/ORDER BY/window support
/// through the alias.
///
/// # Safety
/// `raw_conn` must be a live `duckdb_connection`. Ducklink obtains it via the
/// same C API code path used by aggregate registration (see `RawConnHandle`
/// in reg_duckdb.rs) or by opening a sibling connection on the raw database
/// handle (see `advanced.rs` community-native branch).
pub unsafe fn catalog_alias(
    raw_conn: ffi::duckdb_connection,
    existing: &str,
    new_name: &str,
) -> Result<AliasKind, String> {
    if raw_conn.is_null() {
        return Err("null raw connection".to_string());
    }
    let c_existing =
        CString::new(existing).map_err(|e| format!("existing name contains NUL: {e}"))?;
    let c_new = CString::new(new_name).map_err(|e| format!("new name contains NUL: {e}"))?;
    let mut err_ptr: *mut c_char = std::ptr::null_mut();
    let rc = ducklink_alias_function(
        raw_conn as *mut c_void,
        c_existing.as_ptr(),
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
            let msg = format!(
                "loaded '{name}': {scalars} scalar(s), {tables} table(s), {aggregates} aggregate(s)"
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
        crate::events::emit(
            "load_community_native_ok",
            Some(name_str),
            format!("extension='{community_ext}' aliases={alias_count}"),
        );
        *out_summary = CString::new(format!(
            "installed '{name_str}' via community-extensions:{community_ext} \
             ({alias_count} alias{})",
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
            let msg = format!("installed '{name_str}' at {}", path_str);
            crate::events::emit("load_native_ok", Some(name_str), path_str);
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
    for (ours, theirs) in &pairs {
        // Try the C++ catalog-alias first — it gives real transparency:
        // aggregate DISTINCT/FILTER/ORDER BY/window all work through the
        // alias because the alias IS an AggregateFunctionCatalogEntry.
        match catalog_alias(conn, theirs, ours) {
            Ok(_kind) => {
                created += 1;
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
            let Some(macro_sql) = crate::catalog::build_alias_macro(&ftype, ours, theirs, &params)
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
