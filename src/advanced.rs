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
use std::sync::{Arc, Mutex, OnceLock};

use duckdb::ffi;

use ducklink_runtime::reg;

use crate::engine::{Engine2, LoadedComponent};
use crate::reg_duckdb::{type_code, write_ret_raw};

/// Process-global advanced-tier state. The C++ shim's bridge callbacks (invoked
/// from DuckDB's parser / optimizer / scan) reach the embedded engine and the
/// set of declared rule handles through this.
struct Advanced {
    engine: Arc<Mutex<Engine2>>,
    /// Every component-declared PARSER extension as (owning component, the
    /// component's guest dispatcher handle).
    parsers: Mutex<Vec<(String, u32)>>,
    /// Every component-declared OPTIMIZER rule as (owning component, handle).
    optimizers: Mutex<Vec<(String, u32)>>,
    /// Filterable streaming table functions, keyed by the table's callback
    /// handle: the owning component + its full declared column types (used to
    /// derive the projected schema for writing chunks).
    filterable: Mutex<HashMap<u32, (String, Vec<reg::LogicalType>)>>,
    /// Live streaming cursors, keyed by a bridge-local id (so component cursor
    /// ids can never collide across components).
    cursors: Mutex<HashMap<u32, CursorState>>,
    /// Most recent table-stream bridge error (valid until the next bridge call).
    ts_last_error: Mutex<Option<CString>>,
}

/// Per-cursor state for an open streaming scan.
struct CursorState {
    extension: String,
    /// The table function's callback handle (passed to next/close).
    handle: u32,
    /// The component-side cursor handle returned by open.
    component_cursor: u32,
    /// Type codes of the emitted (post-projection) columns, in emit order.
    col_codes: Vec<u8>,
}

static ADVANCED: OnceLock<Advanced> = OnceLock::new();
/// Bridge-local cursor id generator (0 is reserved for "open failed").
static CURSOR_SEQ: AtomicU32 = AtomicU32::new(1);

fn advanced_or_init(engine: &Arc<Mutex<Engine2>>) -> &'static Advanced {
    ADVANCED.get_or_init(|| Advanced {
        engine: engine.clone(),
        parsers: Mutex::new(Vec::new()),
        optimizers: Mutex::new(Vec::new()),
        filterable: Mutex::new(HashMap::new()),
        cursors: Mutex::new(HashMap::new()),
        ts_last_error: Mutex::new(None),
    })
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
}

/// Wire a freshly loaded component's advanced-tier declarations into DuckDB.
/// Idempotent across components: the global engine handle is set once; each
/// component's parser/optimizer/filterable-table handles are appended.
///
/// `db` is the `duckdb_database` the loader handed the extension; the C++ shim
/// casts it to the internal `DatabaseInstance` to reach `DBConfig`.
pub fn register(db: ffi::duckdb_database, engine: &Arc<Mutex<Engine2>>, loaded: &LoadedComponent) {
    let adv = advanced_or_init(engine);

    if !loaded.parsers.is_empty() {
        {
            let mut guard = adv.parsers.lock().expect("advanced parsers mutex poisoned");
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
            let mut guard = adv.optimizers.lock().expect("advanced optimizers mutex poisoned");
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
            let mut guard = adv.filterable.lock().expect("advanced filterable mutex poisoned");
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
    let adv = match ADVANCED.get() {
        Some(a) => a,
        None => return 0,
    };
    let (extension, columns) = {
        let guard = adv.filterable.lock().expect("advanced filterable mutex poisoned");
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

    let mut engine = adv.engine.lock().expect("engine mutex poisoned");
    let component_cursor = match engine.dispatch_table_open_filtered(
        &extension,
        handle,
        arg_values,
        projection,
        filter_set,
    ) {
        Ok(c) => c,
        Err(err) => {
            drop(engine);
            ts_set_last_error(format!("{err}"));
            return 0;
        }
    };
    drop(engine);

    let cursor_id = CURSOR_SEQ.fetch_add(1, Ordering::Relaxed);
    adv.cursors.lock().expect("cursors mutex poisoned").insert(
        cursor_id,
        CursorState {
            extension,
            handle,
            component_cursor,
            col_codes,
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
pub unsafe extern "C" fn ducklink_ts_fill(_handle: u32, cursor: u32, chunk: *mut c_void) -> bool {
    let adv = match ADVANCED.get() {
        Some(a) => a,
        None => return false,
    };
    let output = chunk as ffi::duckdb_data_chunk;
    if output.is_null() {
        ts_set_last_error("ducklink_ts_fill: null chunk".to_string());
        return false;
    }
    let (extension, handle, component_cursor, col_codes) = {
        let guard = adv.cursors.lock().expect("cursors mutex poisoned");
        match guard.get(&cursor) {
            Some(s) => (
                s.extension.clone(),
                s.handle,
                s.component_cursor,
                s.col_codes.clone(),
            ),
            None => {
                ts_set_last_error(format!("ducklink_ts_fill: unknown cursor {cursor}"));
                return false;
            }
        }
    };

    let rows = {
        let mut engine = adv.engine.lock().expect("engine mutex poisoned");
        match engine.dispatch_table_next(&extension, handle, component_cursor, 2048) {
            Ok(rows) => rows,
            Err(err) => {
                drop(engine);
                ts_set_last_error(format!("{err}"));
                return false;
            }
        }
    };

    if rows.is_empty() {
        ffi::duckdb_data_chunk_set_size(output, 0);
        return false;
    }

    let ncols = col_codes.len();
    ffi::duckdb_data_chunk_set_size(output, rows.len() as ffi::idx_t);
    for (col_idx, &code) in col_codes.iter().enumerate() {
        let vector = ffi::duckdb_data_chunk_get_vector(output, col_idx as ffi::idx_t);
        for (row, row_values) in rows.iter().enumerate() {
            if row_values.len() != ncols {
                ts_set_last_error(format!(
                    "ducklink_ts_fill: row {row} has {} cols, expected {ncols}",
                    row_values.len()
                ));
                return false;
            }
            if let Err(err) = write_ret_raw(code, vector, row, row_values[col_idx].clone()) {
                ts_set_last_error(err);
                return false;
            }
        }
    }
    true
}

/// Close + free a streaming cursor.
#[no_mangle]
pub extern "C" fn ducklink_ts_close(_handle: u32, cursor: u32) {
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
        let mut engine = adv.engine.lock().expect("engine mutex poisoned");
        let _ = engine.dispatch_table_close(&state.extension, state.handle, state.component_cursor);
    }
}

/// Most recent table-stream bridge error (owned by Rust; valid until the next
/// bridge call). Empty C string when none.
#[no_mangle]
pub extern "C" fn ducklink_ts_last_error() -> *const c_char {
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
            .lock()
            .expect("advanced optimizers mutex poisoned");
        if guard.is_empty() {
            return std::ptr::null_mut();
        }
        guard.clone()
    };
    let nodes = flatten_plan_json(plan);
    let mut engine = adv.engine.lock().expect("engine mutex poisoned");
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
    let adv = match ADVANCED.get() {
        Some(a) => a,
        None => return std::ptr::null_mut(),
    };
    if sql.is_null() {
        return std::ptr::null_mut();
    }
    let query = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return std::ptr::null_mut(),
    };
    let handles = {
        let guard = adv.parsers.lock().expect("advanced parsers mutex poisoned");
        if guard.is_empty() {
            return std::ptr::null_mut();
        }
        guard.clone()
    };
    let mut engine = adv.engine.lock().expect("engine mutex poisoned");
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

/// Free a C string returned by the advanced-tier bridge functions.
///
/// # Safety
/// `ptr` must be NULL or a pointer previously returned by a `ducklink_*` bridge.
#[no_mangle]
pub unsafe extern "C" fn ducklink_adv_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(CString::from_raw(ptr));
    }
}
