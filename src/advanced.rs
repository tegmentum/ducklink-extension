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

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::sync::{Arc, Mutex, OnceLock};

use duckdb::ffi;

use crate::engine::{Engine2, LoadedComponent};

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
}

static ADVANCED: OnceLock<Advanced> = OnceLock::new();

fn advanced_or_init(engine: &Arc<Mutex<Engine2>>) -> &'static Advanced {
    ADVANCED.get_or_init(|| Advanced {
        engine: engine.clone(),
        parsers: Mutex::new(Vec::new()),
        optimizers: Mutex::new(Vec::new()),
    })
}

extern "C" {
    /// Install the component-driven ParserExtension on `db` (idempotent).
    fn ducklink_register_parser(db: *mut c_void) -> i32;
    /// Install the component-driven OptimizerExtension on `db` (idempotent).
    fn ducklink_register_optimizer(db: *mut c_void) -> i32;
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
