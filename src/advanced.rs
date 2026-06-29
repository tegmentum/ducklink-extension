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
}

static ADVANCED: OnceLock<Advanced> = OnceLock::new();

extern "C" {
    /// Install the component-driven ParserExtension on `db` (idempotent).
    fn ducklink_register_parser(db: *mut c_void) -> i32;
}

/// Wire a freshly loaded component's advanced-tier declarations into DuckDB.
/// Idempotent across components: the global engine handle is set once; each
/// component's parser/optimizer/filterable-table handles are appended.
///
/// `db` is the `duckdb_database` the loader handed the extension; the C++ shim
/// casts it to the internal `DatabaseInstance` to reach `DBConfig`.
pub fn register(db: ffi::duckdb_database, engine: &Arc<Mutex<Engine2>>, loaded: &LoadedComponent) {
    let adv = ADVANCED.get_or_init(|| Advanced {
        engine: engine.clone(),
        parsers: Mutex::new(Vec::new()),
    });

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
