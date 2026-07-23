//! The Direction-2 engine: loads `duckdb:extension` WebAssembly components into
//! native DuckDB and dispatches DuckDB invocations back into them.
//!
//! This module depends ONLY on `ducklink-runtime` + wasmtime (no DuckDB), so it
//! compiles and is checkable without the DuckDB toolchain. The DuckDB C-API
//! binding that turns a [`ScalarFunc`] into a registered catalog function (and
//! routes per-row calls back to [`Engine2::dispatch_scalar`]) lives behind the
//! crate's `loadable` feature.

use std::cell::Cell;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use anyhow::{anyhow, Context, Result};
use wasmtime::component::Component;
use wasmtime::{Config, Engine};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder};

use ducklink_runtime::duckdb_extension_bindings::duckdb::extension::{
    column_types as extension_column_types, runtime as extension_runtime, types as extension_types,
};
use ducklink_runtime::reg;
use ducklink_runtime::{
    load_component, CallbackRegistry, ConfigError, ExtensionInstance, ExtensionServices, LogEntry,
    LogField, LogLevel, PendingRegistrationsData,
};

#[cfg(feature = "duckdb-api")]
use duckdb::ffi;

/// A live DuckDB connection handle the extension can call the C API against.
/// The pointer is opaque here (never dereferenced except by the DuckDB C API
/// callers below) and Copy — the connection itself is owned by whoever opened
/// it (the `loadable` init path in `src/lib.rs`, the `register_load_function`
/// persistent-connection path in `src/reg_duckdb.rs`), which must keep it
/// alive for the whole process. NativeServices only borrows the pointer.
///
/// `Send + Sync` is asserted below: the raw pointer type is neither by default,
/// but the DuckDB C API for `duckdb_client_context_get_config_option` and
/// friends is safe to call from any thread as long as the connection outlives
/// the call — which the process-wide-lifetime rule above guarantees.
#[cfg(feature = "duckdb-api")]
#[derive(Clone, Copy)]
pub struct DuckConn(pub ffi::duckdb_connection);

#[cfg(feature = "duckdb-api")]
unsafe impl Send for DuckConn {}
#[cfg(feature = "duckdb-api")]
unsafe impl Sync for DuckConn {}

thread_local! {
    /// When set, a reg_duckdb scalar/table dispatcher is currently running on
    /// this thread and holding the DuckDB executor lock. A re-entrant
    /// `NativeServices::query()` call from inside the guest (routed through
    /// this same thread) would deadlock on that lock — so we refuse instead.
    /// `reg_duckdb` sets the guard around every dispatcher body it invokes
    /// (that edit lives in the sibling reg_duckdb branch).
    static QUERY_REENTRANCY_GUARD: Cell<bool> = const { Cell::new(false) };
}

/// Set the re-entrancy guard for the current thread. Returns the previous
/// value so callers can restore it (nested dispatchers stay quiet). Called
/// by `reg_duckdb`'s scalar/table wrappers around every guest dispatch.
///
/// Public so the sibling `reg_duckdb` module can wrap dispatches with:
/// `let prev = set_query_reentrancy_guard(true); ...; set_query_reentrancy_guard(prev);`
pub fn set_query_reentrancy_guard(active: bool) -> bool {
    QUERY_REENTRANCY_GUARD.with(|c| c.replace(active))
}

/// RAII guard around [`set_query_reentrancy_guard`]. Sets the guard to
/// `true` on construction, restores the previous value on drop — safe
/// under early returns and panics, unlike a manual set/unset pair.
///
/// Dispatchers in `delegating_agg` and the sibling `reg_duckdb` scalar /
/// table / cast wrappers hold one of these across every guest call so a
/// re-entrant `NativeServices::query()` from inside the guest can refuse
/// instead of deadlocking on the DuckDB executor lock.
pub struct QueryReentrancyGuard {
    prev: bool,
}

impl QueryReentrancyGuard {
    pub fn new() -> Self {
        Self {
            prev: set_query_reentrancy_guard(true),
        }
    }
}

impl Default for QueryReentrancyGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for QueryReentrancyGuard {
    fn drop(&mut self) {
        set_query_reentrancy_guard(self.prev);
    }
}

/// Build a component-model wasmtime engine for running extension components.
/// Mirrors the host's engine config (component model + wasm exceptions, which
/// DuckDB-targeting components may use).
fn build_engine() -> Result<Engine> {
    let mut config = Config::new();
    config.wasm_component_model(true);
    config.wasm_exceptions(true);
    // K1: forbid runtime relocation of guest linear memory. When the guest's
    // memory can move, Cranelift must re-read the base pointer on every load
    // and store (it can be invalidated by `memory.grow`). With this pinned,
    // Cranelift can hoist the base pointer out of hot loops (loop-invariant
    // code motion), which matters for scalars that read a `list<colvec>` in
    // a tight per-row loop. Trade-off: memory cannot exceed the reserved
    // amount at runtime — for ducklink's use case (2048-row chunks × a
    // handful of args) the default 4GiB reservation is orders of magnitude
    // more than any component will ever use, so this is free.
    config.memory_may_move(false);
    // Cache compiled artifacts on disk. Cranelift-compiling a component dominates
    // load time; with the cache a repeated load of the same component deserializes
    // in ~ms instead of recompiling. Fall back silently if the cache is
    // unavailable (e.g. no writable home dir) — it is a pure performance hint.
    match wasmtime::Cache::from_file(None) {
        Ok(cache) => {
            config.cache(Some(cache));
        }
        Err(err) => {
            eprintln!("[ducklink] wasmtime compile cache unavailable: {err}");
        }
    }
    // wasmtime 46's `wasmtime::Error` is its own type (no longer an alias of
    // `anyhow::Error`), so map it into anyhow before attaching context.
    Engine::new(&config)
        .map_err(anyhow::Error::from)
        .context("failed to create wasmtime engine")
}

/// Config/logging/query sink for native DuckDB. Logging goes to stderr; when
/// Engine2 has been attached to a live DuckDB connection (see
/// [`Engine2::attach_duckdb_connection`]) the config getters read the real
/// DuckDB settings via the C API and `query()` runs read-only SQL against the
/// live database. Without a connection (bench harness, standalone tests) or
/// under the non-`duckdb-api` build, every getter reports `Ok(None)` and
/// `query()` returns an unavailable error — the shape the guest ecosystem
/// already tolerates.
struct NativeServices {
    /// The DuckDB connection to route config lookups and live queries against.
    /// `None` when Engine2 was never attached to a connection (bench / standalone
    /// tests) — every getter then degrades to `Ok(None)` and `query()` to
    /// `Err("live query not available in this host")`. Absent entirely on the
    /// non-`duckdb-api` build (the ffi bindings aren't linked).
    #[cfg(feature = "duckdb-api")]
    conn: Option<DuckConn>,
}

impl NativeServices {
    /// Build a services sink that will use `conn` to answer config reads and
    /// live queries. `conn == None` degrades every getter to `Ok(None)` and
    /// `query()` to `Err(...)` — the default host-services contract.
    #[cfg(feature = "duckdb-api")]
    fn new(conn: Option<DuckConn>) -> Self {
        Self { conn }
    }

    /// Build a services sink with no DuckDB C API access (non-`duckdb-api`
    /// build — bench harness / standalone tests). Every config getter returns
    /// `Ok(None)` and `query()` returns the unavailable error.
    #[cfg(not(feature = "duckdb-api"))]
    fn new() -> Self {
        Self {}
    }
}

/// Grab a scoped `duckdb_client_context` from `conn`, run `f` with it, then
/// destroy it. `f`'s return value is passed through. Returns `None` if the
/// client-context handle came back null (never seen in practice, but guarded
/// against so a fresh DuckDB build that changes the semantics won't segfault).
#[cfg(feature = "duckdb-api")]
unsafe fn with_client_context<T, F>(conn: DuckConn, f: F) -> Option<T>
where
    F: FnOnce(ffi::duckdb_client_context) -> T,
{
    let mut ctx: ffi::duckdb_client_context = std::ptr::null_mut();
    ffi::duckdb_connection_get_client_context(conn.0, &mut ctx);
    if ctx.is_null() {
        return None;
    }
    let out = f(ctx);
    ffi::duckdb_destroy_client_context(&mut ctx);
    Some(out)
}

/// Fetch the DuckDB config option `path` as a `duckdb_value` object. Returns
/// `None` when the option is not registered; callers must destroy the returned
/// value with `duckdb_destroy_value`. The client-context handle is scoped to
/// this call so we do not leak it into the caller.
///
/// Errors only on genuine FFI failures (a `path` containing an interior NUL);
/// absence of the option is `Ok(None)`.
#[cfg(feature = "duckdb-api")]
unsafe fn fetch_config_value(
    conn: DuckConn,
    path: &str,
) -> Result<Option<ffi::duckdb_value>, ConfigError> {
    let cname = std::ffi::CString::new(path)
        .map_err(|_| ConfigError::InvalidKey(format!("path '{path}' contains NUL byte")))?;
    let mut scope: ffi::duckdb_config_option_scope = 0;
    let raw = with_client_context(conn, |ctx| {
        ffi::duckdb_client_context_get_config_option(ctx, cname.as_ptr(), &mut scope)
    });
    match raw {
        Some(v) if !v.is_null() => Ok(Some(v)),
        _ => Ok(None),
    }
}

/// Coerce a returned config `duckdb_value` into a UTF-8 string using
/// `duckdb_get_varchar` (which stringifies any DuckDB type). The value is
/// destroyed after reading. Returns `None` if the C-side returned null or the
/// bytes are not valid UTF-8.
#[cfg(feature = "duckdb-api")]
unsafe fn value_to_string(mut value: ffi::duckdb_value) -> Option<String> {
    let cstr = ffi::duckdb_get_varchar(value);
    let out = if cstr.is_null() {
        None
    } else {
        let s = std::ffi::CStr::from_ptr(cstr).to_str().ok().map(|s| s.to_string());
        ffi::duckdb_free(cstr.cast());
        s
    };
    ffi::duckdb_destroy_value(&mut value);
    out
}

impl ExtensionServices for NativeServices {
    fn provider_version(&mut self) -> Result<String, ConfigError> {
        Ok(concat!("ducklink-extension/", env!("CARGO_PKG_VERSION")).to_string())
    }

    fn list_keys(&mut self, prefix: Option<&str>) -> Result<Vec<String>, ConfigError> {
        #[cfg(feature = "duckdb-api")]
        {
            // The catalog of DuckDB config flags is process-wide (not tied to a
            // connection), so this works even when `self.conn` is None.
            let count = unsafe { ffi::duckdb_config_count() };
            let mut out = Vec::with_capacity(count as usize);
            for i in 0..count {
                let mut name_ptr: *const std::os::raw::c_char = std::ptr::null();
                let mut desc_ptr: *const std::os::raw::c_char = std::ptr::null();
                let state = unsafe {
                    ffi::duckdb_get_config_flag(i, &mut name_ptr, &mut desc_ptr)
                };
                if state != ffi::DuckDBSuccess || name_ptr.is_null() {
                    continue;
                }
                let name = match unsafe { std::ffi::CStr::from_ptr(name_ptr) }.to_str() {
                    Ok(s) => s.to_string(),
                    Err(_) => continue,
                };
                if let Some(p) = prefix {
                    if !name.starts_with(p) {
                        continue;
                    }
                }
                out.push(name);
            }
            Ok(out)
        }
        #[cfg(not(feature = "duckdb-api"))]
        {
            let _ = prefix;
            Ok(Vec::new())
        }
    }

    fn get_string(&mut self, path: &str) -> Result<Option<String>, ConfigError> {
        #[cfg(feature = "duckdb-api")]
        {
            let Some(conn) = self.conn else { return Ok(None) };
            let Some(value) = (unsafe { fetch_config_value(conn, path)? }) else {
                return Ok(None);
            };
            Ok(unsafe { value_to_string(value) })
        }
        #[cfg(not(feature = "duckdb-api"))]
        {
            let _ = path;
            Ok(None)
        }
    }

    fn get_bool(&mut self, path: &str) -> Result<Option<bool>, ConfigError> {
        #[cfg(feature = "duckdb-api")]
        {
            let Some(conn) = self.conn else { return Ok(None) };
            let Some(mut value) = (unsafe { fetch_config_value(conn, path)? }) else {
                return Ok(None);
            };
            // `duckdb_get_value_type` returns a borrowed logical type owned
            // by the value — DuckDB explicitly documents that we must NOT
            // destroy it. Read the type id and drop the reference.
            let ty = unsafe { ffi::duckdb_get_value_type(value) };
            let type_id = if ty.is_null() {
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_INVALID
            } else {
                unsafe { ffi::duckdb_get_type_id(ty) }
            };
            let out = if type_id == ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN {
                Some(unsafe { ffi::duckdb_get_bool(value) })
            } else {
                // Fall back to a stringified reading of the value so callers
                // don't get spurious `None` for an option that DuckDB stores
                // as VARCHAR ("true"/"false"). Type mismatches (e.g. a
                // numeric config option) collapse to `None`.
                unsafe { ffi::duckdb_destroy_value(&mut value) };
                let Some(s) = self.get_string(path)? else { return Ok(None) };
                return Ok(match s.to_ascii_lowercase().as_str() {
                    "true" | "1" | "on" | "yes" => Some(true),
                    "false" | "0" | "off" | "no" => Some(false),
                    _ => None,
                });
            };
            unsafe { ffi::duckdb_destroy_value(&mut value) };
            Ok(out)
        }
        #[cfg(not(feature = "duckdb-api"))]
        {
            let _ = path;
            Ok(None)
        }
    }

    fn get_i64(&mut self, path: &str) -> Result<Option<i64>, ConfigError> {
        #[cfg(feature = "duckdb-api")]
        {
            let Some(conn) = self.conn else { return Ok(None) };
            let Some(mut value) = (unsafe { fetch_config_value(conn, path)? }) else {
                return Ok(None);
            };
            // `duckdb_get_value_type` returns a borrowed logical type owned
            // by the value — DuckDB explicitly documents that we must NOT
            // destroy it. Read the type id and drop the reference.
            let ty = unsafe { ffi::duckdb_get_value_type(value) };
            let type_id = if ty.is_null() {
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_INVALID
            } else {
                unsafe { ffi::duckdb_get_type_id(ty) }
            };
            let out = match type_id {
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => Some(unsafe { ffi::duckdb_get_int64(value) }),
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => {
                    Some(unsafe { ffi::duckdb_get_int32(value) } as i64)
                }
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => {
                    let u = unsafe { ffi::duckdb_get_uint64(value) };
                    if u <= i64::MAX as u64 { Some(u as i64) } else { None }
                }
                _ => {
                    unsafe { ffi::duckdb_destroy_value(&mut value) };
                    let Some(s) = self.get_string(path)? else { return Ok(None) };
                    return Ok(s.parse::<i64>().ok());
                }
            };
            unsafe { ffi::duckdb_destroy_value(&mut value) };
            Ok(out)
        }
        #[cfg(not(feature = "duckdb-api"))]
        {
            let _ = path;
            Ok(None)
        }
    }

    fn get_u64(&mut self, path: &str) -> Result<Option<u64>, ConfigError> {
        #[cfg(feature = "duckdb-api")]
        {
            let Some(conn) = self.conn else { return Ok(None) };
            let Some(mut value) = (unsafe { fetch_config_value(conn, path)? }) else {
                return Ok(None);
            };
            // `duckdb_get_value_type` returns a borrowed logical type owned
            // by the value — DuckDB explicitly documents that we must NOT
            // destroy it. Read the type id and drop the reference.
            let ty = unsafe { ffi::duckdb_get_value_type(value) };
            let type_id = if ty.is_null() {
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_INVALID
            } else {
                unsafe { ffi::duckdb_get_type_id(ty) }
            };
            let out = match type_id {
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => Some(unsafe { ffi::duckdb_get_uint64(value) }),
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => {
                    let i = unsafe { ffi::duckdb_get_int64(value) };
                    if i >= 0 { Some(i as u64) } else { None }
                }
                _ => {
                    unsafe { ffi::duckdb_destroy_value(&mut value) };
                    let Some(s) = self.get_string(path)? else { return Ok(None) };
                    return Ok(s.parse::<u64>().ok());
                }
            };
            unsafe { ffi::duckdb_destroy_value(&mut value) };
            Ok(out)
        }
        #[cfg(not(feature = "duckdb-api"))]
        {
            let _ = path;
            Ok(None)
        }
    }

    fn get_f64(&mut self, path: &str) -> Result<Option<f64>, ConfigError> {
        #[cfg(feature = "duckdb-api")]
        {
            let Some(conn) = self.conn else { return Ok(None) };
            let Some(mut value) = (unsafe { fetch_config_value(conn, path)? }) else {
                return Ok(None);
            };
            // `duckdb_get_value_type` returns a borrowed logical type owned
            // by the value — DuckDB explicitly documents that we must NOT
            // destroy it. Read the type id and drop the reference.
            let ty = unsafe { ffi::duckdb_get_value_type(value) };
            let type_id = if ty.is_null() {
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_INVALID
            } else {
                unsafe { ffi::duckdb_get_type_id(ty) }
            };
            let out = match type_id {
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => Some(unsafe { ffi::duckdb_get_double(value) }),
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => {
                    Some(unsafe { ffi::duckdb_get_int64(value) } as f64)
                }
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => {
                    Some(unsafe { ffi::duckdb_get_uint64(value) } as f64)
                }
                _ => {
                    unsafe { ffi::duckdb_destroy_value(&mut value) };
                    let Some(s) = self.get_string(path)? else { return Ok(None) };
                    return Ok(s.parse::<f64>().ok());
                }
            };
            unsafe { ffi::duckdb_destroy_value(&mut value) };
            Ok(out)
        }
        #[cfg(not(feature = "duckdb-api"))]
        {
            let _ = path;
            Ok(None)
        }
    }

    fn get_bytes(&mut self, _path: &str) -> Result<Option<Vec<u8>>, ConfigError> {
        // DuckDB does not model BLOB-typed config options in the settings
        // catalog, so a get_bytes lookup would never resolve. Stay `Ok(None)`
        // rather than round-tripping through get_varchar, which would return
        // the utf-8 representation of a stringy value.
        Ok(None)
    }

    fn get_string_list(&mut self, path: &str) -> Result<Option<Vec<String>>, ConfigError> {
        // DuckDB's settings catalog exposes list-shaped options as
        // comma-separated VARCHAR (e.g. `allowed_paths`, `custom_extension_repository`
        // remain scalar today; no LIST-typed setting exists). If a caller asks
        // for a string-list-shaped option, we resolve the underlying string and
        // split on ','; if the option is absent we return `Ok(None)`.
        let Some(s) = self.get_string(path)? else { return Ok(None) };
        if s.is_empty() {
            return Ok(Some(Vec::new()));
        }
        Ok(Some(s.split(',').map(|piece| piece.trim().to_string()).collect()))
    }

    fn log(&mut self, level: LogLevel, message: &str, target: Option<&str>) {
        match target {
            Some(t) => eprintln!("[ducklink:{level:?}:{t}] {message}"),
            None => eprintln!("[ducklink:{level:?}] {message}"),
        }
    }
    fn log_fields(&mut self, level: LogLevel, message: &str, fields: &[LogField]) {
        let rendered: Vec<String> = fields
            .iter()
            .map(|f| format!("{}={}", f.key, f.value))
            .collect();
        eprintln!("[ducklink:{level:?}] {message} {{{}}}", rendered.join(", "));
    }

    fn query(&mut self, sql: &str) -> Result<Vec<Vec<String>>, String> {
        // Re-entrancy: a guest scalar/table dispatcher running on this thread
        // already holds the DuckDB executor lock. Calling `duckdb_query` from
        // inside would deadlock — reject cleanly so the guest can degrade.
        if QUERY_REENTRANCY_GUARD.with(|c| c.get()) {
            return Err(
                "query() called re-entrantly from inside a dispatch — not permitted".to_string(),
            );
        }
        #[cfg(feature = "duckdb-api")]
        {
            let Some(conn) = self.conn else {
                return Err("live query not available in this host".to_string());
            };
            let csql = std::ffi::CString::new(sql)
                .map_err(|_| "sql contains a NUL byte".to_string())?;
            let mut result: ffi::duckdb_result = unsafe { std::mem::zeroed() };
            let state = unsafe { ffi::duckdb_query(conn.0, csql.as_ptr(), &mut result) };
            if state != ffi::DuckDBSuccess {
                let err_ptr = unsafe { ffi::duckdb_result_error(&mut result) };
                let msg = if err_ptr.is_null() {
                    "query failed".to_string()
                } else {
                    unsafe { std::ffi::CStr::from_ptr(err_ptr) }
                        .to_string_lossy()
                        .into_owned()
                };
                unsafe { ffi::duckdb_destroy_result(&mut result) };
                return Err(msg);
            }
            let cols = unsafe { ffi::duckdb_column_count(&mut result) };
            let rows = unsafe { ffi::duckdb_row_count(&mut result) };
            let mut out: Vec<Vec<String>> = Vec::with_capacity(rows as usize);
            for r in 0..rows {
                let mut row: Vec<String> = Vec::with_capacity(cols as usize);
                for c in 0..cols {
                    if unsafe { ffi::duckdb_value_is_null(&mut result, c, r) } {
                        row.push(String::new());
                        continue;
                    }
                    let cstr = unsafe { ffi::duckdb_value_varchar(&mut result, c, r) };
                    if cstr.is_null() {
                        row.push(String::new());
                    } else {
                        let s = unsafe { std::ffi::CStr::from_ptr(cstr) }
                            .to_string_lossy()
                            .into_owned();
                        unsafe { ffi::duckdb_free(cstr.cast()) };
                        row.push(s);
                    }
                }
                out.push(row);
            }
            unsafe { ffi::duckdb_destroy_result(&mut result) };
            Ok(out)
        }
        #[cfg(not(feature = "duckdb-api"))]
        {
            let _ = sql;
            Err("live query not available in this host".to_string())
        }
    }
}

/// A scalar function a loaded component registered, ready to bridge into
/// DuckDB's catalog. `callback_handle` routes back through the engine's callback
/// registry to the owning component on each invocation.
#[derive(Clone, Debug)]
pub struct ScalarFunc {
    pub extension: String,
    pub name: String,
    pub arguments: Vec<reg::FuncArg>,
    pub returns: reg::LogicalType,
    pub callback_handle: u32,
}

/// A table function a loaded component registered. `arguments` are the call
/// parameters; `columns` are the result schema; `callback_handle` dispatches.
#[derive(Clone, Debug)]
pub struct TableFunc {
    pub extension: String,
    pub name: String,
    pub arguments: Vec<reg::FuncArg>,
    pub columns: Vec<reg::ColumnDef>,
    pub callback_handle: u32,
}

/// An aggregate function a loaded component registered. `arguments` are the
/// input columns; the component computes over all rows at finalize.
#[derive(Clone, Debug)]
pub struct AggregateFunc {
    pub extension: String,
    pub name: String,
    pub arguments: Vec<reg::FuncArg>,
    pub returns: reg::LogicalType,
    pub callback_handle: u32,
}

/// A DuckDB replacement scan the component asked the host to wire.
/// `extensions` are lower-case file extensions (no dot, e.g. `"gb"`);
/// `function_name` is the target table function's registered name.
///
/// Consumed by `reg_duckdb::register_replacement_scans`, which calls
/// `duckdb_add_replacement_scan` on the connection's database and installs
/// a callback that rewrites `FROM 'x.<ext>'` to `FROM <fn>('x.<ext>')`.
#[derive(Clone, Debug)]
pub struct ReplacementScan {
    pub extension: String,
    pub extensions: Vec<String>,
    pub function_name: String,
}

/// A configuration option a component declared via
/// `runtime.register-setting`. The DuckDB sink is expected to install it as
/// a DB config option so `SET <name>=<value>` reaches the core catalog.
/// `ty` is one of "boolean"/"varchar"/"bigint"/"double"; `scope` is
/// "local" or "global". Consumed by `reg_duckdb` in a later phase.
#[derive(Clone, Debug)]
pub struct Setting {
    pub extension: String,
    pub name: String,
    pub description: String,
    pub ty: String,
    pub default_value: Option<String>,
    pub scope: String,
}

/// A COPY handler the component registered (e.g. `COPY ... TO 'x.parquet'`).
/// `file_extension` is the lower-case extension (no dot) that routes to the
/// already-registered scalar `function_handle`; `function_handle` resolves
/// through the callback registry on every `copy-dispatch` call.
#[derive(Clone, Debug)]
pub struct CopyHandler {
    pub extension: String,
    pub file_extension: String,
    pub function_handle: u32,
}

/// An Arrow-table producer the component registered. `columns` is the result
/// schema; `callback_handle` routes the host's pull calls through the
/// callback registry back to the owning component.
#[derive(Clone, Debug)]
pub struct ArrowTable {
    pub extension: String,
    pub name: String,
    pub columns: Vec<reg::ColumnDef>,
    pub callback_handle: u32,
}

/// A richer scalar the component registered via
/// `runtime-ext.register-scalar-ex`: carries varargs and a NULL-handling mode
/// that plain [`ScalarFunc`] cannot express. `varargs` is the declared
/// trailing repeatable type (`None` = fixed arity); `special_null` = true
/// means the guest is invoked even on NULL inputs (otherwise DuckDB
/// short-circuits to NULL).
#[derive(Clone, Debug)]
pub struct ScalarEx {
    pub extension: String,
    pub name: String,
    pub arguments: Vec<reg::FuncArg>,
    pub varargs: Option<reg::LogicalType>,
    pub returns: reg::LogicalType,
    pub special_null: bool,
    /// Drives whether the direction-specific sink calls
    /// `duckdb_scalar_function_set_volatile`. Populated by the runtime from the
    /// register-scalar-ex attributes; non-volatile is the default (see
    /// `ScalarExReg::volatile`).
    pub volatile: bool,
    pub callback_handle: u32,
}

/// A named cast between two DuckDB types the component registered.
/// `callback_handle` routes every cast call back through the callback
/// registry to the owning component's dispatcher.
///
/// `implicit_cost` (T2-4) carries the DuckDB implicit-conversion cost knob
/// through from the WIT `cast-spec` — `None` = use DuckDB's default (100);
/// `Some(-1)` = explicit-only (parity with the C API convention); any other
/// `Some(v)` = a positive cost. The reg_duckdb consolidator lands this via
/// `duckdb_cast_function_set_implicit_cost` at native-registration time.
#[derive(Clone, Debug)]
pub struct CastEntry {
    pub extension: String,
    pub source: String,
    pub target: String,
    pub callback_handle: u32,
    pub implicit_cost: Option<i32>,
}

/// A user-defined logical type alias the component registered.
/// `name` is the new type; `physical` is the underlying DuckDB type
/// expression (e.g. `"BIGINT"`).
#[derive(Clone, Debug)]
pub struct LogicalTypeEntry {
    pub extension: String,
    pub name: String,
    pub physical: String,
}

/// A SQL macro the component registered (usable in the SELECT clause).
/// `parameters` are positional names; `definition_sql` is the body expression.
#[derive(Clone, Debug)]
pub struct MacroEntry {
    pub extension: String,
    pub schema: String,
    pub name: String,
    pub parameters: Vec<String>,
    pub definition_sql: String,
}

/// A SQL table macro the component registered (usable in the FROM clause).
/// `parameters` are positional names; `body_sql` is the relational body.
#[derive(Clone, Debug)]
pub struct TableMacroEntry {
    pub extension: String,
    pub schema: String,
    pub name: String,
    pub parameters: Vec<String>,
    pub body_sql: String,
}

/// An ENUM type the component registered. `members` is the ordered list of
/// enum member names.
#[derive(Clone, Debug)]
pub struct EnumTypeEntry {
    pub extension: String,
    pub name: String,
    pub members: Vec<String>,
}

/// A logical type registered over a full type-expression (e.g.
/// `DECIMAL(18,3)`). Rides the existing type-expression escape hatch, so
/// the runtime never invents a new WIT arm.
#[derive(Clone, Debug)]
pub struct ModifiedTypeEntry {
    pub extension: String,
    pub name: String,
    pub type_expr: String,
}

/// A storage / catalog backend the component registered. Keyed by an ATTACH
/// `type_name` (e.g. `"sqlite"`); `callback_handle` routes every
/// `storage-dispatch` call back through the callback registry to the owning
/// component.
#[derive(Clone, Debug)]
pub struct StorageEntry {
    pub extension: String,
    pub type_name: String,
    pub callback_handle: u32,
}

/// A log-storage sink the component registered. `callback_handle` routes
/// every log-storage callback back to the owning component. `extension` is
/// materialised from the outer load-time extension name for parity with the
/// sibling entries (the runtime's `PendingLogStorage` doesn't carry it
/// because a log storage is scoped to the loading component by construction).
#[derive(Clone, Debug)]
pub struct LogStorageEntry {
    pub extension: String,
    pub name: String,
    pub callback_handle: u32,
}

/// A `PRAGMA <name>(...)` extension the component registered via
/// `runtime.register-pragma`. The core intercepts the pragma, dispatches
/// through `callback_handle` (callback-dispatch.call-pragma), and the
/// component RETURNS a SQL script for the core to run on the connection —
/// so no mid-callback re-entry into the connection. Consumed by
/// `reg_duckdb` in a later phase.
#[derive(Clone, Debug)]
pub struct PragmaEntry {
    pub extension: String,
    pub name: String,
    pub callback_handle: u32,
}

/// A coordinate reference system (CRS) the component registered via
/// `runtime.register-coordinate-system` (2.2.0, Item 7). Fields mirror
/// [`crate::reg::CoordinateSystemReg`] / `PendingCoordinateSystem` so the
/// drain path is a straight field-by-field map; consumed by `reg_duckdb`
/// in a later phase.
#[derive(Clone, Debug)]
pub struct CoordinateSystemEntry {
    pub extension: String,
    pub auth_name: String,
    pub code: u32,
    pub wkt: String,
}

/// What a component registered: the functions a direction-specific sink bridges
/// into the database.
#[derive(Clone, Debug, Default)]
pub struct LoadedComponent {
    pub scalars: Vec<ScalarFunc>,
    pub tables: Vec<TableFunc>,
    pub aggregates: Vec<AggregateFunc>,
    /// File-extension → registered-table-function name mappings from the
    /// component's `files::register_replacement_scan` calls. Drained from
    /// the runtime's pending state alongside scalars/tables/aggregates;
    /// consumed by `reg_duckdb::register_replacement_scans`.
    pub replacement_scans: Vec<ReplacementScan>,
    /// Casts the component registered via `runtime.register-cast`. Drained
    /// from the runtime's `pending.casts`; previously silently dropped.
    pub casts: Vec<CastEntry>,
    /// SQL macros the component registered via `runtime.register-macro`.
    /// Drained from the runtime's `pending.macros`; previously silently
    /// dropped.
    pub macros: Vec<MacroEntry>,
    /// User-defined logical type aliases from `runtime.register-logical-type`.
    /// Drained from `pending.logical_types`; previously silently dropped.
    pub logical_types: Vec<LogicalTypeEntry>,
    /// Storage / catalog backends from `runtime.register-storage`. Drained
    /// from `pending.storages`; previously silently dropped.
    pub storages: Vec<StorageEntry>,
    /// Configuration options the component declared via
    /// `runtime.register-setting`. Drained from `pending.settings`.
    pub settings: Vec<Setting>,
    /// COPY handlers from `runtime.register-copy-handler`, keyed by file
    /// extension. Drained from `pending.copy_handlers`.
    pub copy_handlers: Vec<CopyHandler>,
    /// Arrow-table producers from `runtime.register-arrow-table`. Drained
    /// from `pending.arrow_tables`.
    pub arrow_tables: Vec<ArrowTable>,
    /// Rich scalars from `runtime-ext.register-scalar-ex` (varargs +
    /// NULL-handling). Drained from `pending.scalar_ex`.
    pub scalar_ex: Vec<ScalarEx>,
    /// SQL table macros from `runtime.register-table-macro`. Drained from
    /// `pending.table_macros`.
    pub table_macros: Vec<TableMacroEntry>,
    /// ENUM types from `runtime.register-enum-type`. Drained from
    /// `pending.enum_types`.
    pub enum_types: Vec<EnumTypeEntry>,
    /// Types registered over a full type-expression (e.g. DECIMAL(18,3)).
    /// Drained from `pending.modified_types`.
    pub modified_types: Vec<ModifiedTypeEntry>,
    /// Log-storage sinks the component registered. Drained from
    /// `pending.log_storages`.
    pub log_storages: Vec<LogStorageEntry>,
    /// PRAGMAs the component registered via `runtime.register-pragma`.
    /// Drained from `pending.pragmas` (the parallel runtime edit adds
    /// that field to `PendingRegistrationsData`).
    pub pragmas: Vec<PragmaEntry>,
    /// Coordinate reference systems the component registered via
    /// `runtime.register-coordinate-system`. Drained from
    /// `pending.coordinate_systems` (the parallel runtime edit adds that
    /// field to `PendingRegistrationsData`); mirrors the pragmas prep.
    pub coordinate_systems: Vec<CoordinateSystemEntry>,
    /// Component-provided documentation parsed from the wasm's `duckdb.docs`
    /// custom section, if present. Overrides catalog docs field-by-field at
    /// query time; `None` for components that don't ship a section.
    pub docs: Option<crate::docs_section::ComponentDocs>,
}

/// Process-wide Direction-2 engine: loads components and dispatches DuckDB
/// invocations into them. A DuckDB extension holds one of these.
///
/// Interior-mutable so callers hold `Arc<Engine2>` (not `Arc<Engine2>`)
/// and every scalar/table/aggregate dispatch takes only the OWNING instance's
/// mutex — not one process-wide lock across the whole extension. Two DuckDB
/// worker threads invoking scalar functions on DIFFERENT components run in
/// parallel; two threads invoking the SAME component still serialize on the
/// instance's wasmtime store, which the store's `!Sync` guarantee mandates.
pub struct Engine2 {
    engine: Engine,
    callbacks: Arc<RwLock<CallbackRegistry>>,
    instances: RwLock<HashMap<String, Arc<Mutex<ExtensionInstance>>>>,
    /// Live DuckDB connection handle for the `NativeServices` config/query
    /// sink. Populated by [`Engine2::attach_duckdb_connection`] after the
    /// extension init opens its persistent connection; used by every
    /// subsequent [`Engine2::load`] to hand the guest a working config /
    /// live-query surface. `None` under the non-`duckdb-api` build (the ffi
    /// bindings aren't linked) and before the first attach.
    #[cfg(feature = "duckdb-api")]
    duckdb_conn: RwLock<Option<DuckConn>>,
}

impl Engine2 {
    pub fn new() -> Result<Self> {
        // Kick off the catalog HTTP fetch in the background so it overlaps
        // with the rest of the extension's cold-start work (wasmtime engine
        // setup below, DuckDB extension registration in loadable.rs). By the
        // time the user's first `SELECT * FROM ducklink.modules` arrives the
        // OnceLock is usually already populated. Best-effort — see
        // `catalog::prewarm_catalog` doc for the race semantics.
        #[cfg(feature = "duckdb-api")]
        crate::catalog::prewarm_catalog();
        Ok(Self {
            engine: build_engine()?,
            callbacks: Arc::new(RwLock::new(CallbackRegistry::new())),
            instances: RwLock::new(HashMap::new()),
            #[cfg(feature = "duckdb-api")]
            duckdb_conn: RwLock::new(None),
        })
    }

    /// Attach a live DuckDB connection to this engine so subsequently-loaded
    /// components can read real config values and run live queries through
    /// `NativeServices`. The connection is opaque here — the caller (the
    /// loadable-extension init in `src/lib.rs`, the `register_load_function`
    /// path in `src/reg_duckdb.rs`) is responsible for keeping it alive for
    /// the process. Idempotent: the last attach wins.
    #[cfg(feature = "duckdb-api")]
    pub fn attach_duckdb_connection(&self, conn: ffi::duckdb_connection) {
        let mut slot = self.duckdb_conn.write().expect("duckdb_conn lock poisoned");
        *slot = if conn.is_null() { None } else { Some(DuckConn(conn)) };
    }

    /// Snapshot the currently-attached connection for a `NativeServices`
    /// under construction. `None` before any attach or on non-`duckdb-api`
    /// builds — the sink degrades to `Ok(None)` getters + `Err(...)` query.
    #[cfg(feature = "duckdb-api")]
    fn duckdb_conn_snapshot(&self) -> Option<DuckConn> {
        *self.duckdb_conn.read().expect("duckdb_conn lock poisoned")
    }

    /// Resolve `extension` to its shared `Arc<Mutex<Instance>>`. Takes a brief
    /// read on the instances map, clones the Arc, drops the read — so the
    /// dispatcher can then lock ONLY that instance's mutex, in isolation from
    /// every other loaded extension. `Err` if the extension isn't loaded.
    fn instance_arc(&self, extension: &str) -> Result<Arc<Mutex<ExtensionInstance>>> {
        let map = self.instances.read().expect("instances lock poisoned");
        map.get(extension)
            .cloned()
            .ok_or_else(|| anyhow!("extension '{extension}' is not loaded"))
    }


    /// Load a `duckdb:extension` component, run its `load()`, and return the
    /// functions it registered. The instance is retained for dispatch.
    pub fn load(&self, extension: &str, path: &Path) -> Result<LoadedComponent> {
        // J3: read the wasm bytes ONCE and share them between the wasmtime
        // compile path and the `duckdb.docs` custom-section scanner.
        // Previously each did its own `std::fs::read` of the same file —
        // ~10ms per load on a 20MB component doing redundant disk I/O.
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading component at {}", path.display()))?;
        let component = Component::from_binary(&self.engine, &bytes)
            .map_err(anyhow::Error::from)
            .with_context(|| format!("loading component at {}", path.display()))?;
        // Parse the optional `duckdb.docs` custom section from the same
        // buffer. Non-fatal: an absent / malformed section returns `None`
        // (verbose-only diagnostic). The parsed docs are cached on the
        // returned `LoadedComponent` so the DuckDB sink can merge them into
        // `ducklink.docs`.
        let docs = crate::docs_section::parse_docs_from_bytes(
            &bytes,
            &path.display().to_string(),
        );
        // Bytes have served their two purposes; drop before crossing into
        // wasmtime's instantiation path.
        drop(bytes);
        // Grant outbound network + name lookup so network-using components (dns,
        // http, httpfs, ...) work. Best-effort, not a sandbox: a component that
        // does not use sockets is unaffected. (A future opt-in gate could mirror
        // the host's DUCKLINK_NETWORK_GRANT.)
        //
        // Also grant filesystem access — preopen `/` at `/` with full perms.
        // The DuckDB process the extension is loaded into already has native
        // OS-level filesystem access; a wasm extension being MORE restricted
        // than a native `.duckdb_extension` is a footgun (component authors
        // reach for `read_text(...)` workarounds), not a security feature.
        // Matches the pattern in `ducklink-host` (crates/ducklink-host/src/lib.rs:6053).
        // A future opt-in gate could mirror the network story: read
        // `DUCKLINK_FS_GRANT=<host>::<guest>[:...]` and preopen only those.
        let mut builder = WasiCtxBuilder::new();
        builder
            .inherit_env()
            .inherit_stdio()
            .inherit_network()
            .allow_ip_name_lookup(true);
        // The preopen call can fail (e.g. `/` doesn't exist? — never on unix,
        // but conceivable in an unusual test env). If it does, we log and
        // continue without fs access — matches the pattern for the network
        // grant, which is also best-effort.
        if let Err(err) = builder.preopened_dir("/", "/", DirPerms::all(), FilePerms::all()) {
            eprintln!(
                "[ducklink] warning: could not preopen '/' for component '{extension}': {err}; \
                 std::fs::* from inside the component will fail — use DuckDB `read_text(...)` \
                 instead"
            );
        }
        let wasi: WasiCtx = builder.build();
        // Route the guest's config getters + `query()` at the live DuckDB
        // connection Engine2 was attached to (`Ok(None)` / unavailable when
        // no connection has been attached yet — the shape the guest already
        // tolerates).
        #[cfg(feature = "duckdb-api")]
        let services: Box<dyn ExtensionServices> =
            Box::new(NativeServices::new(self.duckdb_conn_snapshot()));
        #[cfg(not(feature = "duckdb-api"))]
        let services: Box<dyn ExtensionServices> = Box::new(NativeServices::new());
        let mut instance = load_component(
            &self.engine,
            &component,
            wasi,
            services,
            self.callbacks.clone(),
            extension.to_string(),
        )?;
        let pending: PendingRegistrationsData = instance.drain_pending();
        let scalars = pending
            .scalars
            .into_iter()
            .map(|s| ScalarFunc {
                extension: s.extension,
                name: s.name,
                arguments: s.arguments,
                returns: s.returns,
                callback_handle: s.callback_handle,
            })
            .collect();
        let tables = pending
            .tables
            .into_iter()
            .map(|t| TableFunc {
                extension: t.extension,
                name: t.name,
                arguments: t.arguments,
                columns: t.columns,
                callback_handle: t.callback_handle,
            })
            .collect();
        let aggregates = pending
            .aggregates
            .into_iter()
            .map(|a| AggregateFunc {
                extension: a.extension,
                name: a.name,
                arguments: a.arguments,
                returns: a.returns,
                callback_handle: a.callback_handle,
            })
            .collect();
        let replacement_scans = pending
            .replacement_scans
            .into_iter()
            .map(|r| ReplacementScan {
                extension: r.extension,
                extensions: r.extensions,
                function_name: r.function_name,
            })
            .collect();
        // Additive drains (Phase: drain-plumbing). Every field on the
        // runtime's `PendingRegistrationsData` that Engine2::load was
        // previously discarding — including the four (casts, macros,
        // logical_types, storages) that were already surfaced by the
        // runtime but silently dropped here — is materialised now so
        // reg_duckdb.rs can consume them in the next phase.
        let casts = pending
            .casts
            .into_iter()
            .map(|c| CastEntry {
                extension: c.extension,
                source: c.source,
                target: c.target,
                callback_handle: c.callback_handle,
                // T2-4: thread the WIT-supplied implicit-conversion cost through
                // to LoadedComponent.casts so reg_duckdb's consolidator can call
                // `duckdb_cast_function_set_implicit_cost` (default 100 if None).
                implicit_cost: c.implicit_cost,
            })
            .collect();
        let macros = pending
            .macros
            .into_iter()
            .map(|m| MacroEntry {
                extension: m.extension,
                schema: m.schema,
                name: m.name,
                parameters: m.parameters,
                definition_sql: m.definition_sql,
            })
            .collect();
        let logical_types = pending
            .logical_types
            .into_iter()
            .map(|l| LogicalTypeEntry {
                extension: l.extension,
                name: l.name,
                physical: l.physical,
            })
            .collect();
        let storages = pending
            .storages
            .into_iter()
            .map(|s| StorageEntry {
                extension: s.extension,
                type_name: s.type_name,
                callback_handle: s.callback_handle,
            })
            .collect();
        let settings = pending
            .settings
            .into_iter()
            .map(|s| Setting {
                extension: s.extension,
                name: s.name,
                description: s.description,
                ty: s.ty,
                default_value: s.default_value,
                scope: s.scope,
            })
            .collect();
        let copy_handlers = pending
            .copy_handlers
            .into_iter()
            .map(|c| CopyHandler {
                extension: c.extension,
                file_extension: c.file_extension,
                function_handle: c.function_handle,
            })
            .collect();
        let arrow_tables = pending
            .arrow_tables
            .into_iter()
            .map(|a| ArrowTable {
                extension: a.extension,
                name: a.name,
                columns: a.columns,
                callback_handle: a.callback_handle,
            })
            .collect();
        let scalar_ex = pending
            .scalar_ex
            .into_iter()
            .map(|s| ScalarEx {
                extension: s.extension,
                name: s.name,
                arguments: s.arguments,
                varargs: s.varargs,
                returns: s.returns,
                special_null: s.special_null,
                volatile: s.volatile,
                callback_handle: s.callback_handle,
            })
            .collect();
        let table_macros = pending
            .table_macros
            .into_iter()
            .map(|t| TableMacroEntry {
                extension: t.extension,
                schema: t.schema,
                name: t.name,
                parameters: t.parameters,
                body_sql: t.body_sql,
            })
            .collect();
        let enum_types = pending
            .enum_types
            .into_iter()
            .map(|e| EnumTypeEntry {
                extension: e.extension,
                name: e.name,
                members: e.members,
            })
            .collect();
        let modified_types = pending
            .modified_types
            .into_iter()
            .map(|m| ModifiedTypeEntry {
                extension: m.extension,
                name: m.name,
                type_expr: m.type_expr,
            })
            .collect();
        // `PendingLogStorage` doesn't carry an `extension` field (a log
        // storage is scoped to the loading component by construction), so
        // we materialise it from the outer `extension` parameter for parity
        // with every sibling entry.
        let log_storages = pending
            .log_storages
            .into_iter()
            .map(|l| LogStorageEntry {
                extension: extension.to_string(),
                name: l.name,
                callback_handle: l.callback_handle,
            })
            .collect();
        // `PendingPragma` (= `reg::PragmaReg`) already carries the
        // `extension` field the register-pragma host impl stamps at
        // capture time, so we straight-map it like scalars/tables.
        //
        // NOTE: the sibling `runtime/src/extension.rs` edit adds
        // `pub pragmas: Vec<PendingPragma>` to `PendingRegistrationsData`
        // and wires `drain_pending` to populate it. Until that lands this
        // stays empty; once it does, swap `Vec::new()` for:
        //     pending.pragmas.into_iter().map(|p| PragmaEntry {
        //         extension: p.extension,
        //         name: p.name,
        //         callback_handle: p.callback_handle,
        //     }).collect()
        // The `PragmaEntry` type and `LoadedComponent.pragmas` field are
        // in place so `reg_duckdb.rs` can be written against the final
        // shape without waiting on the swap.
        let pragmas: Vec<PragmaEntry> = Vec::new();
        // T2-2 prep: mirror of the pragmas stub above. The sibling
        // `runtime/src/extension.rs` edit adds
        // `pub coordinate_systems: Vec<PendingCoordinateSystem>` to
        // `PendingRegistrationsData` and wires `drain_pending` to populate
        // it. Until that lands this stays empty; once it does, swap
        // `Vec::new()` for:
        //     pending.coordinate_systems.into_iter().map(|c| CoordinateSystemEntry {
        //         extension: c.extension,
        //         auth_name: c.auth_name,
        //         code: c.code,
        //         wkt: c.wkt,
        //     }).collect()
        // The `CoordinateSystemEntry` type and `LoadedComponent.coordinate_systems`
        // field are in place so `reg_duckdb.rs` can be written against the
        // final shape without waiting on the swap.
        let coordinate_systems: Vec<CoordinateSystemEntry> = Vec::new();
        let instance_arc = Arc::new(Mutex::new(instance));
        // T1-7: take the previous Arc OUT of the map before dropping the
        // write lock so its `impl Drop for ExtensionInstance` (which fires
        // `dispatch_shutdown`) runs with NO Engine2 locks held. A guest
        // shutdown handler that reaches back through NativeServices —
        // e.g. `services.query("SELECT some_scalar(1)")` — would trigger
        // Engine2::dispatch_scalar, whose fallback path takes
        // `self.instances.read()`; dropping under the write lock would
        // deadlock. On first-load `insert` returns `None`, so the extra
        // `drop(prev_arc)` is a no-op.
        let prev_arc = {
            let mut map = self.instances.write().expect("instances lock poisoned");
            map.insert(extension.to_string(), instance_arc.clone())
        };
        drop(prev_arc);
        // F3-b: link the newly-wrapped instance to every callback entry that
        // load_component allocated during this component's setup. Dispatchers
        // then upgrade the Weak in a single atomic load — skipping the
        // `self.instances.read() -> HashMap<String,_>::get(&extension) ->
        // Arc::clone` path that the pre-F3-b prologue paid per invocation.
        // Safe idempotency: relinks on re-load, leaves other extensions' entries
        // alone.
        self.callbacks
            .write()
            .expect("callback registry poisoned")
            .link_extension_instance(extension, &instance_arc);
        Ok(LoadedComponent {
            replacement_scans,
            scalars,
            tables,
            aggregates,
            casts,
            macros,
            logical_types,
            storages,
            settings,
            copy_handlers,
            arrow_tables,
            scalar_ex,
            table_macros,
            enum_types,
            modified_types,
            log_storages,
            pragmas,
            coordinate_systems,
            docs,
        })
    }

    /// Invoke a component scalar for one row. `callback_handle` is the value
    /// handed to DuckDB at registration; it resolves through the shared callback
    /// registry to the owning component instance and its guest dispatcher.
    pub fn dispatch_scalar(
        &self,
        callback_handle: u32,
        row_index: u64,
        args: Vec<reg::DuckValue>,
    ) -> Result<reg::DuckValue> {
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry.dispatcher_handle;
            // F3-b fast path: if the Weak upgrades we hold the instance Arc
            // directly — no HashMap lookup, no Arc<str> clone. Only when the
            // Weak has never been populated (standalone host) or the instance
            // has been unloaded do we fall back to `instance_arc()` and pay
            // for the name string reference.
            match entry.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry.extension)?),
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let wit_args: Vec<extension_types::Duckvalue> =
            args.into_iter().map(neutral_to_wit).collect();
        let ctx = extension_runtime::Invokeinfo {
            rowindex: Some(row_index),
            // TODO: invokeinfo.is_window hardcoded — no DuckDB C API accessor
            // available in libduckdb-sys 1.10504.0 (no `duckdb_*_is_window`
            // symbol on FunctionInfo, no `is_window` flag on the extra-info
            // struct returned by `duckdb_scalar_function_get_extra_info`);
            // component window specialization is a no-op (audit gap T4-22).
            iswindow: false,
        };
        let result = instance
            .dispatch_scalar(dispatcher_handle, &wit_args, ctx)
            .map_err(|e| anyhow!("scalar dispatch failed: {e:?}"))?;
        Ok(wit_to_neutral(result))
    }

    /// Invoke a component scalar over a whole chunk of rows in a single WIT
    /// crossing. `rows[i]` is the argument tuple for row `base_row_index + i`;
    /// the returned vector is the per-row result, one entry per input row. This
    /// collapses the N per-row `dispatch_scalar` boundary crossings of a DuckDB
    /// data chunk into one, which is the dominant cost when N is large.
    pub fn dispatch_scalar_batch(
        &self,
        callback_handle: u32,
        base_row_index: u64,
        wit_rows: &Vec<Vec<extension_types::Duckvalue>>,
    ) -> Result<Vec<extension_types::Duckvalue>> {
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry.dispatcher_handle;
            // F3-b fast path: if the Weak upgrades we hold the instance Arc
            // directly — no HashMap lookup, no Arc<str> clone. Only when the
            // Weak has never been populated (standalone host) or the instance
            // has been unloaded do we fall back to `instance_arc()` and pay
            // for the name string reference.
            match entry.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry.extension)?),
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let ctx = extension_runtime::Invokeinfo {
            rowindex: Some(base_row_index),
            // TODO: invokeinfo.is_window hardcoded — no DuckDB C API accessor
            // available in libduckdb-sys 1.10504.0 (no `duckdb_*_is_window`
            // symbol on FunctionInfo, no `is_window` flag on the extra-info
            // struct returned by `duckdb_scalar_function_get_extra_info`);
            // component window specialization is a no-op (audit gap T4-22).
            iswindow: false,
        };
        instance
            .dispatch_scalar_batch(dispatcher_handle, wit_rows, ctx)
            .map_err(|e| anyhow!("scalar batch dispatch failed: {e:?}"))
    }

    /// Column-native scalar batch dispatch. The DuckDB bridge builds one
    /// `Colvec` per input column (per-column memcpy for the primitive arms)
    /// and hands them straight to `call-scalar-batch-col`; the returned
    /// `Colvec` is written directly into the DuckDB output vector without
    /// a row-major intermediate on either side of the crossing.
    pub fn dispatch_scalar_batch_col(
        &self,
        callback_handle: u32,
        base_row_index: u64,
        args: &[extension_column_types::Colvec],
    ) -> Result<extension_column_types::Colvec> {
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry.dispatcher_handle;
            // F3-b fast path: if the Weak upgrades we hold the instance Arc
            // directly — no HashMap lookup, no Arc<str> clone. Only when the
            // Weak has never been populated (standalone host) or the instance
            // has been unloaded do we fall back to `instance_arc()` and pay
            // for the name string reference.
            match entry.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry.extension)?),
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let ctx = extension_runtime::Invokeinfo {
            rowindex: Some(base_row_index),
            // TODO: invokeinfo.is_window hardcoded — no DuckDB C API accessor
            // available in libduckdb-sys 1.10504.0 (no `duckdb_*_is_window`
            // symbol on FunctionInfo, no `is_window` flag on the extra-info
            // struct returned by `duckdb_scalar_function_get_extra_info`);
            // component window specialization is a no-op (audit gap T4-22).
            iswindow: false,
        };
        instance
            .dispatch_scalar_batch_col(dispatcher_handle, args, ctx)
            .map_err(|e| anyhow!("scalar batch (col) dispatch failed: {e:?}"))
    }

    /// Invoke a component table function with the given call arguments, returning
    /// all result rows. `callback_handle` resolves through the callback registry
    /// to the owning component instance.
    pub fn dispatch_table(
        &self,
        callback_handle: u32,
        args: Vec<reg::DuckValue>,
    ) -> Result<Vec<Vec<extension_types::Duckvalue>>> {
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry.dispatcher_handle;
            // F3-b fast path: if the Weak upgrades we hold the instance Arc
            // directly — no HashMap lookup, no Arc<str> clone. Only when the
            // Weak has never been populated (standalone host) or the instance
            // has been unloaded do we fall back to `instance_arc()` and pay
            // for the name string reference.
            match entry.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry.extension)?),
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let wit_args: Vec<extension_types::Duckvalue> =
            args.into_iter().map(neutral_to_wit).collect();
        // Return WIT-shaped rows directly: the WasmTable bind pivots them into
        // column-major WitVal and reads via write_col_from's fixed-width hoist,
        // so a wit_to_neutral pass here would just be undone by a neutral_to_wit
        // pass in the caller.
        instance
            .dispatch_table(dispatcher_handle, &wit_args)
            .map_err(|e| anyhow!("table dispatch failed: {e:?}"))
    }

    /// Invoke a component aggregate over all accumulated input `rows` (each row is
    /// the function's argument tuple), returning the single aggregate result. The
    /// component computes the whole aggregate at once. `callback_handle` resolves
    /// through the callback registry to the owning component instance.
    pub fn dispatch_aggregate(
        &self,
        callback_handle: u32,
        rows: Vec<Vec<reg::DuckValue>>,
    ) -> Result<reg::DuckValue> {
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry.dispatcher_handle;
            // F3-b fast path: if the Weak upgrades we hold the instance Arc
            // directly — no HashMap lookup, no Arc<str> clone. Only when the
            // Weak has never been populated (standalone host) or the instance
            // has been unloaded do we fall back to `instance_arc()` and pay
            // for the name string reference.
            match entry.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry.extension)?),
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let wit_rows: Vec<Vec<extension_types::Duckvalue>> = rows
            .into_iter()
            .map(|row| row.into_iter().map(neutral_to_wit).collect())
            .collect();
        let result = instance
            .dispatch_aggregate(dispatcher_handle, &wit_rows)
            .map_err(|e| anyhow!("aggregate dispatch failed: {e:?}"))?;
        Ok(wit_to_neutral(result))
    }

    /// Column-native aggregate dispatch. The DuckDB bridge builds one
    /// `Colvec` per input column from its per-group typed accumulator and
    /// hands them straight to `call-aggregate-col`, skipping both the
    /// extension-side neutral→WIT walk (per cell) and the runtime-side
    /// `rows_to_colvecs` pivot (per column, allocates a Vec of pointers).
    pub fn dispatch_aggregate_col(
        &self,
        callback_handle: u32,
        args: &[extension_column_types::Colvec],
    ) -> Result<reg::DuckValue> {
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry.dispatcher_handle;
            // F3-b fast path: if the Weak upgrades we hold the instance Arc
            // directly — no HashMap lookup, no Arc<str> clone. Only when the
            // Weak has never been populated (standalone host) or the instance
            // has been unloaded do we fall back to `instance_arc()` and pay
            // for the name string reference.
            match entry.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry.extension)?),
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let result = instance
            .dispatch_aggregate_col(dispatcher_handle, args)
            .map_err(|e| anyhow!("aggregate (col) dispatch failed: {e:?}"))?;
        Ok(wit_to_neutral(result))
    }

    /// Invoke a component cast for one input value, returning the cast result.
    /// `callback_handle` resolves through the callback registry to the owning
    /// component instance. Wraps the runtime crate's
    /// [`ExtensionInstance::dispatch_cast`] (which itself pivots the single
    /// value through the columnar `call-cast-col` path). The C ABI cast
    /// callback iterates the input DuckDB vector row-by-row and reduces to
    /// this method for each row.
    pub fn dispatch_cast_col(
        &self,
        callback_handle: u32,
        value: reg::DuckValue,
    ) -> Result<reg::DuckValue> {
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry.dispatcher_handle;
            match entry.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry.extension)?),
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let wit_val = neutral_to_wit(value);
        let result = instance
            .dispatch_cast(dispatcher_handle, &wit_val)
            .map_err(|e| anyhow!("cast dispatch failed: {e:?}"))?;
        Ok(wit_to_neutral(result))
    }

    /// Bind a COPY writer for `path` with `columns` schema + `options`. Wraps
    /// [`ExtensionInstance::copy_to_bind`]. The returned `writer` handle is
    /// then passed to `dispatch_copy_to_sink` / `dispatch_copy_to_finalize`
    /// for the same statement.
    pub fn dispatch_copy_to_bind(
        &self,
        callback_handle: u32,
        path: &str,
        columns: &[extension_types::Columndef],
        options: &[(String, String)],
    ) -> Result<u32> {
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry.dispatcher_handle;
            match entry.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry.extension)?),
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        instance
            .copy_to_bind(dispatcher_handle, path, columns, options)
            .map_err(|e| anyhow!("copy_to_bind dispatch failed: {e:?}"))
    }

    /// Sink a batch of rows into a bound COPY writer. `callback_handle`
    /// resolves through the callback registry; `writer` is the writer handle
    /// the guest returned from its `copy-to-bind` on the current file. Delegates
    /// to [`ExtensionInstance::copy_to_sink`].
    pub fn dispatch_copy_to_sink(
        &self,
        callback_handle: u32,
        writer: u32,
        rows: Vec<Vec<reg::DuckValue>>,
    ) -> Result<()> {
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry.dispatcher_handle;
            match entry.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry.extension)?),
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let wit_rows: Vec<Vec<extension_types::Duckvalue>> = rows
            .into_iter()
            .map(|row| row.into_iter().map(neutral_to_wit).collect())
            .collect();
        instance
            .copy_to_sink(dispatcher_handle, writer, &wit_rows)
            .map_err(|e| anyhow!("copy_to_sink dispatch failed: {e:?}"))
    }

    /// Finalize + close a bound COPY writer, returning the total rows written.
    /// `callback_handle` resolves through the callback registry; `writer` is
    /// the writer handle the guest returned from its `copy-to-bind`. Delegates
    /// to [`ExtensionInstance::copy_to_finalize`].
    pub fn dispatch_copy_to_finalize(
        &self,
        callback_handle: u32,
        writer: u32,
    ) -> Result<u64> {
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry.dispatcher_handle;
            match entry.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry.extension)?),
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        instance
            .copy_to_finalize(dispatcher_handle, writer)
            .map_err(|e| anyhow!("copy_to_finalize dispatch failed: {e:?}"))
    }

    /// COPY FROM: bind a reader for `path` with key=value `options`, forwarding
    /// the destination table's `target_columns` schema (from DuckDB's
    /// `duckdb_table_function_bind_get_result_column_*` accessors on the COPY-
    /// FROM install table function). Wraps [`ExtensionInstance::copy_from_bind`].
    /// Returns the guest's reader handle plus the discovered column schema; the
    /// reader handle is then passed to `dispatch_copy_from_scan` (to pull rows)
    /// and finally `dispatch_copy_from_close` (to release reader state). Mirrors
    /// the `dispatch_copy_to_bind` shape (F3-b upgrade-else-fallback registry
    /// resolution) — a COPY FROM installation lands as a
    /// `duckdb_table_function` on the COPY function via
    /// `duckdb_copy_function_set_copy_from_function`, and that table
    /// function's bind/init/func callbacks re-enter here.
    ///
    /// T1-6 landing: accepts `target_columns` in the neutral `reg::ColumnDef`
    /// shape and converts to `extension_types::Columndef` at the WIT boundary
    /// (using the same field mapping as `convert_extension_columndefs`, in
    /// reverse). The guest sees the destination schema in `copy-from-bind` and
    /// MUST prepare rows matching it.
    pub fn dispatch_copy_from_bind(
        &self,
        callback_handle: u32,
        path: &str,
        options: &[(String, String)],
        target_columns: Vec<reg::ColumnDef>,
    ) -> Result<ducklink_runtime::extension::CopyFromBindResult> {
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry.dispatcher_handle;
            match entry.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry.extension)?),
            }
        };
        let wit_target: Vec<extension_types::Columndef> = target_columns
            .into_iter()
            .map(neutral_to_wit_columndef)
            .collect();
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        instance
            .copy_from_bind(dispatcher_handle, path, options, &wit_target)
            .map_err(|e| anyhow!("copy_from_bind dispatch failed: {e:?}"))
    }

    /// COPY FROM: pull up to `max_rows` from a bound reader. An empty result
    /// signals EOF; the caller then invokes [`Self::dispatch_copy_from_close`]
    /// to release reader state. Wraps [`ExtensionInstance::copy_from_scan`],
    /// flattening the returned WIT `Resultset` into neutral `reg::DuckValue`s
    /// (symmetric with [`Self::dispatch_arrow_next`]).
    pub fn dispatch_copy_from_scan(
        &self,
        callback_handle: u32,
        reader: u32,
        max_rows: u32,
    ) -> Result<Vec<Vec<reg::DuckValue>>> {
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry.dispatcher_handle;
            match entry.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry.extension)?),
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let rs = instance
            .copy_from_scan(dispatcher_handle, reader, max_rows)
            .map_err(|e| anyhow!("copy_from_scan dispatch failed: {e:?}"))?;
        Ok(rs
            .into_iter()
            .map(|row| row.into_iter().map(wit_to_neutral).collect())
            .collect())
    }

    /// COPY FROM: close the reader and release its state. Returns whether the
    /// reader was known to the guest. Wraps [`ExtensionInstance::copy_from_close`].
    pub fn dispatch_copy_from_close(
        &self,
        callback_handle: u32,
        reader: u32,
    ) -> Result<bool> {
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry.dispatcher_handle;
            match entry.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry.extension)?),
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        instance
            .copy_from_close(dispatcher_handle, reader)
            .map_err(|e| anyhow!("copy_from_close dispatch failed: {e:?}"))
    }

    /// Deliver one log entry to the component's registered log-storage sink.
    /// `callback_handle` is the value the component passed to
    /// `register-log-storage`; it resolves through the shared callback registry
    /// to the owning component instance and its guest `log-storage-dispatch`
    /// binding. Wraps [`ExtensionInstance::dispatch_write_log_entry`].
    pub fn dispatch_write_log_entry(
        &self,
        callback_handle: u32,
        entry: LogEntry,
    ) -> Result<()> {
        // Contract (log-storage-dispatch.wit): the `handle` handed to the
        // guest MUST be the value the guest passed to `register-log-storage`,
        // NOT the runtime's allocated global `callback_handle`. Resolve the
        // registry entry and forward `entry_ref.dispatcher_handle` — mirror
        // of dispatch_scalar / dispatch_table.
        let (dispatcher_handle, instance_arc) = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry_ref = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            let dispatcher_handle = entry_ref.dispatcher_handle;
            match entry_ref.instance.upgrade() {
                Some(arc) => (dispatcher_handle, arc),
                None => (dispatcher_handle, self.instance_arc(&entry_ref.extension)?),
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        instance
            .dispatch_write_log_entry(dispatcher_handle, entry)
            .map_err(|e| anyhow!("write_log_entry dispatch failed: {e:?}"))
    }

    /// Open a scan cursor against the arrow producer named by `callback_handle`
    /// (the value the component passed to `arrow-ext.register-arrow-table`).
    /// Resolves through the shared callback registry to the owning component
    /// instance and its guest `arrow-ext-dispatch` binding. Wraps
    /// [`ExtensionInstance::dispatch_arrow_open`].
    pub fn dispatch_arrow_open(&self, callback_handle: u32) -> Result<u32> {
        let instance_arc = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry_ref = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            match entry_ref.instance.upgrade() {
                Some(arc) => arc,
                None => self.instance_arc(&entry_ref.extension)?,
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        instance
            .dispatch_arrow_open(callback_handle)
            .map_err(|e| anyhow!("arrow_open dispatch failed: {e:?}"))
    }

    /// Pull the next batch of rows from the guest cursor. An empty
    /// `Vec<Vec<DuckValue>>` signals EOF; the caller then invokes
    /// [`Self::dispatch_arrow_close`] to release the cursor state. Wraps
    /// [`ExtensionInstance::dispatch_arrow_next`], flattening the returned
    /// `Resultset` from WIT `Duckvalue`s to neutral `reg::DuckValue`s.
    pub fn dispatch_arrow_next(
        &self,
        callback_handle: u32,
        cursor: u32,
    ) -> Result<Vec<Vec<reg::DuckValue>>> {
        let instance_arc = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry_ref = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            match entry_ref.instance.upgrade() {
                Some(arc) => arc,
                None => self.instance_arc(&entry_ref.extension)?,
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let rs = instance
            .dispatch_arrow_next(callback_handle, cursor)
            .map_err(|e| anyhow!("arrow_next dispatch failed: {e:?}"))?;
        Ok(rs
            .into_iter()
            .map(|row| row.into_iter().map(wit_to_neutral).collect())
            .collect())
    }

    /// Close the guest cursor and release its state. Returns whether the
    /// cursor was known to the guest. Wraps
    /// [`ExtensionInstance::dispatch_arrow_close`].
    pub fn dispatch_arrow_close(
        &self,
        callback_handle: u32,
        cursor: u32,
    ) -> Result<bool> {
        let instance_arc = {
            let registry = self.callbacks.read().expect("callback registry poisoned");
            let entry_ref = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            match entry_ref.instance.upgrade() {
                Some(arc) => arc,
                None => self.instance_arc(&entry_ref.extension)?,
            }
        };
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        instance
            .dispatch_arrow_close(callback_handle, cursor)
            .map_err(|e| anyhow!("arrow_close dispatch failed: {e:?}"))
    }
}

fn neutral_to_wit(v: reg::DuckValue) -> extension_types::Duckvalue {
    match v {
        reg::DuckValue::Null => extension_types::Duckvalue::Null,
        reg::DuckValue::Boolean(b) => extension_types::Duckvalue::Boolean(b),
        reg::DuckValue::Int64(i) => extension_types::Duckvalue::Int64(i),
        reg::DuckValue::Uint64(u) => extension_types::Duckvalue::Uint64(u),
        reg::DuckValue::Float64(f) => extension_types::Duckvalue::Float64(f),
        reg::DuckValue::Text(s) => extension_types::Duckvalue::Text(s),
        reg::DuckValue::Blob(b) => extension_types::Duckvalue::Blob(b),
        reg::DuckValue::Int32(i) => extension_types::Duckvalue::Int32(i),
        reg::DuckValue::Timestamp(t) => extension_types::Duckvalue::Timestamp(t),
        reg::DuckValue::Int8(i) => extension_types::Duckvalue::Int8(i),
        reg::DuckValue::Int16(i) => extension_types::Duckvalue::Int16(i),
        reg::DuckValue::Uint8(u) => extension_types::Duckvalue::Uint8(u),
        reg::DuckValue::Uint16(u) => extension_types::Duckvalue::Uint16(u),
        reg::DuckValue::Uint32(u) => extension_types::Duckvalue::Uint32(u),
        reg::DuckValue::Float32(f) => extension_types::Duckvalue::Float32(f),
        reg::DuckValue::Date(d) => extension_types::Duckvalue::Date(d),
        reg::DuckValue::Time(t) => extension_types::Duckvalue::Time(t),
        reg::DuckValue::Timestamptz(t) => extension_types::Duckvalue::Timestamptz(t),
        reg::DuckValue::Decimal {
            lower,
            upper,
            width,
            scale,
        } => extension_types::Duckvalue::Decimal(extension_types::Decimalvalue {
            lower,
            upper,
            width,
            scale,
        }),
        reg::DuckValue::Interval {
            months,
            days,
            micros,
        } => extension_types::Duckvalue::Interval(extension_types::Intervalvalue {
            months,
            days,
            micros,
        }),
        reg::DuckValue::Uuid { hi, lo } => {
            extension_types::Duckvalue::Uuid(extension_types::Uuidvalue { hi, lo })
        }
        // T2-1 residual (major-5): 128-bit integer scalars ride first-class
        // WIT arms carrying two u64/s64 halves; the runtime reassembles the
        // value via `((upper as i128) << 64 | lower as i128)`.
        reg::DuckValue::Hugeint { lower, upper } => {
            extension_types::Duckvalue::Hugeint(extension_types::Hugeintvalue { lower, upper })
        }
        reg::DuckValue::UHugeint { lower, upper } => {
            extension_types::Duckvalue::Uhugeint(extension_types::Uhugeintvalue { lower, upper })
        }
        // S1 (major-5): nested SCALAR values have no first-class WIT
        // `duckvalue` arm — the columnar `column-types.column` variant
        // carries their bulk equivalents (list-col/struct-col/map-col/
        // array-col) but the row-major COLD path stays on `complex(json)`
        // per the column-types.wit header note. Degrade via
        // `duckdb_value_json` so the value re-materializes through the
        // duckdb C vector API when the guest lifts it.
        nested @ (reg::DuckValue::List(_)
        | reg::DuckValue::Struct(_)
        | reg::DuckValue::Map(_)
        | reg::DuckValue::Array(_)) => {
            let (type_expr, json) = duckdb_value_json(&nested);
            extension_types::Duckvalue::Complex(extension_types::Complexvalue { type_expr, json })
        }
        reg::DuckValue::Complex { type_expr, json } => {
            extension_types::Duckvalue::Complex(extension_types::Complexvalue { type_expr, json })
        }
    }
}

/// Render a nested-shaped neutral `reg::DuckValue` (LIST / STRUCT / MAP /
/// ARRAY) as a `(type-expression, json)` pair for the WIT `complex` arm.
/// Companion to `duckdb_type_expr`. Guest-side, the runtime reconstructs the
/// real LIST/STRUCT vector from the JSON via the duckdb C vector API (see the
/// `complex` escape-hatch documentation in types.wit).
///
/// The JSON is a best-effort textual rendering — it uses `null` for the WIT
/// `Null` arm and the debug-formatted scalar for everything else. Callers
/// that need lossless nested-scalar transport should wait for structural
/// nested VALUE arms in a future major bump.
fn duckdb_value_json(v: &reg::DuckValue) -> (String, String) {
    // The type-expression is synthesized from the shallow shape; the deep
    // element type of a heterogeneous struct/map field is not tracked on
    // `DuckValue`, so we render the outer shape and let the guest resolve
    // the leaves.
    fn scalar_json(v: &reg::DuckValue) -> String {
        match v {
            reg::DuckValue::Null => "null".to_string(),
            reg::DuckValue::Boolean(b) => b.to_string(),
            reg::DuckValue::Int64(i) => i.to_string(),
            reg::DuckValue::Uint64(u) => u.to_string(),
            reg::DuckValue::Float64(f) => f.to_string(),
            reg::DuckValue::Text(s) => format!("\"{}\"", s.replace('"', "\\\"")),
            reg::DuckValue::Int32(i) => i.to_string(),
            reg::DuckValue::Int8(i) => i.to_string(),
            reg::DuckValue::Int16(i) => i.to_string(),
            reg::DuckValue::Uint8(u) => u.to_string(),
            reg::DuckValue::Uint16(u) => u.to_string(),
            reg::DuckValue::Uint32(u) => u.to_string(),
            reg::DuckValue::Float32(f) => f.to_string(),
            reg::DuckValue::List(items) | reg::DuckValue::Array(items) => {
                let parts: Vec<String> = items.iter().map(scalar_json).collect();
                format!("[{}]", parts.join(","))
            }
            reg::DuckValue::Struct(fields) => {
                let parts: Vec<String> = fields
                    .iter()
                    .map(|(n, val)| format!("\"{n}\":{}", scalar_json(val)))
                    .collect();
                format!("{{{}}}", parts.join(","))
            }
            reg::DuckValue::Map(pairs) => {
                let parts: Vec<String> = pairs
                    .iter()
                    .map(|(k, val)| format!("{}:{}", scalar_json(k), scalar_json(val)))
                    .collect();
                format!("{{{}}}", parts.join(","))
            }
            // Non-JSON-native scalars in a nested payload fall back to a
            // stringified debug rendering. Round-trip fidelity is not
            // guaranteed on this path (see fn doc).
            other => format!("\"{other:?}\""),
        }
    }
    let type_expr = match v {
        reg::DuckValue::List(_) => "LIST",
        reg::DuckValue::Struct(_) => "STRUCT",
        reg::DuckValue::Map(_) => "MAP",
        reg::DuckValue::Array(_) => "ARRAY",
        _ => "COMPLEX",
    }
    .to_string();
    (type_expr, scalar_json(v))
}

fn wit_to_neutral(v: extension_types::Duckvalue) -> reg::DuckValue {
    match v {
        extension_types::Duckvalue::Null => reg::DuckValue::Null,
        extension_types::Duckvalue::Boolean(b) => reg::DuckValue::Boolean(b),
        extension_types::Duckvalue::Int64(i) => reg::DuckValue::Int64(i),
        extension_types::Duckvalue::Uint64(u) => reg::DuckValue::Uint64(u),
        extension_types::Duckvalue::Float64(f) => reg::DuckValue::Float64(f),
        extension_types::Duckvalue::Text(s) => reg::DuckValue::Text(s),
        extension_types::Duckvalue::Blob(b) => reg::DuckValue::Blob(b),
        extension_types::Duckvalue::Int32(i) => reg::DuckValue::Int32(i),
        extension_types::Duckvalue::Timestamp(t) => reg::DuckValue::Timestamp(t),
        extension_types::Duckvalue::Int8(i) => reg::DuckValue::Int8(i),
        extension_types::Duckvalue::Int16(i) => reg::DuckValue::Int16(i),
        extension_types::Duckvalue::Uint8(u) => reg::DuckValue::Uint8(u),
        extension_types::Duckvalue::Uint16(u) => reg::DuckValue::Uint16(u),
        extension_types::Duckvalue::Uint32(u) => reg::DuckValue::Uint32(u),
        extension_types::Duckvalue::Float32(f) => reg::DuckValue::Float32(f),
        extension_types::Duckvalue::Date(d) => reg::DuckValue::Date(d),
        extension_types::Duckvalue::Time(t) => reg::DuckValue::Time(t),
        extension_types::Duckvalue::Timestamptz(t) => reg::DuckValue::Timestamptz(t),
        extension_types::Duckvalue::Decimal(d) => reg::DuckValue::Decimal {
            lower: d.lower,
            upper: d.upper,
            width: d.width,
            scale: d.scale,
        },
        extension_types::Duckvalue::Interval(iv) => reg::DuckValue::Interval {
            months: iv.months,
            days: iv.days,
            micros: iv.micros,
        },
        extension_types::Duckvalue::Uuid(u) => reg::DuckValue::Uuid { hi: u.hi, lo: u.lo },
        // T2-1 residual (major-5): 128-bit integer scalars.
        extension_types::Duckvalue::Hugeint(h) => reg::DuckValue::Hugeint {
            lower: h.lower,
            upper: h.upper,
        },
        extension_types::Duckvalue::Uhugeint(h) => reg::DuckValue::UHugeint {
            lower: h.lower,
            upper: h.upper,
        },
        extension_types::Duckvalue::Complex(c) => reg::DuckValue::Complex {
            type_expr: c.type_expr,
            json: c.json,
        },
    }
}

/// Public inverse of `neutral_to_wit_logicaltype`: lift a WIT
/// `extension_types::Logicaltype` back into the neutral `reg::LogicalType`.
/// Exposed for `reg_duckdb::ducklink_copy_from_bind` (T1-6 landing) which
/// captures the target-table schema as WIT columndefs and needs to forward
/// them through the engine layer in the neutral shape. The consolidator that
/// runs next will fold this call into a single-pass build.
pub fn wit_logicaltype_to_neutral(lt: &extension_types::Logicaltype) -> reg::LogicalType {
    match lt {
        extension_types::Logicaltype::Boolean => reg::LogicalType::Boolean,
        extension_types::Logicaltype::Int64 => reg::LogicalType::Int64,
        extension_types::Logicaltype::Uint64 => reg::LogicalType::Uint64,
        extension_types::Logicaltype::Float64 => reg::LogicalType::Float64,
        extension_types::Logicaltype::Text => reg::LogicalType::Text,
        extension_types::Logicaltype::Blob => reg::LogicalType::Blob,
        extension_types::Logicaltype::Int32 => reg::LogicalType::Int32,
        extension_types::Logicaltype::Timestamp => reg::LogicalType::Timestamp,
        extension_types::Logicaltype::Int8 => reg::LogicalType::Int8,
        extension_types::Logicaltype::Int16 => reg::LogicalType::Int16,
        extension_types::Logicaltype::Uint8 => reg::LogicalType::Uint8,
        extension_types::Logicaltype::Uint16 => reg::LogicalType::Uint16,
        extension_types::Logicaltype::Uint32 => reg::LogicalType::Uint32,
        extension_types::Logicaltype::Float32 => reg::LogicalType::Float32,
        extension_types::Logicaltype::Date => reg::LogicalType::Date,
        extension_types::Logicaltype::Time => reg::LogicalType::Time,
        extension_types::Logicaltype::Timestamptz => reg::LogicalType::Timestamptz,
        // S2 (major-5): DECIMAL now carries width/scale as a `decimalshape`.
        extension_types::Logicaltype::Decimal(shape) => reg::LogicalType::Decimal {
            width: shape.width,
            scale: shape.scale,
        },
        extension_types::Logicaltype::Interval => reg::LogicalType::Interval,
        extension_types::Logicaltype::Uuid => reg::LogicalType::Uuid,
        // T2-1 residual (major-5): fieldless 128-bit integer logical types.
        extension_types::Logicaltype::Hugeint => reg::LogicalType::Hugeint,
        extension_types::Logicaltype::Uhugeint => reg::LogicalType::UHugeint,
        extension_types::Logicaltype::Complex(expr) => reg::LogicalType::Complex(expr.clone()),
    }
}

/// Reverse of `convert_extension_logicaltype` in the runtime crate: lower a
/// neutral `reg::LogicalType` into the WIT `extension_types::Logicaltype` shape
/// the guest expects. Used by `dispatch_copy_from_bind` to forward the
/// destination table's target-column schema (T1-6).
///
/// major-5 (2026-07-23): `Decimal`, `Hugeint`, `UHugeint` are first-class WIT
/// arms and lower structurally. The nested `List` / `Struct` / `Map` / `Array`
/// neutral arms have NO structural WIT counterpart (wit-parser 0.251 forbids
/// recursive VALUE types — see column-types.wit header note), so they degrade
/// to `complex(<duckdb-type-expression>)` via `duckdb_type_expr` — the same
/// escape hatch the WIT `complex` arm documents. Callers that need
/// fully-structural nested logical types must wait for a @6 bump once
/// wit-parser gains recursive-value-type support (or the runtime opts into
/// resource handles).
fn neutral_to_wit_logicaltype(lt: reg::LogicalType) -> extension_types::Logicaltype {
    match lt {
        reg::LogicalType::Boolean => extension_types::Logicaltype::Boolean,
        reg::LogicalType::Int64 => extension_types::Logicaltype::Int64,
        reg::LogicalType::Uint64 => extension_types::Logicaltype::Uint64,
        reg::LogicalType::Float64 => extension_types::Logicaltype::Float64,
        reg::LogicalType::Text => extension_types::Logicaltype::Text,
        reg::LogicalType::Blob => extension_types::Logicaltype::Blob,
        reg::LogicalType::Int32 => extension_types::Logicaltype::Int32,
        reg::LogicalType::Timestamp => extension_types::Logicaltype::Timestamp,
        reg::LogicalType::Int8 => extension_types::Logicaltype::Int8,
        reg::LogicalType::Int16 => extension_types::Logicaltype::Int16,
        reg::LogicalType::Uint8 => extension_types::Logicaltype::Uint8,
        reg::LogicalType::Uint16 => extension_types::Logicaltype::Uint16,
        reg::LogicalType::Uint32 => extension_types::Logicaltype::Uint32,
        reg::LogicalType::Float32 => extension_types::Logicaltype::Float32,
        reg::LogicalType::Date => extension_types::Logicaltype::Date,
        reg::LogicalType::Time => extension_types::Logicaltype::Time,
        reg::LogicalType::Timestamptz => extension_types::Logicaltype::Timestamptz,
        // S2 (major-5): DECIMAL width/scale ride the variant arm.
        reg::LogicalType::Decimal { width, scale } => {
            extension_types::Logicaltype::Decimal(extension_types::Decimalshape { width, scale })
        }
        reg::LogicalType::Interval => extension_types::Logicaltype::Interval,
        reg::LogicalType::Uuid => extension_types::Logicaltype::Uuid,
        // T2-1 residual (major-5): first-class 128-bit integer logical types.
        reg::LogicalType::Hugeint => extension_types::Logicaltype::Hugeint,
        reg::LogicalType::UHugeint => extension_types::Logicaltype::Uhugeint,
        // S1 (major-5): nested LOGICAL types are not first-class on the WIT
        // side; degrade to `complex(<duckdb-type-expression>)`.
        nested @ (reg::LogicalType::List(_)
        | reg::LogicalType::Struct(_)
        | reg::LogicalType::Map(_, _)
        | reg::LogicalType::Array(_, _)) => {
            extension_types::Logicaltype::Complex(duckdb_type_expr(&nested))
        }
        reg::LogicalType::Complex(expr) => extension_types::Logicaltype::Complex(expr),
    }
}

/// Render a neutral `reg::LogicalType` as a DuckDB SQL type-expression string,
/// suitable for stuffing into the WIT `complex` escape-hatch arm when the WIT
/// `logicaltype` variant has no structural counterpart (major-5: LIST / STRUCT
/// / MAP / ARRAY). Non-nested types produce the same identifier DuckDB would
/// accept in a CREATE TABLE column type.
fn duckdb_type_expr(lt: &reg::LogicalType) -> String {
    match lt {
        reg::LogicalType::Boolean => "BOOLEAN".to_string(),
        reg::LogicalType::Int64 => "BIGINT".to_string(),
        reg::LogicalType::Uint64 => "UBIGINT".to_string(),
        reg::LogicalType::Float64 => "DOUBLE".to_string(),
        reg::LogicalType::Text => "VARCHAR".to_string(),
        reg::LogicalType::Blob => "BLOB".to_string(),
        reg::LogicalType::Int32 => "INTEGER".to_string(),
        reg::LogicalType::Timestamp => "TIMESTAMP".to_string(),
        reg::LogicalType::Int8 => "TINYINT".to_string(),
        reg::LogicalType::Int16 => "SMALLINT".to_string(),
        reg::LogicalType::Uint8 => "UTINYINT".to_string(),
        reg::LogicalType::Uint16 => "USMALLINT".to_string(),
        reg::LogicalType::Uint32 => "UINTEGER".to_string(),
        reg::LogicalType::Float32 => "REAL".to_string(),
        reg::LogicalType::Date => "DATE".to_string(),
        reg::LogicalType::Time => "TIME".to_string(),
        reg::LogicalType::Timestamptz => "TIMESTAMPTZ".to_string(),
        reg::LogicalType::Decimal { width, scale } => format!("DECIMAL({width},{scale})"),
        reg::LogicalType::Interval => "INTERVAL".to_string(),
        reg::LogicalType::Uuid => "UUID".to_string(),
        reg::LogicalType::Hugeint => "HUGEINT".to_string(),
        reg::LogicalType::UHugeint => "UHUGEINT".to_string(),
        reg::LogicalType::List(inner) => format!("{}[]", duckdb_type_expr(inner)),
        reg::LogicalType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|(n, t)| format!("{n} {}", duckdb_type_expr(t)))
                .collect();
            format!("STRUCT({})", parts.join(", "))
        }
        reg::LogicalType::Map(k, v) => {
            format!("MAP({}, {})", duckdb_type_expr(k), duckdb_type_expr(v))
        }
        reg::LogicalType::Array(size, inner) => {
            format!("{}[{size}]", duckdb_type_expr(inner))
        }
        reg::LogicalType::Complex(expr) => expr.clone(),
    }
}

/// Lower a neutral `reg::ColumnDef` into `extension_types::Columndef` for the
/// WIT boundary. Companion to `neutral_to_wit_logicaltype`.
fn neutral_to_wit_columndef(c: reg::ColumnDef) -> extension_types::Columndef {
    extension_types::Columndef {
        name: c.name,
        logical: neutral_to_wit_logicaltype(c.logical),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `reg::DuckValue` variant, with distinctive payloads, so a mis-wired
    /// `neutral_to_wit` / `wit_to_neutral` arm (a swapped or dropped field) shows
    /// up as a round-trip mismatch rather than silently corrupting a value.
    fn all_variants() -> Vec<reg::DuckValue> {
        vec![
            reg::DuckValue::Null,
            reg::DuckValue::Boolean(true),
            reg::DuckValue::Int64(-9_000_000_000),
            reg::DuckValue::Uint64(18_000_000_000),
            reg::DuckValue::Float64(1234.5),
            reg::DuckValue::Text("héllo ☃".to_string()),
            reg::DuckValue::Blob(vec![0, 1, 2, 255, 128]),
            reg::DuckValue::Int8(-12),
            reg::DuckValue::Int16(-3000),
            reg::DuckValue::Int32(-2_000_000),
            reg::DuckValue::Uint8(200),
            reg::DuckValue::Uint16(60000),
            reg::DuckValue::Uint32(4_000_000_000),
            reg::DuckValue::Float32(1.5),
            reg::DuckValue::Timestamp(1_700_000_000_000_000),
            reg::DuckValue::Date(19_000),
            reg::DuckValue::Time(86_399_000_000),
            reg::DuckValue::Timestamptz(1_700_000_000_000_001),
            reg::DuckValue::Decimal {
                lower: 0xDEAD_BEEF,
                upper: 0x1234,
                width: 18,
                scale: 3,
            },
            reg::DuckValue::Interval {
                months: 13,
                days: -5,
                micros: 999,
            },
            reg::DuckValue::Uuid {
                hi: 0xABCD_0000_1111_2222,
                lo: 0x3333_4444_5555_6666,
            },
            // T2-1 residual (major-5): 128-bit integer scalars ride first-class
            // WIT arms, so they round-trip cleanly through the neutral <-> WIT
            // converters just like DECIMAL/INTERVAL/UUID.
            reg::DuckValue::Hugeint {
                lower: 0xDEAD_BEEF_CAFE_BABE,
                upper: -0x1234_5678_9ABC_DEF0,
            },
            reg::DuckValue::UHugeint {
                lower: 0xBABE_CAFE_DEAD_BEEF,
                upper: 0x1122_3344_5566_7788,
            },
            // S1 (major-5): nested VALUE arms have no first-class WIT
            // `duckvalue` counterpart -- they degrade to `Complex` via
            // `duckdb_value_json` at the boundary and CANNOT round-trip as
            // their original arm. They are therefore excluded from this test's
            // round-trip cohort (a future @6 with structural nested-value
            // arms lifts this restriction).
            reg::DuckValue::Complex {
                type_expr: "STRUCT(a INT)".to_string(),
                json: "{\"a\":1}".to_string(),
            },
        ]
    }

    /// neutral -> WIT -> neutral is the identity for every variant. Guards the
    /// two big hand-written match tables against drift (e.g. the rich-type
    /// expansion) — the dispatch correctness the whole bridge rests on.
    #[test]
    fn neutral_wit_roundtrip_is_identity_for_all_variants() {
        for v in all_variants() {
            let before = format!("{v:?}");
            let after = format!("{:?}", wit_to_neutral(neutral_to_wit(v)));
            assert_eq!(before, after, "round-trip changed value");
        }
    }

    /// The 24 round-trippable variants must all be distinct after a round-trip
    /// (no two collapse onto the same WIT arm), so the count of unique debug
    /// renderings is stable. Bumped from 22 to 24 in major-5 by the two
    /// first-class HUGEINT / UHUGEINT arms (T2-1 residual); the S1 nested-value
    /// arms are excluded per the note in `all_variants`.
    #[test]
    fn roundtrip_preserves_distinctness() {
        let mut seen = std::collections::HashSet::new();
        for v in all_variants() {
            let s = format!("{:?}", wit_to_neutral(neutral_to_wit(v)));
            assert!(seen.insert(s.clone()), "two variants collapsed to {s}");
        }
        assert_eq!(seen.len(), 24, "expected 24 distinct DuckValue variants");
    }

    /// Dispatching against a callback handle that was never registered must be a
    /// clean `Err` (not a panic / not an index out of bounds) — the registry-miss
    /// arm on every dispatch entry point. No component is loaded, so this also
    /// confirms the lookup fails before any instance is touched.
    #[test]
    fn dispatch_unknown_handle_errors_cleanly() {
        let mut engine = Engine2::new().expect("engine");
        let bad = 999_999u32;

        let s = engine.dispatch_scalar(bad, 0, vec![reg::DuckValue::Int64(1)]);
        assert!(s.is_err(), "scalar dispatch on unknown handle should Err");

        let b = engine.dispatch_scalar_batch(bad, 0, &vec![vec![]]);
        assert!(b.is_err(), "batch dispatch on unknown handle should Err");

        let t = engine.dispatch_table(bad, vec![]);
        assert!(t.is_err(), "table dispatch on unknown handle should Err");

        let a = engine.dispatch_aggregate(bad, vec![vec![reg::DuckValue::Int64(1)]]);
        assert!(a.is_err(), "aggregate dispatch on unknown handle should Err");
    }

    /// Loading a path that is not a valid wasm component must surface an `Err`
    /// rather than panicking — the load-time trust boundary for an attacker- or
    /// operator-supplied artifact.
    #[test]
    fn load_rejects_non_component_bytes() {
        let mut engine = Engine2::new().expect("engine");
        let mut tmp = std::env::temp_dir();
        tmp.push(format!("ducklink_bogus_{}.wasm", std::process::id()));
        std::fs::write(&tmp, b"not a wasm component at all").expect("write tmp");
        let r = engine.load("bogus", &tmp);
        let _ = std::fs::remove_file(&tmp);
        assert!(r.is_err(), "loading garbage bytes should Err, not panic");
    }

    /// A missing artifact path is a clean `Err` too (no panic on the io error).
    #[test]
    fn load_missing_file_errors() {
        let mut engine = Engine2::new().expect("engine");
        let r = engine.load("ghost", std::path::Path::new("/no/such/ducklink/file.wasm"));
        assert!(r.is_err(), "missing file should Err");
    }
}
