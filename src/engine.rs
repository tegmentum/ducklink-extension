//! The Direction-2 engine: loads `duckdb:extension` WebAssembly components into
//! native DuckDB and dispatches DuckDB invocations back into them.
//!
//! This module depends ONLY on `ducklink-runtime` + wasmtime (no DuckDB), so it
//! compiles and is checkable without the DuckDB toolchain. The DuckDB C-API
//! binding that turns a [`ScalarFunc`] into a registered catalog function (and
//! routes per-row calls back to [`Engine2::dispatch_scalar`]) lives behind the
//! crate's `loadable` feature.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use anyhow::{anyhow, Context, Result};
use wasmtime::component::Component;
use wasmtime::{Config, Engine};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder};

use ducklink_runtime::duckdb_extension_bindings::duckdb::extension::{
    column_types as extension_column_types, runtime as extension_runtime, types as extension_types,
};
use ducklink_runtime::reg;
use ducklink_runtime::{
    load_component, CallbackRegistry, ConfigError, ExtensionInstance, ExtensionServices, LogField,
    LogLevel, PendingRegistrationsData,
};

/// Build a component-model wasmtime engine for running extension components.
/// Mirrors the host's engine config (component model + wasm exceptions, which
/// DuckDB-targeting components may use).
fn build_engine() -> Result<Engine> {
    let mut config = Config::new();
    config.wasm_component_model(true);
    config.wasm_exceptions(true);
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

/// Config/logging sink for native DuckDB. Logging goes to stderr; config reads
/// are not yet wired to DuckDB's settings (they return `None`). Routing these to
/// the DuckDB C API is a follow-up; components that only register functions do
/// not depend on it.
struct NativeServices;

impl ExtensionServices for NativeServices {
    fn provider_version(&mut self) -> Result<String, ConfigError> {
        Ok(concat!("ducklink-extension/", env!("CARGO_PKG_VERSION")).to_string())
    }
    fn list_keys(&mut self, _prefix: Option<&str>) -> Result<Vec<String>, ConfigError> {
        Ok(Vec::new())
    }
    fn get_string(&mut self, _path: &str) -> Result<Option<String>, ConfigError> {
        Ok(None)
    }
    fn get_bool(&mut self, _path: &str) -> Result<Option<bool>, ConfigError> {
        Ok(None)
    }
    fn get_i64(&mut self, _path: &str) -> Result<Option<i64>, ConfigError> {
        Ok(None)
    }
    fn get_u64(&mut self, _path: &str) -> Result<Option<u64>, ConfigError> {
        Ok(None)
    }
    fn get_f64(&mut self, _path: &str) -> Result<Option<f64>, ConfigError> {
        Ok(None)
    }
    fn get_bytes(&mut self, _path: &str) -> Result<Option<Vec<u8>>, ConfigError> {
        Ok(None)
    }
    fn get_string_list(&mut self, _path: &str) -> Result<Option<Vec<String>>, ConfigError> {
        Ok(None)
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

/// What a component registered: the functions a direction-specific sink bridges
/// into the database.
#[derive(Clone, Debug, Default)]
pub struct LoadedComponent {
    pub scalars: Vec<ScalarFunc>,
    pub tables: Vec<TableFunc>,
    pub aggregates: Vec<AggregateFunc>,
    /// Advanced tier (INTERNAL C++ ABI): PARSER extensions the component declared.
    pub parsers: Vec<reg::ParserReg>,
    /// Advanced tier: general OPTIMIZER rules the component declared.
    pub optimizers: Vec<reg::OptimizerReg>,
    /// Advanced tier: streaming + FILTER-PUSHDOWN table functions.
    pub filterable_tables: Vec<reg::FilterableTableReg>,
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
    callbacks: Arc<Mutex<CallbackRegistry>>,
    instances: RwLock<HashMap<String, Arc<Mutex<ExtensionInstance>>>>,
}

impl Engine2 {
    pub fn new() -> Result<Self> {
        Ok(Self {
            engine: build_engine()?,
            callbacks: Arc::new(Mutex::new(CallbackRegistry::new())),
            instances: RwLock::new(HashMap::new()),
        })
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

    /// The shared wasmtime [`Engine`] (component-model + exceptions enabled, with
    /// the on-disk compile cache). Cheap to clone (`Engine` is `Arc`-backed).
    /// Exposed so the Python source tier can build its own resident-provider
    /// [`ProviderRegistry`] on the SAME engine — reusing the compile cache for
    /// the ~21 MB pylon endpoint component.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Load a `duckdb:extension` component, run its `load()`, and return the
    /// functions it registered. The instance is retained for dispatch.
    pub fn load(&self, extension: &str, path: &Path) -> Result<LoadedComponent> {
        let component = Component::from_file(&self.engine, path)
            .map_err(anyhow::Error::from)
            .with_context(|| format!("loading component at {}", path.display()))?;
        // Grant outbound network + name lookup so network-using components (dns,
        // http, httpfs, ...) work. Best-effort, not a sandbox: a component that
        // does not use sockets is unaffected. (A future opt-in gate could mirror
        // the host's DUCKLINK_NETWORK_GRANT.)
        let wasi: WasiCtx = WasiCtxBuilder::new()
            .inherit_env()
            .inherit_stdio()
            .inherit_network()
            .allow_ip_name_lookup(true)
            .build();
        let mut instance = load_component(
            &self.engine,
            &component,
            wasi,
            Box::new(NativeServices),
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
        // Advanced tier: the parser / optimizer / filterable-table markers a
        // component declared. These do not flow through `drain_pending` (which
        // covers the common tier); the runtime exposes them as separate pending
        // queues. The native sink wires each to a C++ shim against DuckDB's
        // internal ABI (see src/advanced.rs).
        let parsers = instance.take_pending_parsers();
        let optimizers = instance.take_pending_optimizers();
        let filterable_tables = instance.take_pending_filterable_tables();
        {
            let mut map = self.instances.write().expect("instances lock poisoned");
            map.insert(extension.to_string(), Arc::new(Mutex::new(instance)));
        }
        Ok(LoadedComponent {
            scalars,
            tables,
            aggregates,
            parsers,
            optimizers,
            filterable_tables,
        })
    }

    /// Advanced tier — PARSER. Offer the rejected statement `sql` to the parser
    /// extension `handle` of component `extension` (the component's own guest
    /// dispatcher handle, captured in its `ParserReg`). Returns `Some(rewrite_sql)`
    /// if the component claims it, `None` if it declines. Unlike the scalar path,
    /// parser/optimizer handles are NOT in the shared callback registry; the
    /// owning component is known directly from the registration, as in the wasm
    /// core's `parser_host` routing.
    pub fn dispatch_parse(
        &self,
        extension: &str,
        handle: u32,
        sql: &str,
    ) -> Result<Option<String>> {
        let instance_arc = self.instance_arc(extension)?;
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        instance
            .call_parse(handle, sql)
            .map_err(|e| anyhow!("parser dispatch failed: {e:?}"))
    }

    /// Advanced tier — OPTIMIZER. Offer the flattened `nodes` (id, op-type,
    /// parent, params-json) + source `query` to the rule `handle` of component
    /// `extension`. Returns `Some(rewrite_sql)` for a `rewrite-query` directive,
    /// else `None`.
    pub fn dispatch_optimize(
        &self,
        extension: &str,
        handle: u32,
        nodes: Vec<(u32, String, Option<u32>, String)>,
        query: &str,
    ) -> Result<Option<String>> {
        let instance_arc = self.instance_arc(extension)?;
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        instance
            .call_optimize(handle, nodes, query)
            .map_err(|e| anyhow!("optimizer dispatch failed: {e:?}"))
    }

    /// Advanced tier — TABLE FILTER PUSHDOWN. Open a streaming cursor on the
    /// filterable table function `handle` of component `extension`, with bound
    /// `args`, a column `projection` (empty = all), and the conjunctive `filters`
    /// (column index, op code 0..8 mirroring `filter-op`, operand values). Returns
    /// the component cursor handle. Drives `call-table-open-filtered`.
    pub fn dispatch_table_open_filtered(
        &self,
        extension: &str,
        handle: u32,
        args: Vec<reg::DuckValue>,
        projection: Vec<u32>,
        filters: Vec<(u32, u8, Vec<reg::DuckValue>)>,
    ) -> Result<u32> {
        use ducklink_runtime::extension::TableFilter;
        let instance_arc = self.instance_arc(extension)?;
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let wit_args: Vec<extension_types::Duckvalue> =
            args.into_iter().map(neutral_to_wit).collect();
        let wit_filters: Vec<TableFilter> = filters
            .into_iter()
            .filter_map(|(column, op, values)| {
                ts_filter_op(op).map(|op| TableFilter {
                    column,
                    op,
                    values: values.into_iter().map(neutral_to_wit).collect(),
                })
            })
            .collect();
        let result = instance
            .table_open_filtered(handle, &wit_args, &projection, &wit_filters)
            .map_err(|e| anyhow!("table-stream open failed: {e:?}"))?;
        Ok(result.cursor)
    }

    /// Advanced tier — pull up to `max_rows` from a streaming cursor as neutral
    /// rows. An empty result signals EOF. Drives `call-table-next`.
    pub fn dispatch_table_next(
        &self,
        extension: &str,
        handle: u32,
        cursor: u32,
        max_rows: u32,
    ) -> Result<Vec<Vec<reg::DuckValue>>> {
        let instance_arc = self.instance_arc(extension)?;
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let rows = instance
            .table_next(handle, cursor, max_rows)
            .map_err(|e| anyhow!("table-stream next failed: {e:?}"))?;
        Ok(rows
            .into_iter()
            .map(|row| row.into_iter().map(wit_to_neutral).collect())
            .collect())
    }

    /// Advanced tier — close a streaming cursor. Drives `call-table-close`.
    pub fn dispatch_table_close(
        &self,
        extension: &str,
        handle: u32,
        cursor: u32,
    ) -> Result<()> {
        let instance_arc = self.instance_arc(extension)?;
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        instance
            .table_close(handle, cursor)
            .map_err(|e| anyhow!("table-stream close failed: {e:?}"))?;
        Ok(())
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
        let (extension, dispatcher_handle) = {
            let registry = self.callbacks.lock().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            (Arc::clone(&entry.extension), entry.dispatcher_handle)
        };
        let instance_arc = self.instance_arc(&extension)?;
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let wit_args: Vec<extension_types::Duckvalue> =
            args.into_iter().map(neutral_to_wit).collect();
        let ctx = extension_runtime::Invokeinfo {
            rowindex: Some(row_index),
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
        let (extension, dispatcher_handle) = {
            let registry = self.callbacks.lock().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            (Arc::clone(&entry.extension), entry.dispatcher_handle)
        };
        let instance_arc = self.instance_arc(&extension)?;
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let ctx = extension_runtime::Invokeinfo {
            rowindex: Some(base_row_index),
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
        let (extension, dispatcher_handle) = {
            let registry = self.callbacks.lock().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            (Arc::clone(&entry.extension), entry.dispatcher_handle)
        };
        let instance_arc = self.instance_arc(&extension)?;
        let mut instance = instance_arc.lock().expect("instance lock poisoned");
        let ctx = extension_runtime::Invokeinfo {
            rowindex: Some(base_row_index),
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
        let (extension, dispatcher_handle) = {
            let registry = self.callbacks.lock().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            (Arc::clone(&entry.extension), entry.dispatcher_handle)
        };
        let instance_arc = self.instance_arc(&extension)?;
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
        let (extension, dispatcher_handle) = {
            let registry = self.callbacks.lock().expect("callback registry poisoned");
            let entry = registry
                .resolve(callback_handle)
                .ok_or_else(|| anyhow!("unknown callback handle {callback_handle}"))?;
            (Arc::clone(&entry.extension), entry.dispatcher_handle)
        };
        let instance_arc = self.instance_arc(&extension)?;
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
}

/// Map a C-ABI ts-op code (DUCKLINK_TS_OP_*, mirroring `filter-op`) to the WIT
/// `FilterOp`. Unknown codes are dropped (the engine re-checks the real filter
/// above the scan, so dropping a clause forgoes pruning, never correctness).
fn ts_filter_op(op: u8) -> Option<ducklink_runtime::extension::FilterOp> {
    use ducklink_runtime::extension::FilterOp as Op;
    Some(match op {
        0 => Op::Eq,
        1 => Op::Ne,
        2 => Op::Lt,
        3 => Op::Le,
        4 => Op::Gt,
        5 => Op::Ge,
        6 => Op::IsIn,
        7 => Op::IsNull,
        8 => Op::IsNotNull,
        _ => return None,
    })
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
        reg::DuckValue::Complex { type_expr, json } => {
            extension_types::Duckvalue::Complex(extension_types::Complexvalue { type_expr, json })
        }
    }
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
        extension_types::Duckvalue::Complex(c) => reg::DuckValue::Complex {
            type_expr: c.type_expr,
            json: c.json,
        },
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

    /// The 22 variants must all be distinct after a round-trip (no two collapse
    /// onto the same WIT arm), so the count of unique debug renderings is stable.
    #[test]
    fn roundtrip_preserves_distinctness() {
        let mut seen = std::collections::HashSet::new();
        for v in all_variants() {
            let s = format!("{:?}", wit_to_neutral(neutral_to_wit(v)));
            assert!(seen.insert(s.clone()), "two variants collapsed to {s}");
        }
        assert_eq!(seen.len(), 22, "expected 22 distinct DuckValue variants");
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
