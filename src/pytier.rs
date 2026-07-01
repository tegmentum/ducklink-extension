//! The ducklink Python source tier: `ducklink_run('<script.py>')`.
//!
//! Phase-1 MVP. A user authors a DuckDB extension in plain Python using the
//! `ducklink` authoring SDK (`@ducklink.scalar`, ...). `ducklink_run` loads that
//! `.py` into a RESIDENT CPython interpreter running in wasm — the pylon-endpoint
//! `compose:dynlink/endpoint` provider — reads the manifest the decorators built,
//! and registers each authored function as a real DuckDB SQL function whose
//! dispatch closure calls back into the resident interpreter per row.
//!
//! ## Mechanism
//!
//!   run      -> `runtime.load`      import the script; @ducklink decorators fire
//!   manifest -> `runtime.manifest`  read the JSON-able registry
//!   dispatch -> `offload_arrow`     apply `module:callable` over one Arrow column
//!                                   batch per DuckDB DataChunk (arrow-columnar)
//!
//! The dispatch is ARROW-COLUMNAR: [`PyScalar::invoke`] reads the whole
//! DataChunk's argument columns (with validity) into Arrow arrays, serializes
//! them to ONE Arrow IPC STREAM, and crosses
//! `compose:dynlink/endpoint.handle("offload_arrow", payload)` ONCE per chunk
//! (not once per row). The guest applies the fn row-wise with DuckDB NULL
//! semantics and returns a one-column (`result`) Arrow IPC stream, decoded back
//! into the output vector. The arrow bytes ride inside a small msgpack envelope
//! (`{entry, arrow}`); the per-row msgpack `offload` path is retained as
//! [`invoke_per_row`] for fallback / unsupported types.
//!
//! ## Where the dispatch logic lives
//!
//! The pylon endpoint is GENERIC — it carries zero ducklink code. Its reactor
//! shim imports a `pylon_endpoint` dispatcher module from its `/app` preopen at
//! runtime (the `.py` is not baked into the component), so the HOST decides the
//! dispatcher by what it mounts at `/app`. ducklink therefore ships its OWN
//! dispatcher (`pylib/pylon_endpoint.py` in this crate), which implements the
//! `runtime.load` / `runtime.manifest` / `offload` methods, and stages it into
//! `/app` alongside the ducklink SDK + the user script (see [`stage_app_env`]).
//! A plain `pylon-endpoint.component.wasm` from pylon's `main` thus serves the
//! ducklink Python source tier with no pylon-side ducklink code.
//!
//! ## Residency
//!
//! The pylon endpoint (~21 MB) is instantiated ONCE via a
//! [`ProviderRegistry`]/[`ResidentBackend`] (the same machinery ducklink already
//! uses for aggregate providers) and reused across every `runtime.*`/`offload`
//! call — the CPython interpreter warms once and serves every registered
//! function. All access is synchronous (ducklink's host is the sync
//! `ResidentBackend`, no tokio).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

use duckdb::core::{DataChunkHandle, FlatVector, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::ffi;
use duckdb::ffi::duckdb_string_t;
use duckdb::types::DuckString;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::vtab::arrow::WritableVector;
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::Connection;

use ducklink_runtime::compose_dynlink::{ProviderPreopen, ProviderRegistry, ResidentBackend};
use ducklink_runtime::datalink_dynlink::{ProviderBackend, ResidentHandle};

/// The provider id under which the pylon endpoint is registered. Arbitrary but
/// stable — one resident interpreter serves every `ducklink_run` in the process.
const PYLON_ID: &str = "pylon";

/// Where to find the pylon endpoint component + CPython Lib dir. Overridable via
/// env so a build/test can point at a locally-built artifact; the defaults match
/// the python-wasm build tree.
///
/// The baked default is the LEAN-ARROW endpoint variant (lean CPython + `_struct`
/// + `_arrow_core`, no numpy). `_struct` is what the pure-Python `struct` — and
/// hence the ducklink-staged `pylib/_msgpack.py` — needs, so this variant is what
/// makes the inline-deps in-guest `import` step and the arrow-columnar dispatch
/// path work end-to-end. Override via `DUCKLINK_PYLON_ENDPOINT`.
fn pylon_component_path() -> PathBuf {
    std::env::var_os("DUCKLINK_PYLON_ENDPOINT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").unwrap_or_default();
            PathBuf::from(home).join(
                "git/python-wasm/build/3.14-current/pylon-endpoint-lean-arrow.component.wasm",
            )
        })
}

fn cpython_lib_dir() -> PathBuf {
    std::env::var_os("DUCKLINK_PYLON_LIB")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").unwrap_or_default();
            PathBuf::from(home).join("git/python-wasm/deps/cpython-3.14/Lib")
        })
}

/// The ducklink Python authoring SDK to stage into the script env, so a user
/// script's `import ducklink` resolves inside the guest. Overridable via env.
fn ducklink_sdk_dir() -> PathBuf {
    std::env::var_os("DUCKLINK_PYTHON_SDK")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").unwrap_or_default();
            PathBuf::from(home).join("git/ducklink/python-sdk/ducklink")
        })
}

/// The DUCKLINK-OWNED dispatcher pylib (`pylon_endpoint.py` + `_msgpack.py`) to
/// stage into the `/app` preopen.
///
/// The pylon reactor shim imports a module named `pylon_endpoint` from `/app` at
/// runtime — the `.py` is NOT baked into the component, so the HOST controls the
/// dispatcher by what it mounts there. ducklink therefore ships its OWN
/// dispatcher (this crate's `pylib/`), which implements `runtime.load` /
/// `runtime.manifest` / `offload`. That keeps the pylon endpoint generic (it
/// carries zero ducklink code); a plain `pylon-endpoint.component.wasm` from
/// pylon's `main` serves the ducklink Python source tier. Overridable via env
/// (`DUCKLINK_PYLON_PYLIB`); the default is this crate's `pylib/` dir, resolved
/// from `CARGO_MANIFEST_DIR` (baked in at compile time).
fn pylon_pylib_dir() -> PathBuf {
    std::env::var_os("DUCKLINK_PYLON_PYLIB")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("pylib"))
}

/// Process-wide resident pylon runtime for the Python source tier. Holds the
/// provider registry + a warmed handle (instantiate-once), plus the `/app`
/// staging dir the guest imports from. Constructed lazily on the first
/// `ducklink_run` (the registry needs a wasmtime engine, obtained from the
/// shared `Engine2`).
pub struct PylonRuntime {
    backend: ResidentBackend,
    /// The warmed instance handle (resolving materializes the resident provider
    /// ONCE; every later `offload` reuses it).
    handle: Mutex<Option<ResidentHandle>>,
    /// The host dir mounted at `/app` in the guest (dispatcher + SDK + scripts).
    app_dir: PathBuf,
}

static PYLON: OnceLock<PylonRuntime> = OnceLock::new();

impl PylonRuntime {
    /// Get (constructing on first use) the process-wide pylon runtime. The
    /// wasmtime engine is cloned from the shared `Engine2` so the ~21 MB pylon
    /// component reuses the on-disk compile cache.
    fn get_or_init() -> Result<&'static PylonRuntime, String> {
        if let Some(rt) = PYLON.get() {
            return Ok(rt);
        }
        let rt = Self::build()?;
        // First caller wins; a racing caller's `rt` is simply dropped.
        let _ = PYLON.set(rt);
        Ok(PYLON.get().expect("PYLON set above"))
    }

    fn build() -> Result<PylonRuntime, String> {
        // The engine to compile the provider on: reuse the Direction-2 engine so
        // the pylon component shares the compile cache. The Python tier only
        // registers on the runtime path (a `ducklink_run` call), by which time
        // `LOAD ducklink` has created the `RUNTIME` (and thus the `Engine2`).
        let engine = crate::reg_duckdb::ducklink_engine()
            .ok_or_else(|| "ducklink_run: runtime not initialised (LOAD ducklink first)".to_string())?;

        let component = pylon_component_path();
        if !component.exists() {
            return Err(format!(
                "ducklink_run: pylon endpoint component not found at {} \
                 (set DUCKLINK_PYLON_ENDPOINT)",
                component.display()
            ));
        }
        let lib = cpython_lib_dir();
        if !lib.exists() {
            return Err(format!(
                "ducklink_run: CPython Lib dir not found at {} (set DUCKLINK_PYLON_LIB)",
                lib.display()
            ));
        }

        // Stage the `/app` env: the pylon dispatcher (pylon_endpoint.py +
        // _msgpack.py) + the ducklink SDK. User scripts are copied in per-call.
        let app_dir = stage_app_env()?;

        let registry = ProviderRegistry::new(engine);
        registry
            .register_provider_with_preopens(
                PYLON_ID,
                &component,
                vec![
                    ProviderPreopen::new(lib, "/lib"),
                    ProviderPreopen::new(&app_dir, "/app"),
                ],
            )
            .map_err(|e| format!("ducklink_run: register pylon provider: {e}"))?;

        Ok(PylonRuntime {
            backend: ResidentBackend::new(registry),
            handle: Mutex::new(None),
            app_dir,
        })
    }

    /// Warm the provider (idempotent): materialize the resident interpreter ONCE
    /// and cache the handle. Every later `offload` reuses the same interpreter.
    fn warm(&self) -> Result<ResidentHandle, String> {
        let mut guard = self.handle.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(h) = guard.as_ref() {
            return Ok(h.clone());
        }
        let h = self
            .backend
            .resolve_by_id(PYLON_ID)
            .map_err(|e| format!("ducklink_run: warm pylon: {e:?}"))?;
        *guard = Some(h.clone());
        Ok(h)
    }

    /// Call the resident endpoint's `handle(method, payload)` synchronously.
    fn call(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>, String> {
        let handle = self.warm()?;
        self.backend
            .invoke(&handle, method, payload)
            .map_err(|e| format!("ducklink_run: {method}: {e:?}"))
    }
}

/// Create a fresh per-process `/app` staging dir under the system temp dir,
/// copying in the DUCKLINK-OWNED dispatcher pylib and the ducklink SDK. User
/// scripts are added into this same dir per-call (so `import <script>` resolves
/// alongside `import ducklink`). Returns the host path mounted at `/app`.
///
/// The dispatcher (`pylon_endpoint.py`) is ducklink's own — NOT pylon's — so the
/// pylon endpoint stays generic. The reactor shim imports `pylon_endpoint` from
/// `/app`, so mounting ours here is what wires the ducklink `runtime.*`/`offload`
/// methods into an otherwise-plain pylon interpreter.
fn stage_app_env() -> Result<PathBuf, String> {
    let base = std::env::temp_dir().join(format!("ducklink-pytier-{}", std::process::id()));
    let app = base.join("app");
    std::fs::create_dir_all(&app).map_err(|e| format!("create {}: {e}", app.display()))?;

    // The ducklink-owned dispatcher + its msgpack codec (imported by the reactor
    // shim as `pylon_endpoint` from /app). Both are shipped in this crate's
    // `pylib/`, so a missing file is a build/packaging error, not a soft skip.
    let pylib = pylon_pylib_dir();
    for f in ["pylon_endpoint.py", "_msgpack.py"] {
        let src = pylib.join(f);
        if !src.exists() {
            return Err(format!(
                "ducklink_run: dispatcher asset {} not found (set DUCKLINK_PYLON_PYLIB)",
                src.display()
            ));
        }
        std::fs::copy(&src, app.join(f)).map_err(|e| format!("stage {}: {e}", src.display()))?;
    }

    // The ducklink authoring SDK package -> /app/ducklink.
    let sdk = ducklink_sdk_dir();
    if sdk.exists() {
        copy_dir(&sdk, &app.join("ducklink"))?;
    } else {
        return Err(format!(
            "ducklink_run: ducklink Python SDK not found at {} (set DUCKLINK_PYTHON_SDK)",
            sdk.display()
        ));
    }

    // The PEP 723 inline-dependency staging dir. Created up front (empty) so it is
    // present under the `/app` preopen from first instantiation; the dispatcher
    // prepends `/app/site-packages` to `sys.path`, and `ducklink_run`'s bind
    // unzips a script's resolved pure-Python wheels into this SAME host dir before
    // the script is imported (the preopen is a live host dir, so files added
    // per-call are visible to the resident interpreter).
    std::fs::create_dir_all(app.join(SITE_PACKAGES_DIR))
        .map_err(|e| format!("create {}: {e}", app.join(SITE_PACKAGES_DIR).display()))?;
    Ok(app)
}

/// The `site-packages` subdirectory (under the `/app` preopen) that a script's
/// PEP 723 pure-Python dependencies are unzipped into; the pylon dispatcher adds
/// `/app/site-packages` to `sys.path`.
const SITE_PACKAGES_DIR: &str = "site-packages";

/// Recursively copy `src` dir into `dst` (skipping `__pycache__`).
fn copy_dir(src: &Path, dst: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dst).map_err(|e| format!("create {}: {e}", dst.display()))?;
    for entry in std::fs::read_dir(src).map_err(|e| format!("read {}: {e}", src.display()))? {
        let entry = entry.map_err(|e| format!("read entry: {e}"))?;
        let name = entry.file_name();
        if name == "__pycache__" {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        let ft = entry.file_type().map_err(|e| format!("file type: {e}"))?;
        if ft.is_dir() {
            copy_dir(&from, &to)?;
        } else {
            std::fs::copy(&from, &to).map_err(|e| format!("copy {}: {e}", from.display()))?;
        }
    }
    Ok(())
}

/// Stage a user script into the `/app` env and return the module name to import
/// (the file stem). The script becomes importable as `<stem>` inside the guest.
fn stage_script(app_dir: &Path, script_path: &Path) -> Result<String, String> {
    let stem = script_path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| format!("ducklink_run: bad script path {}", script_path.display()))?
        .to_string();
    let dst = app_dir.join(format!("{stem}.py"));
    std::fs::copy(script_path, &dst)
        .map_err(|e| format!("ducklink_run: stage script {}: {e}", script_path.display()))?;
    Ok(stem)
}

/// Parse the script's PEP 723 inline dependencies, resolve each to a pure-Python
/// wheel on PyPI, and unzip them into the resident interpreter's
/// `/app/site-packages` (Part 1). Returns the `name version` strings staged (for
/// the summary/log). A native/C-extension-only dependency fails with a clear
/// Phase-5-boundary error. No PEP 723 block / no `dependencies` key -> nothing
/// staged (the common case).
fn stage_script_dependencies(rt: &PylonRuntime, script_path: &Path) -> Result<Vec<String>, String> {
    let source = std::fs::read_to_string(script_path)
        .map_err(|e| format!("ducklink_run: read {}: {e}", script_path.display()))?;
    let reqs = crate::pydeps::parse_dependencies(&source)?;
    if reqs.is_empty() {
        return Ok(Vec::new());
    }
    let site_packages = rt.app_dir.join(SITE_PACKAGES_DIR);
    let staged = crate::pydeps::stage_dependencies(&reqs, &site_packages)?;
    Ok(staged
        .into_iter()
        .map(|(n, v)| format!("{n} {v}"))
        .collect())
}

// ---------------------------------------------------------------------------
// msgpack helpers (the offload envelope)
// ---------------------------------------------------------------------------

fn mp_encode(v: &rmpv::Value) -> Vec<u8> {
    let mut out = Vec::new();
    rmpv::encode::write_value(&mut out, v).expect("msgpack encode into Vec is infallible");
    out
}

fn mp_decode(bytes: &[u8]) -> Result<rmpv::Value, String> {
    rmpv::decode::read_value(&mut &bytes[..]).map_err(|e| format!("msgpack decode: {e}"))
}

/// Build the `runtime.load` payload: `{"module": "<stem>"}`.
fn load_payload(module: &str) -> Vec<u8> {
    mp_encode(&rmpv::Value::Map(vec![(
        rmpv::Value::from("module"),
        rmpv::Value::from(module),
    )]))
}

/// Build an `offload` payload: `{"entry": "<mod:fn>", "args": [...]}`.
fn offload_payload(entry: &str, args: Vec<rmpv::Value>) -> Vec<u8> {
    mp_encode(&rmpv::Value::Map(vec![
        (rmpv::Value::from("entry"), rmpv::Value::from(entry)),
        (rmpv::Value::from("args"), rmpv::Value::Array(args)),
    ]))
}

/// Build an `offload_arrow` payload: `{"entry": "<mod:fn>", "arrow": <ipc bytes>}`.
/// The arrow bytes are one Arrow IPC STREAM carrying the whole DataChunk's
/// argument columns (`arg0`, `arg1`, ...); the guest applies the fn row-wise and
/// returns one Arrow IPC stream with a single `result` column.
fn offload_arrow_payload(entry: &str, arrow: Vec<u8>) -> Vec<u8> {
    mp_encode(&rmpv::Value::Map(vec![
        (rmpv::Value::from("entry"), rmpv::Value::from(entry)),
        (rmpv::Value::from("arrow"), rmpv::Value::Binary(arrow)),
    ]))
}

// ---------------------------------------------------------------------------
// arrow-columnar dispatch: encode the DataChunk's arg columns into one Arrow
// IPC stream, decode the returned single-column result stream.
// ---------------------------------------------------------------------------

/// Read column `j` of the DataChunk (type `ty`, length `len`, validity mask
/// `validity`) into an Arrow array. NULLs (per the validity bitmap; a null
/// pointer means all-valid) become Arrow nulls, so the guest sees `None` and
/// applies DuckDB NULL semantics. Reading a NULL VARCHAR cell's `duckdb_string_t`
/// would deref garbage, so a NULL row is appended as null WITHOUT touching the
/// slot.
fn column_to_arrow(
    ty: PyType,
    col: &FlatVector,
    len: usize,
    validity: *const u64,
) -> ArrayRef {
    let valid = |i: usize| validity.is_null() || unsafe { row_valid(validity, i) };
    match ty {
        PyType::Varchar => {
            let s = unsafe { col.as_slice_with_len::<duckdb_string_t>(len) };
            let mut b = StringBuilder::new();
            for i in 0..len {
                if valid(i) {
                    let mut t = s[i];
                    b.append_value(DuckString::new(&mut t).as_str());
                } else {
                    b.append_null();
                }
            }
            Arc::new(b.finish())
        }
        PyType::Bigint => {
            let s = unsafe { col.as_slice_with_len::<i64>(len) };
            let mut b = Int64Builder::with_capacity(len);
            for i in 0..len {
                if valid(i) {
                    b.append_value(s[i]);
                } else {
                    b.append_null();
                }
            }
            Arc::new(b.finish())
        }
        PyType::Double => {
            let s = unsafe { col.as_slice_with_len::<f64>(len) };
            let mut b = Float64Builder::with_capacity(len);
            for i in 0..len {
                if valid(i) {
                    b.append_value(s[i]);
                } else {
                    b.append_null();
                }
            }
            Arc::new(b.finish())
        }
        PyType::Boolean => {
            let s = unsafe { col.as_slice_with_len::<bool>(len) };
            let mut b = BooleanBuilder::with_capacity(len);
            for i in 0..len {
                if valid(i) {
                    b.append_value(s[i]);
                } else {
                    b.append_null();
                }
            }
            Arc::new(b.finish())
        }
    }
}

fn arrow_field(ty: PyType, name: &str) -> Field {
    let dt = match ty {
        PyType::Varchar => DataType::Utf8,
        PyType::Bigint => DataType::Int64,
        PyType::Double => DataType::Float64,
        PyType::Boolean => DataType::Boolean,
    };
    Field::new(name, dt, true)
}

/// Serialize the argument columns into one Arrow IPC STREAM (columns `arg0`,
/// `arg1`, ...). One stream per DataChunk -> one WIT crossing per chunk.
fn encode_arg_batch(
    arg_types: &[PyType],
    cols: &[FlatVector],
    len: usize,
    validities: &[*const u64],
) -> Result<Vec<u8>, String> {
    let mut fields = Vec::with_capacity(arg_types.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(arg_types.len());
    for (j, &ty) in arg_types.iter().enumerate() {
        fields.push(arrow_field(ty, &format!("arg{j}")));
        arrays.push(column_to_arrow(ty, &cols[j], len, validities[j]));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| format!("arrow: build arg batch: {e}"))?;
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| format!("arrow: stream writer: {e}"))?;
        w.write(&batch).map_err(|e| format!("arrow: write batch: {e}"))?;
        w.finish().map_err(|e| format!("arrow: finish stream: {e}"))?;
    }
    Ok(buf)
}

/// Decode the returned Arrow IPC stream (one `result` column of `expect_len`
/// rows) into the output FlatVector per `ret`. Concatenates all batches the
/// guest emitted (it emits one, but the reader is batch-agnostic). NULLs come
/// from the Arrow validity bitmap -> `out.set_null(i)`.
fn decode_result_into(
    ret: PyType,
    bytes: &[u8],
    out: &mut FlatVector,
    expect_len: usize,
) -> Result<(), String> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| format!("arrow: result stream reader: {e}"))?;
    let mut row = 0usize;
    for batch in reader {
        let batch = batch.map_err(|e| format!("arrow: read result batch: {e}"))?;
        if batch.num_columns() != 1 {
            return Err(format!(
                "arrow: result batch must have 1 column, got {}",
                batch.num_columns()
            ));
        }
        let arr = batch.column(0);
        let n = arr.len();
        match ret {
            PyType::Varchar => {
                let a = arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| format!("arrow: expected utf8 result, got {}", arr.data_type()))?;
                for i in 0..n {
                    if a.is_null(i) {
                        out.set_null(row + i);
                    } else {
                        out.insert(row + i, a.value(i));
                    }
                }
            }
            PyType::Bigint => {
                let a = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| format!("arrow: expected int64 result, got {}", arr.data_type()))?;
                // Set nulls first (needs &mut out), then fill values via the slice
                // (a distinct &mut borrow) — the two can't overlap.
                for i in 0..n {
                    if a.is_null(i) {
                        out.set_null(row + i);
                    }
                }
                let slot = unsafe { out.as_mut_slice::<i64>() };
                for i in 0..n {
                    if !a.is_null(i) {
                        slot[row + i] = a.value(i);
                    }
                }
            }
            PyType::Double => {
                let a = arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| format!("arrow: expected float64 result, got {}", arr.data_type()))?;
                for i in 0..n {
                    if a.is_null(i) {
                        out.set_null(row + i);
                    }
                }
                let slot = unsafe { out.as_mut_slice::<f64>() };
                for i in 0..n {
                    if !a.is_null(i) {
                        slot[row + i] = a.value(i);
                    }
                }
            }
            PyType::Boolean => {
                let a = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| format!("arrow: expected bool result, got {}", arr.data_type()))?;
                for i in 0..n {
                    if a.is_null(i) {
                        out.set_null(row + i);
                    }
                }
                let slot = unsafe { out.as_mut_slice::<bool>() };
                for i in 0..n {
                    if !a.is_null(i) {
                        slot[row + i] = a.value(i);
                    }
                }
            }
        }
        row += n;
    }
    if row != expect_len {
        return Err(format!(
            "arrow: result length {row} != chunk length {expect_len}"
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// manifest
// ---------------------------------------------------------------------------

/// A minimal MVP type set: the SQL type names the SDK's manifest emits, mapped
/// to a DuckDB logical type + a marshalling code. Extending the tier = more
/// arms here + in the marshal/unmarshal below.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PyType {
    Varchar,
    Bigint,
    Double,
    Boolean,
}

impl PyType {
    fn parse(name: &str) -> Option<PyType> {
        match name.to_ascii_uppercase().as_str() {
            "VARCHAR" | "TEXT" | "STRING" => Some(PyType::Varchar),
            "BIGINT" | "INT64" | "INT" | "INTEGER" => Some(PyType::Bigint),
            "DOUBLE" | "FLOAT64" | "FLOAT" | "REAL" => Some(PyType::Double),
            "BOOLEAN" | "BOOL" => Some(PyType::Boolean),
            _ => None,
        }
    }

    fn logical(self) -> LogicalTypeHandle {
        LogicalTypeHandle::from(match self {
            PyType::Varchar => LogicalTypeId::Varchar,
            PyType::Bigint => LogicalTypeId::Bigint,
            PyType::Double => LogicalTypeId::Double,
            PyType::Boolean => LogicalTypeId::Boolean,
        })
    }
}

/// One authored scalar the manifest declared, distilled to what registration +
/// dispatch need. (Table/aggregate kinds are recognized but skipped in the MVP.)
#[derive(Clone)]
struct PyScalarSig {
    name: String,
    entry: String,
    args: Vec<PyType>,
    ret: PyType,
}

/// Parse the msgpack manifest (a list of maps) into the scalar signatures the
/// MVP can register. Non-scalar kinds and unsupported types are skipped with a
/// note (so a mixed script still registers what it can).
fn parse_manifest(v: &rmpv::Value) -> Vec<PyScalarSig> {
    let mut out = Vec::new();
    let Some(entries) = v.as_array() else {
        return out;
    };
    for e in entries {
        let Some(map) = e.as_map() else { continue };
        let get = |k: &str| {
            map.iter()
                .find(|(kk, _)| kk.as_str() == Some(k))
                .map(|(_, vv)| vv)
        };
        let name = get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let entry = get("entry").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let kind = get("kind").and_then(|v| v.as_str()).unwrap_or("");
        if kind != "scalar" {
            eprintln!("[ducklink] ducklink_run: skipping '{name}' (kind '{kind}' not yet supported in the MVP; scalar only)");
            continue;
        }
        let ret = get("returns")
            .and_then(|v| v.as_str())
            .and_then(PyType::parse);
        let Some(ret) = ret else {
            eprintln!("[ducklink] ducklink_run: skipping scalar '{name}' (unsupported return type)");
            continue;
        };
        let mut args = Vec::new();
        let mut ok = true;
        if let Some(a) = get("arguments").and_then(|v| v.as_array()) {
            for arg in a {
                let ty = arg
                    .as_map()
                    .and_then(|m| m.iter().find(|(k, _)| k.as_str() == Some("type")))
                    .and_then(|(_, v)| v.as_str())
                    .and_then(PyType::parse);
                match ty {
                    Some(t) => args.push(t),
                    None => {
                        ok = false;
                        break;
                    }
                }
            }
        }
        if !ok || name.is_empty() || entry.is_empty() {
            eprintln!("[ducklink] ducklink_run: skipping scalar '{name}' (unsupported argument type)");
            continue;
        }
        out.push(PyScalarSig { name, entry, args, ret });
    }
    out
}

// ---------------------------------------------------------------------------
// PyScalar: per-row offload dispatch
// ---------------------------------------------------------------------------

/// Per-function state DuckDB hands to `PyScalar::invoke`: the resident pylon
/// runtime (to drive `offload`), the manifest `entry` string, and the arg/return
/// types (for marshalling). One `PyScalar` impl serves every authored scalar.
#[derive(Clone)]
struct PyScalarState {
    entry: String,
    args: Vec<PyType>,
    ret: PyType,
}

// The SQL signature is static per `VScalar::signatures()` (no access to state),
// so it is threaded in via a thread-local, set immediately before the
// (synchronous) registration call — mirroring reg_duckdb's WasmScalar.
thread_local! {
    static PENDING_PY_SIG: std::cell::RefCell<Option<(Vec<PyType>, PyType)>> =
        const { std::cell::RefCell::new(None) };
}

struct PyScalar;

/// The per-row msgpack `offload` dispatch — RETAINED as a fallback for types the
/// arrow-columnar path does not (yet) cover. One WIT crossing per row: read each
/// arg cell to msgpack, `offload`, decode the scalar result. `PyScalar::invoke`
/// now defaults to the arrow-columnar path (one crossing per chunk); this stays
/// wired so a future unsupported-type arm can route to it.
#[allow(dead_code)]
fn invoke_per_row(
    rt: &PylonRuntime,
    state: &PyScalarState,
    cols: &[FlatVector],
    validities: &[*const u64],
    len: usize,
    out: &mut FlatVector,
) -> Result<(), Box<dyn std::error::Error>> {
    let arity = state.args.len();
    for i in 0..len {
        let row_null = (0..arity).any(|j| {
            let val = validities[j];
            !val.is_null() && unsafe { !row_valid(val, i) }
        });
        if row_null {
            out.set_null(i);
            continue;
        }
        let args: Vec<rmpv::Value> = (0..arity)
            .map(|j| read_cell(state.args[j], &cols[j], i))
            .collect();
        let payload = offload_payload(&state.entry, args);
        let resp = rt.call("offload", &payload).map_err(to_boxed)?;
        let val = mp_decode(&resp).map_err(to_boxed)?;
        if let rmpv::Value::Nil = val {
            out.set_null(i);
        } else {
            write_cell(state.ret, out, i, &val)?;
        }
    }
    Ok(())
}

impl VScalar for PyScalar {
    type State = PyScalarState;

    fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let rt = PylonRuntime::get_or_init().map_err(to_boxed)?;
        let len = input.len();
        let arity = state.args.len();
        let cols: Vec<FlatVector> = (0..arity).map(|j| input.flat_vector(j)).collect();
        // Fetch each column's validity mask once (null when the column has no
        // NULLs). Reading a NULL VARCHAR row's duckdb_string_t would dereference
        // garbage, so a NULL input row yields NULL (DuckDB scalar semantics) and
        // the target fn is never called for it — enforced guest-side.
        let raw_chunk = input.get_ptr();
        let validities: Vec<*const u64> = (0..arity)
            .map(|j| unsafe {
                let v = ffi::duckdb_data_chunk_get_vector(raw_chunk, j as u64);
                ffi::duckdb_vector_get_validity(v) as *const u64
            })
            .collect();
        let mut out = output.flat_vector();

        // ARROW-COLUMNAR dispatch: one WIT crossing per DataChunk. Serialize the
        // whole chunk's argument columns into a single Arrow IPC stream, offload
        // once, decode the returned `result` column back into the output vector.
        // (The per-row msgpack `offload` remains as `invoke_per_row` for
        // unsupported/fallback types.)
        let arrow = encode_arg_batch(&state.args, &cols, len, &validities).map_err(to_boxed)?;
        let payload = offload_arrow_payload(&state.entry, arrow);
        let resp = rt.call("offload_arrow", &payload).map_err(to_boxed)?;
        decode_result_into(state.ret, &resp, &mut out, len).map_err(to_boxed)?;
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        let (args, ret) = PENDING_PY_SIG
            .with(|s| s.borrow().clone())
            .expect("PENDING_PY_SIG must be set before registration");
        vec![ScalarFunctionSignature::exact(
            args.into_iter().map(|t| t.logical()).collect(),
            ret.logical(),
        )]
    }
}

fn to_boxed(s: String) -> Box<dyn std::error::Error> {
    s.into()
}

/// True if row `r` is valid (non-NULL) under DuckDB's validity bitmask. A null
/// `validity` pointer means the whole column is valid.
///
/// # Safety
/// `validity`, when non-null, must point to a mask with at least `r + 1` rows.
#[inline]
unsafe fn row_valid(validity: *const u64, r: usize) -> bool {
    *validity.add(r / 64) & (1u64 << (r % 64)) != 0
}

/// Read row `i` of a column into a msgpack value per the argument type.
fn read_cell(ty: PyType, col: &FlatVector, i: usize) -> rmpv::Value {
    match ty {
        PyType::Varchar => {
            let s = unsafe { col.as_slice_with_len::<duckdb_string_t>(i + 1) };
            let mut t = s[i];
            rmpv::Value::from(DuckString::new(&mut t).as_str().into_owned())
        }
        PyType::Bigint => {
            let s = unsafe { col.as_slice_with_len::<i64>(i + 1) };
            rmpv::Value::from(s[i])
        }
        PyType::Double => {
            let s = unsafe { col.as_slice_with_len::<f64>(i + 1) };
            rmpv::Value::from(s[i])
        }
        PyType::Boolean => {
            let s = unsafe { col.as_slice_with_len::<bool>(i + 1) };
            rmpv::Value::from(s[i])
        }
    }
}

/// Write a msgpack result value into row `i` of the output vector per the return
/// type.
fn write_cell(
    ty: PyType,
    out: &mut FlatVector,
    i: usize,
    val: &rmpv::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    match ty {
        PyType::Varchar => {
            let s = val
                .as_str()
                .ok_or_else(|| -> Box<dyn std::error::Error> {
                    format!("ducklink_run: expected VARCHAR result, got {val}").into()
                })?;
            out.insert(i, s);
        }
        PyType::Bigint => {
            let n = val
                .as_i64()
                .ok_or_else(|| -> Box<dyn std::error::Error> {
                    format!("ducklink_run: expected BIGINT result, got {val}").into()
                })?;
            unsafe { out.as_mut_slice::<i64>()[i] = n };
        }
        PyType::Double => {
            let f = val.as_f64().or_else(|| val.as_i64().map(|n| n as f64)).ok_or_else(
                || -> Box<dyn std::error::Error> {
                    format!("ducklink_run: expected DOUBLE result, got {val}").into()
                },
            )?;
            unsafe { out.as_mut_slice::<f64>()[i] = f };
        }
        PyType::Boolean => {
            let b = val
                .as_bool()
                .ok_or_else(|| -> Box<dyn std::error::Error> {
                    format!("ducklink_run: expected BOOLEAN result, got {val}").into()
                })?;
            unsafe { out.as_mut_slice::<bool>()[i] = b };
        }
    }
    Ok(())
}

/// Register every parsed scalar on `con`. Returns the count registered.
/// Idempotent: a duplicate name is logged + skipped (so a re-`ducklink_run` of
/// the same script does not fail).
fn register_py_scalars(con: &Connection, sigs: &[PyScalarSig]) -> duckdb::Result<usize> {
    let mut registered = 0usize;
    for f in sigs {
        let state = PyScalarState {
            entry: f.entry.clone(),
            args: f.args.clone(),
            ret: f.ret,
        };
        PENDING_PY_SIG.with(|s| *s.borrow_mut() = Some((f.args.clone(), f.ret)));
        let result = con.register_scalar_function_with_state::<PyScalar>(&f.name, &state);
        PENDING_PY_SIG.with(|s| *s.borrow_mut() = None);
        match result {
            Ok(()) => registered += 1,
            Err(e) => {
                eprintln!("[ducklink] ducklink_run scalar '{}' not registered (already present?): {e}", f.name);
            }
        }
    }
    Ok(registered)
}

// ---------------------------------------------------------------------------
// ducklink_run table function
// ---------------------------------------------------------------------------

struct WasmRunBind {
    path: String,
    module: String,
    scalars: usize,
    /// Names registered, for the summary row.
    names: String,
}

struct WasmRunInit {
    done: AtomicUsize,
}

/// The `ducklink_run('<script.py>')` table function. Its `bind` warms the
/// resident interpreter, loads the script, reads the manifest, and registers
/// each authored scalar; `func` streams back a single summary row.
struct WasmRun;

impl VTab for WasmRun {
    type InitData = WasmRunInit;
    type BindData = WasmRunBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let arg = bind.get_parameter(0).to_string();

        // Argument 0 is EITHER a filesystem PATH to a `.py` OR an `http(s)://…`
        // URL (the load-your-own / unsigned path). A URL is downloaded + cached
        // (opt-in via DUCKLINK_ALLOW_URL, optional `sha256 :=` verify) and then
        // run exactly like a local script.
        let script_path: PathBuf = if crate::url_fetch::is_http_url(&arg) {
            let sha = bind.get_named_parameter("sha256").map(|v| v.to_string());
            crate::url_fetch::resolve_url_to_cache(
                "ducklink_run",
                &arg,
                "py",
                sha.as_deref(),
            )
            .map_err(to_boxed)?
        } else {
            let p = PathBuf::from(&arg);
            if !p.exists() {
                return Err(format!("ducklink_run: script not found: {}", p.display()).into());
            }
            p
        };

        let rt = PylonRuntime::get_or_init().map_err(to_boxed)?;

        // 1. Resolve + stage the script's PEP 723 inline dependencies (Part 1).
        //    Parse the `# /// script` block, resolve each requirement to a
        //    PURE-PYTHON wheel on PyPI, and unzip it into the `/app/site-packages`
        //    dir the resident interpreter imports from — BEFORE the script is
        //    loaded, so its `import <dep>` resolves. A native/C-extension-only dep
        //    fails here with a clear Phase-5-boundary message.
        let deps_staged = stage_script_dependencies(rt, &script_path).map_err(to_boxed)?;

        // 2. Stage the user script into the resident interpreter's /app env.
        let module = stage_script(&rt.app_dir, &script_path).map_err(to_boxed)?;

        // 3. runtime.load: import the script so its @ducklink decorators fire.
        let n_raw = rt.call("runtime.load", &load_payload(&module)).map_err(to_boxed)?;
        let n_loaded = mp_decode(&n_raw)?.as_i64().unwrap_or(-1);
        eprintln!("[ducklink] ducklink_run: loaded '{module}' -> {n_loaded} function(s) authored");
        if !deps_staged.is_empty() {
            eprintln!("[ducklink] ducklink_run: staged {} PEP 723 dep(s): {}", deps_staged.len(), deps_staged.join(", "));
        }

        // 4. runtime.manifest: read what registered.
        let manifest_raw = rt.call("runtime.manifest", &[]).map_err(to_boxed)?;
        let manifest = mp_decode(&manifest_raw)?;
        let sigs = parse_manifest(&manifest);

        // 5. Register each scalar on the PERSISTENT connection captured at init
        //    (never a reconnect through the dangling `db` handle — see the
        //    DucklinkRuntime safety note). Database-wide, so the functions are
        //    visible on the caller's NEXT statement.
        let con = crate::reg_duckdb::ducklink_connection()
            .ok_or_else(|| -> Box<dyn std::error::Error> {
                "ducklink_run: runtime not initialised (LOAD ducklink first)".into()
            })?;
        let guard = con.lock().unwrap_or_else(|e| e.into_inner());
        let scalars =
            register_py_scalars(&guard, &sigs).map_err(|e| -> Box<dyn std::error::Error> {
                e.to_string().into()
            })?;
        drop(guard);

        let names = sigs
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        bind.add_result_column("script", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("module", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("scalars", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        bind.add_result_column("functions", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        Ok(WasmRunBind {
            path: script_path.to_string_lossy().into_owned(),
            module,
            scalars,
            names,
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(WasmRunInit {
            done: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bind = func.get_bind_data();
        let init = func.get_init_data();
        if init.done.swap(1, Ordering::Relaxed) != 0 {
            output.set_len(0);
            return Ok(());
        }
        output.flat_vector(0).insert(0, bind.path.as_str());
        output.flat_vector(1).insert(0, bind.module.as_str());
        // SAFETY: BIGINT column; row 0 in range (set_len(1) below).
        unsafe {
            output.flat_vector(2).as_mut_slice::<i64>()[0] = bind.scalars as i64;
        }
        output.flat_vector(3).insert(0, bind.names.as_str());
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        // Positional arg 0: a `.py` filesystem path OR an `http(s)://…` URL.
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        // Optional `sha256 :=` to verify a URL-hosted script's bytes (the
        // load-your-own / unsigned path); ignored for a local-path arg.
        Some(vec![(
            "sha256".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )])
    }
}

/// Register the `ducklink_run('<script.py>')` table function on `con`. Called
/// from `register_load_function` alongside `ducklink_load`.
pub fn register_run_function(con: &Connection) -> duckdb::Result<()> {
    con.register_table_function::<WasmRun>("ducklink_run")?;
    Ok(())
}
