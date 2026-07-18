//! ducklink — a native-DuckDB loadable extension that embeds wasmtime to run
//! `duckdb:extension` WebAssembly components (Direction 2).
//!
//! A component is built once against the `duckdb:extension` WIT world and runs
//! identically here and under the standalone `ducklink` host (Direction 1).
//! Both directions share [`ducklink_runtime`] — the bindgen, the neutral
//! `reg::*` capture model, the callback registry, and `load_component`.
//!
//! - [`engine`] is the direction-agnostic glue (load a component, capture its
//!   functions, dispatch invocations back into it). It depends only on
//!   `ducklink-runtime` + wasmtime, so it builds without the DuckDB toolchain.
//! - The `loadable` module (behind the `loadable` feature) is the DuckDB C-API
//!   binding: the extension entry point + the per-function registration that
//!   maps an [`engine::ScalarFunc`] onto a DuckDB scalar function whose callback
//!   re-enters [`engine::Engine2::dispatch_scalar`].

pub mod engine;

/// Read a component's optional `duckdb.docs` wasm custom section (JSON
/// per-function docs the guest can bundle inside the component itself) and
/// merge it into `ducklink.docs` at query time. Pure std + serde_json + a
/// hand-rolled custom-section scanner, so no DuckDB or wasmtime dep is needed.
pub mod docs_section;

/// Process-wide runtime event log — the bounded ring buffer behind the
/// `ducklink.events` system view. Decoupled from any runtime handle so any code
/// path (catalog resolution, load bind, the advanced `LOAD WASM` bridge) can
/// [`events::emit`] an audit record without threading state through. Pure std,
/// always compiled.
pub mod events;

/// Catalog resolution + name->blob fetch/cache/verify for `ducklink_load(<name>)`
/// by catalog NAME. Live HTTPS fetch with a bundled-snapshot fallback; downloaded
/// blobs are sha256-verified against the catalog `content_digest` before caching.
/// Independent of the DuckDB toolchain (reqwest + serde only).
#[cfg(feature = "duckdb-api")]
pub mod catalog;

/// The Direction-2 DuckDB sink (registration + dispatch). Present whenever the
/// duckdb crate is available (the `loadable` and `bundled` features both enable
/// it); the `bundled` end-to-end test lives in this module.
#[cfg(feature = "duckdb-api")]
pub mod reg_duckdb;


#[cfg(feature = "loadable")]
mod loadable {
    use std::error::Error;
    use std::ffi::{CStr, CString};
    use std::sync::Arc;

    use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeId};
    use duckdb::ffi;
    use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
    use duckdb::vtab::arrow::WritableVector;
    use duckdb::Connection;

    use crate::engine::Engine2;
    use crate::reg_duckdb::{
        component_specs_from_env, register_components, register_load_function, set_host_caps,
        HostCaps,
    };

    /// The minimum DuckDB C Extension API version this build targets. Passed
    /// to `duckdb_rs_extension_api_init`; the loader accepts any host >=
    /// this version (forward-compatible on the stable C API). Keep in
    /// lock-step with the `libduckdb-sys` pin.
    const DUCKDB_ABI_VERSION: &str = "v1.5.4";

    /// The host DuckDB's reported library version, read through the stable C
    /// API (`duckdb_library_version`, populated in the loadable
    /// function-pointer table by the API init above). `None` if the pointer
    /// is null or not UTF-8. Reported in `ducklink.host` for diagnostics —
    /// no runtime gate depends on it anymore now that ducklink is
    /// C-API-only.
    unsafe fn host_library_version() -> Option<String> {
        let ptr = ffi::duckdb_library_version();
        if ptr.is_null() {
            return None;
        }
        CStr::from_ptr(ptr).to_str().ok().map(|s| s.to_string())
    }

    /// `ducklink_version()` -> the extension's version string. Registered
    /// unconditionally (needs no WebAssembly component), so
    /// `LOAD ducklink; SELECT ducklink_version();` is a self-contained smoke
    /// test that the extension built and loaded.
    struct DucklinkVersion;

    impl VScalar for DucklinkVersion {
        type State = ();

        fn invoke(
            _: &Self::State,
            input: &mut DataChunkHandle,
            output: &mut dyn WritableVector,
        ) -> Result<(), Box<dyn Error>> {
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
    /// `ducklink.docs` in a scalar-friendly single VARCHAR output. Used
    /// interactively (`SELECT ducklink_help('aba_validate');`) and by tools
    /// that render markdown (Jupyter magics, docs generators). The heavy
    /// lifting lives in `reg_duckdb::render_help` — the scalar is a thin
    /// FFI translator.
    struct DucklinkHelp;

    impl VScalar for DucklinkHelp {
        type State = ();

        fn invoke(
            _: &Self::State,
            input: &mut DataChunkHandle,
            output: &mut dyn WritableVector,
        ) -> Result<(), Box<dyn Error>> {
            let len = input.len();
            let mut in_col = input.flat_vector(0);
            let out = output.flat_vector();
            // Input is a per-row VARCHAR; read each name, render help, emit
            // the markdown blob. The slice deref is unsafe because the DuckDB
            // string_t handles borrow from the chunk's per-row storage.
            let names: Vec<String> = unsafe {
                let s = in_col
                    .as_mut_slice_with_len::<duckdb::ffi::duckdb_string_t>(len);
                (0..len)
                    .map(|i| {
                        let mut t = s[i];
                        duckdb::types::DuckString::new(&mut t).as_str().into_owned()
                    })
                    .collect()
            };
            for (i, name) in names.iter().enumerate() {
                let rendered = crate::reg_duckdb::render_help(name);
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

    /// Loadable-extension entry point, named `ducklink_init_c_api` as DuckDB
    /// expects. Mirrors what the `duckdb_entrypoint_c_api` macro generates, but
    /// keeps the `duckdb_database` handle DuckDB hands us so it can open BOTH a
    /// duckdb-rs [`Connection`] (the safe scalar / table registration path) and a
    /// raw sibling `duckdb_connection` (for aggregates, which duckdb-rs has no
    /// safe API to register). Registrations are database-wide, so every
    /// connection — including the user's — sees the functions.
    ///
    /// # Safety
    /// Called by DuckDB during `LOAD` with a valid `info` / `access` pair.
    #[no_mangle]
    pub unsafe extern "C" fn ducklink_init_c_api(
        info: ffi::duckdb_extension_info,
        access: *const ffi::duckdb_extension_access,
    ) -> bool {
        match init(info, access) {
            Ok(loaded) => loaded,
            Err(e) => {
                // Surface the failure to DuckDB as a load error rather than just
                // returning false with no explanation.
                if let Some(set_error) = (*access).set_error {
                    if let Ok(c) = CString::new(e.to_string()) {
                        set_error(info, c.as_ptr());
                    }
                }
                false
            }
        }
    }

    /// The fallible body of [`ducklink_init_c_api`].
    ///
    /// Loads every component named in the `DUCKLINK_COMPONENTS` environment
    /// variable (a `:`-separated list of `name=path` or `path`) and registers the
    /// scalar / table / aggregate functions it declares:
    ///
    /// ```sh
    /// DUCKLINK_COMPONENTS=sample=/path/sample_extension.wasm \
    ///   duckdb -unsigned -c "LOAD 'ducklink.duckdb_extension'; SELECT sample_plus_one(41);"
    /// ```
    ///
    /// The shared `Engine2` is kept alive by the `Arc` cloned into each
    /// registered function's state.
    ///
    /// # Safety
    /// `info` / `access` must be the valid handles DuckDB passes to the entry
    /// point.
    unsafe fn init(
        info: ffi::duckdb_extension_info,
        access: *const ffi::duckdb_extension_access,
    ) -> Result<bool, Box<dyn Error>> {
        // Populate the loadable C-API function-pointer table. Returns false on an
        // API-version mismatch, in which case loading aborts cleanly.
        if !ffi::duckdb_rs_extension_api_init(info, access, DUCKDB_ABI_VERSION).map_err(stringify)? {
            return Ok(false);
        }
        let get_database = (*access)
            .get_database
            .ok_or_else(|| stringify("get_database is null in duckdb_extension_access"))?;
        let db_ptr = get_database(info);
        if db_ptr.is_null() {
            return Ok(false);
        }
        let db: ffi::duckdb_database = *db_ptr;

        // VERSION GUARD for the advanced tier. The advanced tier (parser /
        // optimizer / filter pushdown) binds DuckDB's INTERNAL C++ ABI through
        // the linked C++ shim, with the internal symbols resolved at LOAD against
        // the host process. That ABI is NOT stable across DuckDB versions, so
        // calling into it on a host that differs from the version the shim was
        // compiled against could crash or corrupt state. We therefore enable the
        // ducklink is C-API-only across every platform — no internal-C++-ABI
        // shim, no per-platform capability drift. The version string is still
        // read for the `ducklink.host` discovery view, but nothing runtime-
        // meaningful gates on it anymore.
        let host_version = host_library_version();

        set_host_caps(HostCaps {
            host_version: host_version.clone(),
            abi_version: ducklink_runtime::CONTRACT_VERSION.to_string(),
        });

        // duckdb-rs Connection for the safe scalar / table registration paths.
        let con = Connection::open_from_raw(db.cast())?;
        // A raw sibling connection on the SAME database for aggregates (no safe
        // duckdb-rs wrapper exists). Registrations are catalog-wide, so it only
        // needs to outlive the registration call.
        let mut raw_con: ffi::duckdb_connection = std::ptr::null_mut();
        let have_raw =
            ffi::duckdb_connect(db, &mut raw_con) == ffi::DuckDBSuccess && !raw_con.is_null();

        // Always-available built-in, so the extension is usable (and testable)
        // even before any component is configured. No `COMMENT ON FUNCTION`
        // description is set: DuckDB blocks it for entries in the system
        // catalog (which is where loadable-extension functions land), and the
        // stable C API has no scalar-function description setter.
        con.register_scalar_function::<DucklinkHelp>("ducklink_help")
            .map_err(stringify)?;
        con.register_scalar_function::<DucklinkVersion>("ducklink_version")
            .map_err(stringify)?;
        let engine = Arc::new(Engine2::new().map_err(stringify)?);

        // Register `ducklink_load(path)`: the in-SQL analogue of DuckDB's `LOAD`,
        // which loads a component at RUNTIME (from a SQL statement) and registers
        // its functions on the live database for use in later statements. It
        // captures the process-wide `db` handle + shared `engine` so its static
        // table-function bind can re-open a sibling connection to register. Shares
        // the SAME `engine` as the env-driven components below. Non-fatal: a
        // failure here must not break `LOAD ducklink`.
        if let Err(e) = register_load_function(&con, db, engine.clone()) {
            eprintln!("[ducklink] could not register ducklink_load: {e}");
        }

        let specs = component_specs_from_env();
        // `db` is retained in the signature for backwards compatibility with
        // callers that once threaded an advanced-tier register call through
        // it — now a no-op inside register_components.
        let registered =
            register_components(&con, have_raw.then_some(raw_con), Some(db), engine, &specs)
                .map_err(stringify)?;

        // The aggregate functions are now in the database catalog; the sibling
        // connection has served its purpose.
        if !raw_con.is_null() {
            ffi::duckdb_disconnect(&mut raw_con);
        }
        // Silent on the common no-preload path (empty DUCKLINK_COMPONENTS).
        // Only announce when something was actually preloaded, or when the
        // aggregate raw-connection could not be opened and aggregates in any
        // preloaded component would have been skipped.
        if !specs.is_empty() || !have_raw {
            eprintln!(
                "[ducklink] loaded {} component(s); registered {registered} function(s){}",
                specs.len(),
                if have_raw {
                    ""
                } else {
                    " (no raw connection; aggregates skipped)"
                }
            );
        }
        Ok(true)
    }

    fn stringify(err: impl std::fmt::Display) -> Box<dyn Error> {
        err.to_string().into()
    }
}
