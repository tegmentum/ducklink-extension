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

/// The Direction-2 DuckDB sink (registration + dispatch). Present whenever the
/// duckdb crate is available (the `loadable` and `bundled` features both enable
/// it); the `bundled` end-to-end test lives in this module.
#[cfg(feature = "duckdb-api")]
pub mod reg_duckdb;

/// The advanced dispatch tier (PARSER / OPTIMIZER / table FILTER pushdown): the
/// Rust side of the C++ shim that binds DuckDB's internal C++ ABI. Present
/// whenever the duckdb crate is available.
#[cfg(feature = "duckdb-api")]
pub mod advanced;

#[cfg(feature = "loadable")]
mod loadable {
    use std::error::Error;
    use std::ffi::CString;
    use std::sync::{Arc, Mutex};

    use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeId};
    use duckdb::ffi;
    use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
    use duckdb::vtab::arrow::WritableVector;
    use duckdb::Connection;

    use crate::engine::Engine2;
    use crate::reg_duckdb::{component_specs_from_env, register_components};

    // The advanced-tier C++ shim, compiled against DuckDB's internal headers and
    // linked into this extension (see build.rs). Internal C++ symbols it
    // references are resolved at LOAD time against the host DuckDB.
    extern "C" {
        fn ducklink_advanced_probe(db: *mut std::ffi::c_void) -> i32;
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
        if !ffi::duckdb_rs_extension_api_init(info, access, "v1.5.4").map_err(stringify)? {
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

        // Build-model proof: reach the advanced-tier C++ shim and have it
        // dereference the database to its internal DBConfig (an internal C++ ABI
        // call resolved at load). A negative result means the cast failed; we log
        // and continue (the common tier does not depend on it).
        let probe = ducklink_advanced_probe(db.cast());
        eprintln!("[ducklink] advanced C++ shim probe: maximum_threads={probe}");

        // duckdb-rs Connection for the safe scalar / table registration paths.
        let con = Connection::open_from_raw(db.cast())?;
        // A raw sibling connection on the SAME database for aggregates (no safe
        // duckdb-rs wrapper exists). Registrations are catalog-wide, so it only
        // needs to outlive the registration call.
        let mut raw_con: ffi::duckdb_connection = std::ptr::null_mut();
        let have_raw =
            ffi::duckdb_connect(db, &mut raw_con) == ffi::DuckDBSuccess && !raw_con.is_null();

        // Always-available built-in, so the extension is usable (and testable)
        // even before any component is configured.
        con.register_scalar_function::<DucklinkVersion>("ducklink_version")
            .map_err(stringify)?;
        let engine = Arc::new(Mutex::new(Engine2::new().map_err(stringify)?));
        let specs = component_specs_from_env();
        let registered =
            register_components(&con, have_raw.then_some(raw_con), Some(db), engine, &specs)
                .map_err(stringify)?;

        // The aggregate functions are now in the database catalog; the sibling
        // connection has served its purpose.
        if !raw_con.is_null() {
            ffi::duckdb_disconnect(&mut raw_con);
        }
        eprintln!(
            "[ducklink] loaded {} component(s); registered {registered} function(s){}",
            specs.len(),
            if have_raw {
                ""
            } else {
                " (no raw connection; aggregates skipped)"
            }
        );
        Ok(true)
    }

    fn stringify(err: impl std::fmt::Display) -> Box<dyn Error> {
        err.to_string().into()
    }
}
