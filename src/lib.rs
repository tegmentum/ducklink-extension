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
/// whenever the duckdb crate is available, EXCEPT on Windows: the shim's
/// deferred-undefined-symbol linking model has no portable PE/COFF equivalent,
/// so the C++ shim is not built there (see build.rs) and the advanced module —
/// along with every reference to its FFI — is compiled out, leaving Windows
/// with the common tier only (scalar/table/aggregate on the stable C API).
#[cfg(all(feature = "duckdb-api", not(target_os = "windows")))]
pub mod advanced;

#[cfg(feature = "loadable")]
mod loadable {
    use std::error::Error;
    use std::ffi::{CStr, CString};
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
    // references are resolved at LOAD time against the host DuckDB. Not built on
    // Windows (the shim is compiled out there), so this declaration — and every
    // call into it — is gated off, leaving no undefined internal symbol in the
    // Windows cdylib.
    #[cfg(not(target_os = "windows"))]
    extern "C" {
        fn ducklink_advanced_probe(db: *mut std::ffi::c_void) -> i32;
    }

    /// The EXACT DuckDB version this extension's advanced-tier C++ shim was
    /// compiled against, locked to the `libduckdb-sys` pin in `Cargo.toml`
    /// (`1.10504.0` = DuckDB v1.5.4). The advanced tier (parser / optimizer /
    /// filter pushdown) binds DuckDB's INTERNAL C++ ABI, which is NOT stable
    /// across DuckDB versions, so it is enabled ONLY when the host DuckDB reports
    /// this exact version. Keep in lock-step with the `duckdb` / `libduckdb-sys`
    /// pin (a DuckDB bump re-anchors this one string + the C++ shim headers).
    const DUCKDB_ABI_VERSION: &str = "v1.5.4";

    /// The host DuckDB's reported library version, read through the STABLE C API
    /// (`duckdb_library_version`, populated in the loadable function-pointer
    /// table by the API init above). `None` if the pointer is null or not UTF-8.
    /// This is the gate that keeps a version mismatch from ever calling an
    /// internal-ABI symbol.
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
        // advanced tier ONLY when the host reports the EXACT built-against version
        // and otherwise DEGRADE GRACEFULLY to the common tier (scalar / table /
        // aggregate, all on the stable C API) — never touching an internal-ABI
        // symbol (not even the probe). `DUCKLINK_DISABLE_ADVANCED` forces the
        // degraded path regardless (testing / belt-and-suspenders).
        //
        // Note the stable C-API init above is a *minimum*-version check (forward
        // compatible), so a NEWER host loads the common tier fine; this exact
        // gate is what disables the unstable tier on that newer host.
        let host_version = host_library_version();
        let forced_off = std::env::var_os("DUCKLINK_DISABLE_ADVANCED").is_some();

        // On Windows the advanced tier is compiled out entirely (no C++ shim is
        // built — see build.rs / the gated `advanced` module), so it is always
        // disabled at compile time and no internal-ABI symbol is ever referenced.
        // Everywhere else, the version guard enables it ONLY when the host reports
        // the EXACT built-against DuckDB version, degrading gracefully to the
        // common tier otherwise. Both paths select the same common-tier-only
        // behavior; on Windows it is just selected at compile time.
        #[cfg(target_os = "windows")]
        let advanced_enabled = false;
        #[cfg(not(target_os = "windows"))]
        let advanced_enabled =
            !forced_off && host_version.as_deref() == Some(DUCKDB_ABI_VERSION);

        #[cfg(not(target_os = "windows"))]
        if advanced_enabled {
            // Internal C++ ABI call, resolved at load against the matching host:
            // dereference the database to its internal DBConfig as a load-time
            // proof the shim is reachable and the ABI resolved.
            let probe = ducklink_advanced_probe(db.cast());
            eprintln!(
                "[ducklink] advanced tier ENABLED (host DuckDB {DUCKDB_ABI_VERSION}); \
                 C++ shim probe maximum_threads={probe}"
            );
        } else {
            let host = host_version.as_deref().unwrap_or("unknown");
            let reason = if forced_off {
                "forced off via DUCKLINK_DISABLE_ADVANCED".to_string()
            } else {
                format!("host DuckDB {host} does not match the built-against {DUCKDB_ABI_VERSION}")
            };
            eprintln!(
                "[ducklink] advanced tier DISABLED ({reason}); parser / optimizer / \
                 filter-pushdown are unavailable on this host. Common tier \
                 (scalar/table/aggregate) is active."
            );
        }
        #[cfg(target_os = "windows")]
        {
            // Reference the otherwise-unused inputs so the common-tier-only build
            // stays warning-clean.
            let _ = (&host_version, forced_off);
            eprintln!(
                "[ducklink] advanced tier NOT BUILT on Windows; parser / optimizer / \
                 filter-pushdown are unavailable here. Common tier \
                 (scalar/table/aggregate) is active."
            );
        }

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
        // Only hand the raw `db` to the registrar when the advanced tier is
        // enabled; with `None`, `register_components` skips ALL internal-ABI C++
        // shim registration (parser / optimizer / filterable tables) and wires
        // only the stable-C-API common tier.
        let advanced_db = advanced_enabled.then_some(db);
        let registered =
            register_components(&con, have_raw.then_some(raw_con), advanced_db, engine, &specs)
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
