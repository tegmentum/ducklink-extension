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

/// The advanced dispatch tier (PARSER / OPTIMIZER / table FILTER pushdown): the
/// Rust side of the C++ shim that binds DuckDB's internal C++ ABI. Compiled in
/// only when the `advanced` feature is built AND the C++ shim was compiled —
/// build.rs sets the `advanced_tier` cfg for non-Windows `--features advanced`
/// builds. The DEFAULT community build does NOT enable `advanced` (it is off by
/// default), and the shim's deferred-undefined-symbol linking model has no
/// portable Windows PE/COFF equivalent, so on the default build and on Windows
/// this module — along with every reference to its FFI — is compiled out,
/// leaving the common tier only (scalar/table/aggregate on the stable C API)
/// with a trivial build script and no undefined internal symbols.
#[cfg(advanced_tier)]
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
    use crate::reg_duckdb::{
        component_specs_from_env, register_components, register_load_function, set_host_caps,
        HostCaps,
    };

    // The advanced-tier C++ shim, compiled against DuckDB's internal headers and
    // linked into this extension (see build.rs). Internal C++ symbols it
    // references are resolved at LOAD time against the host DuckDB. Only present
    // in `advanced_tier` builds (non-Windows `--features advanced`); on the
    // default community build and on Windows the shim is not compiled, so this
    // declaration — and every call into it — is gated off, leaving no undefined
    // internal symbol in the cdylib.
    #[cfg(advanced_tier)]
    extern "C" {
        fn ducklink_advanced_probe(db: *mut std::ffi::c_void) -> i32;
        /// Install the component-driven ParserExtension on `db` (idempotent).
        /// Registered unconditionally when the advanced tier is active so the
        /// `LOAD WASM '<name>'` statement is recognized even before any
        /// component declares a parser of its own.
        fn ducklink_register_parser(db: *mut std::ffi::c_void) -> i32;
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

        // When the advanced tier is not compiled into this artifact (the default
        // community build, or Windows — no C++ shim is built, see build.rs / the
        // gated `advanced` module), it is always disabled at compile time and no
        // internal-ABI symbol is ever referenced. When it IS compiled in
        // (`advanced_tier`), the version guard enables it ONLY when the host
        // reports the EXACT built-against DuckDB version, degrading gracefully to
        // the common tier otherwise.
        #[cfg(not(advanced_tier))]
        let advanced_enabled = false;
        #[cfg(advanced_tier)]
        let advanced_enabled =
            !forced_off && host_version.as_deref() == Some(DUCKDB_ABI_VERSION);

        #[cfg(advanced_tier)]
        if advanced_enabled {
            // Internal C++ ABI call, resolved at load against the matching host:
            // dereference the database to its internal DBConfig as a load-time
            // proof the shim is reachable and the ABI resolved.
            let probe = ducklink_advanced_probe(db.cast());
            eprintln!(
                "[ducklink] advanced tier ENABLED (host DuckDB {DUCKDB_ABI_VERSION}); \
                 C++ shim probe maximum_threads={probe}"
            );
            // Install the component-driven ParserExtension now (idempotent), so
            // `LOAD WASM '<name>'` is recognized from the first statement — it
            // routes through the parser bridge to the `ducklink_load` loader,
            // independent of whether any component has declared a parser yet.
            let prc = ducklink_register_parser(db.cast());
            if prc != 0 {
                eprintln!("[ducklink] failed to install LOAD WASM parser extension (rc={prc})");
            }
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
        #[cfg(not(advanced_tier))]
        {
            // Reference the otherwise-unused inputs so the common-tier-only build
            // stays warning-clean.
            let _ = (&host_version, forced_off);
            let why = if cfg!(target_os = "windows") {
                "not built on Windows (no portable PE/COFF equivalent)"
            } else {
                "not built in this artifact (build with --features advanced to enable)"
            };
            eprintln!(
                "[ducklink] advanced tier {why}; parser / optimizer / \
                 filter-pushdown are unavailable here. Common tier \
                 (scalar/table/aggregate) is active."
            );
        }

        // Record the host capability profile for the `ducklink.capabilities`
        // discovery view (and the `ducklink.modules.compatible` column). Captures
        // the tier gate decision just made, whether the advanced tier is compiled
        // into this artifact at all, the host DuckDB version, and the wasm ABI.
        set_host_caps(HostCaps {
            advanced_enabled,
            advanced_built: cfg!(advanced_tier),
            host_version: host_version.clone(),
            built_against: DUCKDB_ABI_VERSION.to_string(),
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
        // even before any component is configured.
        con.register_scalar_function::<DucklinkVersion>("ducklink_version")
            .map_err(stringify)?;
        // Populate `duckdb_functions().comment` so introspection (and the
        // community-extensions site's "Added Functions" table) shows a real
        // description instead of NULL. ducklink is a transparent host:
        // `ducklink_version` is the only statically-registered function; every
        // other capability is provided by WebAssembly component extensions
        // loaded at runtime. The `description` column is C++-only and not
        // reachable through the stable C API, so `comment` carries this text.
        // Non-fatal: a failed COMMENT must never break `LOAD ducklink`.
        if let Err(e) = con.execute(
            "COMMENT ON FUNCTION ducklink_version IS \
             'Returns the ducklink extension version. ducklink is a transparent \
              host for WebAssembly component extensions: this is the only \
              built-in function — all other functionality is provided by \
              WebAssembly modules loaded at runtime (via the DUCKLINK_COMPONENTS \
              environment variable or LOAD).'",
            [],
        ) {
            eprintln!("[ducklink] could not set ducklink_version comment: {e}");
        }
        let engine = Arc::new(Mutex::new(Engine2::new().map_err(stringify)?));

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
