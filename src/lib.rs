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

#[cfg(feature = "duckdb-api")]
pub mod delegating_agg;


#[cfg(feature = "loadable")]
mod loadable {
    use std::error::Error;
    use std::ffi::{CStr, CString};
    use std::sync::Arc;

    use duckdb::ffi;
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

    // `DucklinkVersion` and `DucklinkHelp` — the two always-available
    // scalars — live in `src/reg_duckdb.rs` alongside the rest of the
    // STABILITY.md § 1.1 surface. `register_load_function` registers all
    // of them in one call, so `ducklink_init_c_api` (this file) and the
    // in-process conformance runner (`tests/conformance.rs`) share one
    // implementation. See the module-level docstring in `reg_duckdb.rs`.

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

        // Read the host DuckDB library version. Ducklink is C-API-only on
        // every platform — no internal C++ ABI probing, so nothing gates on
        // the version at runtime; the string is surfaced through the
        // `ducklink.host` discovery view only.
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

        let engine = Arc::new(Engine2::new().map_err(stringify)?);

        // Register the STABILITY.md § 1.1 SQL surface — `ducklink_load`
        // (TF), `ducklink_prefix` (TF + scalar), `PREFIX` (macro),
        // `ducklink_version` (scalar), `ducklink_help` (scalar) — plus
        // the § 1.2 discovery views. One call, one code path, so the
        // extension entry point and the in-process conformance runner
        // register the same set. Non-fatal: a failure here must not
        // break `LOAD ducklink` for the built-in scalars that were
        // registered earlier.
        if let Err(e) = register_load_function(&con, db, engine.clone()) {
            eprintln!("[ducklink] could not register ducklink surface: {e}");
        }

        let specs = component_specs_from_env();
        // `db` is unused inside `register_components` today but kept in the
        // signature so re-adding a database-level registration path later
        // doesn't churn every call-site.
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
