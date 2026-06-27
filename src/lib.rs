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

/// The multi-provider resolver spine (design A), lifted VERBATIM from
/// `crates/ducklink-host/src/resolver.rs` to prove it drops into a native
/// extension crate unchanged (de-risk spike for design "D"). Self-contained: no
/// wasmtime / no engine types.
pub mod resolver;

/// The Direction-2 DuckDB sink (registration + dispatch). Present whenever the
/// duckdb crate is available (the `loadable` and `bundled` features both enable
/// it); the `bundled` end-to-end test lives in this module.
#[cfg(feature = "duckdb-api")]
pub mod reg_duckdb;

/// The native-passthrough hook (de-risk spike): `ducklink_load('aba')` resolves a
/// provider via [`resolver`] and dual-loads it (wasm arm via the Route-A bridge /
/// native arm via DuckDB's own `LOAD`).
#[cfg(feature = "duckdb-api")]
pub mod passthrough;

#[cfg(feature = "loadable")]
mod loadable {
    use std::error::Error;
    use std::ffi::CString;
    use std::sync::{Arc, Mutex};

    use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeId};
    use duckdb::ffi;
    use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
    use duckdb::types::DuckString;
    use duckdb::vtab::arrow::WritableVector;
    use duckdb::Connection;

    use crate::engine::Engine2;
    use crate::reg_duckdb::{component_specs_from_env, register_components};

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
        let registered = register_components(&con, have_raw.then_some(raw_con), engine, &specs)
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

    // -----------------------------------------------------------------------
    // The DuckLink SHIM entrypoint (transparent LOAD, design "D" first-cut).
    //
    // A DuckLink "shim" is a `.duckdb_extension` named after a LOGICAL extension
    // (e.g. `aba`). Stock DuckDB derives the init symbol from the FILENAME
    // (`<filebase>_init_c_api`), so `aba.duckdb_extension` must export
    // `aba_init_c_api`. After a one-time `ducklink install aba` (INSTALL FROM the
    // DuckLink repo), plain `LOAD aba` on STOCK duckdb calls this symbol; the
    // shim runs the multi-provider resolver IN-PROCESS (reading the manifest) and
    // dual-loads the chosen provider (native passthrough, or the wasm bridge).
    //
    // The shim is identical for every name -- the NAME is the only variable -- so
    // the generator emits one `ducklink_shim!("<name>", <name>_init_c_api);` line
    // per managed extension (all share one cdylib; each copy is renamed +
    // footer-stamped to `<name>.duckdb_extension`).
    // -----------------------------------------------------------------------

    /// Shared shim entry. `name` is the logical extension; provider selection is
    /// manifest-driven (`DUCKLINK_HOME/index.json`) via [`crate::passthrough::shim_load`].
    unsafe fn shim_entry(
        name: &str,
        info: ffi::duckdb_extension_info,
        access: *const ffi::duckdb_extension_access,
    ) -> bool {
        match shim_entry_inner(name, info, access) {
            Ok(loaded) => loaded,
            Err(e) => {
                if let Some(set_error) = (*access).set_error {
                    if let Ok(c) = CString::new(e.to_string()) {
                        set_error(info, c.as_ptr());
                    }
                }
                false
            }
        }
    }

    unsafe fn shim_entry_inner(
        name: &str,
        info: ffi::duckdb_extension_info,
        access: *const ffi::duckdb_extension_access,
    ) -> Result<bool, Box<dyn Error>> {
        let con = match open_connection(info, access)? {
            Some(c) => c,
            None => return Ok(false),
        };
        let report = crate::passthrough::shim_load(&con, name).map_err(stringify)?;
        eprintln!(
            "[ducklink-shim:{name}] resolved provider '{}' [{}]; registered {} fn(s); reasoning: {}",
            report.chosen_id, report.chosen_kind, report.registered, report.reasoning
        );
        Ok(true)
    }

    /// Common entrypoint boilerplate: init the C-API table, open a duckdb-rs
    /// `Connection` on the database DuckDB handed us. `None` => abort cleanly.
    unsafe fn open_connection(
        info: ffi::duckdb_extension_info,
        access: *const ffi::duckdb_extension_access,
    ) -> Result<Option<Connection>, Box<dyn Error>> {
        if !ffi::duckdb_rs_extension_api_init(info, access, "v1.5.4").map_err(stringify)? {
            return Ok(None);
        }
        let get_database = (*access)
            .get_database
            .ok_or_else(|| stringify("get_database is null in duckdb_extension_access"))?;
        let db_ptr = get_database(info);
        if db_ptr.is_null() {
            return Ok(None);
        }
        let db: ffi::duckdb_database = *db_ptr;
        Ok(Some(Connection::open_from_raw(db.cast())?))
    }

    /// Emit a `<name>_init_c_api` shim entrypoint. The generator adds one line
    /// per managed logical extension; the body is the shared, manifest-driven
    /// [`shim_entry`].
    macro_rules! ducklink_shim {
        ($name:literal, $init:ident) => {
            // DuckLink shim entrypoint (auto-generated by `ducklink_shim!`).
            // Safety: called by DuckDB during `LOAD` with a valid info/access pair.
            #[no_mangle]
            pub unsafe extern "C" fn $init(
                info: ffi::duckdb_extension_info,
                access: *const ffi::duckdb_extension_access,
            ) -> bool {
                shim_entry($name, info, access)
            }
        };
    }

    // ---- The managed-extension shim table (the generator owns this list) ----
    ducklink_shim!("aba", aba_init_c_api);

    // -----------------------------------------------------------------------
    // The NATIVE provider implementation for `aba` (a real native
    // `.duckdb_extension`). Served as `aba_native.duckdb_extension` (exports
    // `aba_native_init_c_api`); the shim's native arm `LOAD`s it. This is the
    // native-speed passthrough: `aba_validate` computed in compiled Rust, no wasm.
    // -----------------------------------------------------------------------

    /// ABA routing-number checksum, native Rust (mirrors the wasm component's
    /// semantics: 9 digits, weights 3,7,1,..., sum % 10 == 0; spaces/hyphens
    /// ignored). The provider-neutral conformance suite certifies the two agree.
    fn aba_checksum_valid(s: &str) -> bool {
        let mut digits: Vec<u32> = Vec::with_capacity(s.len());
        for c in s.chars() {
            if c.is_whitespace() || c == '-' {
                continue;
            }
            match c.to_digit(10) {
                Some(d) => digits.push(d),
                None => return false,
            }
        }
        if digits.len() != 9 {
            return false;
        }
        let w = [3u32, 7, 1, 3, 7, 1, 3, 7, 1];
        let sum: u32 = digits.iter().zip(w).map(|(&d, k)| d * k).sum();
        sum % 10 == 0
    }

    struct AbaValidateNative;

    impl VScalar for AbaValidateNative {
        type State = ();
        fn invoke(
            _: &Self::State,
            input: &mut DataChunkHandle,
            output: &mut dyn WritableVector,
        ) -> Result<(), Box<dyn Error>> {
            let len = input.len();
            let in_vec = input.flat_vector(0);
            let strs = unsafe { in_vec.as_slice_with_len::<ffi::duckdb_string_t>(len) };
            let mut out = output.flat_vector();
            let out_slice = unsafe { out.as_mut_slice_with_len::<bool>(len) };
            for i in 0..len {
                let mut t = strs[i];
                let s = DuckString::new(&mut t).as_str();
                out_slice[i] = aba_checksum_valid(&s);
            }
            Ok(())
        }
        fn signatures() -> Vec<ScalarFunctionSignature> {
            vec![ScalarFunctionSignature::exact(
                vec![LogicalTypeId::Varchar.into()],
                LogicalTypeId::Boolean.into(),
            )]
        }
    }

    /// NATIVE provider entrypoint for `aba` (file `aba_native.duckdb_extension`).
    ///
    /// # Safety
    /// Called by DuckDB during `LOAD 'aba_native...'` with a valid pair.
    #[no_mangle]
    pub unsafe extern "C" fn aba_native_init_c_api(
        info: ffi::duckdb_extension_info,
        access: *const ffi::duckdb_extension_access,
    ) -> bool {
        match aba_native_init(info, access) {
            Ok(b) => b,
            Err(e) => {
                if let Some(set_error) = (*access).set_error {
                    if let Ok(c) = CString::new(e.to_string()) {
                        set_error(info, c.as_ptr());
                    }
                }
                false
            }
        }
    }

    unsafe fn aba_native_init(
        info: ffi::duckdb_extension_info,
        access: *const ffi::duckdb_extension_access,
    ) -> Result<bool, Box<dyn Error>> {
        let con = match open_connection(info, access)? {
            Some(c) => c,
            None => return Ok(false),
        };
        con.register_scalar_function::<AbaValidateNative>("aba_validate")
            .map_err(stringify)?;
        eprintln!("[ducklink-native:aba] registered native aba_validate");
        Ok(true)
    }
}
