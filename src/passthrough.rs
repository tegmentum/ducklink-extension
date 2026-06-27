//! The native-passthrough hook (de-risk spike for design "D").
//!
//! This is the seam that answers Q1/Q2 of the spike: a *function* the user
//! invokes (`ducklink_load('aba')`) that runs the lifted [`resolver`] and then
//! DUAL-LOADS the chosen provider through the matching arm:
//!
//!   * **wasm** arm  -> register the component's functions into the connection
//!     via the proven Route-A bridge ([`crate::reg_duckdb::register_components`]),
//!     embedded wasmtime, native speed.
//!   * **native** arm -> hand off to DuckDB's OWN extension loader by issuing a
//!     `LOAD '<artifact>'` on the connection. This is the C-API-tractable form of
//!     the native passthrough: the loaded ducklink extension drives DuckDB's
//!     `dlopen` + `<name>_init` path for the resolved `.duckdb_extension`.
//!   * **remote** arm -> out of scope for this spike.
//!
//! Q1 finding (see the spike report): native DuckDB has no registerable hook that
//! can intercept a transparent `LOAD spatial` for *another* extension's name --
//! `PhysicalLoad::GetDataInternal` calls `ExtensionHelper::LoadExternalExtension`
//! unconditionally and `OnBeginExtensionLoad` returns `void`. So the pragmatic
//! mechanism is this explicit entry point, fronted in production by a
//! `PRAGMA ducklink_load('aba')` / table-function `CALL` (which fires at a
//! statement boundary, where catalog mutation is safe). This module is that
//! entry point's body; the bundled test calls it at a statement boundary exactly
//! as the pragma would.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use duckdb::Connection;

use crate::engine::Engine2;
use crate::reg_duckdb::{register_components, ComponentSpec};
use crate::resolver::{
    self, Conformance, ContentRef, Env, ManifestEntry, ProviderDescriptor, ProviderKind,
    ResolvePolicy,
};

/// Outcome of one `ducklink_load` hook call (the observability the doc's
/// `PRAGMA extension_provider` would surface).
#[derive(Debug)]
pub struct LoadReport {
    pub extension: String,
    pub chosen_id: String,
    pub chosen_kind: &'static str,
    /// Per-candidate reasoning (the chosen + why each loser lost).
    pub reasoning: String,
    /// Functions registered (wasm arm); 0 for the native arm (DuckDB owns it).
    pub registered: usize,
}

/// The hook body: resolve `entry` under `env`/`policy`, then dual-load the chosen
/// provider through its arm. Reuses the resolver (A) verbatim and the Route-A
/// bridge (`register_components`) verbatim; the only new code is the kind->arm
/// dispatch below.
pub fn ducklink_load(
    con: &Connection,
    engine: Arc<Mutex<Engine2>>,
    entry: &ManifestEntry,
    env: &Env,
    policy: &ResolvePolicy,
) -> Result<LoadReport> {
    let res = resolver::resolve(entry, env, policy, None).map_err(|e| anyhow!(e.to_string()))?;
    let reasoning = resolver::render_reasoning(&res.reasoning);

    let registered = match res.chosen_kind {
        // WASM ARM: the Route-A bridge, reused unchanged.
        "wasm" => {
            let path = artifact_path(&res.artifact)?;
            let spec = ComponentSpec {
                name: res.extension.clone(),
                path,
            };
            register_components(con, None, engine, std::slice::from_ref(&spec))?
        }
        // NATIVE ARM: hand off to DuckDB's own native extension loader.
        "native" => {
            let path = artifact_path(&res.artifact)?;
            con.execute_batch(&format!("LOAD '{}';", path.display()))
                .map_err(|e| anyhow!("native arm: DuckDB LOAD of '{}' failed: {e}", path.display()))?;
            0
        }
        other => return Err(anyhow!("provider kind '{other}' not implemented in this spike")),
    };

    Ok(LoadReport {
        extension: res.extension,
        chosen_id: res.chosen_id,
        chosen_kind: res.chosen_kind,
        reasoning,
        registered,
    })
}

fn artifact_path(artifact: &ContentRef) -> Result<PathBuf> {
    match artifact {
        ContentRef::Path(p) => Ok(p.clone()),
        // Spike: a remote/native entry may carry its location as an Oci string;
        // treat it as a path for the local-file native arm.
        ContentRef::Oci(s) => Ok(PathBuf::from(s)),
        ContentRef::Digest(_) => {
            Err(anyhow!("digest artifact resolution not wired in this spike"))
        }
    }
}

/// Build a minimal in-code manifest entry for `aba`: a wasm reference provider
/// (the portable baseline) and, optionally, a native provider. In production this
/// comes from `registry/index.json` via `resolver::read_manifest_entry`; built
/// in-code here to keep the spike self-contained.
pub fn aba_manifest(wasm_artifact: PathBuf, native_artifact: Option<PathBuf>) -> ManifestEntry {
    const CONTRACT: &str = "spike-aba-contract";
    let mut providers = vec![ProviderDescriptor {
        id: "wasm-component".into(),
        kind: ProviderKind::Wasm {
            abi: "duckdb:extension@2.0.0".into(),
            artifact: ContentRef::Path(wasm_artifact),
            content_digest: None,
            browser_safe: false,
        },
        reference: true, // certified-by-construction (the reference baseline)
        conformance: None,
        trust: None,
    }];
    if let Some(p) = native_artifact {
        providers.push(ProviderDescriptor {
            id: "native-local".into(),
            kind: ProviderKind::Native {
                os: std::env::consts::OS.into(),
                arch: std::env::consts::ARCH.into(),
                artifact: ContentRef::Path(p),
            },
            reference: false,
            // Certified at the live contract so the hard gate admits it.
            conformance: Some(Conformance {
                suite: "aba@1".into(),
                suite_digest: String::new(),
                contract_digest: CONTRACT.into(),
                passed: true,
            }),
            trust: None,
        });
    }
    ManifestEntry {
        name: "aba".into(),
        wit_contract: CONTRACT.into(),
        providers,
    }
}

#[cfg(all(test, feature = "bundled"))]
mod tests {
    use super::*;

    fn aba_wasm() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../artifacts/extensions/aba.wasm")
    }

    /// Q2 WASM-ARM PROOF (end-to-end through the hook): `ducklink_load('aba')`
    /// resolves the wasm reference provider, registers `aba_validate` via the
    /// Route-A bridge, and the function then computes the ABA checksum INSIDE the
    /// wasm component for a subsequent query.
    #[test]
    fn wasm_arm_dual_loads_through_hook() {
        let engine = Arc::new(Mutex::new(Engine2::new().expect("engine")));
        let con = Connection::open_in_memory().expect("open duckdb");

        // wasm-only manifest, default env (allow_native = false).
        let entry = aba_manifest(aba_wasm(), None);
        let report = ducklink_load(
            &con,
            engine,
            &entry,
            &Env::default(),
            &ResolvePolicy::default(),
        )
        .expect("hook loads");

        assert_eq!(report.chosen_kind, "wasm", "reasoning: {}", report.reasoning);
        assert_eq!(report.chosen_id, "wasm-component");
        assert!(report.registered >= 1, "expected aba_validate registered");
        eprintln!("[spike] wasm arm: {report:?}");

        // The hook registered the function; a normal query now dispatches into wasm.
        let valid: bool = con
            .query_row("SELECT aba_validate('021000021')", [], |r| r.get(0))
            .expect("query valid");
        assert!(valid, "021000021 is a valid ABA routing number (checksum 0)");

        let invalid: bool = con
            .query_row("SELECT aba_validate('123456789')", [], |r| r.get(0))
            .expect("query invalid");
        assert!(!invalid, "123456789 fails the ABA checksum");
    }

    /// Q2 NATIVE-ARM PROOF (mechanism, through the hook): with a native provider
    /// present and `allow_native = true`, the resolver PICKS native (precedence
    /// native > wasm) and the hook hands off to DuckDB's own `LOAD` -- proving the
    /// native arm reaches DuckDB's dlopen/init substrate. We point it at a missing
    /// artifact, so the error originates from DuckDB's native loader (full e2e
    /// needs a built+signed native `aba.duckdb_extension`; see the report).
    #[test]
    fn native_arm_is_chosen_and_hands_off_to_duckdb_load() {
        let engine = Arc::new(Mutex::new(Engine2::new().expect("engine")));
        let con = Connection::open_in_memory().expect("open duckdb");

        let bogus = PathBuf::from("/nonexistent/aba.duckdb_extension");
        let entry = aba_manifest(aba_wasm(), Some(bogus.clone()));
        let env = Env {
            wasm_runtime: true,
            allow_native: true,
        };

        let err = ducklink_load(&con, engine, &entry, &env, &ResolvePolicy::default())
            .expect_err("native arm should attempt DuckDB LOAD of a missing artifact");
        let msg = err.to_string();
        eprintln!("[spike] native arm error (expected): {msg}");
        // Proof the NATIVE arm was selected and dispatched to DuckDB's loader.
        assert!(
            msg.contains("native arm") && msg.contains("LOAD"),
            "expected native-arm LOAD handoff, got: {msg}"
        );
    }

    /// The native arm is gated off by default (allow_native = false): the SAME
    /// manifest resolves to the wasm baseline -- graceful degradation, not silent
    /// native load.
    #[test]
    fn native_off_by_default_falls_back_to_wasm() {
        let engine = Arc::new(Mutex::new(Engine2::new().expect("engine")));
        let con = Connection::open_in_memory().expect("open duckdb");
        let entry = aba_manifest(aba_wasm(), Some(PathBuf::from("/nonexistent/aba.duckdb_extension")));

        let report = ducklink_load(
            &con,
            engine,
            &entry,
            &Env::default(), // allow_native = false
            &ResolvePolicy::default(),
        )
        .expect("falls back to wasm");
        assert_eq!(report.chosen_kind, "wasm", "reasoning: {}", report.reasoning);
        assert!(report.reasoning.contains("native-local"), "loser recorded");
    }
}
