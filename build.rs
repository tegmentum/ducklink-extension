//! Build script for ducklink.
//!
//! Ducklink is now C-API-only across every platform — no internal-C++-ABI
//! shim, no `cc`, no per-platform capability drift. The only build-time
//! concern is a `cfg=have_corpus` gate that turns wasm-corpus-dependent
//! tests on when a runnable `sample_extension.wasm` exists on disk.

fn main() {
    // `have_corpus` gates the wasm-corpus-dependent tests (bridge_coverage.rs
    // integration suite + a handful of lib tests). Set at BUILD time by probing
    // for `sample_extension.wasm` in either DUCKLINK_CORPUS_DIR or the monorepo
    // default (`../../artifacts/extensions`); when it's absent — the standalone
    // repo checkout, the community-extensions CI — the corpus tests compile out
    // instead of failing.
    println!("cargo:rustc-check-cfg=cfg(have_corpus)");
    println!("cargo:rerun-if-env-changed=DUCKLINK_CORPUS_DIR");
    if corpus_probe::sample_extension_present() {
        println!("cargo:rustc-cfg=have_corpus");
    }
}

/// Probe for the presence of a runnable `sample_extension.wasm` in the corpus
/// directory. Prefers `DUCKLINK_CORPUS_DIR`, falls back to the monorepo
/// default. An empty (0-byte) file counts as absent, since a common
/// monorepo state is a stub-committed artifact rebuilt on demand — running
/// against a 0-byte blob would fail with a wasmtime error, not a clean skip.
mod corpus_probe {
    use std::path::PathBuf;

    pub fn sample_extension_present() -> bool {
        let dir = match std::env::var_os("DUCKLINK_CORPUS_DIR") {
            Some(d) => PathBuf::from(d),
            None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../artifacts/extensions"),
        };
        let path = dir.join("sample_extension.wasm");
        std::fs::metadata(&path)
            .map(|m| m.is_file() && m.len() > 0)
            .unwrap_or(false)
    }
}
