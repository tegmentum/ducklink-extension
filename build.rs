//! Build script for the ADVANCED native dispatch tier.
//!
//! The common tier (scalar/table/aggregate) rides DuckDB's STABLE C Extension
//! API and needs no C++. The advanced tier (parser / optimizer / table-function
//! FILTER pushdown) binds to DuckDB's INTERNAL C++ ABI, which the stable C API
//! does not expose. So we compile a small C++ shim translation unit against
//! DuckDB's internal headers and link it into the loadable extension. This is
//! the single "absorb the C++ churn" layer (mirrors the wasm core's
//! `cpp/wasm_*.cpp`); a DuckDB version bump re-anchors only this file set.
//!
//! Version lock: the C++ ABI must match the DuckDB the artifact loads into. We
//! take the headers from the EXACT `libduckdb-sys` crate this build depends on
//! (its bundled `duckdb.tar.gz` is the v1.5.4 source), so the headers and the
//! `duckdb`/`libduckdb-sys` crate version move together — there is no separate
//! string to keep in sync.
//!
//! Loaded internal C++ symbols (DBConfig, OptimizerExtension::Register,
//! ParserExtension, Parser, Planner, TableFunction, ...) are left UNDEFINED in
//! the shim object and resolved at LOAD time against the host DuckDB process,
//! which exports them (verified: the v1.5.4 CLI exports all of them). On macOS
//! that needs `-undefined dynamic_lookup` on the cdylib link, added below.

fn main() {
    // The source uses `#[cfg(advanced_tier)]` / `#[cfg(not(advanced_tier))]` to
    // gate the advanced module and every internal-ABI reference. Declare the
    // custom cfg unconditionally so it is known to the compiler in EVERY build
    // (advanced on or off, bench, test), keeping check-cfg clean.
    println!("cargo:rustc-check-cfg=cfg(advanced_tier)");

    // The advanced tier (and its `cc` build-dependency + C++ shim) is OPT-IN and
    // OFF BY DEFAULT. The community-extensions CI runs a plain
    // `cargo build --release` with default features only, so the `advanced`
    // feature is absent: this build script does nothing, links no `cc`, compiles
    // no C++, and emits no link args — a trivial no-op build script, exactly like
    // the green v0.4.0. Only `--features advanced` (our native distribution) runs
    // the shim build below.
    #[cfg(feature = "advanced")]
    advanced_build::run();
}

/// Build the advanced-tier C++ shim and link it into the loadable extension.
/// Compiled ONLY under `--features advanced` (so `cc` is an optional build-dep
/// that is absent from the default community build).
#[cfg(feature = "advanced")]
mod advanced_build {
    use std::path::{Path, PathBuf};
    use std::process::Command;

    pub fn run() {
        // The advanced C++ shim is only meaningful when building against a real
        // DuckDB (the `duckdb-api` feature, which `advanced` pulls in). Guard
        // anyway so an odd feature combo never tries to compile it.
        if std::env::var("CARGO_FEATURE_DUCKDB_API").is_err() {
            return;
        }
        // Never compiled for a wasm target (that is the OTHER direction — the
        // wasm core, which compiles its own equivalent shims in-tree).
        let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
        if target_arch == "wasm32" {
            return;
        }
        // Never compiled on Windows. The advanced tier links against DuckDB's
        // internal C++ ABI with internal symbols left UNDEFINED in the shim
        // object, resolved at LOAD time against the host process. That
        // deferred-undefined model has no portable equivalent on Windows
        // PE/COFF (the MSVC linker requires every symbol resolved at link time
        // and rejects the GNU-ld `--allow-shlib-undefined` flag below). So
        // Windows builds the COMMON tier only and the advanced module is
        // compiled out on the Rust side too (`#[cfg(advanced_tier)]`, which we
        // never set here). Skip the C++ shim and emit no cdylib link-arg.
        let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
        if target_os == "windows" {
            return;
        }

        let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
        let ddb_root = resolve_duckdb_source(&out_dir);
        let include_dirs = read_include_dirs(&ddb_root);

        let mut build = cc::Build::new();
        build
            .cpp(true)
            .std("c++17")
            // DuckDB's internal headers gate symbol-visibility macros on this;
            // the bundled DuckDB build sets it, so match it for ABI-identical
            // inlines.
            .define("DUCKDB_BUILD_LIBRARY", None)
            .warnings(false)
            .flag_if_supported("-Wno-unused-parameter");
        for dir in &include_dirs {
            build.include(ddb_root.join(dir));
        }
        for tu in CPP_FILES {
            println!("cargo:rerun-if-changed=cpp/{tu}");
            build.file(format!("cpp/{tu}"));
        }
        build.compile("ducklink_advanced");

        // The advanced tier is compiled into this build: turn on the source-side
        // gate so the `advanced` module and its internal-ABI references compile.
        println!("cargo:rustc-cfg=advanced_tier");

        // Loadable build: the shim references internal DuckDB C++ symbols that
        // live in the loading process, not in this cdylib. Defer them to load
        // time. `rustc-cdylib-link-arg` applies ONLY to the cdylib (the
        // .duckdb_extension), never to the bundled test executable, where the
        // symbols are linked in.
        if cfg!(target_os = "macos") {
            println!("cargo:rustc-cdylib-link-arg=-undefined");
            println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
        } else {
            // ELF: allow undefined symbols in the shared object (resolved at load).
            println!("cargo:rustc-cdylib-link-arg=-Wl,--allow-shlib-undefined");
        }
    }

    /// The C++ shim translation units, compiled together into `libducklink_advanced`.
    const CPP_FILES: &[&str] = &[
        "ducklink_advanced.cpp",
        "ducklink_parser.cpp",
        "ducklink_optimizer.cpp",
        "ducklink_table_stream.cpp",
    ];

/// Resolve the DuckDB v1.5.4 source tree (header root) we compile the shim
/// against, version-locked to the `libduckdb-sys` crate this build depends on.
///
/// - bundled build: `libduckdb-sys` already extracted the source and published
///   its include dir via `DEP_DUCKDB_INCLUDE` (= `<root>/src/include`).
/// - loadable build (wrapper-only): `libduckdb-sys` does NOT extract the source,
///   so we extract its bundled `duckdb.tar.gz` ourselves into `OUT_DIR`.
fn resolve_duckdb_source(out_dir: &Path) -> PathBuf {
    if let Ok(inc) = std::env::var("DEP_DUCKDB_INCLUDE") {
        // <root>/src/include -> <root>
        let p = PathBuf::from(inc);
        if let Some(root) = p.parent().and_then(|p| p.parent()) {
            if root.join("manifest.json").exists() {
                return root.to_path_buf();
            }
        }
    }

    let dest = out_dir.join("duckdb-src");
    let root = dest.join("duckdb");
    if root.join("manifest.json").exists() {
        return root; // cached extraction
    }

    let tarball = find_libduckdb_sys_tarball()
        .expect("could not locate libduckdb-sys-1.10504.0/duckdb.tar.gz in the cargo registry");
    println!("cargo:rerun-if-changed={}", tarball.display());
    std::fs::create_dir_all(&dest).expect("create duckdb-src dir");
    let status = Command::new("tar")
        .arg("xzf")
        .arg(&tarball)
        .arg("-C")
        .arg(&dest)
        .status()
        .expect("failed to run tar to extract duckdb source");
    assert!(status.success(), "tar extraction of {} failed", tarball.display());
    assert!(
        root.join("manifest.json").exists(),
        "extracted duckdb source missing manifest.json at {}",
        root.display()
    );
    root
}

/// Find the version-locked `duckdb.tar.gz` shipped inside the `libduckdb-sys`
/// crate source in the cargo registry. The crate version is pinned in
/// `Cargo.toml`/`Cargo.lock`, so this hard-locks the header version.
fn find_libduckdb_sys_tarball() -> Option<PathBuf> {
    let cargo_home = std::env::var("CARGO_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(std::env::var("HOME").expect("HOME")).join(".cargo")
        });
    let registry_src = cargo_home.join("registry").join("src");
    let entries = std::fs::read_dir(&registry_src).ok()?;
    for entry in entries.flatten() {
        let candidate = entry
            .path()
            .join(LIBDUCKDB_SYS_DIR)
            .join("duckdb.tar.gz");
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

/// The pinned `libduckdb-sys` source directory name (crate version 1.10504.0 =
/// DuckDB v1.5.4). Keep in lock-step with `Cargo.toml`'s `libduckdb-sys` pin.
const LIBDUCKDB_SYS_DIR: &str = "libduckdb-sys-1.10504.0";

/// Read `manifest.json`'s `base.include_dirs` (the exact include set DuckDB
/// compiles its own sources with). Minimal hand parse to avoid a build-dep.
fn read_include_dirs(root: &Path) -> Vec<String> {
    let manifest = std::fs::read_to_string(root.join("manifest.json"))
        .expect("read duckdb manifest.json");
    let key = "\"include_dirs\"";
    let start = manifest
        .find(key)
        .expect("manifest.json missing include_dirs");
    let open = manifest[start..]
        .find('[')
        .map(|i| start + i)
        .expect("include_dirs array open");
    let close = manifest[open..]
        .find(']')
        .map(|i| open + i)
        .expect("include_dirs array close");
    let body = &manifest[open + 1..close];
    let mut dirs = Vec::new();
    let mut chars = body.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if c == '"' {
            // collect until the next unescaped quote
            let rest = &body[i + 1..];
            if let Some(end) = rest.find('"') {
                dirs.push(rest[..end].to_string());
                // advance the iterator past the closing quote
                for _ in 0..rest[..=end].chars().count() {
                    chars.next();
                }
            }
        }
    }
    assert!(!dirs.is_empty(), "no include_dirs parsed from manifest.json");
    dirs
}
} // mod advanced_build
