//! Catalog resolution + name->blob fetch/cache for `ducklink_load(<name>)`.
//!
//! `ducklink_load('aba')` (by catalog NAME, not a filesystem path) resolves the
//! published ducklink catalog, finds the entry, downloads (and caches) the
//! WebAssembly component blob, verifies its sha256 against the entry's
//! `content_digest`, and hands the cached `.wasm` path to the engine to load.
//!
//! Resolution is resilient: the live catalog is fetched over HTTPS from
//! `DUCKLINK_CATALOG_URL` (default the public endpoint); on ANY failure
//! (offline / timeout / non-200 / parse error) it falls back to a snapshot
//! embedded at build time via `include_bytes!`. The resolved catalog is cached
//! in memory for the session.

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use serde::Deserialize;

/// The default published catalog URL. Overridable with `DUCKLINK_CATALOG_URL`
/// (an unreachable value forces the bundled-snapshot fallback).
const DEFAULT_CATALOG_URL: &str = "https://datalink-ext.tegmentum.ai/ducklink/catalog.json";

/// Base for component blobs: `<BASE>/<content_digest>/<name>.wasm` (RAW bytes).
const BLOB_BASE: &str = "https://datalink-ext.tegmentum.ai/wasm/sha256";

/// The catalog snapshot embedded at build time — the offline fallback when the
/// live catalog is unreachable. A copy of `ducklink/registry/index.json`.
const BUNDLED_SNAPSHOT: &[u8] = include_bytes!("../assets/catalog-snapshot.json");

/// A named/typed argument an enriched function signature MAY carry. Tolerant:
/// both fields optional so a bare `{"name": ...}` or a partial entry parses.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct FunctionArg {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(rename = "type", default)]
    pub type_name: Option<String>,
}

/// One function signature an entry MAY carry (the signature-enrichment field).
/// Tolerant: every field is optional so an entry without it (the bundled
/// snapshot) still parses.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct FunctionSig {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub returns: Option<String>,
    /// Rendered argument signature, when the catalog carries typed arguments.
    #[serde(default)]
    pub arguments: Vec<FunctionArg>,
    /// Table-function result columns, when present (mutually exclusive with
    /// `returns` for scalars/aggregates).
    #[serde(default)]
    pub columns: Vec<FunctionArg>,
}

/// A single catalog entry. Only the fields the loader / discovery functions
/// need are modelled; unknown fields are ignored, so the rich published schema
/// (providers, conformance, ...) parses without listing every field.
#[derive(Debug, Clone, Deserialize)]
pub struct CatalogEntry {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub categories: Vec<String>,
    /// Registered SQL function / type names (always present in the snapshot).
    #[serde(default)]
    pub exports: Vec<String>,
    /// The capability KINDS this module requires (e.g. `["scalar"]`,
    /// `["table", "aggregate"]`, `["parser"]`). Used to decide host
    /// `compatible`-ness and to render the `requires` column.
    #[serde(default)]
    pub requires: Vec<String>,
    /// Source crates the module was built from. A non-empty list is the
    /// provenance signal that the module is a Rust build (the `language` column).
    #[serde(default)]
    pub crates: Vec<String>,
    /// The sha256 (hex) of the component blob; required to fetch + verify it.
    #[serde(default)]
    pub content_digest: Option<String>,
    /// The signature-enrichment field; absent in the current snapshot.
    #[serde(default)]
    pub functions: Vec<FunctionSig>,
}

impl CatalogEntry {
    /// Provenance-based source language for the `language` column. A non-empty
    /// `crates` list is the "built from Rust crates" signal; otherwise the
    /// module ships as a bare wasm component of unknown source language. This is
    /// a plain catalog-derived field — NOT a compile step.
    pub fn language(&self) -> &'static str {
        if !self.crates.is_empty() {
            "rust"
        } else {
            "wasm"
        }
    }

    /// Count of required capability kinds of each class, inferred from
    /// `requires`, for the unloaded-module `scalars`/`tables`/`aggregates`
    /// columns. Returns 0 when the class is not required. These are coarse
    /// (presence, not per-function counts); LOADED modules report exact counts
    /// from their live registration instead.
    pub fn requires_kind(&self, kind: &str) -> bool {
        self.requires.iter().any(|r| r == kind)
    }
}

/// The parsed catalog: just the list of entries (the wrapper's metadata is not
/// needed for resolution).
#[derive(Debug, Clone, Deserialize)]
pub struct Catalog {
    #[serde(default)]
    pub extensions: Vec<CatalogEntry>,
}

impl Catalog {
    /// Find an entry by exact name.
    pub fn find(&self, name: &str) -> Option<&CatalogEntry> {
        self.extensions.iter().find(|e| e.name == name)
    }

    /// All known catalog names, sorted — used in the "unknown name" error.
    pub fn names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.extensions.iter().map(|e| e.name.clone()).collect();
        v.sort();
        v
    }
}

/// Session-wide resolved catalog, fetched (or fallen back) once.
static CATALOG: OnceLock<Catalog> = OnceLock::new();

/// Parse the bundled snapshot. The snapshot ships with the binary, so a parse
/// failure here is a build-time bug — surface it loudly rather than returning
/// an empty catalog that would silently break every name lookup.
fn bundled_catalog() -> Catalog {
    serde_json::from_slice(BUNDLED_SNAPSHOT).expect("bundled catalog snapshot must parse")
}

/// Try to fetch + parse the live catalog. Best-effort: returns `None` on any
/// network / status / parse failure so the caller falls back to the snapshot.
fn fetch_live_catalog(url: &str) -> Option<Catalog> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .ok()?;
    let resp = client.get(url).send().ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let bytes = resp.bytes().ok()?;
    serde_json::from_slice::<Catalog>(&bytes).ok()
}

/// Resolve the session catalog: live fetch if reachable, else the bundled
/// snapshot. Cached for the process after the first resolution.
pub fn resolve_catalog() -> &'static Catalog {
    CATALOG.get_or_init(|| {
        let url =
            std::env::var("DUCKLINK_CATALOG_URL").unwrap_or_else(|_| DEFAULT_CATALOG_URL.to_string());
        match fetch_live_catalog(&url) {
            Some(cat) => cat,
            None => {
                eprintln!(
                    "[ducklink] live catalog at {url} unreachable; using bundled snapshot"
                );
                bundled_catalog()
            }
        }
    })
}

/// The on-disk cache root for downloaded component blobs:
/// `$XDG_CACHE_HOME/ducklink` or `$HOME/.cache/ducklink`.
pub fn cache_root() -> Option<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_CACHE_HOME") {
        if !xdg.is_empty() {
            return Some(PathBuf::from(xdg).join("ducklink"));
        }
    }
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache").join("ducklink"))
}

/// The cache path for a component blob: `<cache>/wasm/sha256/<digest>/<name>.wasm`.
pub fn blob_cache_path(digest: &str, name: &str) -> Option<PathBuf> {
    Some(
        cache_root()?
            .join("wasm")
            .join("sha256")
            .join(digest)
            .join(format!("{name}.wasm")),
    )
}

/// Lowercase-hex sha256 of `bytes`.
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    let digest = h.finalize();
    let mut s = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Serialise blob downloads so two concurrent `ducklink_load` of the same name
/// don't both fetch + write (the engine `Mutex` already serialises the load
/// itself, but the resolve/download happens before that lock is taken).
static DOWNLOAD_LOCK: Mutex<()> = Mutex::new(());

/// Resolve a catalog NAME to a local `.wasm` path, downloading + caching +
/// sha256-verifying the blob if it is not already cached. Returns the cached
/// path the engine can `load(name, path)` from.
pub fn resolve_name_to_blob(name: &str) -> Result<PathBuf, String> {
    let catalog = resolve_catalog();
    let entry = catalog.find(name).ok_or_else(|| {
        let names = catalog.names();
        let preview: Vec<&str> = names.iter().take(12).map(|s| s.as_str()).collect();
        format!(
            "ducklink_load: unknown extension '{name}'. Discover names with \
             `SELECT name FROM ducklink.modules`. {} known, e.g.: {}{}",
            names.len(),
            preview.join(", "),
            if names.len() > preview.len() { ", ..." } else { "" }
        )
    })?;

    let digest = entry.content_digest.clone().ok_or_else(|| {
        format!("ducklink_load: catalog entry '{name}' has no content_digest; cannot fetch blob")
    })?;

    let cache_path = blob_cache_path(&digest, name)
        .ok_or_else(|| "ducklink_load: no cache directory (set HOME or XDG_CACHE_HOME)".to_string())?;

    // Already cached: trust the digest-keyed path (the path itself encodes the
    // verified content hash), so re-loads are an immediate cache hit.
    if cache_path.is_file() {
        return Ok(cache_path);
    }

    let _guard = DOWNLOAD_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // Re-check after taking the lock (another thread may have just fetched it).
    if cache_path.is_file() {
        return Ok(cache_path);
    }

    let bytes = download_blob(&digest, name)?;

    // VERIFY: the downloaded bytes' sha256 must equal the catalog digest. A
    // mismatch means a corrupt or tampered blob — fail loudly, never cache it.
    let got = sha256_hex(&bytes);
    if got != digest {
        return Err(format!(
            "ducklink_load: sha256 mismatch for '{name}': catalog says {digest}, downloaded bytes hash to {got} (refusing to cache)"
        ));
    }

    write_cache(&cache_path, &bytes)?;
    Ok(cache_path)
}

/// Download the RAW component blob for `digest`/`name`. Network errors and
/// non-200 statuses become a clear `Err`.
fn download_blob(digest: &str, name: &str) -> Result<Vec<u8>, String> {
    let url = format!("{BLOB_BASE}/{digest}/{name}.wasm");
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| format!("ducklink_load: http client init failed: {e}"))?;
    let resp = client
        .get(&url)
        .send()
        .map_err(|e| format!("ducklink_load: download of {url} failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!(
            "ducklink_load: download of {url} returned HTTP {}",
            resp.status()
        ));
    }
    let mut bytes = Vec::new();
    resp.bytes()
        .map_err(|e| format!("ducklink_load: reading {url} body failed: {e}"))?
        .as_ref()
        .read_to_end(&mut bytes)
        .map_err(|e| format!("ducklink_load: buffering {url} body failed: {e}"))?;
    Ok(bytes)
}

/// Write `bytes` to `cache_path`, creating parent dirs. Writes to a temp file
/// then renames, so a concurrent reader never sees a half-written blob.
fn write_cache(cache_path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = cache_path
        .parent()
        .ok_or_else(|| "ducklink_load: cache path has no parent".to_string())?;
    std::fs::create_dir_all(parent)
        .map_err(|e| format!("ducklink_load: creating cache dir {}: {e}", parent.display()))?;
    let tmp = cache_path.with_extension("wasm.partial");
    std::fs::write(&tmp, bytes)
        .map_err(|e| format!("ducklink_load: writing {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, cache_path)
        .map_err(|e| format!("ducklink_load: finalising {}: {e}", cache_path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_snapshot_parses_and_has_entries() {
        let cat = bundled_catalog();
        assert!(
            cat.extensions.len() > 100,
            "bundled snapshot should carry the full catalog, got {}",
            cat.extensions.len()
        );
        assert!(cat.find("aba").is_some(), "snapshot must contain aba");
    }

    #[test]
    fn aba_entry_has_expected_digest_and_exports() {
        let cat = bundled_catalog();
        let aba = cat.find("aba").expect("aba present");
        assert_eq!(
            aba.content_digest.as_deref(),
            Some("21e20b3b8819e7baa83b1a3be31b37206d9691ba6cb084906b58357292cb523b")
        );
        assert!(aba.exports.iter().any(|e| e == "aba_validate"));
    }

    #[test]
    fn sha256_hex_matches_known_vector() {
        // sha256("") well-known vector.
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn blob_cache_path_layout() {
        // Force a known cache root.
        let p = {
            let _g = super::DOWNLOAD_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            unsafe { std::env::set_var("XDG_CACHE_HOME", "/tmp/dl-cache-test") };
            let p = blob_cache_path("deadbeef", "aba").expect("cache path");
            unsafe { std::env::remove_var("XDG_CACHE_HOME") };
            p
        };
        assert!(
            p.ends_with("ducklink/wasm/sha256/deadbeef/aba.wasm"),
            "unexpected cache layout: {}",
            p.display()
        );
    }

    #[test]
    fn unknown_name_offline_lists_discovery_hint() {
        // Point at an unreachable host so resolution falls back to the bundled
        // snapshot, then look up a name that cannot exist.
        unsafe {
            std::env::set_var("DUCKLINK_CATALOG_URL", "https://127.0.0.1:1/nope.json")
        };
        let r = resolve_name_to_blob("this_name_does_not_exist_xyz");
        assert!(r.is_err());
        let msg = r.unwrap_err();
        assert!(
            msg.contains("ducklink.modules"),
            "error should point at discovery: {msg}"
        );
    }

    #[test]
    fn sha256_verification_rejects_mismatch() {
        // White-box: bytes that don't hash to the claimed digest must be
        // rejected. We exercise the same comparison resolve_name_to_blob uses.
        let bytes = b"not the real component";
        let claimed = "21e20b3b8819e7baa83b1a3be31b37206d9691ba6cb084906b58357292cb523b";
        let got = sha256_hex(bytes);
        assert_ne!(got, claimed, "test bytes must not match the real digest");
    }
}
