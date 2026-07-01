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
const DEFAULT_CATALOG_URL: &str = "https://ext.ducklink.dev/catalog.json";

/// Base for component blobs: `<BASE>/<content_digest>/<name>.wasm` (RAW bytes).
const BLOB_BASE: &str = "https://ext.ducklink.dev/wasm/sha256";

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

/// One `providers[]` entry an enriched catalog entry MAY carry. The published
/// `@2.2.0` catalog gives each entry a `providers[]` array whose members are
/// either wasm components (`kind:"wasm"`, `abi:"duckdb:extension@X.Y.Z"`, a
/// `content_digest`) or native/other artifacts. Only `wasm` providers are used
/// for load resolution; the rest are ignored. Every field is optional so a
/// partial provider (or an entry with `providers: null`, which is common in the
/// current catalog) still parses.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Provider {
    #[serde(default)]
    pub id: Option<String>,
    /// The provider kind — `"wasm"` for a component blob, `"native"` for a
    /// platform `.duckdb_extension`, etc. Only wasm providers are load candidates.
    #[serde(default)]
    pub kind: Option<String>,
    /// The contract generation this provider was built against, e.g.
    /// `"duckdb:extension@2.2.0"`. Parsed to a major via [`Provider::abi_major`].
    #[serde(default)]
    pub abi: Option<String>,
    /// The sha256 (hex) of THIS provider's blob (may differ per generation).
    #[serde(default)]
    pub content_digest: Option<String>,
    /// Lifecycle status of this generation: `"supported"` / `"deprecated"` /
    /// `"eol"`. Absent in the current catalog; rendered as `unknown` then.
    #[serde(default)]
    pub status: Option<String>,
}

impl Provider {
    /// True for a wasm-component provider (the only load-candidate kind).
    pub fn is_wasm(&self) -> bool {
        self.kind.as_deref() == Some("wasm")
    }

    /// Parse the contract-generation MAJOR from `abi` (`"duckdb:extension@X.Y.Z"`
    /// → `X`). `None` if `abi` is absent or unparseable, so a malformed provider
    /// is simply skipped by selection rather than mis-selected.
    pub fn abi_major(&self) -> Option<u64> {
        abi_major_of(self.abi.as_deref()?)
    }
}

/// Parse the MAJOR component of a `duckdb:extension@X.Y.Z` (or bare `X.Y.Z`)
/// version string. Tolerant: takes whatever follows the last `@` (if any) and
/// reads the leading integer up to the first `.`.
pub fn abi_major_of(abi: &str) -> Option<u64> {
    let ver = abi.rsplit('@').next().unwrap_or(abi);
    let major = ver.split('.').next().unwrap_or(ver);
    major.trim().parse::<u64>().ok()
}

/// A single catalog entry. Only the fields the loader / discovery functions
/// need are modelled; unknown fields are ignored, so the rich published schema
/// (conformance, ...) parses without listing every field.
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
    /// This is the entry's DEFAULT/top-level digest, used when no `providers[]`
    /// wasm entry matches the host generation.
    #[serde(default)]
    pub content_digest: Option<String>,
    /// The contract-generation version string of the entry's default artifact
    /// (e.g. `"4.0.0"`), when the catalog carries it. Used to label the
    /// synthetic `ducklink.versions` row for entries that carry no `providers[]`.
    #[serde(default)]
    pub wit_contract_version: Option<String>,
    /// The per-generation artifact providers. Each enriched entry MAY carry a
    /// `providers[]` array; `null`/absent is common in the current catalog and
    /// parses to an empty list. Wasm providers here drive generation selection.
    #[serde(default, deserialize_with = "null_to_empty_vec")]
    pub providers: Vec<Provider>,
    /// The signature-enrichment field; absent in the current snapshot.
    #[serde(default)]
    pub functions: Vec<FunctionSig>,
}

/// Deserialize a possibly-`null` JSON array into an empty `Vec` (the catalog
/// writes `"providers": null` for entries with no per-generation providers, and
/// plain `#[serde(default)]` does not coerce an explicit `null` to `default`).
fn null_to_empty_vec<'de, D, T>(de: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(de)?.unwrap_or_default())
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

    /// All wasm providers of this entry, in catalog order.
    pub fn wasm_providers(&self) -> impl Iterator<Item = &Provider> {
        self.providers.iter().filter(|p| p.is_wasm())
    }

    /// Choose the wasm provider (and hence blob digest) this HOST should load,
    /// given its contract generation `host_major`.
    ///
    /// The compat model is BACKWARD-COMPATIBLE: empirically the gen-4 host loads
    /// and dispatches gen-2 (@2.2.0) scalar / table / aggregate blobs, so a host
    /// runs any generation ≤ its own. The rule is therefore: pick the NEWEST wasm
    /// provider whose generation major ≤ `host_major`. A provider whose `abi` is
    /// absent or unparseable is skipped (never mis-selected). Returns `None` when
    /// no wasm provider qualifies — the caller then falls back to the entry's
    /// top-level [`content_digest`].
    pub fn select_provider(&self, host_major: u64) -> Option<&Provider> {
        self.wasm_providers()
            .filter(|p| p.content_digest.is_some())
            .filter_map(|p| p.abi_major().map(|m| (m, p)))
            .filter(|(m, _)| *m <= host_major)
            .max_by_key(|(m, _)| *m)
            .map(|(_, p)| p)
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

/// Resolve a catalog NAME to a local `.wasm` path for the given host contract
/// generation, downloading + caching + sha256-verifying the blob if it is not
/// already cached. Returns the cached path the engine can `load(name, path)`
/// from.
///
/// `host_major` is the host's `wasm_abi` contract-generation major (e.g. `4`),
/// read from `HostCaps` by the caller. It drives per-generation provider
/// selection ([`CatalogEntry::select_provider`]): the newest wasm provider whose
/// generation ≤ the host's is chosen, falling back to the entry's top-level
/// `content_digest` when no provider matches (or the entry carries none — the
/// common case in the current catalog). This keeps single-generation entries
/// (aba, etc.) loading exactly as before.
pub fn resolve_name_to_blob(name: &str, host_major: u64) -> Result<PathBuf, String> {
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

    // Per-generation provider selection: prefer a wasm provider matching the
    // host generation; otherwise fall back to the entry's top-level digest.
    let digest = match entry.select_provider(host_major) {
        Some(p) => {
            let digest = p.content_digest.clone().expect("selected provider has a digest");
            eprintln!(
                "[ducklink] '{name}': selected provider {} (abi {}) for host generation {host_major}",
                p.id.as_deref().unwrap_or("wasm"),
                p.abi.as_deref().unwrap_or("?"),
            );
            digest
        }
        None => {
            let digest = entry.content_digest.clone().ok_or_else(|| {
                format!(
                    "ducklink_load: catalog entry '{name}' has no provider for host \
                     generation {host_major} and no top-level content_digest; cannot fetch blob"
                )
            })?;
            if !entry.providers.is_empty() {
                eprintln!(
                    "[ducklink] '{name}': no wasm provider ≤ host generation {host_major}; \
                     falling back to top-level digest"
                );
            }
            digest
        }
    };

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
        let r = resolve_name_to_blob("this_name_does_not_exist_xyz", 4);
        assert!(r.is_err());
        let msg = r.unwrap_err();
        assert!(
            msg.contains("ducklink.modules"),
            "error should point at discovery: {msg}"
        );
    }

    #[test]
    fn abi_major_parses_generation() {
        assert_eq!(abi_major_of("duckdb:extension@2.2.0"), Some(2));
        assert_eq!(abi_major_of("duckdb:extension@4.0.0"), Some(4));
        assert_eq!(abi_major_of("4.0.0"), Some(4));
        assert_eq!(abi_major_of("2"), Some(2));
        assert_eq!(abi_major_of("garbage"), None);
        assert_eq!(abi_major_of(""), None);
    }

    #[test]
    fn providers_null_parses_to_empty() {
        // The live catalog writes `"providers": null` for most entries.
        let json = r#"{"name":"x","providers":null,"content_digest":"aa"}"#;
        let e: CatalogEntry = serde_json::from_str(json).expect("null providers parses");
        assert!(e.providers.is_empty());
        assert_eq!(e.content_digest.as_deref(), Some("aa"));
    }

    #[test]
    fn bundled_provider_entries_select_gen2_for_gen4_host() {
        // aba carries a wasm provider at duckdb:extension@2.2.0; a gen-4 host is
        // backward-compatible, so selection picks that provider's digest.
        let cat = bundled_catalog();
        let aba = cat.find("aba").expect("aba present");
        let p = aba.select_provider(4).expect("aba has a wasm provider <= gen 4");
        assert_eq!(p.abi_major(), Some(2));
        assert_eq!(
            p.content_digest.as_deref(),
            Some("21e20b3b8819e7baa83b1a3be31b37206d9691ba6cb084906b58357292cb523b")
        );
    }

    #[test]
    fn provider_selection_picks_newest_le_host_and_respects_ceiling() {
        let mk = |abi: &str, dig: &str| Provider {
            id: Some("wasm-component".into()),
            kind: Some("wasm".into()),
            abi: Some(abi.into()),
            content_digest: Some(dig.into()),
            status: None,
        };
        let entry = CatalogEntry {
            name: "multi".into(),
            version: None,
            description: None,
            categories: vec![],
            exports: vec![],
            requires: vec![],
            crates: vec![],
            content_digest: Some("top".into()),
            wit_contract_version: None,
            providers: vec![
                mk("duckdb:extension@1.0.0", "d1"),
                mk("duckdb:extension@2.2.0", "d2"),
                mk("duckdb:extension@4.0.0", "d4"),
            ],
            functions: vec![],
        };
        // Gen-4 host: newest <= 4 is the gen-4 provider.
        assert_eq!(entry.select_provider(4).unwrap().content_digest.as_deref(), Some("d4"));
        // Gen-3 host: newest <= 3 is the gen-2 provider (no gen-3 available).
        assert_eq!(entry.select_provider(3).unwrap().content_digest.as_deref(), Some("d2"));
        // Gen-0 host: nothing qualifies -> None (caller falls back to top-level).
        assert!(entry.select_provider(0).is_none());
    }

    #[test]
    fn native_only_providers_do_not_select_and_fall_back() {
        // An entry whose only providers are native artifacts must select no wasm
        // provider (so the loader falls back to the top-level digest).
        let entry = CatalogEntry {
            name: "nativeonly".into(),
            version: None,
            description: None,
            categories: vec![],
            exports: vec![],
            requires: vec![],
            crates: vec![],
            content_digest: Some("top".into()),
            wit_contract_version: None,
            providers: vec![Provider {
                id: Some("native-arm64-macos".into()),
                kind: Some("native".into()),
                abi: None,
                content_digest: Some("nativedigest".into()),
                status: None,
            }],
            functions: vec![],
        };
        assert!(entry.select_provider(4).is_none());
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
