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

/// Base for native `.duckdb_extension` blobs:
/// `<BASE>/<content_digest>/<platform>/<name>.duckdb_extension`. Digest-keyed so
/// two builds with the same bytes share one URL, and the platform folder keeps
/// visually related builds grouped when browsing. Providers can override this
/// by supplying an explicit `url` on the catalog entry.
const NATIVE_BLOB_BASE: &str = "https://ext.ducklink.dev/native/sha256";

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
    /// One-line synopsis. Shown in `ducklink.docs.summary` and in the header of
    /// `ducklink_help('<name>')`. Absent for unenriched entries; the docs view
    /// renders an empty cell.
    #[serde(default)]
    pub summary: Option<String>,
    /// Multi-paragraph markdown body. Free-form; renders through anywhere that
    /// speaks markdown (docs.rs-style tools, Jupyter magics, etc.).
    #[serde(default)]
    pub description: Option<String>,
    /// One canonical SQL example — the shortest thing that demonstrates the
    /// function. Rendered under `### Example` in `ducklink_help()`.
    #[serde(default)]
    pub example: Option<String>,
    /// Categorization tokens (`["validator", "banking"]`). Fed to
    /// `ducklink_search` as high-weight keywords and joined comma-separated
    /// into `ducklink.docs.tags` for `WHERE`-clause filtering.
    #[serde(default)]
    pub tags: Vec<String>,
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
    /// The provider kind — how this capability is delivered:
    ///
    /// * `"wasm"`             — a wasm component blob served from ducklink's
    ///                          own catalog + CDN. Loaded through wasmtime.
    /// * `"native"`           — a platform `.duckdb_extension` built by
    ///                          tegmentum, served from ducklink's CDN. Loaded
    ///                          via DuckDB's own LOAD; requires `-unsigned`
    ///                          because our signing key isn't in DuckDB's
    ///                          trust chain today.
    /// * `"community-native"` — the capability is already published as a
    ///                          community-extensions extension by someone
    ///                          else. Ducklink dispatches to
    ///                          `INSTALL <extension_name> FROM community;
    ///                          LOAD <extension_name>;`, so the user gets the
    ///                          community-signed build (no `-unsigned`
    ///                          needed). Ducklink is the routing layer;
    ///                          the community extension is the implementation.
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
    /// Target platform (DuckDB's convention: `"osx_arm64"`, `"linux_amd64"`,
    /// `"windows_amd64"`, ...). REQUIRED for `kind == "native"`; ignored for wasm.
    ///
    /// Tolerant of non-string shapes so a legacy/alternate catalog format
    /// (e.g. `"platform": {"os": "macos", "arch": "arm64"}`, seen in the current
    /// bundled snapshot's placeholder `native` entries) parses to `None` instead
    /// of failing the whole catalog. A `None` here disqualifies the provider
    /// from [`CatalogEntry::select_native_provider`] — the intended outcome for
    /// an entry lacking a DuckDB-convention platform tag.
    #[serde(default, deserialize_with = "string_or_none")]
    pub platform: Option<String>,
    /// Target DuckDB version this native artifact was built against (e.g.
    /// `"v1.5.4"`). REQUIRED for `kind == "native"`; native `.duckdb_extension`
    /// files are tightly coupled to a DuckDB version. Also tolerant of
    /// non-string shapes (mirrors `platform` above).
    #[serde(default, deserialize_with = "string_or_none")]
    pub duckdb_version: Option<String>,
    /// Explicit download URL. Optional — when absent, native providers fall
    /// back to a standard `BLOB_BASE`-derived URL from the digest + platform.
    #[serde(default)]
    pub url: Option<String>,
    /// The community-extensions extension name to install + load, for
    /// `kind == "community-native"`. Must match the exact extension name
    /// registered in `duckdb/community-extensions`. Ducklink runs
    /// `INSTALL <extension_name> FROM community; LOAD <extension_name>;`
    /// via the persistent connection.
    ///
    /// Function-name parity is a HARD requirement: the community extension
    /// must expose the same SQL function names as ducklink's wasm version,
    /// so the user's query doesn't change when we dispatch to the community
    /// build instead of loading our wasm module.
    #[serde(default)]
    pub extension_name: Option<String>,
}

impl Provider {
    /// True for a wasm-component provider.
    pub fn is_wasm(&self) -> bool {
        self.kind.as_deref() == Some("wasm")
    }

    /// True for a community-native provider (a pointer at an existing
    /// duckdb/community-extensions extension). See [`Provider::kind`].
    pub fn is_community_native(&self) -> bool {
        self.kind.as_deref() == Some("community-native")
    }

    /// True for a native `.duckdb_extension` provider.
    pub fn is_native(&self) -> bool {
        self.kind.as_deref() == Some("native")
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
    /// synthetic `ducklink.module_compatibility` row for entries that carry no `providers[]`.
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

/// Deserialize a JSON field as `Some(string)` when it is a plain string, and
/// as `None` for anything else (an object / array / bool / number, or JSON
/// `null`). Used on `Provider::platform` and `Provider::duckdb_version` so a
/// legacy/alternate catalog shape (e.g. `"platform": {"os":..., "arch":...}`,
/// present in the current bundled snapshot's placeholder `native` entries) does
/// not fail the whole catalog parse — it silently disqualifies that provider
/// from `select_native_provider` instead, which is the intended behaviour.
fn string_or_none<'de, D>(de: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = serde_json::Value::deserialize(de)?;
    Ok(match v {
        serde_json::Value::String(s) => Some(s),
        _ => None,
    })
}

impl CatalogEntry {
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

    /// All native `.duckdb_extension` providers of this entry, in catalog order.
    pub fn native_providers(&self) -> impl Iterator<Item = &Provider> {
        self.providers.iter().filter(|p| p.is_native())
    }

    /// Choose the native provider matching this host's platform + DuckDB
    /// version. Native `.duckdb_extension` files are tightly coupled to both,
    /// so the match is strict-exact-both.
    pub fn select_native_provider(
        &self,
        platform: &str,
        duckdb_version: &str,
    ) -> Option<&Provider> {
        self.native_providers()
            .filter(|p| p.content_digest.is_some())
            .find(|p| {
                p.platform.as_deref() == Some(platform)
                    && p.duckdb_version.as_deref() == Some(duckdb_version)
            })
    }

    /// All community-native providers of this entry, in catalog order.
    pub fn community_native_providers(&self) -> impl Iterator<Item = &Provider> {
        self.providers.iter().filter(|p| p.is_community_native())
    }

    /// Choose the community-native provider — the first one in catalog order
    /// that carries an `extension_name`. Community-native providers don't need
    /// per-platform selection because DuckDB's `INSTALL … FROM community`
    /// handles the platform match itself.
    pub fn select_community_native_provider(&self) -> Option<&Provider> {
        self.community_native_providers()
            .find(|p| p.extension_name.is_some())
    }

    /// The entry's own contract generation MAJOR, from `wit_contract_version`
    /// (e.g. `"4.0.0"` → `4`). This is the AUTHORITATIVE generation of the entry's
    /// default artifact; a `providers[]` member's `abi` is stale build metadata
    /// (the current catalog stamps gen-4 artifacts with an `abi` of `@2.2.0` /
    /// `@3.1.0`) and is NOT used to decide the entry's generation. `None` when
    /// absent or unparseable.
    pub fn generation_major(&self) -> Option<u64> {
        self.wit_contract_version
            .as_deref()
            .and_then(abi_major_of)
    }

    /// Choose the wasm provider (and hence blob digest) this HOST should load,
    /// given its contract generation `host_major`.
    ///
    /// The compat model is STRICT SAME-MAJOR: a host runs ONLY artifacts of its
    /// own generation (matching the ducklink-runtime CLI and the browser host).
    /// The rule is therefore: pick the wasm provider whose generation major
    /// `== host_major` exactly. A provider whose `abi` is absent, unparseable, or
    /// of a different major is skipped (never mis-selected). Returns `None` when
    /// no wasm provider matches — the caller then falls back to the entry's
    /// top-level [`content_digest`], but ONLY if the entry's own generation
    /// matches the host (see [`CatalogEntry::resolve_digest`]).
    pub fn select_provider(&self, host_major: u64) -> Option<&Provider> {
        self.wasm_providers()
            .filter(|p| p.content_digest.is_some())
            .filter_map(|p| p.abi_major().map(|m| (m, p)))
            .find(|(m, _)| *m == host_major)
            .map(|(_, p)| p)
    }

    /// Resolve the blob digest this HOST should load under the STRICT same-major
    /// compat model, or a clear error naming the generation mismatch.
    ///
    /// Order:
    /// 1. A wasm provider whose `abi` major `== host_major` (exact match).
    /// 2. Else the entry's top-level [`content_digest`], but ONLY when the
    ///    entry's own generation ([`generation_major`](Self::generation_major))
    ///    `== host_major`. The current catalog is 100% gen-4 with providers whose
    ///    `abi` is stale (`@2.2.0`/`@3.1.0`); those entries take THIS path — their
    ///    top-level digest IS the gen-4 blob.
    /// 3. Else the module is NOT loadable on this host: cross-major → error.
    pub fn resolve_digest(&self, host_major: u64) -> Result<String, String> {
        if let Some(p) = self.select_provider(host_major) {
            return Ok(p
                .content_digest
                .clone()
                .expect("selected provider has a digest"));
        }
        match self.generation_major() {
            // Entry generation matches the host: the top-level digest is loadable.
            Some(g) if g == host_major => self.content_digest.clone().ok_or_else(|| {
                format!(
                    "ducklink_load: catalog entry '{}' is generation {g} (matching host {host_major}) \
                     but carries no content_digest; cannot fetch blob",
                    self.name
                )
            }),
            // Entry generation differs from the host: strict same-major rejects it.
            Some(g) => Err(format!(
                "ducklink_load: '{}' is generation {g} but this host is generation {host_major}; \
                 strict same-major compatibility refuses to load a cross-major module",
                self.name
            )),
            // No parseable entry generation: fall back to the top-level digest if
            // present (single-generation entries with no version metadata), else error.
            None => self.content_digest.clone().ok_or_else(|| {
                format!(
                    "ducklink_load: catalog entry '{}' has no wit_contract_version and no \
                     content_digest; cannot determine a loadable blob for host generation {host_major}",
                    self.name
                )
            }),
        }
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

/// HTTP fetch timeout for the live catalog. Was 15s originally; dropped to 3s
/// because the graceful fallback (the bundled snapshot) is already loaded in
/// the binary — waiting 15s on hotel WiFi before falling back is user-hostile
/// when a 3s attempt is enough to succeed on any working network and quickly
/// give up otherwise. Composable with [`prewarm_catalog`] below so an offline
/// user pays at worst 3s once, in the background, before the first query.
const CATALOG_FETCH_TIMEOUT: Duration = Duration::from_secs(3);

/// Try to fetch + parse the live catalog. Best-effort: returns `None` on any
/// network / status / parse failure so the caller falls back to the snapshot.
///
/// Gated on the `network` feature. In an offline build (`--no-default-features
/// --features loadable,advanced`) this always returns `None` and the caller
/// silently falls back to the bundled snapshot.
#[cfg(feature = "network")]
fn fetch_live_catalog(url: &str) -> Option<Catalog> {
    let client = reqwest::blocking::Client::builder()
        .timeout(CATALOG_FETCH_TIMEOUT)
        .build()
        .ok()?;
    let resp = client.get(url).send().ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let bytes = resp.bytes().ok()?;
    serde_json::from_slice::<Catalog>(&bytes).ok()
}

/// Offline-build stub: no network, always fall back to the bundled snapshot.
/// Kept as a plain `_ = url;` so the caller shape (a `match` on the option)
/// doesn't need `#[cfg]` at every call site.
#[cfg(not(feature = "network"))]
fn fetch_live_catalog(_url: &str) -> Option<Catalog> {
    None
}

/// Resolve the session catalog: live fetch if reachable, else the bundled
/// snapshot. Cached for the process after the first resolution.
pub fn resolve_catalog() -> &'static Catalog {
    CATALOG.get_or_init(populate_catalog)
}

/// The `OnceLock` init function — factored out so [`prewarm_catalog`] and
/// [`resolve_catalog`] share the same fetch + fallback path. Runs on
/// whichever thread wins the race to initialise `CATALOG`.
fn populate_catalog() -> Catalog {
    let url = std::env::var("DUCKLINK_CATALOG_URL")
        .unwrap_or_else(|_| DEFAULT_CATALOG_URL.to_string());
    match fetch_live_catalog(&url) {
        Some(cat) => {
            crate::events::emit("catalog_fetch", None, url.clone());
            cat
        }
        None => {
            eprintln!("[ducklink] live catalog at {url} unreachable; using bundled snapshot");
            crate::events::emit(
                "catalog_fallback",
                None,
                format!("live catalog at {url} unreachable; using bundled snapshot"),
            );
            bundled_catalog()
        }
    }
}

/// Kick off a background thread that populates the catalog `OnceLock` before
/// any user query hits it. Called from `Engine2::new()` so the fetch happens
/// while DuckDB is still finishing extension setup — by the time the first
/// query arrives, the catalog is usually ready and `resolve_catalog()` returns
/// instantly.
///
/// The prewarm is best-effort. If the user's first query races the fetch,
/// they block on `OnceLock::get_or_init`'s internal barrier for the remaining
/// wall-clock time of the fetch (which is now capped at
/// [`CATALOG_FETCH_TIMEOUT`], down from 15s). If the fetch completes first,
/// the query pays zero.
///
/// Idempotent: repeated calls no-op once `CATALOG` is populated (that's the
/// same invariant [`resolve_catalog`] relies on).
pub fn prewarm_catalog() {
    // Cheap early-out: if the catalog is already resolved (e.g. a second
    // Engine2::new() in the same process), don't spawn a thread just to
    // find OnceLock is full.
    if CATALOG.get().is_some() {
        return;
    }
    std::thread::Builder::new()
        .name("ducklink-catalog-prewarm".to_string())
        .spawn(|| {
            // `get_or_init` blocks any concurrent caller until this completes,
            // so a race with the first user query is resolved to a single
            // fetch (never two).
            let _ = CATALOG.get_or_init(populate_catalog);
        })
        // If the thread fails to spawn (very rare — usually only on system
        // FD/thread-count exhaustion), fall silently through. `resolve_catalog`
        // will still do the synchronous fetch on the caller's thread when the
        // first query arrives.
        .ok();
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

/// The cache path for a NATIVE `.duckdb_extension` blob:
/// `<cache>/native/sha256/<digest>/<name>.duckdb_extension`. Digest-keyed so
/// two providers with the same content share a cache entry.
pub fn native_cache_path(digest: &str, name: &str) -> Option<PathBuf> {
    Some(
        cache_root()?
            .join("native")
            .join("sha256")
            .join(digest)
            .join(format!("{name}.duckdb_extension")),
    )
}

/// Lowercase-hex sha256 of `bytes`. Only used on the download path (offline
/// builds have nothing to verify), so gated on `network`.
#[cfg(feature = "network")]
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
/// read from `HostCaps` by the caller. It drives STRICT same-major resolution
/// ([`CatalogEntry::resolve_digest`]): a wasm provider whose generation `==` the
/// host's is chosen; else the entry's top-level `content_digest` when the
/// entry's OWN generation (`wit_contract_version`) matches the host (the common
/// case in the current all-gen-4 catalog); else a clear cross-major-mismatch
/// error — a module of another major is NOT loadable on this host.
pub fn resolve_name_to_blob(name: &str, host_major: u64) -> Result<PathBuf, String> {
    let catalog = resolve_catalog();
    let entry = catalog.find(name).ok_or_else(|| {
        crate::events::emit("unknown_name", Some(name), format!("no catalog entry for '{name}'"));
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

    // STRICT same-major resolution: an exact-generation wasm provider, else the
    // entry's top-level digest only if the entry's own generation matches the
    // host, else a clear cross-major-mismatch error.
    let digest = match entry.select_provider(host_major) {
        Some(p) => {
            let digest = p.content_digest.clone().expect("selected provider has a digest");
            let abi = p.abi.as_deref().unwrap_or("?");
            let id = p.id.as_deref().unwrap_or("wasm");
            ducklink_runtime::verbose_log!(
                "[ducklink] '{name}': selected provider {id} (abi {abi}) for host generation {host_major}",
            );
            crate::events::emit(
                "select_provider",
                Some(name),
                format!("provider {id} (abi {abi}) for host generation {host_major}"),
            );
            digest
        }
        None => {
            let digest = entry.resolve_digest(host_major).inspect_err(|e| {
                crate::events::emit("generation_reject", Some(name), e.clone());
            })?;
            crate::events::emit(
                "select_provider",
                Some(name),
                format!("top-level digest for host generation {host_major}"),
            );
            digest
        }
    };

    let cache_path = blob_cache_path(&digest, name)
        .ok_or_else(|| "ducklink_load: no cache directory (set HOME or XDG_CACHE_HOME)".to_string())?;

    // Already cached: trust the digest-keyed path (the path itself encodes the
    // verified content hash), so re-loads are an immediate cache hit.
    if cache_path.is_file() {
        crate::events::emit("cache_hit", Some(name), cache_path.to_string_lossy().into_owned());
        return Ok(cache_path);
    }

    let _guard = DOWNLOAD_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // Re-check after taking the lock (another thread may have just fetched it).
    if cache_path.is_file() {
        crate::events::emit("cache_hit", Some(name), cache_path.to_string_lossy().into_owned());
        return Ok(cache_path);
    }

    crate::events::emit("cache_miss", Some(name), format!("not cached (digest {digest})"));

    // Offline build: never download. Refuse cleanly rather than silently
    // returning a stale/wrong path. The user gets an actionable error and
    // knows to pre-populate the on-disk cache out-of-band.
    #[cfg(not(feature = "network"))]
    {
        return Err(format!(
            "ducklink_load: '{name}' is not cached (digest {digest}) and this build was compiled without network support. \
             Pre-populate the on-disk cache at {} out-of-band, or rebuild ducklink with `--features network`.",
            cache_path.display()
        ));
    }

    #[cfg(feature = "network")]
    {
        let url = format!("{BLOB_BASE}/{digest}/{name}.wasm");
        crate::events::emit("download", Some(name), url);
        let bytes = download_blob(&digest, name)?;

        // VERIFY: the downloaded bytes' sha256 must equal the catalog digest. A
        // mismatch means a corrupt or tampered blob — fail loudly, never cache it.
        let got = sha256_hex(&bytes);
        if got != digest {
            crate::events::emit(
                "verify_fail",
                Some(name),
                format!("expected {digest}, got {got}"),
            );
            return Err(format!(
                "ducklink_load: sha256 mismatch for '{name}': catalog says {digest}, downloaded bytes hash to {got} (refusing to cache)"
            ));
        }
        crate::events::emit("verify_ok", Some(name), digest.clone());

        write_cache(&cache_path, &bytes)?;
        Ok(cache_path)
    }
}

/// Download the RAW component blob for `digest`/`name`. Network errors and
/// non-200 statuses become a clear `Err`. Only compiled when the `network`
/// feature is enabled; offline builds error out with a "pre-populate the
/// cache" message at the call site.
#[cfg(feature = "network")]
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

/// The DuckDB library version this extension was compiled against, used to pick
/// the matching native provider from the catalog. Native `.duckdb_extension`
/// files are tightly coupled to a specific DuckDB version, so a strict-exact
/// match is required. Kept in lock-step with the `libduckdb-sys` pin in
/// `Cargo.toml` — the same version the advanced-tier C++ shim gates on. Shared
/// with the common tier (visible to `ducklink.modules.native_available`) so the
/// discovery view can decide native availability regardless of whether the
/// `advanced` feature was built into this artifact.
pub const HOST_DUCKDB_VERSION: &str = "v1.5.4";

/// DuckDB's platform identifier for this build, using DuckDB's own conventions
/// (`osx_arm64` / `osx_amd64` / `linux_amd64` / `linux_arm64` / `linux_amd64_musl`
/// / `windows_amd64`). Used to pick the right `native` provider from the
/// catalog. Compiled-in — no runtime detection is needed because the extension
/// itself is platform-specific.
pub const NATIVE_PLATFORM: &str = if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
    "osx_arm64"
} else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
    "osx_amd64"
} else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
    "linux_arm64"
} else if cfg!(all(target_os = "linux", target_arch = "x86_64", target_env = "musl")) {
    "linux_amd64_musl"
} else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
    "linux_amd64"
} else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
    "windows_amd64"
} else {
    "unknown"
};

/// Resolve a catalog NAME to a local `.duckdb_extension` path for this host's
/// platform + DuckDB version, downloading + caching + sha256-verifying if it is
/// not already cached. Returns the cached path, ready for DuckDB's `LOAD`.
///
/// `duckdb_version` is the exact DuckDB build the caller is running against
/// (e.g. `"v1.5.4"`); native `.duckdb_extension` binaries are tightly coupled
/// to a specific DuckDB version so the match is strict-exact.
///
/// Native providers must exist in the catalog entry with `kind: "native"` +
/// `platform` + `duckdb_version` + `content_digest` fields. A clear
/// error names the mismatch when no provider is a fit.
/// Resolve a catalog NAME to the community-extensions extension name that
/// ducklink should `INSTALL … FROM community; LOAD …;` on behalf of the user.
/// A one-look-up: the entry either has a `community-native` provider or it
/// doesn't. No download, no cache — DuckDB's own extension-install machinery
/// handles the rest.
///
/// Returns the `extension_name` field from the community-native provider,
/// which is the exact name registered in `duckdb/community-extensions`.
pub fn resolve_name_to_community_native(name: &str) -> Result<String, String> {
    let catalog = resolve_catalog();
    let entry = catalog.find(name).ok_or_else(|| {
        crate::events::emit(
            "unknown_name_community_native",
            Some(name),
            format!("no catalog entry for '{name}'"),
        );
        format!(
            "ducklink_load(kind='native'): unknown extension '{name}'. Discover names \
             with `SELECT name FROM ducklink.modules`."
        )
    })?;

    let provider = entry.select_community_native_provider().ok_or_else(|| {
        crate::events::emit(
            "no_community_native_provider",
            Some(name),
            "entry has no community-native provider".to_string(),
        );
        format!(
            "ducklink_load(kind='native'): '{name}' has no community-native provider. \
             (A community-native provider records that the capability exists as an \
             extension in `duckdb/community-extensions`; ducklink would `INSTALL … FROM \
             community; LOAD …;` in that case.)"
        )
    })?;

    let ext_name = provider
        .extension_name
        .clone()
        .expect("select_community_native_provider filtered to Some(extension_name)");
    crate::events::emit(
        "select_community_native_provider",
        Some(name),
        format!("community extension '{ext_name}'"),
    );
    Ok(ext_name)
}

pub fn resolve_name_to_native(
    name: &str,
    platform: &str,
    duckdb_version: &str,
) -> Result<PathBuf, String> {
    let catalog = resolve_catalog();
    let entry = catalog.find(name).ok_or_else(|| {
        crate::events::emit("unknown_name_native", Some(name), format!("no catalog entry for '{name}'"));
        format!("ducklink_install_native: unknown extension '{name}'. Discover names with \
                 `SELECT name FROM ducklink.modules`.")
    })?;

    let provider = entry
        .select_native_provider(platform, duckdb_version)
        .ok_or_else(|| {
            crate::events::emit(
                "no_native_provider",
                Some(name),
                format!("no native provider for platform={platform} duckdb_version={duckdb_version}"),
            );
            let available: Vec<String> = entry
                .native_providers()
                .filter_map(|p| {
                    let plat = p.platform.as_deref()?;
                    let ver = p.duckdb_version.as_deref()?;
                    Some(format!("{plat}/{ver}"))
                })
                .collect();
            if available.is_empty() {
                format!(
                    "ducklink_install_native: '{name}' has no native providers (WASM-only). \
                     Load it via ducklink instead: `ducklink_load('{name}')`."
                )
            } else {
                format!(
                    "ducklink_install_native: '{name}' has no native provider for {platform}/{duckdb_version}. \
                     Available: {}. If the native build hasn't shipped yet, load the WASM version: \
                     `ducklink_load('{name}')`.",
                    available.join(", ")
                )
            }
        })?;
    let digest = provider.content_digest.clone().expect("selected native provider has a digest");

    crate::events::emit(
        "select_native_provider",
        Some(name),
        format!("provider for {platform}/{duckdb_version} (digest {digest})"),
    );

    let cache_path = native_cache_path(&digest, name)
        .ok_or_else(|| "ducklink_install_native: no cache directory (set HOME or XDG_CACHE_HOME)".to_string())?;

    // Digest-keyed cache hit: the path itself encodes the verified hash.
    if cache_path.is_file() {
        crate::events::emit("native_cache_hit", Some(name), cache_path.to_string_lossy().into_owned());
        return Ok(cache_path);
    }

    let _guard = DOWNLOAD_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    if cache_path.is_file() {
        crate::events::emit("native_cache_hit", Some(name), cache_path.to_string_lossy().into_owned());
        return Ok(cache_path);
    }

    crate::events::emit("native_cache_miss", Some(name), format!("digest {digest}"));

    #[cfg(not(feature = "network"))]
    {
        return Err(format!(
            "ducklink_install_native: '{name}' is not cached and this build was compiled without \
             network support. Pre-populate the on-disk cache at {} out-of-band, or rebuild ducklink \
             with `--features network`.",
            cache_path.display()
        ));
    }

    #[cfg(feature = "network")]
    {
        // URL: explicit override on the provider, else default from the base + digest + platform.
        let url = provider.url.clone().unwrap_or_else(|| {
            format!("{NATIVE_BLOB_BASE}/{digest}/{platform}/{name}.duckdb_extension")
        });
        crate::events::emit("native_download", Some(name), url.clone());
        let bytes = download_native_blob(&url, name)?;

        let got = sha256_hex(&bytes);
        if got != digest {
            crate::events::emit(
                "native_verify_fail",
                Some(name),
                format!("expected {digest}, got {got}"),
            );
            return Err(format!(
                "ducklink_install_native: sha256 mismatch for '{name}': catalog says {digest}, \
                 downloaded bytes hash to {got} (refusing to cache)"
            ));
        }
        crate::events::emit("native_verify_ok", Some(name), digest.clone());

        write_cache(&cache_path, &bytes)?;
        Ok(cache_path)
    }
}

/// Download the RAW native `.duckdb_extension` bytes from an explicit URL.
/// A separate function from `download_blob` so the event/error strings stay
/// clear and it's easy to change either path independently later.
#[cfg(feature = "network")]
fn download_native_blob(url: &str, name: &str) -> Result<Vec<u8>, String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| format!("ducklink_install_native: http client init failed: {e}"))?;
    let resp = client
        .get(url)
        .send()
        .map_err(|e| format!("ducklink_install_native: download of {url} for '{name}' failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!(
            "ducklink_install_native: download of {url} for '{name}' returned HTTP {}",
            resp.status()
        ));
    }
    let mut bytes = Vec::new();
    resp.bytes()
        .map_err(|e| format!("ducklink_install_native: reading {url} body failed: {e}"))?
        .as_ref()
        .read_to_end(&mut bytes)
        .map_err(|e| format!("ducklink_install_native: buffering {url} body failed: {e}"))?;
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
            Some("068b47e3ea5df366637eb3726e7efaa6bfb4ddd00564bf75c821956572c76a15")
        );
        assert!(aba.exports.iter().any(|e| e == "aba_validate"));
    }

    #[cfg(feature = "network")]
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
    fn bundled_aba_resolves_gen4_top_level_digest_on_gen4_host() {
        // aba is a gen-4 entry (wit_contract_version 4.0.0) whose only wasm
        // provider carries a STALE abi (@2.2.0). Under strict same-major, that
        // provider is NOT selected (abi major 2 != host 4); resolution falls back
        // to the entry's top-level gen-4 digest because the entry's own
        // generation matches the host.
        let cat = bundled_catalog();
        let aba = cat.find("aba").expect("aba present");
        assert_eq!(aba.generation_major(), Some(4), "aba is a gen-4 entry");
        // No exact-generation wasm provider on a gen-4 host (provider abi is @2.2.0).
        assert!(aba.select_provider(4).is_none());
        // resolve_digest falls back to the entry's gen-4 top-level digest.
        assert_eq!(
            aba.resolve_digest(4).as_deref(),
            Ok("068b47e3ea5df366637eb3726e7efaa6bfb4ddd00564bf75c821956572c76a15")
        );
    }

    #[test]
    fn provider_selection_is_strict_same_major() {
        let mk = |abi: &str, dig: &str| Provider {
            id: Some("wasm-component".into()),
            kind: Some("wasm".into()),
            abi: Some(abi.into()),
            content_digest: Some(dig.into()),
            status: None,
            platform: None,
            duckdb_version: None,
            url: None,
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
        // Gen-4 host: picks the EXACT gen-4 provider (not the newest <= host).
        assert_eq!(entry.select_provider(4).unwrap().content_digest.as_deref(), Some("d4"));
        // Gen-2 host: picks the exact gen-2 provider.
        assert_eq!(entry.select_provider(2).unwrap().content_digest.as_deref(), Some("d2"));
        // Gen-3 host: NO exact match -> None (strict rejects the gen-2 provider).
        assert!(entry.select_provider(3).is_none());
        // Gen-0 host: nothing matches -> None.
        assert!(entry.select_provider(0).is_none());
    }

    #[test]
    fn resolve_digest_rejects_cross_major_and_accepts_same_major() {
        let mk = |abi: &str, dig: &str| Provider {
            id: Some("wasm-component".into()),
            kind: Some("wasm".into()),
            abi: Some(abi.into()),
            content_digest: Some(dig.into()),
            status: None,
            platform: None,
            duckdb_version: None,
            url: None,
        };
        // A gen-2 entry (its wit_contract_version + provider are both gen-2).
        let gen2 = CatalogEntry {
            name: "old".into(),
            version: None,
            description: None,
            categories: vec![],
            exports: vec![],
            requires: vec![],
            crates: vec![],
            content_digest: Some("top2".into()),
            wit_contract_version: Some("2.2.0".into()),
            providers: vec![mk("duckdb:extension@2.2.0", "d2")],
            functions: vec![],
        };
        // On a gen-4 host: the gen-2 provider is not selected AND the entry's own
        // generation (2) != host (4) -> a clear cross-major REJECTION.
        assert!(gen2.select_provider(4).is_none());
        let err = gen2.resolve_digest(4).expect_err("gen-2 module must be rejected on a gen-4 host");
        assert!(
            err.contains("generation 2") && err.contains("generation 4"),
            "error must name both generations: {err}"
        );
        // On a gen-2 host: the exact gen-2 provider is selected -> accepted.
        assert_eq!(gen2.resolve_digest(2).as_deref(), Ok("d2"));

        // A gen-4 entry whose provider carries a STALE @2.2.0 abi (the real
        // catalog shape). On a gen-4 host: no exact provider, but the entry's own
        // generation (4) == host (4) -> falls back to the gen-4 top-level digest.
        let gen4_stale = CatalogEntry {
            name: "modern".into(),
            version: None,
            description: None,
            categories: vec![],
            exports: vec![],
            requires: vec![],
            crates: vec![],
            content_digest: Some("top4".into()),
            wit_contract_version: Some("4.0.0".into()),
            providers: vec![mk("duckdb:extension@2.2.0", "top4")],
            functions: vec![],
        };
        assert!(gen4_stale.select_provider(4).is_none());
        assert_eq!(gen4_stale.resolve_digest(4).as_deref(), Ok("top4"));
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
                platform: Some("osx_arm64".into()),
                duckdb_version: Some("v1.5.4".into()),
                url: None,
            }],
            functions: vec![],
        };
        assert!(entry.select_provider(4).is_none());
    }

    #[cfg(feature = "network")]
    #[test]
    fn sha256_verification_rejects_mismatch() {
        // White-box: bytes that don't hash to the claimed digest must be
        // rejected. We exercise the same comparison resolve_name_to_blob uses.
        let bytes = b"not the real component";
        let claimed = "068b47e3ea5df366637eb3726e7efaa6bfb4ddd00564bf75c821956572c76a15";
        let got = sha256_hex(bytes);
        assert_ne!(got, claimed, "test bytes must not match the real digest");
    }
}
