//! Optional live-network smoke for the name->blob resolver. Ignored by default
//! (needs outbound HTTPS to datalink-ext.tegmentum.ai); run explicitly with:
//!   cargo test --no-default-features --features bundled --test live_catalog_smoke -- --ignored
#![cfg(feature = "bundled")]

use ducklink::catalog;

#[test]
#[ignore]
fn live_fetch_downloads_and_verifies_aba() {
    // Fresh, empty cache so the blob is actually DOWNLOADED + sha256-verified.
    let cache = std::env::temp_dir().join(format!("dl_live_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cache);
    unsafe { std::env::set_var("XDG_CACHE_HOME", &cache) };
    // Ensure the live catalog URL (default) is used.
    unsafe { std::env::remove_var("DUCKLINK_CATALOG_URL") };

    // resolve_name_to_blob only returns Ok AFTER the downloaded bytes' sha256 is
    // verified against the catalog content_digest, so a successful return into an
    // empty cache proves the live download + verify path. (The live catalog may
    // carry a NEWER digest than the bundled snapshot — we don't hardcode it.)
    let path = catalog::resolve_name_to_blob("aba").expect("resolve aba live");
    assert!(path.is_file(), "blob should be cached at {}", path.display());
    let bytes = std::fs::read(&path).expect("read cached blob");
    assert!(bytes.len() > 1000, "aba.wasm should be a real component");
    // The cache path is digest-keyed; its directory segment is the 64-hex sha256.
    let digest_seg = path
        .parent()
        .unwrap()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    assert_eq!(digest_seg.len(), 64, "digest path segment is a sha256 hex");
    assert!(
        path.to_string_lossy().contains("/wasm/sha256/"),
        "digest-keyed cache layout: {}",
        path.display()
    );
    let _ = std::fs::remove_dir_all(&cache);
}

/// OFFLINE FALLBACK: with the catalog URL pointed at an unreachable host, the
/// resolved catalog must still come from the bundled snapshot (~193 entries).
/// Runs in its own process so the session catalog OnceLock starts empty.
#[test]
fn offline_falls_back_to_bundled_snapshot() {
    unsafe { std::env::set_var("DUCKLINK_CATALOG_URL", "https://127.0.0.1:1/unreachable.json") };
    let cat = catalog::resolve_catalog();
    assert!(
        cat.extensions.len() > 150,
        "bundled snapshot should yield ~193 entries offline, got {}",
        cat.extensions.len()
    );
    assert!(cat.find("aba").is_some(), "snapshot has aba offline");
}
