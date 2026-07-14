//! Live-network smoke: real INSTALL FROM community + community-native alias
//! generation, driven end-to-end through `ducklink_load()`.
//!
//! Serves a synthetic ducklink catalog from a scratch HTTP server on
//! `127.0.0.1:0`, points `DUCKLINK_CATALOG_URL` at it, then runs
//! `FROM ducklink_load('crypto', kind => 'native')`. That call exercises:
//!
//!   1. The catalog resolver (`resolve_name_to_community_native`) — the
//!      catalog JSON shape used here is the same shape a community-native
//!      publisher would ship, so this test doubles as a working example.
//!   2. `INSTALL crypto FROM community; LOAD crypto;` — a real community
//!      extension registers its own scalar and aggregate functions.
//!   3. The advanced-tier catalog-alias shim (`cpp/ducklink_alias.cpp`) —
//!      copies `crypto_hash_agg` into a `CatalogEntry` under ducklink's
//!      chosen name (`hash_agg`) so the alias IS a real
//!      `AggregateFunctionCatalogEntry`.
//!
//! The transparency assertions run DISTINCT / FILTER / ORDER BY / OVER
//! queries through BOTH the alias and community's original name, and
//! demand byte-identical output. `crypto_hash_agg` is `ORDER_DEPENDENT` in
//! its C++ definition, so `ORDER BY` isn't optional — if the shim didn't
//! propagate the modifier through, DuckDB's binder would refuse the call
//! with the exact error crypto's C++ raises.
//!
//! Ignored by default (needs outbound HTTPS to reach `community-extensions`).
//! Run explicitly with:
//!
//!   cargo test --no-default-features --features bundled,advanced,network \
//!     --test live_community_alias_smoke -- --ignored --nocapture
//!
//! `network` is required so ducklink's `fetch_live_catalog` actually fetches
//! from the scratch HTTP fixture — without it the offline stub returns None
//! and ducklink falls back to the bundled snapshot (which of course has no
//! entry for this test's synthetic catalog).
#![cfg(all(feature = "bundled", feature = "network", advanced_tier))]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

use duckdb::{ffi, Connection};

use ducklink::engine::Engine2;
use ducklink::reg_duckdb::register_load_function;

/// Minimal ducklink catalog. One entry ("crypto") carries a single
/// community-native provider that maps community's `crypto_hash` and
/// `crypto_hash_agg` under ducklink's chosen names. This JSON is the same
/// shape a real publisher writes — the test is also a working example of
/// what a community-native manifest entry looks like.
const CATALOG_JSON: &str = r#"{
  "extensions": [
    {
      "name": "crypto",
      "providers": [
        {
          "id": "cn-crypto",
          "kind": "community-native",
          "extension_name": "crypto",
          "function_mapping": {
            "hash": "crypto_hash",
            "hash_agg": "crypto_hash_agg"
          }
        }
      ]
    }
  ]
}"#;

/// Spin up a one-shot HTTP responder on `127.0.0.1:0` that serves
/// [`CATALOG_JSON`] for every request it accepts (only the first is needed
/// in practice — ducklink caches the catalog in a process-wide `OnceLock`).
/// Returns the URL clients should target. The thread lives for the process
/// lifetime; the OS reclaims the socket on exit.
fn serve_catalog() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let port = listener.local_addr().unwrap().port();
    std::thread::Builder::new()
        .name("ducklink-catalog-fixture".to_string())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                // Drain the request headers best-effort — we serve the same
                // body regardless of path or headers, so parsing them is
                // wasted work in a fixture.
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: application/json\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\r\n{}",
                    CATALOG_JSON.len(),
                    CATALOG_JSON
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        })
        .expect("spawn catalog fixture thread");
    format!("http://127.0.0.1:{port}/catalog.json")
}

/// Runs a single-row query, returning the first column as a lowercase hex
/// string. `crypto_hash_agg` returns BLOB, and `Vec<u8>` -> hex is the
/// canonical byte-identical comparison shape.
fn hex_row(con: &Connection, sql: &str) -> String {
    let bytes: Vec<u8> = con
        .query_row(sql, [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("query {sql:?} failed: {e}"));
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
#[ignore]
fn community_native_alias_wraps_real_aggregate_transparently() {
    // Isolate INSTALL side effects to a scratch HOME. DuckDB writes its
    // extension cache under `~/.duckdb`; a fresh HOME per test run means
    // this always exercises the download path, not a stale cache.
    let scratch = std::env::temp_dir().join(format!("dl_cn_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("scratch");
    let catalog_url = serve_catalog();
    unsafe {
        std::env::set_var("HOME", &scratch);
        std::env::set_var("XDG_CACHE_HOME", scratch.join(".cache"));
        std::env::set_var("DUCKLINK_CATALOG_URL", &catalog_url);
    }

    // Bootstrap: raw db so we hand `register_load_function` the same shape
    // the loadable entry point does.
    let (db, con) = unsafe {
        let mut db: ffi::duckdb_database = std::ptr::null_mut();
        assert_eq!(
            ffi::duckdb_open(c":memory:".as_ptr(), &mut db),
            ffi::DuckDBSuccess,
            "duckdb_open"
        );
        let con = Connection::open_from_raw(db.cast()).expect("open_from_raw");
        (db, con)
    };
    let engine = Arc::new(Engine2::new().expect("engine"));
    register_load_function(&con, db, engine).expect("register ducklink_load");

    // Full routing: resolver -> INSTALL crypto FROM community -> LOAD crypto
    // -> alias generation (via cpp/ducklink_alias.cpp). The row shape mirrors
    // the community-native path in reg_duckdb.rs.
    let (name, path): (String, String) = con
        .query_row(
            "SELECT name, path FROM ducklink_load('crypto', kind => 'native')",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("ducklink_load community-native");
    assert_eq!(name, "crypto");
    assert!(
        path.starts_with("community-extensions:"),
        "unexpected path shape from community-native load: {path}"
    );

    // Both names must be registered simultaneously — community's original
    // stays callable; ducklink's alias is an additional entry.
    let registered: Vec<String> = {
        let mut stmt = con
            .prepare(
                "SELECT DISTINCT function_name FROM duckdb_functions() \
                 WHERE function_name IN ('hash', 'hash_agg', 'crypto_hash', 'crypto_hash_agg') \
                 ORDER BY function_name",
            )
            .expect("prepare fn list");
        stmt.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };
    assert_eq!(
        registered,
        vec![
            "crypto_hash".to_string(),
            "crypto_hash_agg".to_string(),
            "hash".to_string(),
            "hash_agg".to_string(),
        ],
        "both community and ducklink names should register"
    );

    // Seed a small deterministic table for aggregate assertions.
    con.execute_batch(
        "CREATE TABLE t(g INT, s VARCHAR, ok BOOL); \
         INSERT INTO t VALUES \
           (1, 'a', true), (1, 'a', false), (1, 'b', true), \
           (2, 'c', true), (2, 'd', true);",
    )
    .expect("seed");

    // The transparency check. `crypto_hash_agg` is ORDER_DEPENDENT and refuses
    // to run without an ordering — so ORDER BY appears in every case, and if
    // the shim didn't propagate it, the call would error out with crypto's
    // own diagnostic. Every case cross-checks alias output against community's
    // original: byte-identical BLOBs proves the alias is the same aggregate,
    // not a wrapped surrogate.
    for (label, alias_sql, orig_sql) in [
        (
            "basic ORDER BY",
            "SELECT hash_agg('sha2-256', s ORDER BY s) FROM t",
            "SELECT crypto_hash_agg('sha2-256', s ORDER BY s) FROM t",
        ),
        (
            "DISTINCT + ORDER BY",
            "SELECT hash_agg(DISTINCT 'sha2-256', s ORDER BY s) FROM t",
            "SELECT crypto_hash_agg(DISTINCT 'sha2-256', s ORDER BY s) FROM t",
        ),
        (
            "FILTER + ORDER BY",
            "SELECT hash_agg('sha2-256', s ORDER BY s) FILTER (WHERE ok) FROM t",
            "SELECT crypto_hash_agg('sha2-256', s ORDER BY s) FILTER (WHERE ok) FROM t",
        ),
    ] {
        let a = hex_row(&con, alias_sql);
        let b = hex_row(&con, orig_sql);
        assert_eq!(
            a, b,
            "transparency mismatch on {label}\n  alias:    {alias_sql}\n  original: {orig_sql}"
        );
        assert!(
            !a.is_empty(),
            "aggregate returned empty for {label} — did the alias silently return NULL?"
        );
    }

    // Window context: the alias must accept `OVER (…)` and produce the same
    // per-row running hash community's original does. Same test-shape as the
    // shim's built-in `sum -> total` bundled test but against a real BLOB
    // aggregate this time.
    let by_alias: Vec<String> = {
        let mut stmt = con
            .prepare(
                "SELECT hash_agg('sha2-256', s) OVER (PARTITION BY g ORDER BY s) \
                 FROM t ORDER BY g, s",
            )
            .expect("prepare OVER alias");
        stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))
            .unwrap()
            .map(|r| r.unwrap().iter().map(|b| format!("{b:02x}")).collect::<String>())
            .collect()
    };
    let by_orig: Vec<String> = {
        let mut stmt = con
            .prepare(
                "SELECT crypto_hash_agg('sha2-256', s) OVER (PARTITION BY g ORDER BY s) \
                 FROM t ORDER BY g, s",
            )
            .expect("prepare OVER orig");
        stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))
            .unwrap()
            .map(|r| r.unwrap().iter().map(|b| format!("{b:02x}")).collect::<String>())
            .collect()
    };
    assert_eq!(by_alias.len(), 5, "OVER produces one row per input");
    assert_eq!(
        by_alias, by_orig,
        "OVER window results must match through the alias"
    );

    // Scalar path — same alias mechanism, simpler shape.
    let a = hex_row(
        &con,
        "SELECT hash('sha2-256', 'ducklink community-native transparency') AS h",
    );
    let b = hex_row(
        &con,
        "SELECT crypto_hash('sha2-256', 'ducklink community-native transparency') AS h",
    );
    assert_eq!(a, b, "scalar alias must match community's original");

    let _ = std::fs::remove_dir_all(&scratch);
}
