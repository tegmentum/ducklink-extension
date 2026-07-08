//! `duckdb.docs` wasm custom-section overhead.
//!
//! Every `Engine2::load` now unconditionally calls `parse_docs_from_wasm(path)`
//! after wasmtime compiles the component, so every component pays for:
//!
//! - one `std::fs::read` of the `.wasm` file (a second read — wasmtime's
//!   `Component::from_file` already read it), and
//! - one linear scan through the wasm section stream looking for a
//!   `duckdb.docs` custom section (top-level first, then one level into
//!   nested core modules for the component encoding), and
//! - a JSON parse if the section is found.
//!
//! This bench isolates each of those costs against real corpus wasm binaries.
//! The two cases that matter most:
//!
//! - `scan_no_section` — the STEADY-STATE cost for the entire catalog today,
//!   since no shipped component embeds a `duckdb.docs` section yet.
//! - `parse_from_disk_no_section` — same as above but includes the disk read,
//!   representing the exact overhead added to `Engine2::load` per call.
//!
//! Run:
//!
//!   cargo bench --no-default-features --features bundled --bench docs_section

use std::hint::black_box;
use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use ducklink::docs_section::{parse_docs_from_bytes, parse_docs_from_wasm};

/// Directory holding the prebuilt corpus `*.wasm` artifacts (see
/// `tests/bridge_coverage.rs`). Overridable with `DUCKLINK_CORPUS_DIR`.
fn corpus_dir() -> PathBuf {
    match std::env::var_os("DUCKLINK_CORPUS_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../artifacts/extensions"),
    }
}

/// Encode `v` in little-endian ULEB128, appending to `out`.
fn write_uleb128(out: &mut Vec<u8>, mut v: u32) {
    loop {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
        }
        out.push(b);
        if v == 0 {
            break;
        }
    }
}

/// Append a single top-level `duckdb.docs` custom section carrying `json` to
/// the end of `bytes`. WASM custom sections have `id=0x00`, a ULEB128 payload
/// size, then payload = `ULEB128 name_len | name | content`. Appending at the
/// tail is legal — the scanner walks sections in stream order and matches on
/// name, so a trailing custom section is discovered without disturbing any
/// preceding module content. `find_custom_section`'s top-level pass finds
/// this in a single traversal (no need to descend into nested core modules).
fn append_docs_section(bytes: &[u8], json: &[u8]) -> Vec<u8> {
    let name = b"duckdb.docs";
    let mut payload = Vec::with_capacity(1 + name.len() + json.len());
    write_uleb128(&mut payload, name.len() as u32);
    payload.extend_from_slice(name);
    payload.extend_from_slice(json);

    let mut out = Vec::with_capacity(bytes.len() + payload.len() + 8);
    out.extend_from_slice(bytes);
    out.push(0x00); // custom section id
    write_uleb128(&mut out, payload.len() as u32);
    out.extend_from_slice(&payload);
    out
}

/// A realistic-shape docs payload: 5 functions, each carrying an ~200-byte
/// description + example + 5 tags. Total ~2 KB — matches what an actively
/// documented component would ship.
fn realistic_docs_json() -> String {
    let mut fns = Vec::new();
    for i in 0..5 {
        fns.push(format!(
            r#"{{
                "name": "example_fn_{i}",
                "summary": "Realistic one-line synopsis, roughly the length a real one would take.",
                "description": "A multi-sentence markdown description that explains what the function does, when to use it, and what the edge cases look like. Long enough to exercise the JSON parser rather than short-circuit on it.",
                "example": "SELECT example_fn_{i}('021000021');   -- true (Bank of NY)",
                "tags": ["validator", "banking", "aba", "routing-number", "us"]
            }}"#
        ));
    }
    format!(r#"{{"functions":[{}]}}"#, fns.join(","))
}

fn bench_docs_section(c: &mut Criterion) {
    // Locate the corpus wasm we'll reuse across every case. Prefer aba
    // (smaller, more chunk-scan turnover) with sample_extension as a size
    // sanity check.
    let dir = corpus_dir();
    let aba = dir.join("aba.wasm");
    if !aba.exists() {
        eprintln!("aba.wasm missing from corpus; skipping docs_section bench");
        return;
    }

    let baseline_bytes = std::fs::read(&aba).expect("read aba.wasm");
    let empty_section_bytes = append_docs_section(&baseline_bytes, br#"{"functions":[]}"#);
    let realistic_json = realistic_docs_json();
    let realistic_section_bytes = append_docs_section(&baseline_bytes, realistic_json.as_bytes());

    let mut group = c.benchmark_group("docs_section");
    // Throughput = wasm bytes scanned, so results normalize across cases with
    // different post-append sizes.
    group.throughput(Throughput::Bytes(baseline_bytes.len() as u64));

    // Case 1 — the section is ABSENT. This is what every existing catalog
    // component pays today. Isolates the "scan the sections, find nothing,
    // return None" path from the disk read.
    group.bench_function("scan_no_section", |b| {
        b.iter(|| {
            let out = parse_docs_from_bytes(black_box(&baseline_bytes), "aba");
            black_box(out);
        });
    });

    // Case 2 — the section is present but empty. Section is discovered
    // immediately (top-level, near end); JSON parse succeeds on a two-key
    // object. The delta vs `scan_no_section` is roughly the JSON parse for
    // the empty object.
    group.bench_function("scan_empty_section", |b| {
        b.iter(|| {
            let out = parse_docs_from_bytes(black_box(&empty_section_bytes), "aba");
            black_box(out);
        });
    });

    // Case 3 — realistic (~2 KB) payload for 5 functions. Delta vs case 2 is
    // the extra JSON traversal for the larger tree.
    group.bench_function("scan_realistic_section", |b| {
        b.iter(|| {
            let out = parse_docs_from_bytes(black_box(&realistic_section_bytes), "aba");
            black_box(out);
        });
    });

    // Case 4 — end-to-end from disk. Wraps `parse_docs_from_bytes` with a
    // `std::fs::read` on every call. This IS the per-Engine2::load overhead
    // added by the feature; the pre-feature baseline was zero for this
    // helper. The two other cases exist to attribute the number to
    // section-walking vs. JSON parse vs. disk read.
    group.bench_function("parse_from_disk_no_section", |b| {
        b.iter(|| {
            let out = parse_docs_from_wasm(black_box(&aba));
            black_box(out);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_docs_section);
criterion_main!(benches);
