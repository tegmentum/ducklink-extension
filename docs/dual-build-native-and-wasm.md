# Dual-build: same source, WASM + native

**Status:** design pattern. Applies to perf-sensitive corpus components.
**Where the work happens:** the corpus monorepo, not this repo.
**Motivation:** WASM overhead is 25-40× vs native DuckDB built-ins. For perf-sensitive extensions (crypto, encoding, tight-loop validators), a native build eliminates that gap without rewriting the extension.

## The structure is already there

Corpus components already separate pure logic from binding:

```
extensions/aba-component/     ← WASM binding (wit-bindgen, cdylib for wasm32)
extensions/aba-core/          ← pure Rust logic (target-agnostic)
```

The pattern for a dual build adds one crate per perf-sensitive component:

```
extensions/aba-native/        ← native binding (duckdb-rs, cdylib for native)
                                depends on aba-core, unchanged
```

`aba-core` stays the single source of truth. The two binding crates are thin — the WASM one implements `duckdb:extension`'s WIT world; the native one implements `duckdb-rs`'s `VScalar` / `VTab` / `VAggregate` traits.

## What the native binding looks like

For a scalar like `aba_validate`:

```rust
// aba-native/src/lib.rs
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::core::{DataChunkHandle, LogicalTypeId, Inserter};
use duckdb::vtab::arrow::WritableVector;
use duckdb::Connection;
use aba_core::is_valid;

struct AbaValidate;

impl VScalar for AbaValidate {
    type State = ();
    fn invoke(_: &(), input: &mut DataChunkHandle, output: &mut dyn WritableVector)
        -> Result<(), Box<dyn std::error::Error>>
    {
        // Same logic aba-core::is_valid() the WASM binding calls,
        // wrapped in the duckdb-rs vectorized-scalar contract.
    }
    fn signatures() -> Vec<ScalarFunctionSignature> { /* VARCHAR -> BOOLEAN */ }
}

#[no_mangle]
pub extern "C" fn ducklink_aba_init_c_api(con: &Connection) -> duckdb::Result<()> {
    con.register_scalar_function::<AbaValidate>("aba_validate")
}
```

## What ships

Two artifacts, one source, **completely independent distributions**:

| Distribution | Artifact | Where | Entry in duckdb/community-extensions |
|---|---|---|---|
| WASM (portable, sandboxed) | `aba-component.wasm` | ducklink catalog | (part of `ducklink`) |
| Native (fast, per-platform) | `aba.duckdb_extension` | community-extensions | **new `aba` entry, its own extension** |

The native variant is **its own community extension** — not a feature of ducklink. `extensions/aba/description.yml` in duckdb/community-extensions, its own PR, its own release cycle. It has no runtime dependency on ducklink; a user could `INSTALL aba; LOAD aba; SELECT aba_validate('021000021');` with no `ducklink` involvement at all.

Users pick what they need:
- `INSTALL aba FROM community; LOAD aba;` — native, fast, per-platform.
- `LOAD ducklink; ducklink_load('aba');` — WASM, portable, sandboxed, dynamic.

Same three functions in either case. Different tradeoffs, different distribution.

Ducklink itself stays valuable for:
- Extensions where sandbox / dynamic-load / portability matter more than raw speed
- Discovery and browsing (`ducklink.modules` still lists everything)
- Extensions that aren't performance-critical enough to warrant the per-platform native build effort

`ducklink.modules` could optionally advertise "also available natively" for entries that ship both variants — but that's a nice-to-have, not required.

## Tradeoffs

Native gains:
- **25-40× throughput** on tight-loop workloads (matches DuckDB's vectorized executor)
- Zero WASM overhead — same as any DuckDB built-in

Native costs:
- **Per-platform builds** (linux_amd64, linux_arm64, osx_amd64, osx_arm64, windows_amd64) instead of one WASM
- **DuckDB version coupling** — community-extensions rebuilds per DuckDB release
- **Sandbox lost** — native code runs in-process with full DuckDB privileges
- **Distribution splits** — WASM via ducklink catalog, native via community-extensions

## Which components to prioritise

Best return on the effort (in decreasing order):

1. **High call frequency + tight compute**: validators (aba, credit-card checksums), encoders (base58, ascii85, bech32), hashes.
2. **String manipulation over large row sets**: encoding conversions, sanitisers.
3. **Numeric transforms on many rows**: unit conversions, geohash, currency math.

Not worth the effort:
- I/O-bound extensions (network, filesystem) — WASM overhead dominated by I/O anyway.
- Rarely-called extensions — 30× on 100 calls/day is meaningless.
- Extensions with heavy per-call work — WASM overhead is amortised, ratio drops to 2-5×.

## What this repo does

Nothing changes in ducklink-extension. It still loads `.wasm` components via the catalog. This doc lives here because it's the natural follow-on to the perf-ceiling work (`docs/perf-ceiling-measurement.md`) and the SIMD handoff (`docs/guest-simd-investigation.md`) — three related "here's what perf work remains, and where it lives" pointers for future readers.

## Concrete first move

Pick `aba` as the pilot. `aba-core` already exists and is trivial to bind twice. Build once for community-extensions distribution, benchmark against ducklink's WASM `aba`, publish the ratio. If it's the expected 25-30×, the pattern is proven and generalises to the rest of the perf-sensitive components in the corpus.
