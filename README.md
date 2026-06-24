# ducklink (DuckDB community extension)

Run WebAssembly **component** extensions inside DuckDB.

A `duckdb:extension` component is built once and runs unmodified on every
platform DuckDB supports — no per-platform native extension builds. `ducklink`
embeds [wasmtime](https://wasmtime.dev), loads the component, runs its `load()`
to discover the functions it registers, and bridges them into DuckDB's catalog.

```sh
# Name the component(s) to expose, then LOAD ducklink registers their functions.
DUCKLINK_COMPONENTS=sample=/path/sample_extension.wasm \
  duckdb -unsigned -c "LOAD 'ducklink.duckdb_extension'; SELECT sample_plus_one(41);"
```

Catalog registration happens at extension-load time (DuckDB's model), so the
components are named up front via `DUCKLINK_COMPONENTS` (a `:`-separated list of
`name=path` or `path`) rather than an in-query `CALL`.

## Three deployment scenarios, one component

The same `duckdb:extension` component artifact, built once, runs unmodified in
three deployments:

1. **Native DuckDB + the `ducklink` extension** (*this crate*) — native DuckDB
   loads `ducklink`, which embeds the wasmtime WebAssembly runtime and runs the
   component inside the native process. Lets a single portable component extend
   DuckDB on any platform without per-platform native extension builds.
2. **Standalone WebAssembly DuckDB** — the `ducklink` host runs
   DuckDB-compiled-to-WebAssembly and loads components alongside it, as a native
   CLI/server. WebAssembly throughout, no native DuckDB.
3. **WebAssembly DuckDB in a web browser** — the same WebAssembly DuckDB build,
   running extension components directly in-browser (the `web/` build). Extensions
   ship and run client-side with zero install.

Scenario 1 is "embed WebAssembly into native DuckDB"; scenarios 2 and 3 are
"run a WebAssembly DuckDB that hosts WebAssembly extensions" — natively and in
the browser respectively.

All three share the
[`ducklink-runtime`](https://github.com/tegmentum/ducklink/tree/main/crates/ducklink-runtime)
engine crate (consumed here as a pinned git dependency): the `duckdb:extension`
wasmtime bindings, the neutral `reg::*` registration model, and the callback
registry. A component therefore loads identically in every scenario.

## Layout

- `src/engine.rs` — the direction-agnostic engine glue: `Engine2::load` loads a
  component, runs its `load()`, and returns the functions it registered;
  `dispatch_scalar_batch` / `dispatch_table` / `dispatch_aggregate` route a DuckDB
  invocation back into the component through the shared callback registry. Depends
  only on `ducklink-runtime` + wasmtime, so it builds and is checked **without**
  the DuckDB toolchain.
- `src/reg_duckdb.rs` — the DuckDB sink (behind the `duckdb-api` feature): turns
  the functions a component registered into real DuckDB scalar / table / aggregate
  functions and marshals each call across the WIT boundary. Scalars and tables use
  the safe duckdb-rs `VScalar` / `VTab` APIs; aggregates use the raw C aggregate
  API (duckdb-rs has no safe wrapper). Every FFI entry point is wrapped so a panic
  surfaces as a query error rather than aborting the host process.
- `src/lib.rs` — the `loadable` module (behind the `loadable` feature): the
  `ducklink_init_c_api` entry point plus a built-in `ducklink_version()` scalar.
- `tests/` — `bridge_coverage.rs` (end-to-end against an in-process DuckDB) and
  `scenario1_corpus.rs` (the prebuilt component corpus).
- `benches/` — criterion benchmarks of the scalar dispatch hot path
  (`scalar_dispatch`, `scalar_query`).

## Build

The `loadable` feature is on by default, so a plain release build produces the
loadable artifact for the **native** host triple via the DuckDB Rust C Extension
API (`build: cargo`) — exactly what the community-extensions CI runs:

```
cargo build --release
```

To check just the direction-agnostic engine glue against `ducklink-runtime` —
without the DuckDB toolchain — disable the default feature:

```
cargo check --no-default-features    # engine.rs only, no DuckDB
```

The community-extensions CI builds it with the `rust` and `python3` toolchains.
It is excluded from the `wasm_*` platforms (it embeds a JIT) and from the
static-musl / mingw triples.

## Status

Working and verified end-to-end against a real in-process DuckDB (the `bundled`
test): `LOAD ducklink` loads each `DUCKLINK_COMPONENTS` entry, registers its
functions, and `SELECT fn(x)` dispatches every row into the wasm component
(`SELECT sample_plus_one(41)` → 42, computed in wasm). `SELECT ducklink_version()`
is a built-in that needs no component, so it confirms the extension loaded.

Coverage:
- **Scalar functions** — any arity, all logical types
  (`INT64`/`UINT64`/`DOUBLE`/`BOOLEAN`/`VARCHAR`/`BLOB`). One dynamic `WasmScalar`
  serves every signature (the per-function signature is fed to the static
  `VScalar::signatures()` via a thread-local set during registration). NULL
  inputs follow SQL semantics — a row with any NULL argument yields NULL — and the
  chunk is marshalled column-major into a reused buffer, so steady-state
  evaluation allocates no per-row memory.
- **Table functions** — `SELECT * FROM sample_emit_sequence(5)` streams rows from
  the component through a `VTab` bridge.
- **Aggregate functions** — bridged through the raw C aggregate API
  (init/update/combine/finalize over per-group state). The loadable entry point
  takes the `duckdb_database` DuckDB hands it and opens a raw sibling connection
  on it, so aggregates register database-wide alongside scalars and tables. NULL
  inputs are skipped, per SQL aggregate semantics.

The bridge is covered by a bundled test suite (per-type marshalling, NULL
propagation, multi-chunk evaluation, multi-component registration, concurrency,
and the aggregate path) and dispatch benchmarks:

```
cargo test  --no-default-features --features bundled        # full suite
cargo bench --no-default-features --bench scalar_dispatch   # hot-path micro-bench
```

Packaging the `loadable` cdylib (which exports `ducklink_init_c_api`) as a
loadable `.duckdb_extension` — the metadata footer + a DuckDB-version-matched
build — is handled by the community-extensions CI.
