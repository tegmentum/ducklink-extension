# Native advanced-tier dispatch (follow-on)

Status as of v0.4.0 (`duckdb:extension@4.0.0`, contract digest
`a2ad9764ac971345d6a650b92edbda034b160980acf148d354126f7e6f92ba40`).

v0.4.0 retargets the embedded runtime to the major-4 COLUMNAR contract: the hot
dispatch path (scalar / aggregate / cast over a whole DataChunk) now crosses the
canonical ABI as typed `colvec`s — one bulk transfer per fixed-width column —
instead of the major-3 row-major `list<list<duckvalue>>` tagged-variant batch.
The native bridge marshals each DuckDB chunk once and the shared runtime pivots
it to columns at the boundary, so the per-cell variant marshalling is gone. The
common tier (scalar / table / aggregate / window-over-aggregate) is otherwise
unchanged and still on the stable C API.

## The stability model

We guard exactly ONE external surface: the `duckdb:extension` WIT contract. It is
frozen (major 4, additive minors only; the contract is identified by the witcanon
digest of the canonical WIT, not a hand-maintained string). Components target the
WIT world and are version-independent.

DuckDB's internal C++ ABI churns per DuckDB release. That churn is ABSORBED in ONE
binding layer (the native extension's DuckDB-facing code). "We have to keep up, but
it is only one thing that needs to keep up." A DuckDB version bump re-anchors that
one layer; it never bumps the WIT contract.

## What v0.4.0 ships natively (the common tier — on the STABLE C API)

The native extension is a Rust loadable `.duckdb_extension` built against DuckDB's
STABLE C Extension API (`duckdb_ext_api_v1`, frozen since DuckDB 1.2.0) via
`duckdb-rs` + `libduckdb-sys` (loadable-extension). On that stable C ground it
loads `@4.0.0` columnar components and drives the bulk of the catalog:

- scalar functions  — `register_scalar_function_with_state`
- table functions   — `register_table_function_with_extra_info` (whole-batch
  `call-table`; PROJECTION pushdown only — see below)
- aggregate functions — `duckdb_register_aggregate_function`
- window functions over component aggregates — for free: DuckDB's WINDOW machinery
  reuses the registered C aggregate's init/update/combine/finalize callbacks
  (frame rows -> update, one finalize per output row). No extra native code.

This covers the common tier — the bulk of the ~190-component catalog
(scalar/table/aggregate). It is stable: a DuckDB release does not perturb it
(stable C API), and a WIT additive minor does not perturb it (older components
load un-rebuilt).

## What now ships natively too (the advanced tier — on the INTERNAL C++ ABI)

STATUS UPDATE (task #200): the advanced tier — PARSER, OPTIMIZER, and
table-function FILTER pushdown — is now IMPLEMENTED in the native loadable
extension, exactly as the follow-on plan below prescribed. A small C++ shim TU
set (`cpp/ducklink_*.cpp`) compiled against DuckDB's INTERNAL headers is linked
into the loadable `.duckdb_extension`; it registers the real extension points and
calls back into the embedded wasmtime engine through `extern "C"` bridge
functions (`src/advanced.rs`). All three tiers are proven end-to-end against a
real DuckDB v1.5.4 CLI (see "Build-model verdict" and "Proof" below).

These tiers cannot be driven through the stable C API. The stable C surface
exposes NONE of:

- `ParserExtension` registration (`DBConfig::parser_extensions`)
- `OptimizerExtension::Register` (`DBConfig::optimizer_extension*`)
- a way to mark a table function filter-pushdown-capable or read the pushed
  `TableFilter` set (the C table-function API exposes only projection pushdown:
  `duckdb_table_function_supports_projection_pushdown` /
  `duckdb_init_get_column_index`).

These bind to DuckDB's INTERNAL C++ ABI — there is no stable C anchor (confirmed
in the v3 audit; see the duckdb-wasm `docs/v3-core-shim-plan.md`). The wasm core
implements them in C++ because that core IS DuckDB-compiled-to-wasm with full
internal-header access (`core/cpp/wasm_parser.cpp` /
`wasm_component_optimizer.cpp` / the `wasm_storage.cpp` filter-pushdown
`TableFunction`, driven from `core/src/lib.rs`). The native equivalent is a C++
shim translation unit linked into this extension and compiled against DuckDB's
INTERNAL headers (the "absorb the C++ churn in one layer" layer) — NOT something
the current Rust C-API-only build links today.

### The native follow-on plan (mirrors the wasm-core C++ shims)

Add a C++ shim TU to the extension build that registers, against the loaded
`DatabaseInstance` / `DBConfig`:

1. PARSER — a `ParserExtension` whose `parse_function` hands the rejected statement
   text to the owning component's `parser-dispatch.call-parse` (through the
   `duckdb-extension-parser` world bindings already in `ducklink-runtime`); on
   `rewrite(sql)` re-parse with a fresh `Parser` and splice in `plan_function`
   (by-value-safe: only the rewrite string crosses the WIT boundary). Register via
   `config.parser_extensions.push_back(...)`. wasm-core proof: `LOAD ggsql;
   VISUALIZE SELECT 'apple' AS label, 3 AS n ...` -> `(apple,3,###) (pear,1,#)`.
2. OPTIMIZER — a `WasmComponentOptimizer : OptimizerExtension` that flattens the
   bound plan to the neutral `optimizer-dispatch.plan-node` JSON shape (op-type via
   `EnumUtil::ToString`, params/exprs via `Expression::ToString()` — no DuckDB
   struct crosses the boundary), offers it to declared rules via
   `optimizer-dispatch.call-optimize`, and applies the returned directive
   (`rewrite-query` re-binds+re-plans; structured `apply` dispatches a core-owned
   rewrite). Register via `OptimizerExtension::Register`. wasm-core proof: `LOAD
   qopt; SELECT x FROM optme` -> `99`.
3. TABLE-FN FILTER PUSHDOWN — a C++ streaming `TableFunction` with
   `filter_pushdown = true` (the `wasm_storage.cpp` pattern): init reads
   `column_ids` + `filters`, flattens to the neutral filter descriptor, opens the
   cursor via `table-stream-dispatch.call-table-open-filtered` (the host driver
   `ExtensionInstance::table_open_filtered` already exists in `ducklink-runtime`).
   The additive `register-filterable-table` marker exists in the contract; the
   boundary test already proves a pushed filter prunes rows at the component
   source.

Per-release cost: when DuckDB bumps, re-anchor this one C++ shim TU against the new
internal headers. That is the single layer that "keeps up"; the WIT contract and
the ~190 components do not move.

OPERATOR extensions remain out of scope (infeasible by-value over WIT — steer to
table functions), as in the v3 stabilization audit.

## The build-model change (how the C++ shim links into a Rust loadable)

The loadable `.duckdb_extension` is a Rust `cdylib` built via `duckdb-rs` +
`libduckdb-sys` against the STABLE C API only — the stable C loadable mechanism
routes every `duckdb_*` call through a function-pointer table, so the cdylib
normally has NO undefined DuckDB symbols. The advanced tier adds a C++ TU set
that references DuckDB INTERNAL C++ symbols (`DBConfig`, `ParserExtension`,
`OptimizerExtension`, `Parser`, `Planner`, `TableFunction`, ...). Those are left
UNDEFINED in the shim and resolved at LOAD time against the host DuckDB process,
which exports them (a standard DuckDB C++ extension links the same way).

Concretely (`build.rs`, gated on `CARGO_FEATURE_DUCKDB_API`, never for wasm):

- HEADERS, version-locked. The internal headers come from the EXACT
  `libduckdb-sys` crate this build already depends on (pinned `1.10504.0` =
  DuckDB v1.5.4). In the `bundled` build `libduckdb-sys` publishes its extracted
  source via `DEP_DUCKDB_INCLUDE`; in the loadable (wrapper-only) build it does
  not, so `build.rs` extracts the crate's bundled `duckdb.tar.gz` into `OUT_DIR`.
  The header version therefore moves in lock-step with the `duckdb` /
  `libduckdb-sys` crate version — there is no separate string to maintain.
- INCLUDE DIRS. `build.rs` reads the source's `manifest.json` `base.include_dirs`
  (the exact set DuckDB compiles itself with: `src/include` + the `third_party/*`
  dirs) and feeds them to the `cc` build, with `-DDUCKDB_BUILD_LIBRARY`, C++17.
- LINK. The shim object is linked into the crate as a static archive. For the
  cdylib only (`cargo:rustc-cdylib-link-arg`), `build.rs` adds
  `-undefined dynamic_lookup` (macOS) / `--allow-shlib-undefined` (ELF) so the
  internal C++ symbols defer to load time. The bundled test executable is
  unaffected (its symbols are linked in).
- New deps: `[build-dependencies] cc`, and `serde_json` (parse the optimizer
  plan JSON into node tuples).

## Build-model verdict: FEASIBLE (proven)

The model was validated mechanically and end-to-end:
- A C++ TU including `duckdb.hpp` + internal headers and referencing
  `DBConfig::GetConfig` COMPILES against the v1.5.4 headers, exports its own
  symbol, and leaves `duckdb::DBConfig::GetConfig(DatabaseInstance&)` undefined.
- It LINKS into the cdylib with `-undefined dynamic_lookup`.
- The packaged `.duckdb_extension` LOADS into a real DuckDB v1.5.4 CLI (which
  exports the ~23k internal symbols) and the probe resolves `DBConfig::GetConfig`
  at load (`ducklink_advanced_probe` returns `maximum_threads`).

## Proof (real DuckDB v1.5.4 CLI, `LOAD` + SQL)

All three tiers are exercised by `test/advanced/smoke.sh` (skips cleanly when a
v1.5.4 CLI / the artifact / the component corpus are absent; `STRICT=1` to fail
instead). Proven outputs:

1. PARSER — `LOAD ggsql; VISUALIZE SELECT 'apple' AS label, 3 AS n UNION ALL
   SELECT 'pear', 1` -> `(apple,3,###) (pear,1,#)`.
2. OPTIMIZER — `LOAD qopt; SELECT x FROM optme` -> `99`.
3. TABLE-FN FILTER PUSHDOWN — `LOAD numstream; SELECT v FROM numstream(10)
   WHERE v > 5` -> `6,7,8,9` (the pushed filter is delivered to
   `call-table-open-filtered` and the component prunes at the source).

The corpus `qopt.wasm` / `numstream.wasm` were rebuilt against the frozen
`@4.0.0` WIT (the shipped artifacts were stale at `@3.0.0`); `ggsql.wasm` was
already `@4.0.0`.

### Implementation map

| tier | C++ shim | Rust bridge / engine |
| --- | --- | --- |
| build-model probe | `cpp/ducklink_advanced.cpp` | `ducklink_advanced_probe` call in `src/lib.rs` |
| parser | `cpp/ducklink_parser.cpp` | `advanced::ducklink_parser_try_rewrite` -> `Engine2::dispatch_parse` |
| optimizer | `cpp/ducklink_optimizer.cpp` | `advanced::ducklink_optimizer_try_rewrite` -> `Engine2::dispatch_optimize` |
| table filter pushdown | `cpp/ducklink_table_stream.cpp` | `advanced::ducklink_ts_{open,fill,close}` -> `Engine2::dispatch_table_*` |

C ABI between the two sides: `cpp/ducklink_advanced.h`.
