# Native advanced-tier dispatch (follow-on)

Status as of v0.3.0 (`duckdb:extension@3.1.0`, contract digest
`1b94f15d2c41f9a81de50c6d4d8cf508bf76333c4b901c00f511b811b8eb4983`).

## The stability model

We guard exactly ONE external surface: the `duckdb:extension` WIT contract. It is
frozen (major 3, additive minors only; the contract is identified by the witcanon
digest of the canonical WIT, not a hand-maintained string). Components target the
WIT world and are version-independent.

DuckDB's internal C++ ABI churns per DuckDB release. That churn is ABSORBED in ONE
binding layer (the native extension's DuckDB-facing code). "We have to keep up, but
it is only one thing that needs to keep up." A DuckDB version bump re-anchors that
one layer; it never bumps the WIT contract.

## What v0.3.0 ships natively (the common tier — on the STABLE C API)

The native extension is a Rust loadable `.duckdb_extension` built against DuckDB's
STABLE C Extension API (`duckdb_ext_api_v1`, frozen since DuckDB 1.2.0) via
`duckdb-rs` + `libduckdb-sys` (loadable-extension). On that stable C ground it
loads `@3.1.0` components and drives the bulk of the catalog:

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

## What is deferred (the advanced tier — needs the INTERNAL C++ ABI)

parser / optimizer / table-function FILTER pushdown cannot be driven through the
stable C API. The stable C surface exposes NONE of:

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
   The `@3.1.0` additive `register-filterable-table` marker exists; the boundary
   test already proves a pushed filter prunes rows at the component source.

Per-release cost: when DuckDB bumps, re-anchor this one C++ shim TU against the new
internal headers. That is the single layer that "keeps up"; the WIT contract and
the ~190 components do not move.

OPERATOR extensions remain out of scope (infeasible by-value over WIT — steer to
table functions), as in the v3 stabilization audit.
