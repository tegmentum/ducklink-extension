# Native advanced-tier dispatch (follow-on)

Status as of v0.5.0 (`duckdb:extension@4.0.0`, contract digest
`a2ad9764ac971345d6a650b92edbda034b160980acf148d354126f7e6f92ba40`). v0.5.0
ships the advanced tier, version-guarded; the WIT contract is unchanged from
v0.4.0 (additive native capability — see "Hardening & graceful degradation").

## v0.5.2: the advanced tier is an OPT-IN cargo feature (off by default)

The advanced tier (parser / optimizer / table FILTER pushdown) is gated behind a
cargo feature, `advanced`, that is **OFF by default**:

- **Default build** (`cargo build --release`, what the community-extensions CI
  runs) → the COMMON tier only. The build script is a trivial no-op: it pulls no
  `cc` build-dependency, compiles no C++ shim, and emits no link args — exactly
  like the green v0.4.0, which had no build.rs at all. This is portable across
  every platform (Linux / macOS / Windows). The `advanced_tier` cfg is unset, so
  the `advanced` module and every internal-ABI reference are compiled out.
- **Native distribution** (`cargo build --release --features advanced`) → the
  FULL tier. build.rs compiles `cpp/ducklink_*.cpp` and sets the `advanced_tier`
  cfg, enabling the parser / optimizer / filter-pushdown dispatch. Still
  compiled out on Windows (no portable PE/COFF deferred-undefined-symbol model).

Why: the community macOS CI (macOS 26 / arm64, vcpkg toolchain, ccache on PATH)
failed to *execute* the cc-linked host `build-script-build` binary
(`cannot execute binary file`, exit 126) — a regression introduced purely by the
presence of the cc-using build.rs (the runtime crate's trivial build script built
fine in the same env). Making the advanced tier opt-in restores the
guaranteed-green common-tier community artifact while preserving the full tier for
our own native builds. The community-shipped artifact is therefore COMMON-tier;
ship `--features advanced` builds through our own native distribution channel for
parser / optimizer / filter-pushdown support.

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

STATUS UPDATE: SHIPPED in v0.5.0, version-guarded (task #200 implemented it;
task #203 hardened it for release). The advanced tier — PARSER, OPTIMIZER, and
table-function FILTER pushdown — is IMPLEMENTED in the native loadable
extension, exactly as the follow-on plan below prescribed. A small C++ shim TU
set (`cpp/ducklink_*.cpp`) compiled against DuckDB's INTERNAL headers is linked
into the loadable `.duckdb_extension`; it registers the real extension points and
calls back into the embedded wasmtime engine through `extern "C"` bridge
functions (`src/advanced.rs`). All three tiers are proven end-to-end against a
real DuckDB v1.5.4 CLI (see "Build-model verdict" and "Proof" below).

It is additive native capability on top of the unchanged `duckdb:extension`
@4.0.0 WIT contract — no contract bump, no component rebuild.

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

## Hardening & graceful degradation (shipped v0.5.0)

The advanced tier binds DuckDB's INTERNAL C++ ABI, which is NOT stable across
DuckDB versions. If the host DuckDB differs from the version-locked headers the
shim was built against, the deferred internal symbols could be ABI-incompatible
and crash/corrupt at first use. v0.5.0 makes that impossible:

1. VERSION GUARD (the key safety mechanism). At LOAD, after the stable-C-API
   init, the extension reads the host's reported version through the STABLE C API
   (`duckdb_library_version`, routed via the loadable function-pointer table) and
   enables the advanced tier ONLY when it EXACTLY matches `DUCKDB_ABI_VERSION`
   (`v1.5.4`, locked to the `libduckdb-sys` pin). On ANY mismatch it DEGRADES
   GRACEFULLY to the common tier (scalar/table/aggregate, on the stable C API)
   and never touches a single internal-ABI symbol — not even the probe — so a
   mismatch cannot segfault. A clear warning is emitted naming the host vs.
   built-against version. `src/lib.rs` gates this by passing the raw `db` handle
   to `register_components` only when enabled (`None` otherwise skips ALL C++
   shim registration). Exact match is deliberate: the internal ABI can change on
   any version (including patch/dev builds), so anything but `v1.5.4` is unsafe.
   - The stable C-API init is a MINIMUM-version check (forward compatible): a
     NEWER host loads the common tier fine; this exact gate is what disables the
     unstable tier on that newer host. An OLDER host is rejected even earlier by
     the extension's metadata `duckdb_version` footer (also a clean, no-crash
     failure).
   - `DUCKLINK_DISABLE_ADVANCED=1` forces the degraded branch on any host (the
     same code path a real mismatch takes), which is how the smoke suite proves
     graceful degradation deterministically on a matching host.

2. FFI PANIC GUARD. Every advanced-tier Rust bridge function called from the C++
   shim (parser / optimizer / table-stream open/fill/close/last-error/free) wraps
   its body in `catch_unwind`, so a Rust panic — e.g. a poisoned engine/state
   mutex after an earlier failure — can NEVER unwind across the C++/Rust boundary
   (which is UB). Panics convert to the function's error sentinel; the
   table-stream path also records a last-error so the C++ TableFunction raises a
   clean SQL error rather than silently treating the panic as end-of-stream. The
   C++ side already wraps each registration entrypoint and the optimizer re-plan
   in `try/catch`, so no C++ exception escapes either.

3. BOUNDS GUARD. `ducklink_ts_fill` rejects a component batch larger than the
   chunk's vector capacity (`duckdb_vector_size`) instead of writing out of
   bounds — defensive at the component trust boundary.

Memory-safety audit (manual; ASAN is not feasible here because the loaded
extension resolves its internal symbols against a non-instrumented host DuckDB,
so an ASAN-built shim would mismatch the host runtime): the FFI boundary is the
whole surface. Rewrite strings are Rust-allocated (`CString::into_raw`) and
Rust-freed (`ducklink_adv_free`) — matched, single free. Filter/arg text crossing
the boundary is kept alive C++-side for the duration of the `open` call and copied
to owned values in Rust before return. Cursors are tracked in a bridge-local map
and closed from the C++ global-state destructor (covering the mid-scan-error
path). Null/empty pointers and zero counts are checked on every entry; NULL filter
constants are dropped (un-pushable); malformed optimizer plan JSON degrades to an
empty node list (bounded at 2^16 nodes). Table scans are single-threaded
(`MaxThreads()==1`) and all engine/state access is mutex-serialized.

### Expanded smoke suite

`test/advanced/smoke.sh` now covers, beyond the three happy paths: parser
pass-through + malformed SQL (no crash); optimizer no-op; filter pushdown with
zero / all rows and IS NULL / IS NOT NULL; all three advanced extensions loaded
together; idempotent double-LOAD; a common-tier scalar alongside the active shim
(no-regression); the version-guard degraded path; and loading into a non-matching
host (clean rejection, no crash). Every check also fails on any crash marker.
`STRICT=1` is green on a real v1.5.4 CLI with the `@4.0.0` corpus.

### Implementation map

| tier | C++ shim | Rust bridge / engine |
| --- | --- | --- |
| build-model probe | `cpp/ducklink_advanced.cpp` | `ducklink_advanced_probe` call in `src/lib.rs` |
| parser | `cpp/ducklink_parser.cpp` | `advanced::ducklink_parser_try_rewrite` -> `Engine2::dispatch_parse` |
| optimizer | `cpp/ducklink_optimizer.cpp` | `advanced::ducklink_optimizer_try_rewrite` -> `Engine2::dispatch_optimize` |
| table filter pushdown | `cpp/ducklink_table_stream.cpp` | `advanced::ducklink_ts_{open,fill,close}` -> `Engine2::dispatch_table_*` |

C ABI between the two sides: `cpp/ducklink_advanced.h`.
