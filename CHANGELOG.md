# Changelog

Notable changes to ducklink. Format follows [Keep a Changelog](https://keepachangelog.com);
versioning is [SemVer](https://semver.org). Stability guarantees are
defined in [STABILITY.md](STABILITY.md).

## [5.0.0] — unreleased

**The stabilization release.** Everything called out below is a
deliberate break to shed capabilities that couldn't ship uniformly
across platforms and cluttered the SQL surface. After this release,
the surface described in `STABILITY.md` is committed for the life of
v5.x.

### Removed (breaking)

- **The advanced tier is gone.** Parser extensions, general optimizer
  rules, and streaming table-function filter pushdown are removed
  from every ducklink build. They bound DuckDB's internal C++ ABI,
  which is not stable across DuckDB releases and cannot be exposed
  uniformly on Linux / macOS / Windows. `src/advanced.rs` (~2100
  LOC), the `cpp/` shim tree (~4500 LOC), the Cargo `advanced`
  feature, the `cc` build-dep, and the `advanced_tier` cfg flag are
  all removed.
- **`DUCKLINK LOAD <name>` SQL statement removed.** Use the table
  function `FROM ducklink_load('<name>', kind => 'wasm'|'native')`.
- **`DUCKLINK PREFIX <alias>: <namespace>` SQL statement removed.** Use
  `FROM ducklink_prefix('<alias>', '<namespace>')`, or the equivalent
  scalar / `PREFIX(...)` macro.
- **Colon syntax `c:hash(x)` removed.** The parser hook that rewrote
  it lived in the advanced tier. Use standard SQL `c.hash(x)`.
- **Catalog entries stripped.** Seven entries declared capabilities
  no ducklink host now satisfies and had no path forward on the
  stable C API:
  - `ggsql` (`parser`) — the `VISUALIZE` statement.
  - `dplyr` (`parser`) — dplyr → SQL rewrite.
  - `prql_parser` (`parser`) — PRQL at the DuckDB entry point.
  - `sqlitewasm` (`storage`) — SQLite `StorageExtension`.
  - `rtreefns` (`index`) — R-Tree spatial index type.
  - `hnswfns` (`index`) — HNSW vector index type.
  - `autocomplete` (`query`) — autocomplete provider.
  See `docs/extension-scope.md` for the position on why the parser
  entries in particular don't belong as extensions.
- **Engine RPC methods removed.** `Engine2::dispatch_parse`,
  `dispatch_optimize`, `dispatch_table_open_filtered`,
  `dispatch_table_next`, `dispatch_table_close`, and the
  now-orphaned `ts_filter_op` helper. `LoadedComponent` loses its
  `parsers`, `optimizers`, and `filterable_tables` fields.

### Deprecated

The following WIT surfaces and Rust types remain in the shipping
`duckdb:extension@4.0.0` contract for backward compatibility with
components built against v4.x. They are scheduled for REMOVAL at
the next `duckdb:extension` MAJOR bump (which will coincide with
ducklink v6.0.0). Components that call them today still load, but
the host no longer wires them to DuckDB — registrations are silent
no-ops.

- WIT interfaces: `parser`, `parser-dispatch`, `optimizer`,
  `optimizer-dispatch`, `table-stream`, `table-stream-dispatch`.
- WIT worlds: `duckdb-extension-parser`, `duckdb-extension-optimizer`,
  `duckdb-extension-table-stream`.
- Rust types in `ducklink-runtime`: `reg::ParserReg`,
  `reg::OptimizerReg`, `reg::FilterableTableReg`.

### Added

- **Delegating aggregate wrapper** for community-native aggregate
  aliases. Aggregate aliases are now registered as real C-API
  `AggregateFunction`s that delegate to the target aggregate via a
  nested SQL query on a sibling connection. `DISTINCT`, `FILTER
  (WHERE ...)`, `GROUP BY`, `OVER (...)` window frames (every frame
  shape — `ROWS`/`RANGE`, `PRECEDING`/`FOLLOWING`/`CURRENT ROW`/
  `UNBOUNDED`, `EXCLUDE CURRENT ROW`/`GROUP`/`TIES`), and `ORDER
  BY` inside the aggregate call all propagate transparently
  through the alias.
- **`ducklink_prefix(alias, namespace)` scalar form** alongside the
  existing table function. `SELECT ducklink_prefix('c','crypto');`
  runs the same body as `FROM ducklink_prefix(...)` and returns a
  VARCHAR summary.
- **`PREFIX(alias, namespace)` macro** — shortest surface for the
  same operation. All three shapes share `run_ducklink_prefix()`;
  see the "one implementation, N surfaces" invariant in
  `STABILITY.md`.
- **Design docs staked out**:
  - `docs/extension-scope.md` — extensions compose in the SQL
    grammar; they don't replace it. Why ggsql / prql / dplyr belong
    as separate tools above DuckDB.
  - `docs/visualize-design.md` — position on visualization; the seam
    belongs at Vega-Lite JSON.
  - `docs/vegalite-plan.md` — plan for the `ducklink_vegalite`
    scalar; separate `ducklink-vegalite-native` /
    `ducklink-vegalite-wasm` crates outside this repo.
- **`STABILITY.md`** — the stability policy this release commits to.

### Fixed

Bugs surfaced by the aggregate delegation stress test:

- `finalize` now honours its `offset` parameter. Windows split a
  result vector across multiple finalize calls with different
  offsets; the previous code wrote to slot `i` instead of `offset +
  i`, corrupting neighbouring cells.
- NULL results from the target aggregate no longer error. `sum` of
  an all-null column, or an empty frame after `EXCLUDE CURRENT
  ROW`, correctly writes NULL rather than failing with a
  null → i64 coercion error.
- `update` detects DuckDB's "shared-state" C-API dispatch. Sorted-
  aggregate and full-partition (`ROWS UNBOUNDED..UNBOUNDED`) paths
  hand us one initialised state at slot 0 and NULL at slot 1 as a
  sentinel; probing slots 1..N used to crash on unmapped pages.
- `combine` no longer mem-takes rows from the source. DuckDB's
  segment-tree windowing may combine the same source into several
  overlapping frames; a destructive move emptied the source on the
  second visit and produced wrong intermediate results.
- `build_delegation_sql`'s all-columns-constant path emits `n_rows`
  synthetic rows rather than one, so a running-sum frame with
  repeated values returns the correct sum instead of a single-row
  aggregate.

Catalog metadata:

- `talib` now correctly advertises `requires: ["aggregate"]` (was
  `["aggregate", "window"]`). Its `sma` / `ema` / `rsi` are
  aggregates typically called with `OVER (...)`, which the
  delegating wrapper now handles transparently. This is a
  LIGHTING-UP change, not a break — talib is compatible on every
  ducklink host.
- `sample_extension` metadata: dropped a historical `catalog`
  requirement that never had a matching host capability. Its
  actual functions are all `scalar`/`table`/`aggregate`/`macro`.

### Changed

- Catalog totals: **200 → 193 entries**, all now compatible on every
  ducklink host. Zero incompatibles remaining.
- Namespace-qualified aliases (`crypto.hash(x)` for a catalog entry
  with `namespace: "crypto"`) are now registered as
  `CREATE OR REPLACE MACRO <schema>.<name>` in the namespace
  schema — was a `Catalog::CreateFunction` shim call under the
  advanced tier. Same binding surface, portable implementation.
- `docs/native-advanced-dispatch.md` deleted (~280 LOC). Described a
  feature that no longer exists.

---

## [4.6.x] — pre-stability

Iteration series that shaped the surfaces committed in 5.0.0. See git
history from `551ef4d` (2026-07-07) forward for the incremental path.
Highlights:

- Delegating aggregate prototype → generalized wrapper (multi-type,
  multi-column, constant-inlined delegation SQL).
- Advanced tier removed and namespace / prefix support reimplemented
  through the stable C API.
- Colon syntax stripped from docs.
- Runtime WIT interfaces for the removed tier marked deprecated.
- Stress-test-driven fixes to the aggregate wrapper (see 5.0.0
  "Fixed").
- Vega-Lite / extension-scope position docs.

Nothing prior to 5.0.0 is covered by the stability guarantees in
`STABILITY.md`.
