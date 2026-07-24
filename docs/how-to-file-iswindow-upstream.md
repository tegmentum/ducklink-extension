# How to file: expose `is_window` on scalar `FunctionInfo`

Companion checklist for filing `duckdb-scalar-is-window-issue.md` upstream.

## Target repo

**Upstream DuckDB C++ core** — [`duckdb/duckdb`](https://github.com/duckdb/duckdb). The gap is in the stable C Extension API (`duckdb.h`), which lives there. `duckdb-rs` / `libduckdb-sys` only re-export what upstream ships; filing there first would bounce.

- Do **not** file against `duckdb/duckdb-rs`.
- Do **not** file against `duckdb/community-extensions`.

## Issue tracker

GitHub Issues on `duckdb/duckdb`. Use the **Feature request** template.

## Labels / prefixes

Repo maintainers apply labels; contributors don't self-label. Conventional title prefix:

```
[C API] Expose is_window on duckdb_function_info for scalar functions
```

Mention `Extension C API` in the first line so it routes to the extension-API owners.

## Body

Paste `docs/duckdb-scalar-is-window-issue.md` verbatim. It already has Motivation / Proposed API / Alternatives / Impact / References sections matching upstream's expected shape. Version citation (`libduckdb-sys 1.10505.0`) was re-verified against `Cargo.lock` and the shipped `bindgen_bundled_version_loadable.rs` on 2026-07-24 — zero `window` symbols at any level.

## Follow-up

1. Link the filed issue number back into the two `TODO` blocks in `src/engine.rs` (lines ~1279 and ~1322) so future readers can find it.
2. Watch the issue for an accepted API name — likely `duckdb_scalar_function_info_is_window` but maintainers may prefer a different spelling.
3. Once the symbol lands in a `libduckdb-sys` release, unblock **T3-2** (audit gap T4-22): replace the hardcoded `iswindow: false` with the real accessor and drop the `TODO`.
