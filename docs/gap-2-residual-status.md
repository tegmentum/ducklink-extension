# Gap 2 residual status — hardcoded DECIMAL(18, 3) sites

Sweep-9 (post sweep-8 batch) audit of every remaining `(18, 3)` occurrence in
`src/reg_duckdb.rs`. Sweep-8 threaded `Option<duckdb_logical_type>` through
`read_arg_raw` / `write_ret_raw` and switched `read_col_to_colvec` +
`write_ret` to `vec.logical_type()`, leaving the sites below.

Grep used to enumerate: `grep -n "width: 18\|scale: 3\|DECIMAL(18\|decimal(18" src/reg_duckdb.rs`
(15 hits).

## Legend

- **FIXABLE** — a live `reg::LogicalType` or `duckdb_logical_type` already
  reaches the site (or its immediate caller) and is being discarded on its
  way in. Threading it through would replace the hardcoded shape.
- **INTERIM-OK** — no live handle is in scope, the site is a documented
  self-consistent fallback in the code-only path, and its peers behave the
  same way. Retained by design.
- **STALE-COMMENT** — a comment describing behaviour that changed
  (e.g. asserts DECIMAL(18, 3) is "the" column shape when the peer now
  honours per-column width/scale). No code change; comment reword only.
- **BY-DESIGN** — DECIMAL(18, 3) is the correct semantic default (matches
  DuckDB's own `DECIMAL` → `DECIMAL(18, 3)` promotion when no `(w, s)` is
  supplied). Not a Gap 2 residual.
- **TEST** — appears in a `#[test]` fixture; the shape is arbitrary.

## Site table

| # | Line | Site | Category | Notes |
|---|-----:|------|----------|-------|
| 1 | 228  | Doc comment on `logical_type(code)` DECIMAL arm — "see write_ret Decimal arm" for "known limitation" | STALE-COMMENT | The write_ret DECIMAL arm no longer silently accepts a mismatch; as of sweep-9 it reads `vec.logical_type()` and eprintln's on shape mismatch (line 1219-1229). Reword the "known limitation" pointer. |
| 2 | 230  | `T_DECIMAL => return LogicalTypeHandle::decimal(18, 3)` inside `fn logical_type(code: u8)` | **FIXABLE** | See fix sketch (a) below. |
| 3 | 2620 | Doc comment on `duckdb_type_of` referring to `logical_type_ffi`'s DECIMAL(18, 3) shape | INTERIM-OK | Accurately describes the current `logical_type_ffi(code: u8)` behaviour. |
| 4 | 2682 | Doc comment inside `logical_type_ffi` T_DECIMAL arm — "Interim shape: DECIMAL(18, 3). Aligns with `logical_type()`" | INTERIM-OK | Accurately describes site #5 below. |
| 5 | 2687 | `ffi::duckdb_create_decimal_type(18, 3)` inside `logical_type_ffi(code: u8)` | INTERIM-OK | User-flagged. Function takes only `code: u8`; `logical_type_ffi_from_lt(&reg::LogicalType)` at line 2717 supersedes it for callers that hold the shape. Kept for the aggregate raw path + cast route that only carry a code. |
| 6 | 2720 | Doc comment on `logical_type_ffi_from_lt` — "Round-trips ... as long as the caller passes the same shape" | INTERIM-OK | Correct — describes the S2/major-5 fix, not a residual. |
| 7 | 2772 | Doc comment on `read_arg_raw` `lt` param — "`None` keeps the DECIMAL(18, 3) interim shape (Gap 2 continuation)" | INTERIM-OK | Current: describes the sweep-8 `Option<lt>` contract. |
| 8 | 2829 | Comment inside `read_arg_raw` DECIMAL arm — describes `None` → `(18, 3)` fallback | INTERIM-OK | Peer of the code at 2836; the fallback branch is the only remaining path when the caller has no handle (e.g. one-off callers outside the batch/copy paths). |
| 9 | 2836 | Literal `(18, 3)` in `read_arg_raw` DECIMAL arm's `else` branch | INTERIM-OK | Same. Every real caller in this file (aggregate raw path at 3120/3146, copy paths at 9810/10391) now passes `Some(col_lts[c])`. |
| 10 | 2981 | Comment inside `write_ret_raw` DECIMAL arm — "column's LogicalType (DECIMAL(18, 3) per `logical_type()`) determines interpretation" | STALE-COMMENT | The parenthetical is wrong: the column's declared LogicalType is whatever the target vector carries, not necessarily DECIMAL(18, 3). Peer `write_ret` (line 1219-1229) now cross-checks via `vec.logical_type()`. See finding (b) below for an optional code-level tightening. |
| 11 | 7632 | `Decimal { width: 18, scale: 3 }` inside `type_codes_are_distinct_per_logical_type` test | TEST | Test only cares that the variant maps to a distinct bridge code. |
| 12 | 8978 | Doc comment on `wit_logicaltype_from_code` — "Handle-less callers ... `(18, 3)` fallback stands — still a Gap 2 continuation" | INTERIM-OK | Current: describes the sweep-5 fix + the residual for `None` callers. |
| 13 | 9011 | Comment inside `wit_logicaltype_from_code` T_DECIMAL arm | INTERIM-OK | Peer of code at 9018. |
| 14 | 9018 | Literal `(18, 3)` in `wit_logicaltype_from_code` DECIMAL arm's `else` branch | INTERIM-OK | Same. `ducklink_copy_bind` (line 9331) and `copy_from_bind` (line 9637) both now pass `Some(lt)`. No production caller reaches the `None` branch — kept as a defensive fallback + for any future non-bind caller that lacks a handle. |
| 15 | 9329 | Comment inside `ducklink_copy_bind` — "instead of the DECIMAL(18, 3) fallback the code-only path used to emit" | INTERIM-OK | Correct — describes the sweep-5 fix. |
| 16 | 9635 | Comment inside `copy_from_bind` — same shape as #15, for COPY FROM | INTERIM-OK | Correct — describes the sweep-5 fix. |
| 17 | 10075 | Comment inside `read_col_to_neutral` (peer of `read_arg_raw`) DECIMAL arm — "keep the (18, 3) fallback" | INTERIM-OK | Peer of code at 10083. |
| 18 | 10083 | Literal `(18, 3)` in `read_col_to_neutral` DECIMAL arm's `else` branch | INTERIM-OK | Same. |
| 19 | 11470 | `"DECIMAL" \| "NUMERIC" => reg::LogicalType::Decimal { width: 18, scale: 3 }` in `logical_type_from_expr` | BY-DESIGN | Bare `DECIMAL` / `NUMERIC` with no `(w, s)` in a SQL type expression. DuckDB itself promotes the same way. NOT a Gap 2 residual. |

(Grep returned 15 hit lines; a couple of lines span >1 concept and are split
into separate rows above, hence the 19-row table for 15 grep lines.)

## Summary by category

- **FIXABLE**: 1 site (row #2, line 230).
- **STALE-COMMENT**: 2 sites (rows #1 line 228, #10 line 2981) — no code
  change, comment reword only.
- **INTERIM-OK**: 15 sites (comments + defensive `else` branches whose real
  callers now all pass a live handle).
- **BY-DESIGN**: 1 site (row #19, line 11470).
- **TEST**: 1 site (row #11, line 7632).

## Fix sketches for the FIXABLE site

### (a) Line 230 — `logical_type(code: u8) -> LogicalTypeHandle`

**Current signature:**

```rust
fn logical_type(code: u8) -> LogicalTypeHandle
```

Called from four sites:

| Caller | File:Line | What it discards |
|---|---|---|
| `WasmScalar::signatures` | 1508-1515 | reads `PENDING_SIGNATURE: (Vec<u8>, u8)` — the caller `install_singleton_vscalar` at 1720 has `f.arguments[i].logical: reg::LogicalType` and `f.returns: reg::LogicalType`, then discards to `type_code(…)`. |
| `WasmTable::bind` | 1844 | reads `WasmTableExtra::col_codes: Vec<u8>` — the caller `register_tables` at 1941 has `t.columns[i].logical: reg::LogicalType`, then discards via `type_code`. |
| `WasmTable::parameters` | 1926 | reads `PENDING_TABLE_PARAMS: Option<Vec<u8>>` — populated at 1950 from `arg_codes`, upstream of which is `t.arguments[i].logical`. |
| `ArrowShim::bind` | 10277 | reads `ArrowShimExtra::col_codes: Vec<u8>` — the caller at line 10454-10471 has `t.columns[i].logical: reg::LogicalType`, then discards via `type_code`. |

**Proposed change:** add a structural sibling and route callers that hold
the full shape through it, leaving the code-only `logical_type` in place
for backwards-compat but stop calling it from paths that never need to
lose information.

```rust
fn logical_type_from_lt(lt: &reg::LogicalType) -> LogicalTypeHandle {
    match lt {
        reg::LogicalType::Decimal { width, scale } =>
            LogicalTypeHandle::decimal(*width, *scale),
        // (nested arms recurse via child handles + duckdb-rs's list/struct/map/array
        //  builders — same shape as `logical_type_ffi_from_lt` at 2717)
        _ => logical_type(type_code(lt)),
    }
}
```

Then thread `Vec<reg::LogicalType>` through the extras that today carry
`Vec<u8>` (or add a parallel `Vec<reg::LogicalType>` alongside `col_codes`
if the hot path needs to keep the fast `u8` for match dispatch):

1. `PENDING_SIGNATURE: RefCell<Option<(Vec<u8>, u8)>>` at line 1365 →
   `RefCell<Option<(Vec<reg::LogicalType>, reg::LogicalType)>>` (or add a
   parallel tuple). `install_singleton_vscalar` at line 1729 sets it from
   `f.arguments`/`f.returns` (already available). `WasmScalar::signatures`
   at 1508-1515 uses `logical_type_from_lt` on each entry.
2. `WasmTableExtra` at line 1801 gains a `Vec<reg::LogicalType>` beside
   `col_codes`. `register_tables` at 1940-1949 already has
   `t.columns[i].logical` in scope. `WasmTable::bind` at 1844 uses the new
   field via `logical_type_from_lt`.
3. `PENDING_TABLE_PARAMS: RefCell<Option<Vec<u8>>>` at line 1828 →
   `RefCell<Option<Vec<reg::LogicalType>>>`. Populated at 1950 from
   `t.arguments[i].logical` (already available). `WasmTable::parameters`
   at 1926 uses `logical_type_from_lt` in the map closure.
4. `ArrowShimExtra` at line 10194 gains a `Vec<reg::LogicalType>` beside
   `col_codes`. `install_arrow_tables` at 10470-10476 already has
   `t.columns[i].logical` in scope. `ArrowShim::bind` at 10277 uses the
   new field via `logical_type_from_lt`.

**Effect:** a scalar/table/arrow function whose declared arg or column type
is `DECIMAL(20, 5)` is registered as `DECIMAL(20, 5)` instead of
`DECIMAL(18, 3)`. Removes the last "declared column type silently wrong"
site outside the intentional `logical_type_ffi(code)` fallback and the
`None`-branch defenses.

## Optional code-level tightening for site #10 (line 2981)

`write_ret_raw` currently accepts `lt: Option<duckdb_logical_type>` but
its body ignores `lt` (`let _ = lt;` at line 2925). The peer `write_ret`
at 1219-1229 reads `vec.logical_type()` and eprintlns when the guest's
returned `(width, scale)` disagrees with the column's declared shape.
Mirror that check in `write_ret_raw`'s DECIMAL arm using the passed `lt`
(when `Some`). Not a hardcoded-shape fix — the raw storage is
width/scale-agnostic i128 — but closes the observability parity between
the two writers. Same category as the sweep-8 mismatch warning, not a
Gap 2 site per se.

## Not on this list

- `parse_decimal_expr` at line 11835 is the correct DECIMAL expression
  parser (uses actual `(w, s)` from the SQL text).
- `sql_type_name` at line 279 already formats `DECIMAL({width}, {scale})`
  from the structural variant.
- `type_code(reg::LogicalType::Decimal { .. }) => T_DECIMAL` at line 181 is
  the direction we WANT to preserve information from — the code-only
  descent is what loses it downstream. See fix sketch (a).
