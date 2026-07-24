# duckdb-rs: expose `Connection` raw handle

Investigation for T2-6 tail. External repo — action item, not a patch.

## Current state (duckdb-rs `main`, and pinned `1.10504.0`)

`crates/duckdb/src/lib.rs:467` (identical at `1.10504.0` lib.rs:266):

```rust
pub struct Connection {
    db: RefCell<InnerConnection>,   // private
    cache: StatementCache,          // private
    path: Option<PathBuf>,          // private
}
```

All fields are fully private (stricter than the `pub(crate)` we assumed).
`InnerConnection.con: ffi::duckdb_connection` is `pub` on the struct
(inner_connection.rs:61) but `mod inner_connection;` is declared without
`pub`, so the pointer is unreachable from downstream. `open_from_raw`
(lib.rs:324) accepts a raw `duckdb_database` — the reverse direction does
not exist.

## Why we want it

`register_scalars_with_raw` (`src/reg_duckdb.rs:1578`) needs
`ffi::duckdb_connection` for `duckdb_scalar_function_set` (multi-overload
groups) and the column-major raw invoke. Production entry points open
DuckDB via the C API and already have the raw pointer. Tests get only a
`duckdb::Connection` and cannot construct a sibling raw connection
without re-opening the database, so they fall back to VScalar — keeping
that fallback path alive and blocking full VScalar deletion.

## Proposed API

```rust
impl Connection {
    /// Borrow the underlying `duckdb_connection` FFI handle.
    ///
    /// # Safety
    /// The returned pointer is valid only while `&self` is live and must
    /// not be closed by the caller; the `Connection` retains ownership.
    /// Concurrent use from another thread is UB (matches `!Sync`).
    pub unsafe fn raw_handle(&self) -> ffi::duckdb_connection {
        self.db.borrow().con
    }
}
```

Rationale: `unsafe` because lifetime and ownership move outside the
borrow checker; no feature-gate because the surface is one method, gated
symmetry with existing `open_from_raw` (also `unsafe`, ungated). Precedent
is PR #493 (`InterruptHandle`) which resolved the analogous #342
"private `InnerConnection`" wall by wrapping — but scalar registration is
raw C API from top to bottom, so wrapping does not fit.

## Alternatives considered

- **Safe wrapper (`ScalarSetHandle`)** — mirrors #493. Rejected: the
  wrapper would need to expose `duckdb_scalar_function_set` end-to-end,
  which is out of scope for duckdb-rs.
- **Fork duckdb-rs** — one-line delta, but forces us to track upstream
  DuckDB bumps by hand. Rejected unless upstream declines.
- **Patch downstream via `[patch.crates-io]`** — same cost as a fork
  with worse ergonomics.
- **Re-open database via C API in tests** — status quo VScalar fallback;
  keeps parallel code alive forever.

## Recommendation

File the issue upstream citing #342 / PR #493 as precedent, offer the
one-method patch. Keep the VScalar fallback in-tree until it lands.
Fork only if the request is rejected.
