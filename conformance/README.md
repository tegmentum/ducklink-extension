# Ducklink conformance suite

Cross-host SQL conformance for the surface committed in
[STABILITY.md](../STABILITY.md). Any implementation of the ducklink
surface — the native DuckDB extension in this repo, the standalone
workspace host at `~/git/ducklink/crates/ducklink-host`, and anything
built later — must pass this suite.

The suite is intentionally SQL, not Rust. Two reasons:

1. **Portable across hosts.** The workspace host's test harness runs
   over a different runtime than duckdb-rs; if the conformance were
   Rust it would only be meaningful for one host.
2. **User-readable.** A user who wants to know "what is the SQL
   surface of ducklink" can read `scripts/*.sql` end-to-end and see
   every guaranteed entry point exercised concretely, alongside the
   expected results.

## Layout

- `scripts/<name>.sql` — a self-contained SQL script that exercises
  one aspect of the surface (one entry point, one view family, one
  scenario). Every script starts with `LOAD ducklink;` and does its
  own setup; scripts don't share state.
- `expected/<name>.out` — the expected output of running that script,
  in DuckDB's default `table` output format.
- `runner/` — per-host adapters that run the scripts and diff the
  output. `runner/extension.rs` runs them under this repo's native
  extension; the workspace host adds its own runner.

## What each script guarantees

Read the header comment of each `.sql` file. A script MUST:

- Assert the name and signature of the surface it tests via
  `SELECT * FROM duckdb_functions() WHERE function_name = '<n>'` or
  `SELECT column_name, data_type FROM (DESCRIBE <view>)`.
- Exercise the surface with realistic inputs.
- Emit output that will diff cleanly across hosts (deterministic
  ordering, no host-specific paths, no timestamps).

A script MUST NOT:

- Assume a specific catalog is loaded (each script sets up the
  minimum it needs).
- Depend on external network state.
- Use `ducklink_version()`'s exact return value in a diff (only
  its shape: "ducklink <semver>").

## Adding new surfaces

When a new stable surface lands in STABILITY.md, add a matching
script. Update the expected output. Include the update in the same
release that lands the surface. Nothing goes into STABILITY.md
without a conformance script backing it.

## Running the suite

Under this extension's test infrastructure:

```
cargo test --release --no-default-features --features bundled --test conformance
```

Any other host implementing the surface must produce identical
`expected/<name>.out` output when running the scripts through its own
runner. Divergence is a drift bug.
