# Stability policy

Ducklink follows [Semantic Versioning](https://semver.org). This
document names the surfaces that MAJOR / MINOR / PATCH commitments
apply to, so downstream projects — dashboards, notebooks,
data-pipeline glue, other extensions calling into ducklink — know
what they can lean on.

Effective from **v5.0.0**. Everything below is guaranteed until v6.0.0.

## 1. What is guaranteed stable

### 1.1 SQL entry points

The following are the callable SQL surface of ducklink and are frozen
in shape (name, argument list, argument types, return type):

| Entry point | Shape | Notes |
|---|---|---|
| `ducklink_load(name [, kind => ...])` | table function | `kind` in `{'wasm','native'}`. Default `wasm`. |
| `ducklink_prefix(alias, namespace)` | table function | Returns `(alias, namespace, macros BIGINT)`. |
| `ducklink_prefix(alias, namespace)` | scalar | Returns VARCHAR summary. |
| `PREFIX(alias, namespace)` | scalar macro | Delegates to the scalar. |
| `ducklink_version()` | scalar | Returns VARCHAR. |
| `ducklink_help(name)` | scalar | Returns markdown VARCHAR. |

**Invariant — one implementation, N surfaces.** Multiple call shapes
that share a name (`ducklink_prefix` as TF + scalar; `PREFIX` as
macro) share one implementation body. New behaviour is added to the
shared body; shape-specific short-circuits or additional options are
NOT allowed. This is the discipline that keeps three call surfaces
readable as one capability.

### 1.2 Discovery views (`ducklink.*` schema)

Column names, types, and semantics of these views are frozen:

- `ducklink.modules`
- `ducklink.functions`
- `ducklink.host`
- `ducklink.host_capabilities`
- `ducklink.cache`
- `ducklink.module_compatibility`
- `ducklink.events`
- `ducklink.docs`
- `ducklink.search`
- `ducklink.prefixes`

Adding new columns to a view is a MINOR change. Removing / renaming /
changing a column type is MAJOR. Adding new views is MINOR.

### 1.3 Catalog schema

The JSON shape of `assets/catalog-snapshot.json` and the same fields
served from `ext.ducklink.dev/catalog.json` are the interchange format
between ducklink and downstream tooling (catalog browsers,
alternative loaders, IDE integrations). Documented in
`docs/catalog-authoring.md`.

Adding new optional fields is MINOR. Removing / renaming / changing
the meaning of a field is MAJOR. Populating a previously-unset optional
field on an existing entry is not a break.

### 1.4 Environment variables

- `DUCKLINK_COMPONENTS` — colon-separated `name=path[:name=path]*`
  preloaded at extension init.
- `DUCKLINK_CATALOG_URL` — override the default catalog endpoint.
- `DUCKLINK_CORPUS_DIR` — dev-only, points at a wasm corpus for tests.

Semantics and parsing are frozen. Adding new variables is MINOR.

### 1.5 `duckdb:extension` WIT contract

Ducklink implements the WIT contract identified by digest
`99a5f94eba956bc0a7f828e8501e95560fa7d626349e78bd5548ac56d1e2f219`,
which corresponds to `duckdb:extension@5.0.0`. Components that
target this contract are supported for the life of ducklink v5.x.
Components built against the earlier `duckdb:extension@4.0.0`
contract are cross-major-rejected at load time and must be rebuilt
against `@5.0.0`.

Adding new opt-in worlds (new interfaces components can import when
they want a new capability) is MINOR. Modifying an existing WIT
interface in place is MAJOR.

The DEPRECATED interfaces (see below) are the exception: they are
scheduled for removal at the next `duckdb:extension` major bump,
which will be gated on a matching ducklink MAJOR bump.

### 1.6 `ducklink-runtime` public Rust API

The re-exports from `ducklink-runtime` used by embedders — the
`reg::` module's neutral types, `load_component`, `ExtensionServices`,
`CallbackRegistry`, `ExtensionInstance` — follow semver. Internal
modules (private, or `pub` items marked `#[doc(hidden)]`) can change
in any release.

## 2. What is NOT stable

- **`ducklink-extension` crate's internal Rust modules.** `src/reg_duckdb.rs`,
  `src/delegating_agg.rs`, `src/catalog.rs`, `src/engine.rs`, `src/events.rs`
  are all crate-private and can change freely. The extension is
  consumed as a loadable `.duckdb_extension` — the surface people
  actually see is the SQL entry points and the WIT contract, not
  these Rust modules.
- **Wire format of blob URLs** (`ext.ducklink.dev/wasm/sha256/...`,
  `ext.ducklink.dev/native/sha256/...`). The catalog entry names
  where blobs live; if we move them, the catalog is updated in the
  same release. Downstream tools should not hard-code the URL
  scheme.
- **Bundled catalog snapshot's exact contents.** Which modules are
  listed and their metadata can change any release. Callers who
  depend on a specific entry existing should pin the snapshot they
  built against.

## 3. Deprecation policy

A stable surface is deprecated by:

1. A `DEPRECATED` marker in the doc-comment / doc block for that surface.
2. An entry in the CHANGELOG under the release that added the
   deprecation.
3. A note in the module or view listing (`ducklink.modules`,
   `ducklink.functions`, etc.) where a `deprecated` field is
   surface-appropriate.

Removal is gated on both:

- A MAJOR version bump.
- At least two MINOR releases between the deprecation announcement
  and the removal.

Currently deprecated for future MAJOR removal (see CHANGELOG v5.0.0):

- `duckdb:extension` WIT interfaces `parser`, `parser-dispatch`,
  `optimizer`, `optimizer-dispatch`, `table-stream`,
  `table-stream-dispatch` — no host consumes registrations from
  them since v4.6.0.
- The corresponding worlds `duckdb-extension-parser`,
  `duckdb-extension-optimizer`, `duckdb-extension-table-stream`.
- Rust types `reg::ParserReg`, `reg::OptimizerReg`,
  `reg::FilterableTableReg` in `ducklink-runtime`.

## 4. The conformance suite

The surfaces named in §§ 1.1–1.2 are backed by a cross-host
conformance suite at `conformance/`. Every entry point and every
discovery view is exercised by a portable SQL script with a golden
`.out` file; any implementation of the ducklink surface — the native
extension in this repo, the standalone workspace host at
`~/git/ducklink/crates/ducklink-host`, and anything built later —
must produce byte-identical output for the same scripts.

Adding a new committed surface to §§ 1.1–1.2 requires a matching
conformance script in the same commit. Removing or changing a
surface requires the corresponding golden update AND a CHANGELOG
entry for the release.

The conformance runner for this repo lives at `tests/conformance.rs`
and is exercised by
`cargo test --release --no-default-features --features bundled --test conformance`.
Other hosts add their own runner; the SQL scripts and `.out` files
are shared across hosts and are the source of truth.

## 5. Breaking-change discipline

Anything below constitutes a MAJOR bump:

- Removing or renaming an entry from §1.1 or §1.2.
- Changing an argument's type, adding a required argument, or
  changing default values of existing optional arguments.
- Changing the semantics of an existing catalog field.
- Changing the meaning of an existing environment variable.
- Modifying an existing WIT interface in place.
- Removing a previously-guaranteed field from a discovery view.

Anything below is MINOR:

- Adding new SQL entry points, views, or columns.
- Adding new optional catalog fields.
- Adding new WIT interfaces or worlds.
- Broadening argument acceptance (e.g. accepting VARCHAR where only
  TEXT was accepted before).
- Adding new environment variables.
- New catalog entries appearing in the bundled snapshot.

Anything below is PATCH:

- Bug fixes that don't change documented behaviour.
- Performance improvements.
- Documentation-only changes.
- Adding conformance tests.

## 6. Version support window

Ducklink commits to shipping bug fixes for the current MAJOR series
and PATCH releases for the previous MAJOR series for six months
after the current MAJOR ships, whichever is longer. Downstream
projects have a predictable window to migrate.

Concretely: when v6.0.0 ships, v5.x will continue to receive
security and correctness patches for at least six months.

## 7. What this means in practice

If you're building a notebook plugin, a data-pipeline stage, or
another DuckDB extension that calls into ducklink, you can:

- Take a dependency on any name in §1 and expect it to keep working
  through the current MAJOR series.
- Read the discovery views and treat them as a stable API.
- Ship components targeting the `duckdb:extension@5.0.0` contract
  and expect them to load on any ducklink v5.x. Components built
  against `duckdb:extension@4.0.0` are cross-major-rejected on
  v5.x and must be rebuilt against `@5.0.0`.

If you're building against a name that isn't in §1, we don't
promise anything. Reach out and we can consider stabilizing it.
