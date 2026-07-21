# Prefix model decision

## Decision

**Adopt Host A's `prefix.name` model (extension v5.0.0). Workspace host migrates.**

Neither model has been released to real users — v5.0.0 is still in
community-extensions review as of this decision, and the workspace's
`prefix__name` machinery has only ever been in workspace-internal
branches. Cutover is clean; no user-facing deprecation cycle needed.

The four tradeoffs the design analysis surfaced (no collation/pragma
prefixing, last-loaded-wins bare-name collisions, aggregate modifiers
only through the qualified name rather than the schema-alias short
form, and no expansion/prefix decoupling) are **seen and accepted**.
The reasoning: SQL-native syntax is a stronger organizing principle
than the workarounds those gaps would require. `EXPLAIN` legibility,
`search_path` participation, and first-class SQL objects at every
alias level matter more than the categorical coverage `prefix__name`
would provide for shapes we don't yet ship (collations, pragmas).

Everything below preserves the pre-decision analysis as-is so the
tradeoffs stay visible; only this top section captures the outcome.

## Context

Two ducklink hosts implement the same conceptual capability ("give a component's functions a short user-declared prefix") with divergent designs:

- **Host A (this repo, v5.0.0 shipping)** uses SQL schema qualification: `ducklink_prefix('c', 'crypto')` creates schema `c` and populates it with `CREATE OR REPLACE MACRO c.hash(x) AS crypto.hash(x)` for every function in `crypto`. State lives in `ducklink.prefixes(alias, namespace)`; re-applied on reconnect by `replay_persisted_prefixes`. Prefix is user-declared at query time, persistent per catalog.
- **Host B (workspace ducklink, feat branch)** uses SPARQL-style double-underscore concatenation: every function is dual-registered as bare `name(x)` and `<prefix>__name(x)`. Prefix is registry-declared and load-time-automatic. State lives in `__ducklink_prefix`, `__ducklink_prefix_function`, `__ducklink_prefix_pin`. A PIN table lets a user assign a bare name to a specific expansion, surviving load-order changes.

Both hosts share `duckdb:extension@4.0.0` and the same wasm components.

## 1. The essential tradeoffs

**What `prefix.name` (Host A) does that `prefix__name` (Host B) can't:**

- Real DuckDB schemas. `SET search_path = 'main,crypto'`, `USE crypto`, and `information_schema.routines` filtered by `routine_schema` all work naturally.
- `EXPLAIN` shows `crypto.hash(x)` verbatim — SQL-legible.
- Users can pick their own short alias per session (`ducklink_prefix('c', 'crypto')` or `ducklink_prefix('crypt', 'crypto')`); the prefix is user-side ergonomics, not a component-author decision baked into the registry.
- The four call shapes (bare community name, ducklink bare alias, namespace, alias) are all first-class SQL objects, not string-mangled identifiers.

**What `prefix__name` (Host B) does that `prefix.name` (Host A) can't:**

- **Covers collations, pragmas, and macros.** DuckDB has no schema container for collations or pragmas — you cannot express `crypto.icu_en` as a collation or `crypto.set_indent` as a pragma. Host A's model silently omits these shapes (its `duckdb_functions()` scan filters to `scalar|aggregate|table_macro|scalar_macro|macro|table`). Host B handles all six name-keyed shapes uniformly.
- **Deterministic bare-name resolution under collision.** When two components both register `hash(x)`, Host A gives you last-loaded-wins for the bare name and a "declare an alias to disambiguate" workaround; there is no way to say "bare `hash` should always mean the `crypto` component regardless of load order." Host B's `.prefix prefer hash crypto` writes `__ducklink_prefix_pin` and forces bare `hash` via a `CREATE OR REPLACE MACRO hash(...) AS (crypto__hash(...))` wrapper that survives future loads.
- **Aggregate modifier propagation through the short form.** Host A's docs admit that `DISTINCT`, `FILTER`, `ORDER BY`, and `OVER` do NOT propagate through the prefix macro for aggregates — users must fall back to the namespace-qualified form. Host B's dual-registration is a real registration (not a wrapper), so the qualified name is a full aggregate with modifiers intact.
- **Zero-ceremony ergonomics.** No `ducklink_prefix(...)` call required; the prefixed form is available immediately after `LOAD`.
- **Global-identity expansion.** The reverse-DNS `expansion` (e.g. `com.tegmentum.ducklink.json`) is a stable identity token independent of the SQL-usable `prefix`. Users can rename the prefix without losing pins.

**Tooling legibility trade:** Host A wins on `EXPLAIN` and catalog-introspection readability. Host B wins on collision reporting (`.prefix conflicts` returns a table of every `(name, shape, n_args)` registered by more than one expansion — Host A has no equivalent view).

## 2. Recommendation

**Adopt `prefix__name` (Host B's design). Retire the schema-qualification model at Host A's next MAJOR (v6.0.0).**

Reasoning, ordered by weight:

1. **Collation/pragma coverage is a categorical gap, not a UX preference.** Host A cannot represent these shapes at all under its model. Users of a wasm collation or pragma component would have no prefixing story on Host A. This alone is disqualifying if we care about a single unified model across hosts.
2. **The PIN mechanism solves a real production problem** — deterministic bare-name resolution across components — that Host A has no answer for. Any serious multi-component deployment eventually hits this.
3. **Aggregate modifier propagation is a correctness win**, not a nicety. `SELECT approx_count_distinct(x) FILTER (WHERE ...)` should work through the short form, and it does under Host B, not Host A.
4. **The `expansion` concept is architecturally cleaner.** Prefix rename doesn't invalidate pins; global identity is decoupled from SQL surface.
5. Host A's advantages (real schemas, `search_path` participation) are recoverable at low cost: components that WANT a schema-qualified surface can still register under a schema in `main` under the new model — the `prefix__name` form is additive.

The Host A readability advantage is genuine but not load-bearing. `EXPLAIN` output showing `jsonfns__json_valid(x)` is ugly, but it's unambiguous and greppable, which matters more than SQL aesthetics.

## 3. Migration plan for the loser (Host A → `prefix__name`)

**Scope of change in Host A:**

1. **Dual-registration at load time** in `src/reg_duckdb.rs`. At the point community-native aliases are minted (`create_community_aliases`), also register each function under `{prefix}__{name}`. Real dual-registration on the C API — not a macro wrapper — so aggregate modifiers propagate. Reuse Host B's `qualified_name`/`sanitize_prefix`/`sanitize_name` verbatim from `crates/ducklink-host/src/prefix.rs` (they're in the shared `datalink-prefix` crate, so no fork). **~150 LOC.**
2. **Port the three internal tables and their DDL** (`__ducklink_prefix`, `__ducklink_prefix_function`, `__ducklink_prefix_pin`) plus `build_prefix_table_sql`. Copy from `crates/ducklink-host/src/prefix.rs:313-383`. **~100 LOC (mostly SQL constants).**
3. **Port `CollisionTracker`, `RetainedDefs`, `apply_pins`, `pin_macro_sql`.** Non-trivial: Host A registers directly on the C API rather than through `PendingRegistrationsData`, so the retained-def path needs to hook into `register_scalars`/`register_tables`/`register_aggregates` directly instead of into a drain pass. **~400 LOC.**
4. **Compatibility shim for `ducklink_prefix(alias, namespace)`.** Keep the SQL entry point in § 1.1 alive, but redirect the body from `create_prefix_aliases` to `INSERT INTO __ducklink_prefix(name=alias, expansion=namespace, …)` followed by an `apply_pins` refresh. Return the same tuple shape `(alias, namespace, macros BIGINT)` — where `macros` now counts dual-registered functions. Emit a one-time deprecation notice pointing at the new `.prefix` interface. **~80 LOC.**
5. **New `.prefix`-equivalent surface.** Host A has no dotcmd layer, so the equivalent is a small family of scalars: `ducklink_prefix_list()`, `ducklink_prefix_conflicts()`, `ducklink_prefix_prefer(name, target[, shape, args])`, `ducklink_prefix_unprefer(name[, shape, args])`. Views over `__ducklink_prefix*` cover the read side; only prefer/unprefer need new callables. **~200 LOC.**
6. **`docs/catalog-authoring.md` § 3c rewrite.** Replace `namespace` field guidance with `prefix` + `expansion` fields matching Host B's registry shape. Mark existing `namespace` field DEPRECATED but honored (during co-existence, populate a synthetic `expansion = "ducklink-catalog://<namespace>"` under the hood). **~100 lines of docs.**

**Rough total: ~800–1000 LOC changed/added**, most of it ported from Host B verbatim through the shared `datalink-prefix` crate.

**Data-migration:** any file-backed catalog with pre-existing rows in `ducklink.prefixes` would find that `SELECT c.hash(x)` (schema-qualified alias) silently stops working after upgrade. Practically, v5.0.0 is the shipping version and the surface is new — real-world affected catalogs should be nil, but the migration script should nonetheless read `ducklink.prefixes`, mint corresponding `__ducklink_prefix` rows, and drop the old schemas. **~50 LOC one-shot migration** invoked from the loader on first startup.

**Deprecation / co-existence window:** one MINOR release cycle (v5.x). Ship dual-registration and the pin machinery IN ADDITION TO the schema-alias path at v5.next; the schema aliases are still created but emit a `deprecated:ducklink_prefix_schema` event. Remove the schema-creation half at v6.0.0.

## 4. Impact on STABILITY.md § 1.1

This IS a stability-committed change. Current § 1.1 commits:

- `ducklink_prefix(alias, namespace)` TF returning `(alias, namespace, macros BIGINT)`
- `ducklink_prefix(alias, namespace)` scalar returning VARCHAR summary
- `PREFIX(alias, namespace)` macro delegating to the scalar
- The one-implementation-N-surfaces invariant

Under the new model:

- **The three call surfaces stay.** Their return shapes stay wire-compatible (`(alias, namespace, macros BIGINT)` still meaningful — `macros` becomes "functions dual-registered"). This is compatible under § 1.1.
- **`ducklink.prefixes` view (§ 1.2) stays.** It becomes a view over `__ducklink_prefix` and gains optional `expansion` and `description` columns. Adding columns is MINOR per § 5, so this is not a break.
- **The SEMANTICS change.** `SELECT c.hash(x)` (schema-qualified through a user-declared alias) STOPS resolving. This is a hard break. Under § 5, "changing the semantics of an existing catalog field" is MAJOR; the same logic applies to changing observable SQL behavior of a § 1.1 entry point.

**Recommended STABILITY.md update:**

- In v5.x deprecation cycle: add a note in § 3 (Deprecation policy) listing "schema-qualified prefix aliases (`<alias>.<fn>` where `<alias>` was declared via `ducklink_prefix`)" as scheduled for removal at v6.0.0. Add a matching CHANGELOG entry.
- In § 1.1's invariant paragraph: extend "one implementation, N surfaces" to acknowledge that the shared body will switch to dual-registration + pin at v6.0.0; the three call surfaces are preserved by name and return shape but their body changes.
- No change to the § 1.1 table required for v5.x. At v6.0.0, add an explicit callout that the schema-side call form is gone.

The current stability commitment for v5.x holds; the migration lands at the natural MAJOR boundary. This does not require an emergency amendment to § 1.1 today.
