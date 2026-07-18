# Load model

How ducklink modules are made available to a session — what the user runs, why,
and what's coming.

## Today (v4.2.x)

Modules must be loaded explicitly. `LOAD ducklink;` gets you the runtime,
discovery views, and `ducklink_load()` — but no module-specific functions
until you name what you want:

```sql
LOAD ducklink;
FROM ducklink_load('aba');                       -- WASM (default)
SELECT aba_validate('021000021');                -- works
```

### Call form

```sql
FROM ducklink_load('<name>' [, kind => 'wasm' | 'native']);
```

- Default kind: **WASM**. Safer trust posture (sandboxed; no `allow_unsigned_extensions` change needed).
- `kind => 'wasm'` is redundant when the default is what you want; provided for symmetry.
- `kind => 'native'` opts into the native `.duckdb_extension` build for perf, at the cost of `-unsigned`-required trust.

### Preloading via environment

```
DUCKLINK_COMPONENTS=aba,creditcard duckdb
```

The extension init reads this comma-separated list and loads each at session start. Good for known-set deployments (CI, ETL, embedded).

## Why not transparent autoload

DuckDB's binder can autoload extensions when it encounters an unknown function name — that's how `SELECT read_parquet(...)` works with no prior LOAD. The mapping from function name to extension name is a compile-time array in the DuckDB binary (`src/include/duckdb/main/extension_entries.hpp:42`).

**There's no public API for a third-party extension to add entries to that map at runtime.** Ducklink can't register the ~1000 function names its catalog knows about, so DuckDB's autoload doesn't fire on them.

Working around this from userspace requires either:
- Pre-registering signature stubs for every catalog export at `LOAD ducklink` (namespace pollution + signature-mismatch failure modes), or
- Intercepting all incoming SQL text (no such hook in DuckDB), or
- Preloading everything at session start (unacceptable startup cost for 200+ modules).

None of those are honest UX. We chose to require an explicit `ducklink_load()` call and be upfront about it.

## What's coming

We're pursuing this upstream. See `docs/duckdb-upstream-function-autoload.md` — proposal for a small DuckDB API (`ExtensionHelper::RegisterFunctionEntry` or similar) that lets loaded extensions extend the function → extension autoload map at runtime.

If that lands, ducklink can register every catalog export at `LOAD ducklink` time, and then:

```sql
LOAD ducklink;
SELECT aba_validate('021000021');   -- ducklink autoloads aba WASM, query proceeds
```

Until then: `FROM ducklink_load('<name>');` explicit, `DUCKLINK_COMPONENTS=…` env preload for known-set deployments.

## Collision resolution (when autoload eventually lands)

When two modules export the same function name, autoload should **fail fast** with the list of matching modules. The user picks one:

```sql
LOAD ducklink;
SELECT some_shared_name('x');
-- Error: 'some_shared_name' is exported by multiple ducklink modules:
--   moduleA, moduleB, moduleC.
-- Load one explicitly:  FROM ducklink_load('moduleA');
```

This posture is safer than "first wins" (which introduces silent behaviour drift as the catalog grows) and clearer than "load them all" (which does more work than the user asked for and can introduce runtime cross-module conflicts).

## Design docs

- `docs/dual-build-native-and-wasm.md` — the WASM/native split rationale.
- `docs/duckdb-upstream-custom-trusted-keys.md` — trust-posture upstream ask (companion to this one).
- `docs/duckdb-upstream-function-autoload.md` — the autoload upstream ask.
