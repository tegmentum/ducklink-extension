# DuckDB upstream: extensible function → extension autoload registration

Draft for submission to https://github.com/duckdb/duckdb/discussions. Body below is ready to paste.

Companion to `docs/duckdb-upstream-custom-trusted-keys.md`. Both are about "let extensions participate in DuckDB's ecosystem more flexibly with small, targeted APIs."

---

## Title

Proposal: allow extensions to register function → extension mappings for autoload

## The ask, in one sentence

Add a small public API — `ExtensionHelper::RegisterFunctionEntry(function_name, extension_name)` (or an equivalent shape) — so a loaded extension can extend the function-name-to-autoload-extension map that today lives as a `static constexpr` array at `src/include/duckdb/main/extension_entries.hpp:42`.

## Motivation

DuckDB already supports autoloading an extension when a user references a function name it registers — `Catalog::AutoLoadExtensionByCatalogEntry` (`src/catalog/catalog.cpp:616`) looks up unresolved function names in `EXTENSION_FUNCTIONS[]` and calls `ExtensionHelper::AutoLoadExtension(name)` on the mapped extension. This is what makes `SELECT read_parquet(...)` work with no prior `LOAD parquet;` — a great DX.

The limitation: `EXTENSION_FUNCTIONS[]` is compile-time-fixed in the DuckDB binary. Only extensions the DuckDB team knows about at release time can participate. A third-party extension that itself distributes a catalog of SQL functions — including all the community-extensions authors, and the dynamic-load-catalog use cases like ducklink — cannot make its function names auto-loadable, even after being explicitly loaded.

Result: users of any third-party extension have to know the specific per-extension load statement (`DUCKLINK LOAD 'aba'`, `INSTALL x FROM community`, etc.) before they can call any function from it. The autoload mechanism DuckDB already ships is invisible to them.

## Concrete use case

We (tegmentum) run [ducklink](https://github.com/tegmentum/ducklink-extension) — a DuckDB extension that dynamically loads WebAssembly and native components from a catalog of hundreds of SQL-callable capabilities (aba_validate, iban_country, cc_type, ...). Users can already do `DUCKLINK LOAD 'aba'` to get the `aba_*` functions. What we'd love: after the user runs `LOAD ducklink;` once, `SELECT aba_validate('021000021');` "just works" — ducklink resolves `aba_validate` against its catalog, loads the owning module, and DuckDB proceeds.

This is exactly the model DuckDB uses for its own bundled extensions. We just want to plug in as an equal citizen.

Query.farm's Haybarn and other third-party extension distributions face the same friction from a different angle — see #23388 for related context. Both proposals (this one and #23388's per-origin trust) unlock the "third-party extensions as first-class citizens" story that DuckDB's community-extensions blog explicitly acknowledges as a real gap.

## Proposed shape

A minimal, additive API. Two possible entry points:

### Option A (simplest) — a static registration function

```cpp
namespace duckdb {
class ExtensionHelper {
public:
    // Register a function name → extension mapping at runtime, so DuckDB's
    // autoload mechanism can resolve unknown function names to `extension_name`.
    // Idempotent; last write wins on duplicate names.
    DUCKDB_API static void RegisterFunctionEntry(
        const string &function_name,
        const string &extension_name,
        CatalogType function_type = CatalogType::SCALAR_FUNCTION_ENTRY);
};
}
```

Called by the extension during its own `_init_c_api` (after successfully connecting), once per catalog entry:
```cpp
for (auto &entry : ducklink_catalog.exports()) {
    ExtensionHelper::RegisterFunctionEntry(entry.function_name, "ducklink");
}
```

DuckDB's `FindExtensionInFunctionEntries` at `src/catalog/catalog.cpp:621` grows a corresponding runtime-registered-entries check.

### Option B (more powerful) — a callback hook

```cpp
class ExtensionCallback {
public:
    // Called by the binder when a function name can't be resolved. Return the
    // extension name to try loading, or empty string to defer to the standard
    // autoload map / fail as usual.
    virtual string OnFunctionResolveFailure(ClientContext &context,
                                            const string &function_name,
                                            CatalogType function_type) { return ""; }
};
```

Extensions register via existing `ExtensionUtil::RegisterExtensionCallback`. Ducklink implements the callback to look up in its runtime-loaded catalog.

## Which we prefer

**Option A.** Smaller API surface, matches the shape of `EXTENSION_FUNCTIONS[]` exactly, no runtime callback overhead. Extensions call it once per name at load, DuckDB's existing autoload flow handles the rest with a single additional lookup in a runtime map.

Option B is more flexible but adds a per-lookup callback call, and the callback semantics need care (what if it's slow? What if it errors? What about re-entrancy?). Option A avoids all of that.

## Security discussion

**Q: Is this a trust-boundary change?**

A: No. Autoload only fires when the extension is already loadable (either signed, or the user has explicitly opted into unsigned via `allow_unsigned_extensions`, or the extension is in `custom_extension_repository`). Registering a function-name mapping doesn't change what extensions are trusted — only the discovery path from function name to which extension to load. All the existing gates (signature check, unsigned-flag, community-repo check) still apply.

**Q: What if two extensions register the same function name?**

A: Two options:
- **Last-write-wins** (simplest): matches the current per-extension situation where a user can `LOAD parquet;` and then get a different resolution than before.
- **First-wins** (safer): registration is one-shot per name.
- **Fail-fast**: the second registrar gets an error.

Deferred to your team's preference — none of these are hard to implement, and the API is additive either way.

**Q: What if a malicious extension registers `read_parquet -> malicious_ext`?**

A: The malicious extension already has to be loaded (via one of the trust paths) to make the registration call. If a user has loaded an extension they don't trust, they have bigger problems than name shadowing. The registration API doesn't create a new attack surface; it uses the existing extension-load trust boundary.

## Implementation sketch

Rough scope: ~50 lines of C++ + tests.

- `src/include/duckdb/main/extension_helper.hpp` — add `RegisterFunctionEntry` declaration + a `std::unordered_map<string, ExtensionAutoloadEntry>` static member (mutex-protected).
- `src/main/extension/extension_helper.cpp` — implement `RegisterFunctionEntry`; extend `FindExtensionInFunctionEntries` to check the runtime map alongside the static array.
- `test/sql/extension/` — sqllogictest fixture that loads a test extension, calls the registration API, then references a function name to verify autoload fires.

No changes to the binder or catalog paths beyond the single additional lookup in `FindExtensionInFunctionEntries`.

## What we're asking

Before we invest in a PR:

1. Is the direction agreeable in principle?
2. Preference on Option A vs Option B?
3. Preference on collision semantics (last-wins / first-wins / fail-fast)?
4. Anything about the `EXTENSION_FUNCTIONS[]` shape you'd want preserved / mirrored in the runtime map?

Happy to prototype in a fork after directional feedback.

## References

- `src/catalog/catalog.cpp:616` — `Catalog::AutoLoadExtensionByCatalogEntry` (the entry point we'd extend)
- `src/main/extension/extension_helper.cpp` — `FindExtensionInFunctionEntries` (the lookup we'd extend)
- `src/include/duckdb/main/extension_entries.hpp:42` — the hardcoded `EXTENSION_FUNCTIONS[]` we'd augment
- Community-Extensions blog on ecosystem gaps: https://duckdb.org/2024/07/05/community-extensions
- Related upstream RFC #23388: trusted custom extension repositories
- `docs/duckdb-upstream-custom-trusted-keys.md` in this repo — the companion upstream ask

---

_Filed by: tegmentum (ducklink authors), 2026-07-09._
