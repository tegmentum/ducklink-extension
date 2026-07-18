# Catalog authoring: enriching `functions[]` for the doc surface

The ducklink extension ships an in-database documentation surface — the
`ducklink.docs` view, the `ducklink_search('query')` table function, and the
`ducklink_help('name')` scalar. Everything they render comes from a single JSON
document: the ducklink catalog. This guide is for catalog authors who want a
module's functions to appear (and rank well) in that surface.

## 1. What the catalog is

The catalog is a JSON document that lists every ducklink module and the
capabilities each one advertises. At load time the extension fetches it live
from `https://ext.ducklink.dev/catalog.json` (overridable with the
`DUCKLINK_CATALOG_URL` environment variable) and falls back to a build-time
snapshot at `assets/catalog-snapshot.json` if the live URL is unreachable.

The Rust-side schema lives in [`src/catalog.rs`](../src/catalog.rs) — see
`CatalogEntry`, `FunctionSig`, `FunctionArg`, `Provider`, and the top-level
`Catalog`. Every field is optional so a partial entry still parses; unknown
fields on the wire are silently ignored. That tolerance is deliberate: the
authoring workflow can move faster than the parser.

The doc surface only reads from a subset of `CatalogEntry`:

- `name` — the module identifier (e.g. `aba`, `creditcard`).
- `functions[]` — the array this guide is about.

Everything else (`content_digest`, `providers[]`, `wit_contract_version`, ...)
concerns loading and dispatch, not documentation.

## 2. The `functions[]` shape

Each element of `functions[]` describes one SQL-callable function the module
registers. The Rust struct (from `src/catalog.rs`) is:

```rust
pub struct FunctionSig {
    pub name: Option<String>,        // the SQL identifier
    pub kind: Option<String>,        // "scalar" | "table" | "aggregate"
    pub returns: Option<String>,     // SQL type; scalar/aggregate only
    pub arguments: Vec<FunctionArg>, // (name, type) pairs
    pub columns: Vec<FunctionArg>,   // table-function result columns
    pub summary: Option<String>,     // one-line synopsis
    pub description: Option<String>, // markdown body
    pub example: Option<String>,     // one canonical SQL snippet
    pub tags: Vec<String>,           // categorisation tokens
}
```

The **minimum** the doc surface requires is a `name`. Without it the row is
dropped (see `build_doc_rows` in `src/reg_duckdb.rs`), so bare exports without a
signature never pollute the docs view. Everything else is optional but strongly
recommended — an entry with only `name` yields a signature-less, summary-less
row that is essentially invisible to search.

The **model enrichment** is the `aba` entry in the bundled snapshot. Here is
its first function, annotated:

```json
{
  "name": "aba_validate",                       // required — the SQL identifier
  "kind": "scalar",                             // "scalar" | "table" | "aggregate"
  "returns": "BOOLEAN",                         // SQL type of the scalar/aggregate result
  "arguments": [
    { "name": "routing_number", "type": "VARCHAR" }
  ],
  "summary": "Validate an ABA routing number by its mod-10 checksum.",
  "description": "The ABA routing number is a 9-digit code identifying US banks. This function verifies the check-digit (positions 1..8 weighted [3,7,1,3,7,1,3,7,1], mod 10 must equal position 9). Returns `TRUE` for a valid number, `FALSE` for a malformed one, and `NULL` for `NULL` input.",
  "example": "SELECT aba_validate('021000021');   -- true (Bank of NY)",
  "tags": ["validator", "banking", "aba", "routing-number", "us"]
}
```

The `sample_extension` entry is the second worked example. It covers all three
function kinds — scalar, table, and aggregate — and shows how `columns[]`
replaces `returns` for table functions:

```json
{
  "name": "sample_emit_sequence",
  "kind": "table",
  "arguments": [ { "name": "limit", "type": "INT64" } ],
  "columns":   [ { "name": "value", "type": "INT64" } ],
  "summary": "Table function emitting 0..limit as a single INT64 column.",
  "description": "Analogous to DuckDB's built-in `range()`. Included so the sample component exercises the ducklink table-dispatch path.",
  "example": "FROM sample_emit_sequence(5);   -- 0, 1, 2, 3, 4",
  "tags": ["sample", "demo", "table-function"]
}
```

Signature rendering (`render_signature` in `src/reg_duckdb.rs`) prints scalars
and aggregates as `name(arg T, ...) -> RETURNS` and table functions as
`name(arg T, ...) TABLE(col T, ...)`. Set `kind` to `"table"` *and* populate
`columns[]` for the table-function form to render.

## 3. Field-by-field guidance

### `summary`

- **What it is.** A one-line synopsis, the elevator pitch for the function.
- **Style.** Imperative sentence ending in a period. Under ~100 characters.
  Plain text — this cell renders in tabular contexts (`ducklink.docs.summary`),
  not markdown.
- **Where it shows.** `ducklink.docs.summary` column; the header line under
  `## <signature>` in `ducklink_help('name')` output; scored **3×** by
  `ducklink_search`.
- **Avoid.** Don't restate the function name (`"aba_validate validates ABA"`),
  don't wrap it in quotes, don't leave a trailing "This function does X…"
  dangling into a paragraph. If you find yourself needing two sentences, the
  second one belongs in `description`.

### `description`

- **What it is.** The full-body explanation of behaviour, edge cases, and
  semantics.
- **Style.** Declarative markdown. Multi-paragraph is fine but rare;
  bullet lists for edge cases (`NULL` handling, error surfaces) work well.
  Backtick-fence SQL keywords and identifiers.
- **Where it shows.** `ducklink.docs.description` column; the body paragraph in
  `ducklink_help('name')` output (renders wherever the caller pipes markdown);
  scored **1×** by `ducklink_search`.
- **Avoid.** Don't repeat the summary verbatim, don't cite the SQL type again
  (the signature already prints it), don't include the canonical example — that
  goes in `example` so `ducklink_help` can render it under its own heading.

### `example`

- **What it is.** One self-contained SQL snippet — the shortest thing that
  demonstrates the function.
- **Style.** SQL-only, one or two statements. Include an inline `--` comment
  showing the expected result (`SELECT aba_validate('021000021'); -- true`).
  Do not include `LOAD ducklink;` / `LOAD WASM 'aba';` boilerplate; the
  surrounding help output already implies the module has been loaded. Prefer
  literals over `?` parameters or `:name` binds. Keep it under one screen.
- **Where it shows.** Rendered under a `### Example` heading in
  `ducklink_help('name')`; also visible as the raw string in
  `ducklink.docs.example`.
- **Avoid.** Don't cram multiple demos into one field — pick the canonical one.
  Don't reference tables the reader can't reproduce (`FROM production.txns`);
  use `range()` or an inline `VALUES` clause if you need data.

### `tags`

- **What they are.** Categorisation tokens — the primary hook for discovery via
  `ducklink_search`.
- **Style.** Lower-case, kebab-cased (`routing-number`, not
  `Routing_Number`). Three to seven tags per function is a reasonable range.
  Mix domain tags (`banking`, `finance`), functional tags (`validator`,
  `codec`, `hash`), and regional/standard tags (`us`, `iso-6166`).
- **Where they show.** Joined comma-separated into `ducklink.docs.tags`
  (`LIKE`-friendly for `WHERE tags LIKE '%banking%'`); scored **5×** by
  `ducklink_search` — second only to a name match.
- **Avoid.** Don't repeat the function name as a tag (it already scores 10×).
  Don't use whitespace (`"credit card"` — write `credit-card`). Don't invent a
  taxonomy for one function; look at neighbouring modules first and reuse
  existing tags where you can.

## 3a. Native providers (`providers[]` with `kind: "native"`)

The catalog can advertise a native `.duckdb_extension` alongside — or instead
of — a WASM component. When an entry carries a native provider, `LOAD NATIVE
'name'` resolves it directly to a platform-specific `.duckdb_extension` file
and hands the cached path to DuckDB's own `LOAD`. There is no wasmtime host on
that path; the extension is native code linked against DuckDB's C Extension
API.

Native providers live in the same `providers[]` array as WASM providers.
The Rust struct for one is (see `src/catalog.rs`):

```rust
pub struct Provider {
    pub id: Option<String>,               // free-form identifier
    pub kind: Option<String>,             // "native"
    pub content_digest: Option<String>,   // sha256 of the .duckdb_extension bytes
    pub platform: Option<String>,         // DuckDB's convention: "osx_arm64", "linux_amd64", ...
    pub duckdb_version: Option<String>,   // exact DuckDB version, e.g. "v1.5.4"
    pub url: Option<String>,              // optional download URL override
    pub status: Option<String>,           // "supported" | "deprecated" | "eol"
    // (abi is ignored for native providers)
}
```

### Required fields, and the strict match rule

`LOAD NATIVE 'name'` resolves a native provider by an **exact** match on both
`platform` **and** `duckdb_version`. Native `.duckdb_extension` binaries are
tightly coupled to a specific host — a `v1.5.4`-built extension will not load
into `v1.5.3` — so the resolver refuses anything but an exact fit. Concretely,
a native provider is only selected when:

- `kind` is exactly `"native"`, and
- `platform` matches the host build's `NATIVE_PLATFORM` string (DuckDB's
  convention, e.g. `"osx_arm64"`, `"osx_amd64"`, `"linux_amd64"`,
  `"linux_arm64"`, `"linux_amd64_musl"`, `"windows_amd64"`), and
- `duckdb_version` matches the exact DuckDB version DuckDB reports (leading
  `v`, e.g. `"v1.5.4"`), and
- `content_digest` is a lowercase-hex sha256 of the `.duckdb_extension` bytes.

If no provider matches, the resolver emits a clear error naming the requested
`platform/duckdb_version` and (if any native providers exist at all) listing
the available ones — a strong hint to fall back to `ducklink_load('name')` for
the WASM version.

### `url` — optional download override

If `url` is present, it is the URL the loader `GET`s to fetch the blob.
Otherwise the loader constructs one from `NATIVE_BLOB_BASE`:

```
https://ext.ducklink.dev/native/sha256/<content_digest>/<platform>/<name>.duckdb_extension
```

Either way, the downloaded bytes' sha256 must match `content_digest` — a
mismatch is a hard error; the corrupt blob is never cached. The verified blob
is stored digest-keyed at
`$XDG_CACHE_HOME/ducklink/native/sha256/<digest>/<name>.duckdb_extension`, so
two providers that ship the same content share one cache entry.

### Example: a native-only entry

`ducklink_native` is the bundled reference — the curated bundle of
perf-sensitive scalars (ABA, IBAN, ISBN, Luhn, credit-card) compiled
directly against DuckDB's C Extension API. Its entry carries a single native
provider and no WASM one:

```json
{
  "name": "ducklink_native",
  "description": "Curated native DuckDB extension bundle: perf-sensitive scalars compiled directly against duckdb-rs.",
  "categories": ["curated"],
  "exports": ["aba_validate", "iban_validate", "cc_validate", "..."],
  "requires": ["scalar"],
  "providers": [
    {
      "id": "native-osx-arm64-v1.5.4",
      "kind": "native",
      "platform": "osx_arm64",
      "duckdb_version": "v1.5.4",
      "content_digest": "0a02e570f7a8b538b88a2e437c66bc190d7f80474b21902e3e6abfbc677f5565",
      "status": "supported"
    }
  ]
}
```

`LOAD NATIVE 'ducklink_native'` on an `osx_arm64` DuckDB `v1.5.4` build picks
this provider, downloads (or cache-hits) the `.duckdb_extension`, verifies its
sha256, and hands the path to DuckDB. On any other platform or DuckDB version
the resolver refuses with the mismatch message.

### Example: an entry with both WASM and native providers

An entry can advertise both a WASM component AND per-platform natives. The
two paths coexist and are chosen by which loader the user calls:
`ducklink_load('name')` selects a WASM provider (strict same-major on
`abi`); `LOAD NATIVE 'name'` selects a native provider (strict-exact on
`platform` + `duckdb_version`). A future `aba` entry could look like:

```json
{
  "name": "aba",
  "description": "ABA routing-number (US) checksum validation.",
  "categories": ["validators"],
  "exports": ["aba_validate"],
  "requires": ["scalar"],
  "wit_contract_version": "4.0.0",
  "content_digest": "068b47e3ea5df366637eb3726e7efaa6bfb4ddd00564bf75c821956572c76a15",
  "providers": [
    {
      "id": "wasm-component",
      "kind": "wasm",
      "abi": "duckdb:extension@4.0.0",
      "content_digest": "068b47e3ea5df366637eb3726e7efaa6bfb4ddd00564bf75c821956572c76a15",
      "status": "supported"
    },
    {
      "id": "native-osx-arm64-v1.5.4",
      "kind": "native",
      "platform": "osx_arm64",
      "duckdb_version": "v1.5.4",
      "content_digest": "cafef00dcafef00dcafef00dcafef00dcafef00dcafef00dcafef00dcafef00d",
      "status": "supported"
    },
    {
      "id": "native-linux-amd64-v1.5.4",
      "kind": "native",
      "platform": "linux_amd64",
      "duckdb_version": "v1.5.4",
      "content_digest": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
      "status": "supported"
    }
  ]
}
```

WASM and native providers are independent: adding a native for one platform
does not require adding it for the others, and a missing native for the
host's platform + version simply routes the user to the WASM path.

## 3b. Community-native providers (`kind: "community-native"`)

Sometimes the capability already exists as a published `duckdb/community-extensions`
extension. Rather than rebundle it, a catalog entry can advertise a **community-native
provider** — a pointer at the community-published extension. Ducklink then dispatches
`INSTALL <extension_name> FROM community; LOAD <extension_name>;` on the user's
behalf when they ask for the native path, and (optionally) generates SQL macro
aliases so the community extension's functions are callable under ducklink's
chosen names.

Provider shape (minimal):

```json
{
  "id": "cn-official",
  "kind": "community-native",
  "extension_name": "shellfs"
}
```

Fields:

- `kind: "community-native"` (required).
- `extension_name` (required) — the **exact** name registered in
  `duckdb/community-extensions`. Must match `[A-Za-z0-9_]+` (identifier-shape).
  Ducklink refuses to run INSTALL / LOAD if the name contains anything else,
  so a bad catalog entry can't inject SQL.
- `community_prefix` (optional) — a systematic prefix that community's
  extension puts on every exported function name. Ducklink strips this
  prefix when generating aliases, so `t_sma` becomes `sma`.
- `function_mapping` (optional) — an object `{"our_name": "their_name"}`
  giving explicit per-function renames. Overrides the prefix when both are
  set — an author who wants "prefix for most, override a handful" can list
  the overrides here.

No `content_digest`, `platform`, `duckdb_version`, or `url` — DuckDB's
`INSTALL … FROM community` machinery handles all of that (per-platform
resolution, per-DuckDB-version resolution, signature verification against
the community key).

### Aliasing model: ducklink owns the SQL surface

Ducklink is the routing layer. The community extension is what actually
executes. Users write SQL against **ducklink's chosen names**; ducklink
translates those names to community's names via `CREATE OR REPLACE MACRO`
at load time. The macro is inlined at query planning, so there's no
per-row overhead — the community function runs exactly as it would if
called directly.

Community's own names remain callable too. Both point at the same
implementation, so no signal is lost. A user who happens to know community
uses `t_sma` can still call it directly; a user reading ducklink's docs
sees `sma` and calls that.

### How ducklink registers the alias, per build

After `LOAD <extension_name>` succeeds, ducklink discovers community's
functions via `duckdb_functions()` and registers ducklink's chosen name
per pair. The exact mechanism depends on which ducklink build is in use:

* **Advanced-tier build (default in our distribution)** — the C++ shim
  in `cpp/ducklink_alias.cpp` copies community's
  `AggregateFunctionCatalogEntry` / `ScalarFunctionCatalogEntry` /
  `TableFunctionCatalogEntry` into a fresh CatalogEntry under ducklink's
  name. DuckDB's binder sees a real entry at the alias, so aggregate
  `DISTINCT`, `FILTER (WHERE …)`, `ORDER BY`, and window-context
  (`OVER (…)`) all work transparently through the alias. Zero per-row
  overhead — same code path as calling community's original name.

* **Loadable-only build (the community-extensions CI ships this form —
  `advanced` is stripped there)** — the shim isn't compiled in, so
  ducklink falls back to `CREATE OR REPLACE MACRO`:

  | Community kind | Macro emitted | Overhead |
  |---|---|---|
  | `scalar` | `CREATE OR REPLACE MACRO our(args) AS their(args);` | Inlined at plan time — zero |
  | `table` / `table_macro` | `CREATE OR REPLACE MACRO our(args) AS TABLE SELECT * FROM their(args);` | Inlined at plan time — zero |
  | `aggregate` (single-arg only) | `CREATE OR REPLACE MACRO our(x) AS list_aggregate(list(x), 'their');` | One `list()` build per group + `list_aggregate` dispatch |

  In this build only, aggregate aliases do NOT propagate `DISTINCT` /
  `FILTER` / `ORDER BY` / window modifiers, and multi-argument
  aggregates are skipped. Community's original names remain callable —
  a user who needs those modifiers writes `SELECT their_name(x)`.

Same catalog authoring works for both builds — the aliasing story is
build-transparent; only the mechanism differs.

### Function-name parity is a soft preference

Where you CAN keep names identical between ducklink's WASM version and
the community-native provider, do — a query then works unchanged when
users switch between `WASM` and `NATIVE`. But it's no longer a hard
requirement: if community's naming diverges (e.g. they use a `t_` prefix
you don't want), set `community_prefix` and let the aliasing layer bridge
the gap.

### Selection order when `kind: "native"` is requested

`DUCKLINK LOAD 'name' NATIVE` and `FROM ducklink_load('name', kind => 'native')`
consult providers in this order:

1. **community-native** if the entry has one — best trust posture (signed
   by the community key; no `-unsigned` needed); best perf (real native).
2. **ducklink-native** matching this host's platform + DuckDB version —
   our own build, requires `-unsigned` because our signing key isn't in
   DuckDB's trust chain today.
3. **Error** — no native backing available. The user should either use
   the WASM path (`DUCKLINK LOAD 'name' WASM`, the default) or wait for
   a native provider to land.

### Example: names match — no aliasing needed

```json
{
  "name": "shellfs",
  "version": "0.1.0",
  "description": "Read files via a shell one-liner from SQL",
  "exports": ["shellfs_read", "shellfs_glob"],
  "providers": [
    {
      "id": "wasm-primary",
      "kind": "wasm",
      "abi": "duckdb:extension@4.0.0",
      "content_digest": "aa1122...",
      "status": "supported"
    },
    {
      "id": "cn-shellfs",
      "kind": "community-native",
      "extension_name": "shellfs"
    }
  ]
}
```

Behaviour:

- `DUCKLINK LOAD 'shellfs';` → WASM (the safe default).
- `DUCKLINK LOAD 'shellfs' WASM;` → WASM (explicit).
- `DUCKLINK LOAD 'shellfs' NATIVE;` → routes to `INSTALL shellfs FROM community; LOAD shellfs;`. No aliases created because names already match; the user's `SELECT shellfs_read(...)` calls work either way.

### Example: community uses a prefix — strip it via `community_prefix`

Community's tapa-technical-analysis extension registers everything as
`t_sma`, `t_ema`, `t_rsi`, etc. Ducklink advertises `sma`, `ema`, `rsi`:

```json
{
  "name": "ta",
  "version": "0.1.0",
  "description": "Technical-analysis indicators for time-series data",
  "exports": ["sma", "ema", "rsi", "macd"],
  "providers": [
    {
      "id": "cn-tapa",
      "kind": "community-native",
      "extension_name": "tapa",
      "community_prefix": "t_"
    }
  ]
}
```

`DUCKLINK LOAD 'ta' NATIVE;` INSTALLs + LOADs `tapa`, then generates
`CREATE OR REPLACE MACRO sma(x) AS t_sma(x)`, etc. — one per exported
function that matches the prefix. Both `SELECT sma(price)` and
`SELECT t_sma(price)` work.

### Example: explicit `function_mapping` for renames

When names don't fit a clean prefix, spell them out:

```json
{
  "name": "geo",
  "version": "0.1.0",
  "description": "Spatial primitives via community's spatial extension",
  "exports": ["distance", "buffer"],
  "providers": [
    {
      "id": "cn-spatial",
      "kind": "community-native",
      "extension_name": "spatial",
      "function_mapping": {
        "distance": "st_distance",
        "buffer":   "st_buffer"
      }
    }
  ]
}
```

`function_mapping` and `community_prefix` can appear together: the
explicit map wins for anything it names, and the prefix fills in the
rest.

### When to use each provider kind

| Kind | Ships in ducklink infra? | Trust source | Best when |
|---|---|---|---|
| `wasm` | Yes (WASM blob on our CDN) | Sandboxed; no signing setup | Default; always safe; portable |
| `native` | Yes (`.duckdb_extension` on our CDN) | Our signing key (needs `-unsigned` today) | User has explicitly opted into unsigned + perf-critical hot path + no community equivalent |
| `community-native` | No (we just point at community-extensions) | Community-extensions signing key | An equivalent extension already exists there — never re-ship it |

## 3c. Namespaces and prefixes

Entries can declare a canonical **namespace** so their functions are
callable both under DuckDB's `main` schema (bare) and under a
schema-qualified form (`<namespace>.<fn>`). Users can layer session
aliases on top with `ducklink_prefix('<alias>', '<namespace>')`.
Every call shape binds the exact same underlying function set, so the
common aggregate modifiers (`DISTINCT`, `FILTER`, `GROUP BY`)
propagate through every one of them.

### Catalog fields

Add two optional fields to a catalog entry (both alongside the
existing `providers[]`, not inside a provider):

```json
{
  "name": "crypto",
  "namespace": "crypto",
  "prefix": "c",
  "providers": [
    {
      "kind": "community-native",
      "extension_name": "crypto",
      "function_mapping": {
        "hash": "crypto_hash",
        "hash_agg": "crypto_hash_agg"
      }
    }
  ]
}
```

- **`namespace`** (optional) — canonical schema name. When set, every
  function ducklink aliases is DOUBLE-registered: once in `main`
  (backcompat: bare `hash(x)` and community's original bare
  `crypto_hash(x)` both keep working), and once in a schema of this
  name (`crypto.hash(x)` becomes bindable). Absent → today's
  behaviour, `main` only.
- **`prefix`** (optional) — a short-alias hint for tools. Ducklink
  does NOT auto-declare it; it's a discovery signal so an IDE or
  `ducklink.modules` view can suggest
  `FROM ducklink_prefix('c', 'crypto')` to users.

### Load-time behaviour

`DUCKLINK LOAD 'crypto' NATIVE` (or `ducklink_load('crypto', kind =>
'native')`) INSTALLs the community extension, LOADs it, then:

- **Without a `namespace`**: registers `hash` / `hash_agg` in `main`
  (existing behaviour).
- **With a `namespace`**: registers each function TWICE — in `main`
  AND in the declared namespace schema — sharing the same underlying
  `AggregateFunctionSet` / `ScalarFunctionSet` / `TableFunctionSet`.
  DuckDB's binder sees a real catalog entry at each qualifier, so
  aggregate modifiers propagate transparently through both forms.
  The schema is created on-demand — no manual `CREATE SCHEMA`.

Two loaded modules that declare the same `namespace` **merge** into
that schema (same rule DuckDB already uses for extensions all sharing
`main`). Functions with overlapping signatures across the merged
modules will error at registration with a clear "already exists"
message.

### User-side session aliases: `ducklink_prefix()`

Users add their own short aliases with a table-function call:

```sql
FROM ducklink_prefix('c', 'crypto');
```

That creates an alias schema `c` and re-registers every function in
schema `crypto` under `c.<fn>`. Both alias and namespace forms bind
the same catalog entry.

Persistence: the declaration is stored in a `ducklink.prefixes(alias,
namespace)` table inside the user's default catalog. On file-backed
databases, `DUCKLINK LOAD 'crypto' NATIVE` after a reconnect
automatically replays the persisted alias (no need to redeclare). On
:memory: databases, prefixes die with the connection — the user's
mental model matches DuckDB's own :memory: semantics.

Redeclaring the same alias is idempotent (`INSERT OR REPLACE` on the
persisted row, `CREATE OR REPLACE` on the alias schema entries).
`alias` and `namespace` are both required to match `[A-Za-z0-9_]+`;
anything else errors cleanly.

### The four call shapes

For an entry with `namespace: "crypto"` and after
`FROM ducklink_prefix('c', 'crypto');`, ALL FOUR of these are
equivalent — same underlying function set, same modifier support,
same result:

```sql
SELECT crypto_hash(x)     FROM t;   -- 1. community's original bare name (main)
SELECT hash(x)            FROM t;   -- 2. ducklink's alias bare name (main)
SELECT crypto.hash(x)     FROM t;   -- 3. namespace-qualified
SELECT c.hash(x)          FROM t;   -- 4. alias-qualified
```

Bare short names (`hash(x)`) do NOT auto-resolve if their function
lives only in a non-main schema — users opt in via `SET search_path
= 'main,crypto'` themselves. Auto-prepending would silently change
resolution for existing bare-name queries in the session, which is
worse than requiring an explicit opt-in.

### Complete worked example

```json
{
  "name": "crypto",
  "description": "Cryptographic hashes and HMAC via query-farm/crypto",
  "exports": ["hash", "hmac", "hash_agg"],
  "namespace": "crypto",
  "prefix": "c",
  "providers": [
    {
      "id": "cn-crypto",
      "kind": "community-native",
      "extension_name": "crypto",
      "function_mapping": {
        "hash":     "crypto_hash",
        "hmac":     "crypto_hmac",
        "hash_agg": "crypto_hash_agg"
      }
    }
  ]
}
```

After `DUCKLINK LOAD 'crypto' NATIVE`, users have `crypto_hash(x)`
(community's own), `hash(x)` (ducklink's alias), and `crypto.hash(x)`
(namespace-qualified) all working. Any user can layer
`FROM ducklink_prefix('c', 'crypto');` to add `c.hash(x)`, persisted
for their next reconnect. The `prefix: "c"` hint tells tools which
alias to suggest; if two users pick different aliases
(`ducklink_prefix('crypt', 'crypto')`) that works fine too — the hint
is advisory.

## 4. How `ducklink_search` ranking works

`ducklink_search('query')` splits the query on whitespace, lower-cases each
token, and computes a per-row score as the sum of case-insensitive substring
hits, weighted by field:

| Field         | Weight |
|---------------|-------:|
| `name`        |   10×  |
| `tags`        |    5×  |
| `summary`     |    3×  |
| `description` |    1×  |

(See `score_doc` in `src/reg_duckdb.rs`.) Rows with score `0` are dropped;
survivors are sorted by score descending, then by module and function name for
a stable ordering.

Practical implications for authors:

- The **best lever** for discoverability is a well-chosen function name and
  well-chosen tags. A single tag substring hit outweighs three separate
  description hits.
- A **precise summary** helps a search like `SELECT * FROM
  ducklink_search('check digit')` land on the right function even when neither
  word appears in the name.
- **Long descriptions are cheap** for discovery but do not carry the day on
  their own. Prioritise summary and tag quality before writing prose.

## 5. Testing your enrichment locally

The doc surface reads exclusively from the resolved catalog. To iterate on an
enrichment without touching the live endpoint:

1. **Edit** `assets/catalog-snapshot.json`. Find the entry by `"name": "<module>"`
   and add or update its `functions[]` array with the fields described above.

2. **Force the bundled snapshot fallback** by pointing the live URL at
   something unreachable:

   ```bash
   export DUCKLINK_CATALOG_URL=https://unreachable.invalid/x
   ```

   With this set, `catalog.rs::resolve_catalog` skips the live fetch and loads
   the snapshot compiled into the binary.

3. **Rebuild** the release binary:

   ```bash
   make release
   ```

   Note: `assets/catalog-snapshot.json` is embedded via `include_bytes!`, and
   Cargo does not track that file as a rebuild dependency. If your edit does
   not seem to be picked up, touch the source that owns the macro so Cargo
   re-runs the build:

   ```bash
   touch src/catalog.rs && cargo build --release
   ```

4. **Query the doc surface** from DuckDB to preview:

   ```sql
   LOAD ducklink;

   -- Full markdown help for a function or a whole module.
   SELECT ducklink_help('aba_validate');
   SELECT ducklink_help('aba');

   -- Ranked search.
   SELECT * FROM ducklink_search('routing number');

   -- Browse the raw docs table.
   SELECT module, function, summary, tags
   FROM ducklink.docs
   WHERE module = 'aba';
   ```

   If a function you added does not appear in `ducklink.docs`, check that its
   `name` is non-empty — `build_doc_rows` skips rows without a name.

## 6. Style guide

A compact conventions list, extracted from the sections above:

- **Summary.** One line, imperative, ends in a period. `Validate X.`, `Extract
  Y.`, `Compute Z.` Under ~100 characters.
- **Description.** Declarative markdown. State semantics, `NULL` handling, and
  error behaviour. Backtick-fence identifiers and SQL keywords. Do not repeat
  the summary or restage the example.
- **Example.** One self-contained SQL snippet, one or two statements, with an
  inline `--` comment showing the expected result. No `LOAD` boilerplate. No
  external tables — use literals, `VALUES`, or `range()`.
- **Tags.** Lower-case, kebab-cased tokens. Three to seven per function. Mix
  domain, functional, and standards/regional axes. Reuse tags neighbouring
  modules already use.
- **Consistency across a module.** All functions in a module should share a
  core set of tags (`aba`, `banking` for every `aba_*` function), then add
  function-specific tags on top.
- **Naming.** The doc surface treats `name` case-insensitively for lookups but
  renders it verbatim. Match the exact identifier the module registers with
  DuckDB.

If in doubt, re-read the `aba` and `sample_extension` entries in
`assets/catalog-snapshot.json`. They are the working reference for every
convention in this guide.
