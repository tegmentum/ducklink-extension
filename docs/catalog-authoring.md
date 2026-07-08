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
