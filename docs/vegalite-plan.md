# `ducklink_vegalite`: chart-spec emission plan

**Status:** design. Nothing is built. This plan proposes a scalar (and
companion table) function that returns a Vega-Lite JSON specification
so callers upstream (notebooks, chart-rendering tools, dashboard
builders) can render however they choose.

**Position it implements:** `docs/extension-scope.md` — extensions
compose IN the SQL grammar, they don't replace it. And
`docs/visualize-design.md` — the seam belongs at Vega-Lite JSON, not
at a `VISUALIZE` keyword or an in-process HTTP server.

Related design docs:
- `docs/extension-scope.md` — the general "an extension is a set of
  named catalog entries" principle. This plan is the first
  spec-emitting scalar we build against it.
- `docs/catalog-authoring.md` — the multi-provider catalog schema.
  `vegalite` will advertise both a `wasm` and a `ducklink-native`
  provider.
- `docs/dual-build-native-and-wasm.md` — the WASM/native split
  rationale.

---

## 1. Where the code lives

**The implementations do NOT live in this repository.** Ducklink is
the loader and the catalog. Feature-specific implementations we ship
belong in their own repos:

- `ducklink-vegalite-wasm` — a `duckdb:extension@4.0.0` WebAssembly
  component (Rust, wit-bindgen). Ducklink loads it through the
  standard `ducklink_load('vegalite')` path with `kind => 'wasm'`
  (the default).
- `ducklink-vegalite-native` — a native `.duckdb_extension` built
  from the same core library. Advertised as a `ducklink-native`
  provider on the same catalog entry; users opt in with
  `ducklink_load('vegalite', kind => 'native')`.

Both binaries wrap the same Rust core (`ducklink-vegalite-core`
crate). One JSON-spec builder, two shipping targets.

Why this shape:

- **Ducklink stays thin.** This repo's job is loader + host + catalog.
  Shipping a growing set of feature-specific implementations from
  inside this crate turns it into a monorepo of unrelated tools;
  keeping each in its own crate scales cleanly.
- **Native ownership is honest.** A `ducklink-native` provider is a
  DuckDB extension signed by us, distinct from `community-native`
  (dispatched via `INSTALL FROM community`). Users who see
  `kind: "ducklink-native"` in the catalog should be able to click
  through to a repo we own and audit the code that will run in their
  process. Bundling it inside the loader hides that lineage.
- **Independent versioning.** The Vega-Lite spec grammar evolves;
  the loader ABI evolves; the two shouldn't share a release cycle.

Ducklink's contribution to this feature is exactly one addition to
`assets/catalog-snapshot.json` (an entry with the two providers) and
the entries in `ducklink.docs`.

## 2. Catalog entry

```json
{
  "name": "vegalite",
  "version": "0.1.0",
  "description": "Emit a Vega-Lite JSON specification from a DuckDB result set. Renderer-agnostic; the returned VARCHAR is a valid Vega-Lite v5 spec that vega-embed / altair / vegawidget / notebook cells render directly.",
  "exports": ["ducklink_vegalite"],
  "requires": [],
  "providers": [
    {
      "id": "wasm-primary",
      "kind": "wasm",
      "abi": "duckdb:extension@4.0.0",
      "artifact": "artifacts/extensions/vegalite.wasm",
      "content_digest": "...",
      "status": "supported"
    },
    {
      "id": "ducklink-native",
      "kind": "ducklink-native",
      "extension_name": "vegalite",
      "platforms": ["osx_arm64", "osx_amd64", "linux_amd64", "linux_arm64", "windows_amd64"]
    }
  ]
}
```

`requires: []` — pure scalar surface, no parser / optimizer / stream
capabilities. Loads on any ducklink host without divergence.

## 3. The scalar surface

Primary shape:

```sql
ducklink_vegalite(
    rowset,               -- LIST<STRUCT<...>>  -- the data payload
    mark   := 'bar',      -- VARCHAR             -- 'bar'|'line'|'point'|'area'|'tick'|'rect'|'circle'|'square'|'rule'
    x      := NULL,       -- VARCHAR             -- x-axis field name  (must appear in each struct)
    y      := NULL,       -- VARCHAR             -- y-axis field name
    color  := NULL,       -- VARCHAR             -- optional color-encoding field
    size   := NULL,       -- VARCHAR             -- optional size-encoding field
    tooltip := NULL,      -- LIST<VARCHAR>       -- optional tooltip field list
    title  := NULL,       -- VARCHAR             -- optional chart title
    width  := 400,        -- INTEGER             -- pixel width  (Vega-Lite default: 200 responsive)
    height := 300         -- INTEGER             -- pixel height
) -> VARCHAR
```

Named arguments only for everything past `rowset`. Positional would
turn every call into a nine-argument line — hard to read, hard to
diff. Named makes `mark => 'bar', x => 'month', y => 'sales'` scan
like a chart definition.

The `rowset` input is `LIST<STRUCT<...>>` (a "row-of-structs" list
that DuckDB constructs via `list({...})`). This keeps the boundary
narrow: one argument carries both the schema and the data, and DuckDB
already has a natural literal form for it.

Return: a VARCHAR containing a valid Vega-Lite v5 JSON spec, with the
data payload inlined as `spec.data.values`.

### Worked example

```sql
WITH sales AS (
  SELECT month, sum(amount) AS total
  FROM orders
  WHERE order_date >= DATE '2026-01-01'
  GROUP BY month
  ORDER BY month
)
SELECT ducklink_vegalite(
    (SELECT list({month: month, total: total}) FROM sales),
    mark  := 'bar',
    x     := 'month',
    y     := 'total',
    title := 'Sales by month, 2026 YTD'
) AS spec;
```

`spec` renders in any Jupyter cell that ships the `application/
vnd.vegalite.v5+json` mimetype handler (altair, notebook, JupyterLab
with vega extension). A dashboard tool `POST`s it to vega-embed.
A CLI wrapper writes it to a file and pipes it into `vl-convert` for
a PNG.

## 4. Companion table function

The scalar returns ONE spec. Small multiples, dashboards, and A/B
comparisons need MANY. Companion:

```sql
FROM ducklink_vegalite_grid(
    facets,               -- LIST<STRUCT<name VARCHAR, rowset LIST<STRUCT<...>>>>
    mark := 'bar',
    x    := 'month',
    y    := 'total',
    -- ... rest as the scalar
);
-- returns rows: (name VARCHAR, spec VARCHAR)
```

One row per facet, each `spec` a standalone Vega-Lite spec. Callers
join it against a dashboard-config table, feed it into a `COPY ... TO
'charts/{name}.json'` (once the syntax settles), or paginate it in a
notebook.

Not urgent; punt to a second cut once the scalar is real.

## 5. Chart-spec model

Design goals for the spec builder:

1. **Emit legal Vega-Lite v5.** Every returned string must
   round-trip through `vega-lite`'s schema without validation
   errors. Reject the input at bind time when the requested encoding
   can't be honoured (missing field, unknown mark). No "we did our
   best" partially-valid specs.
2. **Small footprint.** The scalar builds spec objects declaratively;
   no template engine, no ad-hoc string concatenation, no JSON
   escaping bugs. Serialize via a struct → JSON path (`serde_json`
   in the native crate; a WIT-idiomatic equivalent in wasm).
3. **Schema stable across ducklink versions.** The chart-spec model
   is versioned. `SET vegalite_spec_version = 5;` (default 5).
   Adding new marks / encodings never changes existing output. If we
   ever need to break backward compat we'll bump the version and
   keep the old one bindable.
4. **Data-inlining only.** No support for `data.url` (which would
   ask the renderer to fetch a URL from unknown context) or `data.
   name` with a lookup table (opaque state). Every returned spec
   carries its data literally so a reader can see exactly what will
   render.

Non-goals:

- Chart TYPES beyond the base marks. Vega-Lite supports transforms,
  layers, concat, repeat, facet, hconcat, and much more; the initial
  surface targets one-mark-one-encoding charts. Callers who need
  layered / faceted / transformed charts can hand-write specs and
  DuckDB will happily return them as VARCHAR literals.
- Vega (the lower-level grammar). Vega-Lite compiles to Vega; users
  who need Vega-level control write it themselves.

## 6. Argument validation

At scalar bind time:

- `mark` must be one of the whitelisted mark names. Unknown → error
  before we do anything expensive.
- `x`, `y`, `color`, `size`, `tooltip[i]` if set must be names that
  appear in the struct schema of `rowset[0]`. DuckDB has already
  computed the schema at bind; we walk it and error on unknowns.
  This is where we get "column not found" errors from ducklink
  rather than opaque JSON output that vega-lite rejects at render
  time.
- `width`, `height` must be positive integers under some sane
  ceiling (say 32768, matching typical browser texture limits).

Everything else — encoding channel types (nominal / ordinal /
quantitative / temporal) — inferred from the struct-field DuckDB
logical types (VARCHAR → nominal by default, DATE/TIMESTAMP →
temporal, numeric → quantitative). Callers who need finer control
can hand-write the spec or pass an `encoding_types` override map
(v0.2).

## 7. Testing

Reference oracle: the [Vega-Lite JSON Schema](https://vega.github.io/schema/vega-lite/v5.json).
Every fixture we emit must validate against it. Integration tests do
that validation in-process (either against a bundled copy of the
schema or via `jsonschema` in the test harness).

Round-trip fixtures: a corpus of (input rowset + args, expected
spec) golden files. Native and wasm builds must emit
byte-identical output for the same input — one Rust core, two
shipping targets.

## 8. Rollout

**Milestone 1 — scalar, native only.** The `ducklink-vegalite-core`
crate lands with the six or seven common marks and the primary
encodings. `ducklink-vegalite-native` builds a `.duckdb_extension`
per platform. Catalog entry advertises `ducklink-native` only.
Users load via `ducklink_load('vegalite', kind => 'native')`
(requires `-unsigned` today — see `docs/duckdb-upstream-custom-trusted-keys.md`).

**Milestone 2 — wasm.** Same Rust core exposed through the
`duckdb:extension@4.0.0` WIT bindings. Catalog entry adds the
`wasm-primary` provider. `ducklink_load('vegalite')` (default kind)
now serves the wasm build; the native path remains available.

**Milestone 3 — table function.** `ducklink_vegalite_grid`,
per §4.

**Milestone 4 — encoding-type override map + a few more marks.**
Nominal-vs-ordinal control, log/time scale hints, small-multiples
via `column`/`row` encoding.

## 9. Out of scope permanently

- **A `VISUALIZE` keyword or any other SQL syntax extension.** See
  `docs/extension-scope.md` and `docs/visualize-design.md`.
- **Rendering.** The scalar returns spec text. Nothing in ducklink
  ever opens a socket, spawns a subprocess, or writes to a display
  to render a chart. Callers render.
- **Reading from `data.url` at spec-emit time.** Any URL fetching is
  the renderer's concern. Emitted specs use inlined values.
- **A DuckDB-side vega-lite compiler.** We emit vega-lite. We do
  not compile it to vega and we do not render vega directly. Both
  jobs have existing tools with an ecosystem attached.
