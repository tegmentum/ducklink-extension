# Visualization: the ducklink take

## Position

Ducklink will NOT ship a `VISUALIZE` SQL statement, and will NOT render
charts directly. We will ship a scalar (or table) function that returns
a **Vega-Lite JSON spec** as text; anything upstream — a notebook, a
web UI, a CLI wrapper — is responsible for rendering.

This is a deliberate split from the community `ggsql` extension, which
does both things ducklink deliberately avoids:

1. **Parses `VISUALIZE <select>` as a bespoke statement.** That's a
   `ParserExtension` grab on an English keyword. It relies on DuckDB's
   internal C++ ABI (which is why ducklink dropped that whole tier
   for portability), and it establishes a namespace precedent — any
   extension can now claim any keyword, and load order decides what
   the query means. See `docs/community-native-audit.md` for the
   session-state failure mode.
2. **Directly renders vega-lite in an in-process HTTP server + browser
   tab.** That's a side effect the SQL user didn't ask for — plan
   viewers, migration tools, audit logs, and row-level policies all
   see the rewritten SQL, not the browser tab. And whether a SQL
   statement opens a browser is not a decision an extension should be
   making for the host process.

Both choices conflate three concerns that should stay separate:
data shaping (SQL), chart specification (a declarative artifact), and
rendering (a specific runtime with side effects). Community `ggsql`
collapses all three into one keyword and one browser process. That's
too much.

## The ducklink shape

```sql
SELECT ducklink_vegalite(
    -- some query result (or a name of a table we produced)
    (SELECT list({month: month, sales: sales}) FROM sales_by_month),
    mark => 'bar',
    x    => 'month',
    y    => 'sales'
) AS spec;
```

`spec` is a VARCHAR containing a valid Vega-Lite JSON specification.
The caller does whatever it wants with it: paste it into
`vega-lite.github.io/editor/`, feed it to a Jupyter magic that renders
via `altair` / `vega-embed`, POST it at their internal Vega renderer,
save it to a file. Ducklink does not care. Ducklink does not open
sockets. Ducklink does not spawn a browser.

Table-function variant if we need to emit multiple specs from one
call (small multiples, dashboards):

```sql
FROM ducklink_vegalite_grid(sales_by_month, ...);
```

## Why the seam belongs at Vega-Lite JSON

Vega-Lite is a declarative JSON grammar with a big rendering ecosystem:
`vega-embed` (browser), `altair` (Python), `vegawidget` (R), and every
notebook environment that has a `application/vnd.vegalite.v5+json`
mimetype handler will render it automatically. Emitting the spec puts
ducklink into the pipeline in the position it should occupy —
"transform tabular data into a well-formed visualization
specification" — and lets every renderer downstream do its job
without ducklink knowing which one is on the other side.

It also composes with SQL cleanly. `ducklink_vegalite(...)` returns a
VARCHAR; it goes into a CTE, gets joined against a table of chart
metadata, gets written to a `.json` file via `COPY`, gets stored as a
row in a dashboard config table — all the normal SQL affordances.
`VISUALIZE <select>` participates in none of that.

## What we're NOT doing (and why)

- **No parser extension, ever.** See position above. If the SQL surface
  needs to look different, that's `CREATE MACRO` territory — a
  MACRO gives you `visualize(x, y, kind => 'bar')` reading as SQL,
  binds through the normal resolver, participates in DDL / linters
  / audit, and can't grab a keyword.
- **No HTTP server, no browser spawn, no display side effects from
  SQL.** A SQL statement should return data. If a caller wants a
  browser to open, that's a decision made in the code that renders
  the returned spec, not in the SQL engine.
- **No proprietary chart grammar.** Vega-Lite exists, it's stable,
  it's widely rendered. We use it. We don't invent one.

## Catalog status

The `ggsql` entry has been removed from the catalog snapshot as of
v5.0.0 (along with the other two `requires: ["parser"]` entries —
`dplyr` and `prql_parser`). All three were unreachable after the
advanced tier came out; nothing satisfies `requires: ["parser"]` on
any current ducklink host, and the removed parser-extension WIT
interface will not come back until DuckDB ships a stable C-API for
it (and possibly not even then — see position).

A `ducklink_vegalite(...)` scalar in the ducklink native/wasm set is
a follow-on task once the catalog contract for chart-spec-emitting
functions is settled.
