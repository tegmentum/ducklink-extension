# What belongs in a DuckDB extension

## Principle

**Extensions compose IN the SQL grammar. They do not replace it.**

A ducklink module — and, we'd argue, any well-behaved DuckDB
extension — earns its place by adding NAMED catalog entries to a SQL
engine: scalar functions, table functions, aggregates, macros, casts.
Those entries bind through the normal resolver, show up in
`duckdb_functions()`, participate in `EXPLAIN`, compose with CTEs and
joins and views, and are legible to every tool that reads SQL text
(linters, migration analyzers, row-level policy checks, audit logs).

The moment an extension steps outside that model — grabbing a keyword,
rewriting text before the parser sees it, replacing SQL with a
different query language, or triggering side effects the SQL text
does not describe — the value it delivers is bought at the price of
making the host engine harder to reason about for everyone else.

## The shape that fails the test

Concretely, three community extensions we looked at as candidates for
inclusion (see `community-native-audit.md`) sit in this category:

- **`ggsql`** — a `VISUALIZE <select>` statement. Adds a keyword to
  the parser (via `ParserExtension`) and, on execution, opens an
  in-process HTTP server and a browser tab to render vega-lite.
- **`prql`** — accepts PRQL text at the DuckDB entry point,
  transpiles it to SQL under the hood, and executes the SQL. From
  outside the process it looks like DuckDB is running PRQL. It isn't.
- **`dplyr`** — same shape as `prql` for R's `dplyr` verb syntax.

All three make DuckDB less legible to tools that read SQL. When your
migration analyzer greps a codebase for `INSERT INTO orders`, it can
no longer be sure that's the total footprint of writes to the orders
table — some other query text the parser silently accepted may
translate to an insert too. When your row-level policy engine walks
`EXPLAIN`, it sees the rewritten SQL, not the source. When a security
review reads a stored query, it may not even parse as SQL to a human
reader without also knowing which extensions were loaded in the
session that wrote it.

That's a lot of load-bearing invisible state, in exchange for a
feature (a different syntax on top) that has a natural home somewhere
else entirely.

## Where those features belong

These are useful ideas. They just aren't extensions.

- **PRQL** is a query LANGUAGE. Its natural home is a compiler that
  reads PRQL and emits SQL. That compiler can be a CLI, a client
  library (Python / R / JS), an editor plugin, a notebook kernel — any
  of those can hand DuckDB SQL. DuckDB stays a SQL engine; the tool
  that speaks PRQL is a separate thing that happens to use DuckDB. If
  a lightweight in-engine surface is genuinely useful, the way to
  expose it is `prql_to_sql(varchar) -> varchar` and
  `prql_is_valid(varchar) -> boolean` — SCALARS, one catalog entry
  each, unambiguous bind, no parser hook.
- **dplyr** is the same shape. A dplyr → SQL transpiler is a natural
  R library; it does not need to inhabit the query engine.
- **`VISUALIZE`** is a rendering FRONTEND. Its natural home is a
  notebook environment or a chart-rendering tool. DuckDB's role is to
  emit the CHART SPEC as data (a Vega-Lite JSON payload — see
  `visualize-design.md`); the tool downstream does the rendering.

The pattern each time: the useful thing is a separate LAYER above or
beside DuckDB. Squeezing it into an extension just so it looks like
part of DuckDB is what makes the composition fall over.

## The reasonable extension boundary

If your feature can be described as "add a named function that takes
these arguments and returns this value" — scalar, table, aggregate,
macro, or cast — an extension is the right home for it. That's every
scalar in the ducklink catalog today.

If your feature is "let users write text that isn't SQL and I'll
figure out what to do with it," an extension is the wrong home. Build
a client library. Build a transpiler. Build a notebook kernel. Ship a
CLI. Any of those can talk to DuckDB, and everyone's tools keep working.

If your feature is "trigger a side effect the SQL didn't ask for" —
open a socket, spawn a subprocess, write a file the query text does
not name — that isn't an extension either. That belongs in whatever
code CALLED DuckDB, where the side effect is visible in the caller's
control flow.

## Consequence for the ducklink catalog

The v5.0.0 catalog drops `ggsql`, `dplyr`, and `prql_parser` for
mechanical reasons (all three declared `requires: ["parser"]` and no
ducklink host satisfies that capability anymore). That deletion also
lines up with the principle above: none of those features BELONG as
extensions to begin with. We don't plan to add them back if DuckDB
eventually re-exposes a parser hook, and we don't intend to build
ducklink-hosted equivalents that reintroduce the same problems under
different names.

What we ARE willing to build in that space:

- Scalar functions returning declarative specifications
  (`ducklink_vegalite(...)` — see `visualize-design.md`).
- Scalar transpilers if a lightweight in-engine surface is useful
  (`prql_to_sql(...)` returning VARCHAR).
- Anything else that fits inside `CREATE FUNCTION` / `CREATE MACRO` /
  `CREATE AGGREGATE` shapes and binds through the SQL resolver.

That's the full shape. Everything past that seam belongs in the layer
above DuckDB, not inside it.
