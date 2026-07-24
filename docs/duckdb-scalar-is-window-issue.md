# Extension C API: expose is_window on FunctionInfo for scalar functions

## Motivation

When writing a DuckDB extension that ships scalar functions, we need to know at call time whether the function is being invoked inside an `OVER (...)` window context. The current C API surfaces no such signal — `duckdb_function_info` carries no `is_window` accessor, so extension authors must assume non-window semantics unconditionally.

Concrete cases where this matters:

- **Inter-row memoization.** A scalar with expensive setup wants to cache across rows in a plain projection, but reset per partition when called from a window frame. Without an `is_window` hint the cache either grows unboundedly across partitions or is defeated by defensive resets.
- **Window-local state.** A geospatial or ML scoring UDF wants to bind auxiliary buffers to the frame it's evaluated over — e.g. a running envelope — and free them at frame boundaries. It needs to know it's in a window at all before it can subscribe to that lifecycle.
- **Dispatch strategy.** Some extensions have a vectorized fast path that is only sound outside window evaluation (row order assumptions). Picking the wrong path silently produces wrong answers.

## Proposed API

```c
bool duckdb_scalar_function_info_is_window(duckdb_function_info info);
```

Placed on `duckdb_function_info` (per-call) rather than on the scalar function registration because the same registered function can be invoked in both contexts within a single query; the flag is a property of the call site, not the registration.

The nearest existing symbol, `duckdb_scalar_function_get_extra_info`, only returns registration-time `extra_info` and cannot carry per-call context.

## Alternatives considered

- **Query-plan introspection.** Not exposed to extensions through the stable C API; would require a much larger surface.
- **Register the scalar as a distinct aggregate/window function.** Doesn't help: different function signature, different registration path, and callers would have to opt in at SQL level.
- **Infer from call cadence or vector sizes.** Brittle and racy; produces silent misclassification under load and depends on internal executor behavior.

## Impact

Narrow: only extensions that specialize behavior on window context are affected. But the failure mode is a silent semantic mismatch — the extension returns plausible-looking but incorrect results, or leaks/thrashes state — rather than a loud error. A single-bit accessor removes the ambiguity at negligible ABI cost.

## References

Surfaced during a DuckLink wasm-extension audit while wiring WIT interfaces that already carry an `invokeinfo.is_window` field through every scalar dispatch call site. Verified against `libduckdb-sys` **1.10505.0** bindgen output: zero `window` occurrences anywhere in the bindings (no `duckdb_*_is_window` symbol on `duckdb_function_info`, no `is_window` flag on the extra-info struct). The DuckLink dispatch layer currently hardcodes `is_window: false` with a `TODO` in `ducklink-extension/src/engine.rs` pending an upstream accessor.
