# WIT ABI — streaming scalar dispatch (v5.0.0 proposal)

**Status:** design conversation, not implemented.
**Motivation:** the 48ms `scalar_query/plus_one_sum_1M` benchmark ceiling.
**Scope:** WIT semver-major bump; new dispatch world; guest SDK redesign; ~2000+ LOC; migration path required for every existing component.
**Projected win:** ~30% on typical scalar-composed queries; ~10-15% on cases where guest work already dominates.

## The core insight

`plus_one_sum_1M` is 488 dispatches × ~99µs each. Native DuckDB does the same work in ~1ms. Our per-dispatch cost is ~85µs, of which:

- ~30µs — guest wasm computation (unavoidable; 2-3x native for tight loops)
- ~20µs — wasmtime dispatch machinery per crossing (stack transition, store lock, canonical-ABI setup, invokeinfo lift)
- ~15µs — wasmtime `list<T>` memcpy in + out
- ~15µs — host-side marshalling (F4/G2 optimized; ~5µs after)
- ~5µs — DuckDB context transition (C++ → C API → Rust → wasmtime)

**The dispatch machinery is fired 488 times per query.** No amount of per-chunk optimization changes that count. Even zero-copy WIT (Options A/B in the shared-memory doc) leaves ~20µs of pure dispatch overhead × 488 = ~10ms of unavoidable machinery cost.

**Streaming dispatch fires the machinery ONCE per query.** The guest owns the pull/push loop.

## The shape

New WIT world alongside the existing `callback-dispatch`:

```wit
// New in duckdb:extension@5.0.0
// runtime/wit-canonical/duckdb-extension/streaming-dispatch.wit

package duckdb:extension@5.0.0;

use types;
use column-types;

interface streaming-dispatch {
  use types.{duckerror, invokeinfo};
  use column-types.{colvec};

  /// Signal from the guest to the host asking for the next input chunk.
  /// Returns None when the host has no more chunks (EOF); the guest then
  /// exits its loop and returns.
  ///
  /// This is a HOST-IMPORTED function the guest calls into. The host binds
  /// each concrete pull to the current scan cursor (managed in Rust,
  /// per-dispatch).
  pull-chunk: func(cursor: u32) -> option<list<colvec>>;

  /// Signal from the guest to the host offering the next output chunk.
  /// The host writes it into the DuckDB output vector for the current
  /// window of rows, advances its scan cursor, and returns.
  push-chunk: func(cursor: u32, result: colvec) -> result<_, duckerror>;

  /// The dispatched scalar. Called ONCE per query. The guest allocates
  /// nothing to communicate progress back — pull/push carry the flow.
  /// Return value is the accumulated error (if any) or unit.
  ///
  /// `cursor` is opaque to the guest; it's the host's scan-context handle.
  /// `ctx` carries the aggregate row-index base and window flag.
  call-scalar-stream: func(
    handle: u32,
    cursor: u32,
    ctx: invokeinfo
  ) -> result<_, duckerror>;
}
```

**Host side**: `Engine2::dispatch_scalar_stream(callback_handle, base_row, cursor)` sets up a pull/push cursor (per DuckDB thread), calls `call-scalar-stream` ONCE, and returns when the guest exits its loop.

**Guest side** (SDK helper):
```rust
fn my_plus_one(cursor: u32, ctx: InvokeInfo) -> Result<(), DuckError> {
    while let Some(args) = ducklink::pull_chunk(cursor) {
        let mut out = colvec_i64(args[0].len());
        for i in 0..args[0].len() {
            out.push(args[0].as_i64_slice()[i] + 1);
        }
        ducklink::push_chunk(cursor, out)?;
    }
    Ok(())
}
```

## Why ~30% and not more

The savings compound but not multiplicatively:

- **Dispatch machinery**: 488 × 20µs → 1 × 20µs. Save ~9.7ms. **Direct win.**
- **Wasmtime memcpy per chunk**: still fires per pull/push, but on the same guest allocation (no reallocation between chunks). Save ~5µs per chunk × 488 = ~2.4ms. **Partial win.**
- **DuckDB context transitions**: 488 → 1 outer. Save ~5µs × 487 = ~2.4ms. **Direct win.**
- **Guest wasm computation**: unchanged, ~30µs × 488 = 14.6ms. **No change.**
- **Guest register / warm-up state**: 488 cold starts → 1 warm loop. Guest JIT keeps hot code / constants in registers across chunks. Modest saving; measured on similar systems as ~5-10% of guest computation time.

Estimated total: `48ms - 9.7ms - 2.4ms - 2.4ms - 1ms ≈ 32.5ms`. **~33% win.**

## Migration story

Zero-flag-day. v4 components keep working through v5's compatibility window:

1. **Additive world.** `streaming-dispatch` lives alongside `callback-dispatch`. A v4 component (implementing only `call-scalar-batch-col`) continues to load. The host inspects the component at load and picks the fastest dispatch it supports.

2. **Contract check.** The runtime's `check_component_contract` (currently rejects a major mismatch) is extended to accept both `duckdb:extension@4.x` and `duckdb:extension@5.x` for a transition window. v5-only host semantics get gated behind a v5 declaration.

3. **Guest SDK.** The Rust guest SDK ships two traits: `ScalarBatch` (v4 shape, per-chunk return) and `ScalarStream` (v5 shape, pull/push loop). Existing derives keep working; new derives can opt in.

4. **Sunset plan.** v6.0.0 (or a later minor) removes the v4 shape entirely. Deprecation warning at load time in v5. Downstream authors have the entire v5.x window to migrate.

## Open design questions

Discussion needed for each of these before implementation starts:

### Q1: Cursor lifetime + Send/Sync

The cursor holds a pointer into DuckDB's scan state. Guest holds it while `call-scalar-stream` is running. If DuckDB parallel-scans a table across worker threads, each thread gets a distinct cursor, so no shared mutable state on the guest side — but each thread requires its own guest instance OR the guest store must serialize per-thread dispatch. Same story as today's per-instance mutex (F3-a) but the crossing point is different.

### Q2: NULL propagation

Currently, `WasmScalar::invoke` computes `null_mask` in Rust and overrides the guest's output with NULL for input-null rows (I1's scratch). Under streaming: does the host pre-mask the pull-chunk result? Or does the guest push NULL results on NULL inputs? Cleanest option: pre-mask on the host, guest sees no NULLs; but that costs an extra input scan. Alternative: pass validity into pull-chunk, guest decides.

### Q3: TEXT / BLOB variable-size input

Guest can't return a fixed-size Colvec::Text — each element allocates. Pull-chunk carries String / Vec<u8> in canonical ABI (unavoidable). Push-chunk similar. Same cost as today per crossing; the win comes from crossing fewer times, so still net positive.

### Q4: Aggregate + table streaming (out of scope for v5.0.0?)

Analog exists for aggregate: `call-aggregate-stream` that loops pull/update, returning the finalized value. And for table: `call-table-stream` that loops push-chunk, returning EOF. These would land in the same v5 world but as separate exports; scalar is the highest-impact target and the natural first mover.

### Q5: Error propagation mid-stream

Guest hits an error at row 500K of 1M. Options:
- **Fail fast**: `push-chunk` returns error, guest returns Err. Host stops the query. Simple.
- **Skip row**: guest sees `push-chunk` return error, decides whether to continue. Complex.

Recommendation: fail fast. Matches SQL semantics.

### Q6: The invokeinfo `rowindex`

Currently, each dispatch carries `rowindex` = base row of the chunk. Under streaming: the base moves as pull-chunk advances. Options:
- Pass rowindex ONCE at stream start, guest tracks locally.
- Pull-chunk returns `(colvec, base_row)`.
- Guest doesn't need rowindex (some functions ignore it).

Recommendation: pull-chunk returns `(colvec, base_row)`. Cheap and covers all cases.

### Q7: Backpressure

Guest could push faster than the host can consume (parallel scans, downstream ops). Wasmtime doesn't have async yielding at the WIT boundary; `push-chunk` is synchronous. Host serializes internally. Probably fine at scale but worth measuring.

### Q8: Wasmtime tail calls

Component-model tail calls (proposal-tail-call) would let the guest's pull-loop compile to a tight tail-recursive loop without stack growth. Wasmtime supports it. If we spec streaming-dispatch to encourage this shape, guests get a small extra win.

## Cost estimate

- **WIT + bindgen regen**: 2-3 days.
- **Runtime dispatch impl (host-imported pull/push, cursor management)**: 5-7 days.
- **Guest SDK ScalarStream trait + macros**: 3-5 days.
- **DuckDB extension side (Engine2, reg_duckdb WasmScalar rewrite)**: 3-5 days.
- **Migration of the 3 in-tree corpus components (aba, sample_extension, creditcard)**: 2-3 days.
- **Docs + design review + iteration**: 3-5 days.

**Total: ~20-30 person-days.** A month of focused work by one engineer. Realistic v5.0.0 release cycle: 4-8 weeks depending on parallel work.

## Alternatives considered

1. **Wider chunks**: DuckDB pins at 2048. Would require patching DuckDB. Non-starter.
2. **Precompile hot guests to native**: cranelift AOT or aot-cranelift. Real 10-15% win on guest work; orthogonal to streaming. Could layer on top.
3. **Skip the WIT change, invest in guest computation**: If we shipped a "fast path" i64 hot loop in the SDK's plus_one, that saves ~5-10µs per chunk. But every guest has to opt in, and it's a per-function optimization, not architectural.
4. **Do the Option A/B shared-memory change first, then streaming**: adds engineering cost without commensurate benefit. Streaming subsumes A/B.

## Recommendation

Streaming dispatch is the answer to "how do we make dispatch competitive with native DuckDB." Every other perf project is either <10% (per-chunk tuning we've already done) or infrastructure (native compilation).

If perf is genuinely the roadmap priority, **this is the next quarter's engineering focus.** Everything smaller is table stakes.

**Next steps**:
1. Design review of this doc with stakeholders who own the guest SDK contract.
2. Prototype the WIT + a single working scalar (plus_one) to measure the actual win against `plus_one_sum_1M`.
3. If measured win ≥ projected, commit to v5.0.0.
4. If below, reconsider the roadmap (native compilation may be a better target).

## References

- `docs/wit-shared-memory-result.md` — the "small" alternative (Options A/B).
- `runtime/wit-canonical/duckdb-extension/callback-dispatch.wit` — the v4 shape being replaced.
- `runtime/wit-canonical/duckdb-extension/aggregate-incr-dispatch.wit` — existing incremental pattern (row-major); prior art for pull/push shape.
- `runtime/wit-canonical/duckdb-extension/table-stream-dispatch.wit` — existing streaming pattern for table functions; also prior art.
- Benchmark numbers: `scalar_query/plus_one_sum_1M` = 48.5ms (post-F/G/H/I); `scalar_dispatch/plus_one_col_i64_2048` = 86µs.
