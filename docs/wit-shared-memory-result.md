# WIT ABI — zero-copy scalar dispatch result buffer

**Status:** design proposal, not implemented.
**Motivation:** perf ceiling of the current scalar hot path.
**Cost:** additive-minor WIT bump; bindgen regen; guest SDK update; ~500-800 lines of engineering work; ~7% projected win on scalar throughput.

## The problem

On the current `call-scalar-batch-col` export, wasmtime's canonical ABI does two full linear-memory copies per invocation:

1. **Host → guest:** the `list<colvec>` args are serialised into wasm linear memory (a realloc + memcpy inside the guest's allocator).
2. **Guest → host:** the `result<colvec, duckerror>` return value is lifted into a fresh Rust `Vec<T>` on the host, then that Vec is consumed by `write_colvec` which memcpys it *again* into the DuckDB flat vector.

Per chunk, roughly:
- input memcpy per column: ~2-3 µs (16 KB per primitive column)
- result lift alloc + memcpy: ~3-5 µs
- host-side re-copy to DuckDB flat vector: ~1 µs

Total wasmtime overhead per chunk: **~6-9 µs**. On the 86 µs `plus_one_col_i64_2048` dispatch benchmark that's ~7-10% of the per-chunk cost. On the 48.5 ms `scalar_query/plus_one_sum_1M` bench (488 chunks × 99 µs) that's **~3-4 ms of the total, or ~7% of the query time**.

`SCALAR_ARGS_SCRATCH` (F4) reuses the *host-side* Colvec buffers. But wasmtime still memcpys them into linear memory on every call, so the win is only on the read-out-of-DuckDB side. Same on write: `write_colvec` reuses nothing.

## The proposal

Add a new column-oriented WIT export whose result semantics are "write into a host-managed buffer" instead of "return a fresh Vec." Two variants under consideration:

### Option A — resource-based result sink

```wit
// New in duckdb:extension@4.1.0 / callback-dispatch.wit
resource result-sink {
    /// Push a fixed-width column into the sink. Host implementation writes
    /// directly into the DuckDB flat vector (or scratch), no intermediate
    /// Vec on the host.
    push-column: func(col: colvec) -> result<_, duckerror>;

    /// Set the sink's validity mask.
    push-validity: func(bits: list<u8>) -> result<_, duckerror>;
}

// New export (host imports on the sink resource; guest calls into it)
call-scalar-batch-col-sink: func(
    handle: u32,
    args: list<colvec>,
    sink: borrow<result-sink>,
    ctx: invokeinfo
) -> result<_, duckerror>;
```

**Pros:**
- Semantically clean; the guest still expresses "here is one output column."
- Host controls the destination — can write straight into the DuckDB `duckdb_vector_get_data()` pointer, skipping one copy.
- No guest-side allocator involvement for the result.

**Cons:**
- Still requires memcpy across linear memory (the sink call passes a `colvec` which is still `list<T>`).
- Adds a resource type — bindgen support for host-imported resources is available but adds indirection.

**Realistic win:** ~1-2 µs per chunk (skip the intermediate host Vec). ~2-3% on the 48 ms query.

### Option B — pre-allocated shared linear-memory region

```wit
// The host reserves a region of the guest's linear memory (via a WASI-like
// buffer allocation call at load time); dispatch takes an offset/length
// tuple pointing INTO that region.
record shared-buffer {
    offset: u64,   // in guest linear memory
    length: u64,   // in bytes
}

call-scalar-batch-col-shared: func(
    handle: u32,
    /// Args live in this host-managed region. Guest reads directly.
    args_buffer: shared-buffer,
    args_metadata: list<colvec-header>,  // just codes + rows + validity offset
    result_buffer: shared-buffer,
    ctx: invokeinfo
) -> result<colvec-header, duckerror>;  // metadata only
```

**Pros:**
- Zero copies. The host writes args into the shared buffer, the guest reads them there, writes results back into the same region, the host reads from there and writes straight into DuckDB.
- Total wasmtime cost per chunk drops from ~7 µs to ~500 ns (just the dispatch mechanics).

**Cons:**
- Shared buffer must be inside the guest's linear memory (wasmtime doesn't share memory with the host). So the host still has to write into linear memory once — but only once, not twice as today. Still saves ~50%.
- Metadata bookkeeping is significant: colvec-header (codes, rows, validity offset within the buffer) must be encoded per column.
- Guest SDK complexity: components must know how to read/write into the shared buffer following the layout the host expects. If the layout is wrong, the guest reads garbage.
- Buffer sizing: needs to accommodate the largest chunk × all columns. Adds a load-time capacity negotiation.

**Realistic win:** ~4-5 µs per chunk. ~5-7% on the 48 ms query.

## Migration and compat

Both options are additive minor WIT changes:

- WIT version: `4.0.0` → `4.1.0`. Contract-check guard in the runtime rejects components declared against `4.0.x` if the new export is called — but the fallback path uses `call-scalar-batch-col` (the current export) unchanged, so **existing components keep working**.
- **Host-side capability detection:** at `Engine2::load`, look up the new export on the component's instance. If present, mark the callback for the fast-path dispatcher. If absent, use `call-scalar-batch-col` as today. Same shape as F3-b's Weak-instance dispatch fallback.
- **Guest SDK update:** the `ducklink-sdk` crate (that guests link against) grows a new "col-native + sink" trait. Existing components using the old trait auto-fall-back. New components can opt in for the fast path.

## Cost-benefit summary

| Path | Engineering cost | Win on 48 ms bench | Verdict |
|---|---|---|---|
| Option A (sink resource) | ~600 lines | ~2-3% | Marginal |
| Option B (shared buffer) | ~1000 lines + guest SDK | ~5-7% | Real but bounded |
| **Streaming scalar dispatch** (radical redesign) | ~2000+ lines, semver-major, all guests rewrite | ~30% | The real answer |

## Recommendation

The two "small WIT change" options land ~5-7% at best. That's not a great ROI vs the ~500-1000 lines of engineering — including a guest SDK release that every downstream component author has to react to.

The 30% win is **streaming scalar dispatch** — a single WIT call per query, where the guest calls back into the host to pull chunks and push results. That's an ABI major bump (or a whole new dispatch world) and a redesign of the guest programming model.

If we want a real perf jump, the streaming path is the answer. If we want the incremental win, Option B is preferable to Option A. If we want to preserve engineering budget for the streaming change, defer both A and B and put the effort into planning streaming instead.

## References

- `runtime/wit-canonical/duckdb-extension/callback-dispatch.wit` — current shape.
- `src/reg_duckdb.rs:1103-1198` — `WasmScalar::invoke` (host side).
- `src/reg_duckdb.rs:1085-1096` — `SCALAR_ARGS_SCRATCH` (already reuses the host-side buffer; wasmtime still memcpys it).
- `runtime/src/extension.rs:2413` — `dispatch_scalar_batch_col` (runtime side of the crossing).
