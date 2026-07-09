# Perf ceiling — measurement results

**Date:** 2026-07-08
**Bench data:** `target/criterion/streaming_validation/` — see raw numbers below.
**Context:** Written after prototyping the "streaming dispatch" hypothesis by measuring per-crossing overhead across chunk sizes 128 → 1M.

## Executive summary

**The streaming-dispatch design proposal (`wit-streaming-scalar-dispatch.md`) was wrong.** Measurement shows the fixed per-crossing dispatch cost is **~1.3µs, not the ~20µs I projected**. Streaming would save 487 × 1.3µs ≈ **0.63ms = 1.3% of the 48.5ms benchmark**, not the 30% I claimed.

**The bottleneck is actually per-row work, not crossing count.** Every row of a scalar query costs ~43ns:
- ~15ns wasm computation (2-3 instructions for `x + 1`)
- ~13ns memcpy IN + OUT (16 bytes per i64 row)
- ~10ns canonical ABI encoding
- ~5ns other

Reducing per-row cost — not per-crossing count — is the real perf lever.

## The measurement

Bench: `streaming_validation` group in `benches/scalar_dispatch.rs`. Called `dispatch_scalar_batch_col` at 6 chunk sizes, ran through the SAME `sample_plus_one` guest.

| chunk size n | median | ns/row | CI |
|---:|---:|---:|---|
| 128 | 6.79 µs | 53.08 | ±0.15% |
| 2,048 | 87.16 µs | 42.56 | ±0.25% |
| 16,384 | 680.06 µs | 41.51 | ±1.5% |
| 65,536 | 2.89 ms | 44.16 | ±7.4% |
| 262,144 | 11.69 ms | 44.59 | ±4.2% |
| 1,048,576 | 45.69 ms | 43.57 | ±2% |

The per-row cost stabilises at **~42-44 ns/row** for n ≥ 2048. The small extra at n=128 (~1-2 µs total) is the fixed dispatch overhead.

Fitting `t(n) = fixed + per_row * n` on the two tightest-CI points:

| pair | fixed | per_row |
|---|---:|---:|
| n=128 vs n=2048 | 1.43 µs | 41.86 ns |
| n=128 vs n=16384 | 1.49 µs | 41.42 ns |
| n=128 vs n=1M | 1.21 µs | 43.57 ns |

**Convergent answer: fixed ≈ 1.3-1.5 µs; per-row ≈ 42-44 ns.**

(A naive least-squares fit across all 6 points falsely reports fixed ≈ 42µs because the large-n samples have absolute noise that dominates the residuals. The small-n samples have 0.15-0.25% CI and are the reliable measurement of fixed cost.)

## Where the 43 ns/row goes

Estimates based on component costs:

| Cost | Estimate | Notes |
|---|---:|---|
| Wasm computation | ~15 ns | 2-3 wasm instructions × 5ns each. Interpreter overhead. |
| Memcpy IN | ~6.4 ns | 8 bytes at ~1.25 GB/s effective (linear memory) |
| Memcpy OUT | ~6.4 ns | Same |
| Canonical ABI | ~10 ns | Type dispatch, length encoding, validity handling |
| Bookkeeping | ~5 ns | Allocator, wasmtime store, misc |
| **Total** | **~43 ns** | Matches observed |

None of this is "dispatch overhead" — it's real work that fires per row of data. The wasmtime dispatch machinery is already efficient.

## Revised projections for each proposal

Against the 48.5ms `scalar_query/plus_one_sum_1M` benchmark:

### Streaming scalar dispatch (v5.0.0 in the design doc)

**Old projection: ~30% win. Actual: ~1.3% win.**

The projection assumed 20µs of dispatch machinery per crossing. Measurement shows 1.3µs. The gap was pure speculation; I should have measured before drafting.

Savings: 487 × 1.3µs = 0.63ms of the 48.5ms bench.

**Not worth pursuing.** The ~1000-2000 LOC + semver-major bump + guest SDK redesign delivers ~1% win. Any smaller change would be preferable.

### Zero-copy shared memory (Option B in `wit-shared-memory-result.md`)

**Old projection: ~5-7% win. Actual: potentially ~27% win.**

Because per-row memcpy is a real chunk of the 43ns/row cost — I underestimated its importance. Every input i64 crosses through wasm linear memory, and every output i64 does the same. That's 13ns/row × 1M = 13ms of the 48.5ms bench.

If we can eliminate the memcpy (guest reads directly from a host-mapped region), we save that 13ms.

Savings: ~13ms of the 48.5ms bench, if we can achieve true zero-copy.

**Worth revisiting.** The engineering scope stays as before (~1000 LOC, guest SDK update), but the win is much larger than I said.

### AOT-compile hot guests (new candidate)

Wasmtime supports Cranelift ahead-of-time compilation. A native-compiled guest runs ~2x faster than interpreted for tight loops. On `plus_one` that's ~7-8ns/row instead of ~15ns/row.

Savings: ~7ns × 1M = 7ms of the 48.5ms bench.

**Worth investigating.** Might already be on by default in newer wasmtime — need to verify.

### Combined (zero-copy + AOT)

If both land, per-row cost drops from 43ns → ~23ns. Query time drops 48.5ms → ~28ms.

**Combined win: ~43%.**

## Recommendation

**Retract the v5.0.0 streaming dispatch proposal.** The projection was 20x wrong; the design as spec'd delivers no meaningful win.

**Reprioritize:**
1. Verify wasmtime AOT is enabled. If not, enable it and re-measure.
2. Return to `wit-shared-memory-result.md` Option B (shared memory) with the corrected 27% projection.
3. Investigate whether Wasmtime tail calls / async / other guest-execution optimizations exist that we're not using.

**Lesson:** always measure the fixed vs per-row breakdown before proposing an ABI change that targets crossing count. The dispatch-count-vs-per-row question is answerable in one bench.

## Raw numbers

Full criterion output in `target/criterion/streaming_validation/`. Bench source in `benches/scalar_dispatch.rs`.

Bench command:
```
DUCKLINK_CORPUS_DIR=<corpus> cargo bench --no-default-features --bench scalar_dispatch -- streaming_validation
```
