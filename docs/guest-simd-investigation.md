# Guest-side SIMD — investigation and handoff (K2)

**Status:** investigation complete; implementation is corpus-side work.
**Where the win would land:** the ~15 ns/row of guest computation portion of the 43 ns/row per-row cost. Realistic gains: 15-40% of that portion = ~5-15% of a scalar throughput benchmark, per-component.
**Blocker:** all 3 in-tree corpus components are compiled without SIMD; retrofitting requires rebuilding them, which lives in the ducklink monorepo's corpus build, not this repo.

## Current state (measured)

Corpus components have zero SIMD instructions:

```
aba.wasm             (198 KB): 0 v128 / i64x2 / i32x4 / f64x2 / f32x4 / etc. opcodes
sample_extension.wasm (222 KB): 0 SIMD opcodes
creditcard.wasm      (missing from local cache): unmeasured
```

Wasmtime's SIMD support is on by default (`wasm_simd` feature; base CPU requirement SSE2 on x86_64, always available on aarch64). Host is ready; guests are not compiled with it.

## What would enable SIMD

The corpus components are Rust wasm32 builds. For SIMD they need:

1. **Target flag**: `RUSTFLAGS="-C target-feature=+simd128"` at the build command. Enables the `v128` opcodes wasmtime already accepts.
2. **Source-level SIMD** in the hot loops. Two options:
   - `std::simd` (portable_simd, nightly for stable-quality types, or on beta as of 1.84+).
   - `std::arch::wasm32::*` explicit intrinsics — stable Rust but per-op.

The `+simd128` flag alone will let LLVM auto-vectorize simple hot loops via loop-vectorizer heuristics, which for a `x + 1` loop across 2048 i64s is likely to fire without explicit SIMD source code.

## Realistic wins per component

**sample_plus_one** (i64 -> i64, +1):
- Current: 15 ns/row native-JIT'd guest computation for `x+1`.
- With i64x2 SIMD: 2 rows per iteration → ~7-8 ns/row on the arithmetic. Load/store bandwidth becomes the constraint.
- **Realistic gain**: ~50% on the guest-computation portion = ~15-20% of the 43 ns/row per-row cost.
- **Query-level impact**: `plus_one_sum_1M` at 48.5 ms → ~40-42 ms (~13-17%).

**aba_validate** (VARCHAR -> BOOL, checksum):
- Binary shows 230 i32/i64 multiplications total (across all functions, not just aba_validate). The checksum multiplies each digit by a weight (3,7,1,3,7,1,3,7,1). Per row: 9 multiplies + 8 adds.
- SIMD lets you parallelize the digit-parallel weighted sum. With i32x8 or i16x8, all 9 digits can multiply in one instruction.
- **Realistic gain**: 2-4x speedup on the guest side. Since this is one of the more compute-heavy functions, the query-level impact could be larger.

**sample_sum** (aggregate):
- Whole-column reduction. Already highly amenable to SIMD via reduction intrinsics.
- **Realistic gain**: 2-4x on the reduction; total query benefit ~10-20%.

## Effort estimate (in the corpus repo, not here)

1. Add `RUSTFLAGS="-C target-feature=+simd128"` to the corpus build script for wasm32 targets. — 1 line.
2. Rebuild each component, re-hash, update the catalog. — 15 minutes.
3. Bench the new components locally (via ducklink's benches). — 30 minutes.
4. Confirm no test regressions. — 15 minutes.
5. For higher wins (per-function SIMD source), rewrite the hot loops with `std::simd`. — few hours per function.

Total for the trivial pass (target-feature only): **~1 hour + benchmark measurement**.

Total for the per-function SIMD rewrites: **~1-2 days**.

## Verifying whether SIMD landed

After a rebuild, check with `wasm-tools dump component.wasm | grep -c v128`. Zero means SIMD did not get through the compilation; nonzero means at least some SIMD landed.

If the numbers look promising, the change can flow into the catalog as a straight-through version bump: new `content_digest` for each rebuilt component, catalog snapshot updated, ducklink users get the SIMD version on next `ducklink_load` cache miss.

## What we know for sure

- K1 (host `memory_may_move(false)`) landed and shipped a 1-2% real win. That's the last host-side lever.
- K2 (guest SIMD) is the last known avenue for measurable scalar throughput improvement. Realistic upper bound is ~10-20% per hot query, achieved entirely on the corpus side.
- Everything else in the current architecture has been squeezed. If a bigger win is needed after K2, the answer is streaming dispatch or a fundamentally different execution model — see the two RETRACTED / CANCELLED WIT design docs for the analysis of why those don't pay.

## Recommendation

- **Land K1** (done).
- **Hand K2 to the corpus repo** with this doc as the pointer. The trivial `RUSTFLAGS` change is worth trying immediately — it costs nothing and might land 5-10% for free.
- **Do not invest further host-side perf effort** on the scalar hot path. Measure any incoming perf issue against these results first.
