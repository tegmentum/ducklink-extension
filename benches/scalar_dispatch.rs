//! Micro-benchmark of the Direction-2 scalar dispatch hot path.
//!
//! Measures `Engine2::dispatch_scalar_batch` over one full DuckDB vector (2048
//! rows): the single canonical-ABI crossing into the guest, the guest's per-row
//! work, and lifting the results back out. The chunk is supplied already in the
//! WIT value type (as the bridge's `read_col_into` produces it), so the dispatch
//! path itself does no value conversion. This is the per-chunk cost a scalar
//! query pays for every data chunk. It depends only on `Engine2` (no DuckDB), so
//! build with `--no-default-features`:
//!
//!   cargo bench --no-default-features --bench scalar_dispatch
//!
//! Uses the `sample_extension` corpus component (`sample_plus_one`: i64 -> i64).

use std::hint::black_box;
use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};

use ducklink::engine::Engine2;
use ducklink_runtime::duckdb_extension_bindings::duckdb::extension::types::{
    Complexvalue as WitComplex, Decimalvalue as WitDecimal, Duckvalue as WitVal,
    Intervalvalue as WitInterval, Uuidvalue as WitUuid,
};
use ducklink_runtime::reg::DuckValue;

const ROWS: usize = 2048; // one DuckDB STANDARD_VECTOR_SIZE chunk

// Replica of the engine's private neutral->WIT codec, to measure the conversion
// cost in isolation (attribution: is the per-chunk Vec<Vec<>> rebuild a real cost,
// or does Vec's in-place collect specialization make it ~free for same-size enums?).
// Only the numeric `Int64` arm is exercised by this bench; the remaining arms keep
// the match exhaustive against the full `reg::DuckValue` (which carries every rich
// logical type), so the codec stays a faithful replica of the engine's.
#[inline]
fn n2w(v: DuckValue) -> WitVal {
    match v {
        DuckValue::Null => WitVal::Null,
        DuckValue::Boolean(b) => WitVal::Boolean(b),
        DuckValue::Int64(i) => WitVal::Int64(i),
        DuckValue::Uint64(u) => WitVal::Uint64(u),
        DuckValue::Float64(f) => WitVal::Float64(f),
        DuckValue::Text(s) => WitVal::Text(s),
        DuckValue::Blob(b) => WitVal::Blob(b),
        DuckValue::Int8(i) => WitVal::Int8(i),
        DuckValue::Int16(i) => WitVal::Int16(i),
        DuckValue::Int32(i) => WitVal::Int32(i),
        DuckValue::Uint8(u) => WitVal::Uint8(u),
        DuckValue::Uint16(u) => WitVal::Uint16(u),
        DuckValue::Uint32(u) => WitVal::Uint32(u),
        DuckValue::Float32(f) => WitVal::Float32(f),
        DuckValue::Timestamp(t) => WitVal::Timestamp(t),
        DuckValue::Date(d) => WitVal::Date(d),
        DuckValue::Time(t) => WitVal::Time(t),
        DuckValue::Timestamptz(t) => WitVal::Timestamptz(t),
        DuckValue::Decimal {
            lower,
            upper,
            width,
            scale,
        } => WitVal::Decimal(WitDecimal {
            lower,
            upper,
            width,
            scale,
        }),
        DuckValue::Interval {
            months,
            days,
            micros,
        } => WitVal::Interval(WitInterval {
            months,
            days,
            micros,
        }),
        DuckValue::Uuid { hi, lo } => WitVal::Uuid(WitUuid { hi, lo }),
        DuckValue::Complex { type_expr, json } => {
            WitVal::Complex(WitComplex { type_expr, json })
        }
    }
}

/// Directory holding the prebuilt corpus `*.wasm` artifacts (see
/// `tests/bridge_coverage.rs`). Overridable with `DUCKLINK_CORPUS_DIR`.
fn corpus_dir() -> PathBuf {
    match std::env::var_os("DUCKLINK_CORPUS_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../artifacts/extensions"),
    }
}

fn artifact(name: &str) -> PathBuf {
    corpus_dir().join(format!("{name}.wasm"))
}

fn bench_scalar_dispatch(c: &mut Criterion) {
    let path = artifact("sample_extension");
    if !path.exists() {
        eprintln!("sample_extension corpus artifact missing; skipping dispatch bench");
        return;
    }
    let mut engine = Engine2::new().expect("build engine");
    let loaded = engine
        .load("sample_extension", &path)
        .expect("load sample_extension");
    let handle = loaded
        .scalars
        .iter()
        .find(|s| s.name == "sample_plus_one")
        .expect("sample_plus_one scalar")
        .callback_handle;

    let mut group = c.benchmark_group("scalar_dispatch");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("plus_one_i64_2048", |b| {
        b.iter_batched(
            // Setup (untimed): a fresh i64 argument chunk in the WIT value type,
            // exactly as reg_duckdb's read_arg now produces it from a DuckDB
            // input vector -- no neutral->WIT step remains on this path.
            || {
                (0..ROWS as i64)
                    .map(|i| vec![WitVal::Int64(i)])
                    .collect::<Vec<_>>()
            },
            // Timed: WIT crossing into the guest, guest work, and result lift.
            |rows| {
                let rows = black_box(rows);
                let out = engine
                    .dispatch_scalar_batch(handle, 0, &rows)
                    .expect("dispatch");
                black_box(out);
            },
            BatchSize::SmallInput,
        );
    });

    // Attribution A: the neutral->WIT conversion alone (no WIT crossing, no guest).
    // If this is ~free, Vec's in-place collect specialization already elides the
    // Vec<Vec<>> rebuild and the marshalling optimization is not in the conversion.
    group.bench_function("convert_only_i64_2048", |b| {
        b.iter_batched(
            || {
                (0..ROWS as i64)
                    .map(|i| vec![DuckValue::Int64(i)])
                    .collect::<Vec<_>>()
            },
            |rows| {
                let wit: Vec<Vec<WitVal>> = rows
                    .into_iter()
                    .map(|r| r.into_iter().map(n2w).collect())
                    .collect();
                black_box(wit);
            },
            BatchSize::SmallInput,
        );
    });

    // Attribution B: per-row dispatch (2048 separate WIT crossings) -- the cost the
    // batch path was introduced to amortize. The gap vs the batched case is the
    // per-crossing overhead; what remains in the batched case is per-cell ABI + guest.
    group.bench_function("per_row_i64_2048", |b| {
        b.iter(|| {
            for i in 0..ROWS as u64 {
                let out = engine
                    .dispatch_scalar(handle, i, vec![DuckValue::Int64(i as i64)])
                    .expect("dispatch");
                black_box(out);
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_scalar_dispatch);
criterion_main!(benches);
