//! Cross-extension parallel scalar dispatch.
//!
//! Bucket D's per-instance locking is only visible when TWO threads dispatch
//! into DIFFERENT loaded instances at the same time. Under the old
//! `Arc<Mutex<Engine2>>`, both threads serialize on the process-wide engine
//! mutex; under the new `Arc<Engine2>` with per-instance mutexes, they only
//! contend on a brief `RwLock::read()` on the instances map and then lock
//! ONLY their own instance mutex.
//!
//! We load the same `sample_extension.wasm` TWICE under different names, so
//! each thread targets its own independent `ExtensionInstance` (and thus its
//! own mutex). Same-instance parallelism cannot speed up: wasmtime's Store is
//! `!Sync`, so two threads on the SAME instance always serialize regardless
//! of whether the outer engine lock is coarse or fine-grained. The
//! `parallel_same_ext` control case bounds that hard ceiling; the
//! `parallel_cross_ext` case is where bucket D shows up.
//!
//!   cargo bench --no-default-features --features bundled --bench parallel_scalar

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};

use ducklink::engine::Engine2;
use ducklink_runtime::duckdb_extension_bindings::duckdb::extension::column_types::{
    Column as ColvecColumn, Colvec,
};

const CHUNKS: usize = 64;
const ROWS: usize = 2048;

fn corpus_dir() -> PathBuf {
    match std::env::var_os("DUCKLINK_CORPUS_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../artifacts/extensions"),
    }
}

fn artifact(name: &str) -> PathBuf {
    corpus_dir().join(format!("{name}.wasm"))
}

/// One Int64 arg column of ROWS values (0, 1, 2, ...) — the input for
/// `sample_plus_one(i)`.
fn make_args() -> Vec<Colvec> {
    let data = (0..ROWS as i64).collect::<Vec<_>>();
    vec![Colvec {
        data: ColvecColumn::Int64(data),
        validity: Vec::new(),
        rows: ROWS as u32,
    }]
}

fn bench_parallel_scalar(c: &mut Criterion) {
    let sample_path = artifact("sample_extension");
    if !sample_path.exists() {
        eprintln!("sample_extension wasm missing; skipping parallel bench");
        return;
    }

    // Load the SAME .wasm under two different names, giving us two independent
    // ExtensionInstances (and thus two independent per-instance mutexes) that
    // still expose the same scalar. Bucket D unlocks cross-INSTANCE
    // parallelism; whether the underlying component is literally the same
    // binary is irrelevant.
    let engine = Arc::new(Engine2::new().expect("build engine"));
    let a = engine
        .load("sample_a", &sample_path)
        .expect("load sample_a");
    let b = engine
        .load("sample_b", &sample_path)
        .expect("load sample_b");
    let handle_a = a
        .scalars
        .iter()
        .find(|s| s.name.ends_with("plus_one"))
        .expect("plus_one scalar (a)")
        .callback_handle;
    let handle_b = b
        .scalars
        .iter()
        .find(|s| s.name.ends_with("plus_one"))
        .expect("plus_one scalar (b)")
        .callback_handle;

    let mut group = c.benchmark_group("parallel_scalar");
    // Both cases process 2 * CHUNKS chunks of ROWS rows in total.
    group.throughput(Throughput::Elements((CHUNKS * ROWS * 2) as u64));
    group.sample_size(30);

    // Baseline: one thread does 2*CHUNKS chunks sequentially.
    group.bench_function("serial_one_thread", |b| {
        b.iter_batched(
            || (0..(2 * CHUNKS)).map(|_| make_args()).collect::<Vec<_>>(),
            |batches| {
                for args in batches {
                    let out = engine
                        .dispatch_scalar_batch_col(handle_a, 0, &args)
                        .expect("dispatch");
                    black_box(out);
                }
            },
            BatchSize::PerIteration,
        );
    });

    // Control: two threads dispatching on the SAME instance. The wasmtime
    // store is !Sync, so the per-instance mutex serializes them regardless of
    // engine-level locking. Should NOT beat serial_one_thread.
    group.bench_function("parallel_same_ext", |b| {
        b.iter_batched(
            || {
                let a = (0..CHUNKS).map(|_| make_args()).collect::<Vec<_>>();
                let b_ = (0..CHUNKS).map(|_| make_args()).collect::<Vec<_>>();
                (a, b_)
            },
            |(av, bv)| {
                let e1 = engine.clone();
                let ta = thread::spawn(move || {
                    for args in av {
                        let out = e1
                            .dispatch_scalar_batch_col(handle_a, 0, &args)
                            .expect("dispatch");
                        black_box(out);
                    }
                });
                let e2 = engine.clone();
                let tb = thread::spawn(move || {
                    for args in bv {
                        let out = e2
                            .dispatch_scalar_batch_col(handle_a, 0, &args)
                            .expect("dispatch");
                        black_box(out);
                    }
                });
                ta.join().unwrap();
                tb.join().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    // The point: two threads on DIFFERENT instances. Under main's
    // `Arc<Mutex<Engine2>>` the process-wide mutex serialized these anyway;
    // under HEAD's per-instance mutex they should run concurrently and beat
    // the serial baseline (approaching 2x on a multi-core host).
    group.bench_function("parallel_cross_ext", |b| {
        b.iter_batched(
            || {
                let a = (0..CHUNKS).map(|_| make_args()).collect::<Vec<_>>();
                let b_ = (0..CHUNKS).map(|_| make_args()).collect::<Vec<_>>();
                (a, b_)
            },
            |(av, bv)| {
                let e1 = engine.clone();
                let ta = thread::spawn(move || {
                    for args in av {
                        let out = e1
                            .dispatch_scalar_batch_col(handle_a, 0, &args)
                            .expect("dispatch");
                        black_box(out);
                    }
                });
                let e2 = engine.clone();
                let tb = thread::spawn(move || {
                    for args in bv {
                        let out = e2
                            .dispatch_scalar_batch_col(handle_b, 0, &args)
                            .expect("dispatch");
                        black_box(out);
                    }
                });
                ta.join().unwrap();
                tb.join().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_parallel_scalar);
criterion_main!(benches);
