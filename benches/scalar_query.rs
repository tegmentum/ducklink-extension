//! End-to-end scalar throughput through a real in-process DuckDB.
//!
//! Unlike `scalar_dispatch` (which calls the engine directly and builds the
//! argument chunk in untimed setup), this drives a real SQL query, so the timed
//! region includes the full per-chunk work DuckDB pays: `reg_duckdb::invoke`
//! reading each input vector into the WIT marshalling buffer (`read_arg`), the
//! batched dispatch into the component, and writing results back (`write_ret`).
//! That makes the per-row marshalling allocations visible -- which the
//! direct-dispatch bench cannot measure. Requires an in-process DuckDB:
//!
//!   cargo bench --no-default-features --features bundled --bench scalar_query

#[cfg(feature = "bundled")]
mod bundled {
    use std::hint::black_box;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use criterion::{Criterion, Throughput};
    use duckdb::Connection;

    use ducklink::engine::Engine2;
    use ducklink::reg_duckdb::{register_components, ComponentSpec};

    fn artifact(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../artifacts/extensions")
            .join(format!("{name}.wasm"))
    }

    pub fn bench(c: &mut Criterion) {
        let path = artifact("sample_extension");
        if !path.exists() {
            eprintln!("sample_extension corpus artifact missing; skipping query bench");
            return;
        }
        let con = Connection::open_in_memory().expect("open in-memory duckdb");
        let engine = Arc::new(Mutex::new(Engine2::new().expect("engine")));
        let specs = vec![ComponentSpec {
            name: "sample_extension".to_string(),
            path,
        }];
        // Scalars register on the duckdb-rs connection; no raw sibling needed
        // (no aggregates exercised here).
        register_components(&con, None, engine, &specs).expect("register components");

        const N: u64 = 1_000_000; // ~488 chunks of STANDARD_VECTOR_SIZE
        let sql = format!("SELECT sum(sample_plus_one(i)) FROM range({N}) t(i)");

        let mut group = c.benchmark_group("scalar_query");
        group.throughput(Throughput::Elements(N));
        group.bench_function("plus_one_sum_1M", |b| {
            b.iter(|| {
                let s: i64 = con.query_row(&sql, [], |r| r.get(0)).expect("query");
                black_box(s);
            });
        });
        group.finish();
    }
}

#[cfg(feature = "bundled")]
criterion::criterion_group!(benches, bundled::bench);
#[cfg(feature = "bundled")]
criterion::criterion_main!(benches);

#[cfg(not(feature = "bundled"))]
fn main() {
    eprintln!("scalar_query bench requires --features bundled; skipped");
}
