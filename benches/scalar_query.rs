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
    use std::sync::Arc;

    use criterion::{Criterion, Throughput};
    use duckdb::Connection;

    use ducklink::engine::Engine2;
    use ducklink::reg_duckdb::{register_components, ComponentSpec};

    fn artifact(name: &str) -> PathBuf {
        // Defaults to the monorepo layout; overridable with `DUCKLINK_CORPUS_DIR`
        // so the bench is runnable from the standalone repo checkout too.
        let dir = match std::env::var_os("DUCKLINK_CORPUS_DIR") {
            Some(dir) => PathBuf::from(dir),
            None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../artifacts/extensions"),
        };
        dir.join(format!("{name}.wasm"))
    }

    pub fn bench(c: &mut Criterion) {
        let path = artifact("sample_extension");
        if !path.exists() {
            eprintln!("sample_extension corpus artifact missing; skipping query bench");
            return;
        }
        // Open via the raw C API so we hold both a duckdb-rs `Connection` (for the
        // scalar query) and a raw sibling `duckdb_connection` on the SAME database,
        // which the aggregate registration needs. Functions register db-wide, so
        // both `sample_plus_one` and `sample_sum` are visible to the query.
        let mut db: duckdb::ffi::duckdb_database = std::ptr::null_mut();
        assert_eq!(
            unsafe { duckdb::ffi::duckdb_open(std::ptr::null(), &mut db) },
            duckdb::ffi::DuckDBSuccess,
            "duckdb_open"
        );
        let con = unsafe { Connection::open_from_raw(db) }.expect("open_from_raw");
        let mut raw_con: duckdb::ffi::duckdb_connection = std::ptr::null_mut();
        assert_eq!(
            unsafe { duckdb::ffi::duckdb_connect(db, &mut raw_con) },
            duckdb::ffi::DuckDBSuccess,
            "duckdb_connect"
        );
        let engine = Arc::new(Engine2::new().expect("engine"));
        let specs = vec![ComponentSpec {
            name: "sample_extension".to_string(),
            path,
        }];
        // The advanced-tier `db` handle is unused by this bench (bundled DuckDB,
        // common-tier only).
        register_components(&con, Some(raw_con), None, engine, &specs)
            .expect("register components");

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

        // Aggregate hot path: `sample_sum` accumulates every input row's argument
        // tuple in per-group state (`update`), then crosses into the component once
        // at `finalize`. This is the per-row marshalling cost the raw-C aggregate
        // bridge pays; benched at the same scale so the two paths are comparable.
        let agg_sql = format!("SELECT sample_sum(i) FROM range({N}) t(i)");
        let mut agg_group = c.benchmark_group("aggregate_query");
        agg_group.throughput(Throughput::Elements(N));
        agg_group.bench_function("sample_sum_1M", |b| {
            b.iter(|| {
                let s: i64 = con.query_row(&agg_sql, [], |r| r.get(0)).expect("agg query");
                black_box(s);
            });
        });
        agg_group.finish();
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
