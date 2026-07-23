//! Delegating aggregate wrappers — the C-API-only path to full aggregate
//! transparency for community-native aliases.
//!
//! Ducklink registers a REAL `AggregateFunction` via the DuckDB C API under
//! ducklink's chosen name (e.g. `crypto.hash_agg`), whose implementation
//! accumulates every input row's column values into a per-group state and
//! then, at `finalize`, runs a nested SQL query invoking the community
//! aggregate on the accumulated rows.
//!
//! Because ducklink's wrapper IS a real aggregate to DuckDB's binder, every
//! aggregate modifier (`DISTINCT`, `FILTER`, `GROUP BY`) works transparently:
//! the binder deduplicates / filters / groups the input rows BEFORE our
//! `update` callback sees them, so our accumulator ends up holding exactly
//! the values the target aggregate should see. `finalize` then invokes the
//! target on that pre-processed input via a nested query on a sibling
//! connection.
//!
//! # Scope of the current implementation
//!
//! Modifier propagation — all through DuckDB's binder before our
//! callbacks see any rows — is exhaustive:
//!
//! - `DISTINCT`, `FILTER (WHERE ...)`, `GROUP BY`.
//! - `OVER (...)` window aggregates, both the implicit-frame form
//!   (`OVER (PARTITION BY g)`) and every explicit `ROWS`/`RANGE` frame
//!   with `PRECEDING`/`FOLLOWING`/`CURRENT ROW`/`UNBOUNDED` bounds.
//! - Frame `EXCLUDE` clauses (`CURRENT ROW`, `GROUP`, `TIES`, `NO OTHERS`).
//! - `ORDER BY <expr>` INSIDE the aggregate call (sorted-aggregate
//!   path). DuckDB pre-sorts by the sort key and calls our `update` in
//!   sorted order; our row-major accumulator preserves that order, and
//!   the delegation `VALUES ... t(cN)` clause hands rows to the target
//!   aggregate in insertion order. Multi-argument aggregates
//!   (`string_agg(x, sep ORDER BY y)`) work.
//!
//! Four contract details make the above hold together:
//!
//! - `combine` is non-destructive (clones rows out of the source
//!   rather than moving), because DuckDB's segment-tree windowing may
//!   combine the same source into several overlapping frames.
//! - `finalize` honours its `offset` parameter so window aggregates
//!   that split a result vector across multiple finalize calls write
//!   to the right output slots.
//! - `build_delegation_sql` emits `n_rows` synthetic rows in the
//!   all-columns-constant degenerate path, so a running-sum frame with
//!   repeated values (`sum(20) FROM ...` over two rows) returns 40
//!   rather than 20.
//! - `update` detects DuckDB's "shared-state" call convention —
//!   sorted-aggregate and full-partition-frame paths hand us one
//!   initialised state at slot 0 and NULL at slot 1 as the sentinel;
//!   we fold every row of the input chunk into slot 0 rather than
//!   dereferencing the uninitialised slots (which contain arbitrary
//!   memory, not zeroed).
//!
//! Storage: row-major, per-row `duckdb::types::Value` accumulator.
//! Multi-column signatures via row-major accumulation.
//! Types today: `BIGINT`, `INTEGER`, `DOUBLE`, `VARCHAR`, `BLOB`,
//! `BOOLEAN`. Extending is a matter of adding a branch in
//! [`extract_value`] and the type-code mapping.

use std::os::raw::c_void;
use std::sync::{Arc, Mutex};

use duckdb::ffi;
use duckdb::ffi::duckdb_string_t;
use duckdb::types::{DuckString, ToSql, Value};
use duckdb::Connection;

/// Ducklink type-code enum — matches the small ints ducklink uses across the
/// rest of the extension to identify DuckDB logical types by code. Only the
/// types the delegating wrapper reads today are enumerated; extending is a
/// matter of adding a variant and a branch in [`extract_value`].
///
/// The u8 encoding here matches ducklink's `type_code` scheme (see
/// `reg_duckdb::type_code`) so callers can pass the codes they already
/// have from `duckdb_functions()` introspection.
pub const T_BIGINT: u8 = 3;
pub const T_INTEGER: u8 = 2;
pub const T_DOUBLE: u8 = 6;
pub const T_VARCHAR: u8 = 10;
pub const T_BLOB: u8 = 11;
pub const T_BOOLEAN: u8 = 0;

/// Convert a ducklink `type_code` to a `duckdb_type` for C API calls.
fn duckdb_type_of(code: u8) -> ffi::duckdb_type {
    match code {
        T_BOOLEAN => ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
        T_INTEGER => ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
        T_BIGINT => ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
        T_DOUBLE => ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
        T_VARCHAR => ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
        T_BLOB => ffi::DUCKDB_TYPE_DUCKDB_TYPE_BLOB,
        _ => ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    }
}

/// Per-registration metadata retrieved from every callback via
/// `duckdb_aggregate_function_get_extra_info`. Boxed onto the heap and
/// handed to DuckDB via `set_extra_info(fn, ptr, destroy)`.
///
/// The sibling `Connection` is held in an `Arc<Mutex<...>>` so `finalize`
/// (which may run concurrently across groups on the same aggregate
/// instance) serialises access to the underlying DuckDB connection while
/// keeping the accumulator lock-free per group.
pub struct DelegatingAggExtra {
    /// Bare name of the target aggregate ducklink is delegating to
    /// (e.g. `"sum"`, `"crypto_hash_agg"`, `"string_agg"`). Spliced
    /// directly into the nested `finalize` query; the caller must have
    /// validated it via `crate::catalog::is_safe_identifier`.
    pub target_name: String,
    /// Per-argument type codes, in call order.
    pub arg_types: Vec<u8>,
    /// Return type code — must match the target aggregate's return type.
    pub return_type: u8,
    /// A sibling connection on the same DuckDB database, opened once at
    /// registration and reused for every `finalize` nested query. Avoids
    /// contention on the runtime's main connection.
    pub con: Arc<Mutex<Connection>>,
}

/// Per-group accumulator. Row-major so building the nested query's
/// `VALUES` clause is a straight iteration.
#[derive(Default)]
pub struct DelegatingAggState {
    /// One entry per input row; each inner `Vec` has `arg_types.len()`
    /// elements, one per column, in argument order.
    pub rows: Vec<Vec<Value>>,
}

/// Extract one row's value from a DuckDB vector as a `duckdb::types::Value`.
/// Returns `Value::Null` for null cells.
///
/// # Safety
/// `vec` must be a live `duckdb_vector` for the current chunk; `row` must
/// be in bounds; `type_code` must match the vector's actual logical type.
unsafe fn extract_value(
    vec: ffi::duckdb_vector,
    row: usize,
    type_code: u8,
) -> Value {
    let validity = ffi::duckdb_vector_get_validity(vec) as *const u64;
    if !validity.is_null() {
        let word = *validity.add(row / 64);
        if (word >> (row % 64)) & 1 == 0 {
            return Value::Null;
        }
    }
    match type_code {
        T_BIGINT => {
            let data = ffi::duckdb_vector_get_data(vec) as *const i64;
            Value::BigInt(*data.add(row))
        }
        T_INTEGER => {
            let data = ffi::duckdb_vector_get_data(vec) as *const i32;
            Value::Int(*data.add(row))
        }
        T_DOUBLE => {
            let data = ffi::duckdb_vector_get_data(vec) as *const f64;
            Value::Double(*data.add(row))
        }
        T_BOOLEAN => {
            let data = ffi::duckdb_vector_get_data(vec) as *const u8;
            Value::Boolean(*data.add(row) != 0)
        }
        T_VARCHAR => {
            let data = ffi::duckdb_vector_get_data(vec) as *const duckdb_string_t;
            let mut t = *data.add(row);
            Value::Text(DuckString::new(&mut t).as_str().into_owned())
        }
        T_BLOB => {
            let data = ffi::duckdb_vector_get_data(vec) as *const duckdb_string_t;
            let mut t = *data.add(row);
            Value::Blob(DuckString::new(&mut t).as_bytes().to_vec())
        }
        _ => Value::Null,
    }
}

// -- C API callbacks ----------------------------------------------------------

/// Format a caught-panic payload as a one-line reason. Mirrors the
/// `panic_msg` helper in `reg_duckdb.rs`; kept local to avoid a
/// cross-module `pub` on a purely defensive helper.
fn panic_reason(p: &(dyn std::any::Any + Send + 'static)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Sweep-8 FIX 5: run an aggregate callback body under `catch_unwind` so a
/// Rust panic cannot unwind across the DuckDB `extern "C"` boundary (which
/// would abort the host process — duckdb-rs installs no catch of its own).
/// A caught panic is logged to stderr and surfaced to DuckDB via
/// `duckdb_aggregate_function_set_error`, then the callback returns without
/// further work — mirroring the shape of `reg_duckdb::agg_guard` and the
/// `ExtensionInstance::drop` T1-7 catch_unwind pattern.
///
/// # Safety
/// `info` must be the valid `duckdb_function_info` for the running aggregate.
unsafe fn agg_guard(info: ffi::duckdb_function_info, fn_name: &str, f: impl FnOnce()) {
    if let Err(p) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        let reason = panic_reason(&*p);
        eprintln!("[ducklink] delegating_agg::{fn_name} panicked: {reason}");
        if let Ok(c) =
            std::ffi::CString::new(format!("delegating_agg::{fn_name} panicked: {reason}"))
        {
            ffi::duckdb_aggregate_function_set_error(info, c.as_ptr());
        }
    }
}

pub unsafe extern "C" fn state_size(_info: ffi::duckdb_function_info) -> ffi::idx_t {
    std::mem::size_of::<*mut DelegatingAggState>() as ffi::idx_t
}

pub unsafe extern "C" fn init(_info: ffi::duckdb_function_info, state: ffi::duckdb_aggregate_state) {
    let slot = state as *mut *mut DelegatingAggState;
    *slot = Box::into_raw(Box::new(DelegatingAggState::default()));
}

pub unsafe extern "C" fn update(
    info: ffi::duckdb_function_info,
    input: ffi::duckdb_data_chunk,
    states: *mut ffi::duckdb_aggregate_state,
) {
    // Sweep-8 FIX 5: wrap in catch_unwind via agg_guard so an unexpected
    // panic in `extract_value` (malformed vector) or elsewhere fails one
    // query rather than aborting the host process. Body extracted to
    // `update_impl` — >50 lines inlined would obscure the guard.
    agg_guard(info, "update", || update_impl(info, input, states));
}

unsafe fn update_impl(
    info: ffi::duckdb_function_info,
    input: ffi::duckdb_data_chunk,
    states: *mut ffi::duckdb_aggregate_state,
) {
    // Hold the DuckDB executor lock while this dispatcher runs — a
    // re-entrant `NativeServices::query()` routed back to this thread
    // would deadlock. The RAII guard restores the previous value even
    // if `extract_value` panics on a malformed vector.
    let _guard = crate::engine::QueryReentrancyGuard::new();
    let extra = &*(ffi::duckdb_aggregate_function_get_extra_info(info) as *const DelegatingAggExtra);
    let n = ffi::duckdb_data_chunk_get_size(input) as usize;
    if n == 0 {
        return;
    }
    // Fetch each column's vector once — the vector handle is stable for
    // the whole chunk, so `duckdb_data_chunk_get_vector` on every row
    // would be pure overhead.
    let vectors: Vec<ffi::duckdb_vector> = (0..extra.arg_types.len())
        .map(|c| ffi::duckdb_data_chunk_get_vector(input, c as u64))
        .collect();

    // Detect the "shared-state" call pattern DuckDB uses for sorted
    // aggregates and full-partition window frames. In that mode, DuckDB
    // hands us one initialised state at slot 0 and leaves slots 1..N
    // as uninitialised memory (NOT zero-filled) — so probing them
    // reads garbage pointers and dereferencing crashes on unmapped
    // pages. The DuckDB signal is: slot 0 is a valid pointer, slot 1
    // (when present) is a NULL sentinel. When we see that shape, fold
    // every input row into slot 0.
    if n >= 1 {
        let slot0 = *states as *mut *mut DelegatingAggState;
        let single_state = if !slot0.is_null() && n >= 2 {
            let slot1 = *states.add(1) as *mut *mut DelegatingAggState;
            slot1.is_null()
        } else {
            false
        };
        if single_state {
            let state_ptr = *slot0;
            if !state_ptr.is_null() {
                for r in 0..n {
                    let row_values: Vec<Value> = extra
                        .arg_types
                        .iter()
                        .enumerate()
                        .map(|(c, &t)| extract_value(vectors[c], r, t))
                        .collect();
                    (*state_ptr).rows.push(row_values);
                }
            }
            return;
        }
    }

    for r in 0..n {
        let slot = *states.add(r) as *mut *mut DelegatingAggState;
        if slot.is_null() {
            // Sparse per-row slot — nothing to do for this row.
            continue;
        }
        let state_ptr = *slot;
        if state_ptr.is_null() {
            continue;
        }
        let row_values: Vec<Value> = extra
            .arg_types
            .iter()
            .enumerate()
            .map(|(c, &t)| extract_value(vectors[c], r, t))
            .collect();
        (*state_ptr).rows.push(row_values);
    }
}

pub unsafe extern "C" fn combine(
    info: ffi::duckdb_function_info,
    source: *mut ffi::duckdb_aggregate_state,
    target: *mut ffi::duckdb_aggregate_state,
    count: ffi::idx_t,
) {
    // Sweep-8 FIX 5: catch_unwind wrapper; see `update` for rationale.
    agg_guard(info, "combine", || {
        // Same re-entrancy reasoning as `update`; combine runs on DuckDB
        // executor threads holding the same lock.
        let _guard = crate::engine::QueryReentrancyGuard::new();
        for i in 0..count as usize {
            let src_slot = *source.add(i) as *mut *mut DelegatingAggState;
            let tgt_slot = *target.add(i) as *mut *mut DelegatingAggState;
            if src_slot.is_null() || tgt_slot.is_null() {
                continue;
            }
            let src = *src_slot;
            let tgt = *tgt_slot;
            if src.is_null() || tgt.is_null() {
                continue;
            }
            // Clone rather than `mem::take`: DuckDB's window framework uses
            // segment trees over partial states and may combine the same
            // source into multiple targets (a stable segment-tree node feeds
            // several overlapping frames). A destructive move would leave the
            // source empty on the second visit, so later frames see only the
            // rows appended after the destructive combine — the bug the OVER
            // (...) snapshot test surfaced.
            (*tgt).rows.extend((*src).rows.iter().cloned());
        }
    });
}

pub unsafe extern "C" fn finalize(
    info: ffi::duckdb_function_info,
    source: *mut ffi::duckdb_aggregate_state,
    result: ffi::duckdb_vector,
    count: ffi::idx_t,
    offset: ffi::idx_t,
) {
    // Sweep-8 FIX 5: catch_unwind wrapper; see `update` for rationale.
    agg_guard(info, "finalize", || {
        // Finalize runs the nested delegation query on `extra.con` while
        // still on a DuckDB executor thread; a guest-side re-entrant call
        // into `NativeServices::query()` from this thread would deadlock.
        let _guard = crate::engine::QueryReentrancyGuard::new();
        let extra =
            &*(ffi::duckdb_aggregate_function_get_extra_info(info) as *const DelegatingAggExtra);
        ffi::duckdb_vector_ensure_validity_writable(result);
        let base = offset as usize;

        for i in 0..count as usize {
            // `i` indexes the STATE array (source[0..count]); the RESULT
            // vector slot is `base + i`. Window aggregates split a result
            // vector across multiple finalize calls with different offsets;
            // ignoring `offset` writes past the vector's end / into other
            // frames' cells.
            let out_idx = base + i;
            let slot = *source.add(i) as *mut *mut DelegatingAggState;
            if slot.is_null() {
                write_result_null(result, out_idx);
                continue;
            }
            let state_ptr = *slot;
            if state_ptr.is_null() {
                write_result_null(result, out_idx);
                continue;
            }
            let state = &mut *state_ptr;

            if state.rows.is_empty() {
                write_result_null(result, out_idx);
                continue;
            }

            if let Err(e) = run_delegation_and_write(extra, &state.rows, result, out_idx) {
                eprintln!(
                    "[ducklink] delegating aggregate '{}' finalize failed: {e}",
                    extra.target_name
                );
                write_result_null(result, out_idx);
            }
        }
    });
}

/// Build the nested delegation SQL, invoke it on the sibling connection,
/// read the result cell as the target Rust type, and write it into
/// `result[idx]`. Returns Err on any of those steps.
fn run_delegation_and_write(
    extra: &DelegatingAggExtra,
    rows: &[Vec<Value>],
    result: ffi::duckdb_vector,
    idx: usize,
) -> duckdb::Result<()> {
    // Detect columns whose value is constant across every row of this
    // group. Target aggregates like `string_agg` and community's
    // `crypto_hash_agg` require certain arguments to be constants at
    // bind time — DuckDB refuses to bind them if we pass a per-row
    // placeholder. Inlining constants as literals in the SQL sidesteps
    // the "must be a constant" error and matches the semantics users
    // wrote in the outer query (a constant arg IS constant per group).
    let constant_cols: Vec<Option<String>> = (0..extra.arg_types.len())
        .map(|col| constant_literal_for_col(rows, col, extra.arg_types[col]))
        .collect();
    let sql = build_delegation_sql(extra, rows.len(), &constant_cols);
    // Only bind params for the columns that aren't constant.
    let flat: Vec<&dyn ToSql> = rows
        .iter()
        .flat_map(|row| {
            row.iter()
                .enumerate()
                .filter(|(c, _)| constant_cols[*c].is_none())
                .map(|(_, v)| v as &dyn ToSql)
        })
        .collect();
    let params: &[&dyn ToSql] = &flat;
    let con = extra.con.lock().unwrap_or_else(|e| e.into_inner());

    // Read the result cell using the RETURN TYPE's native Rust type.
    // Every arm reads through `Option<T>` so a legitimate NULL from the
    // target aggregate — e.g. `sum` of an all-null column, or an empty
    // window frame after `EXCLUDE CURRENT ROW` — writes a NULL rather
    // than erroring from a null→T coercion.
    unsafe {
        match extra.return_type {
            T_BIGINT => {
                let v: Option<i64> = con.query_row(&sql, params, |r| r.get(0))?;
                match v {
                    Some(v) => {
                        let data = ffi::duckdb_vector_get_data(result) as *mut i64;
                        *data.add(idx) = v;
                        set_valid(result, idx);
                    }
                    None => write_result_null(result, idx),
                }
            }
            T_INTEGER => {
                let v: Option<i32> = con.query_row(&sql, params, |r| r.get(0))?;
                match v {
                    Some(v) => {
                        let data = ffi::duckdb_vector_get_data(result) as *mut i32;
                        *data.add(idx) = v;
                        set_valid(result, idx);
                    }
                    None => write_result_null(result, idx),
                }
            }
            T_DOUBLE => {
                let v: Option<f64> = con.query_row(&sql, params, |r| r.get(0))?;
                match v {
                    Some(v) => {
                        let data = ffi::duckdb_vector_get_data(result) as *mut f64;
                        *data.add(idx) = v;
                        set_valid(result, idx);
                    }
                    None => write_result_null(result, idx),
                }
            }
            T_BOOLEAN => {
                let v: Option<bool> = con.query_row(&sql, params, |r| r.get(0))?;
                match v {
                    Some(v) => {
                        let data = ffi::duckdb_vector_get_data(result) as *mut u8;
                        *data.add(idx) = v as u8;
                        set_valid(result, idx);
                    }
                    None => write_result_null(result, idx),
                }
            }
            T_VARCHAR => {
                let v: Option<String> = con.query_row(&sql, params, |r| r.get(0))?;
                match v {
                    Some(v) => {
                        let bytes = v.as_bytes();
                        ffi::duckdb_vector_assign_string_element_len(
                            result,
                            idx as u64,
                            bytes.as_ptr() as *const std::os::raw::c_char,
                            bytes.len() as u64,
                        );
                        set_valid(result, idx);
                    }
                    None => write_result_null(result, idx),
                }
            }
            T_BLOB => {
                let v: Option<Vec<u8>> = con.query_row(&sql, params, |r| r.get(0))?;
                match v {
                    Some(v) => {
                        ffi::duckdb_vector_assign_string_element_len(
                            result,
                            idx as u64,
                            v.as_ptr() as *const std::os::raw::c_char,
                            v.len() as u64,
                        );
                        set_valid(result, idx);
                    }
                    None => write_result_null(result, idx),
                }
            }
            other => {
                eprintln!(
                    "[ducklink] delegating aggregate: unsupported return type code {other}"
                );
                write_result_null(result, idx);
            }
        }
    }
    Ok(())
}

/// Build the parameterised SQL. `constant_cols[c]` is `Some(literal)`
/// when column `c` was detected as constant across every row of this
/// group; that column is REMOVED from the FROM subquery and its literal
/// is spliced directly into the target function's arg list. Non-constant
/// columns contribute `?` placeholders to the VALUES clause and their
/// column names to the function call.
///
/// This matters because DuckDB's binder rejects "argument must be a
/// constant" requirements (e.g. `string_agg`'s separator, community
/// aggregates that pin a config arg) when the argument is a column
/// reference — even a constant-valued one. Only a LITERAL in the call
/// expression itself satisfies the binder.
///
/// Shape:
/// ```text
/// SELECT <target>(<lit_or_col>, <lit_or_col>, ...)
/// FROM (VALUES (?, ?, ...), ...) t(<varying_col_names>)
/// ```
fn build_delegation_sql(
    extra: &DelegatingAggExtra,
    nrows: usize,
    constant_cols: &[Option<String>],
) -> String {
    let ncols = extra.arg_types.len();

    // Names of the columns that varied — the ones actually present in the
    // FROM subquery. `varying_col_names[c]` is `Some("cN")` when column c
    // is passed as a `?` placeholder, `None` when it's a constant literal.
    let mut varying_col_names: Vec<Option<String>> = Vec::with_capacity(ncols);
    let mut varying_ct = 0usize;
    for cc in constant_cols.iter() {
        if cc.is_some() {
            varying_col_names.push(None);
        } else {
            varying_col_names.push(Some(format!("c{varying_ct}")));
            varying_ct += 1;
        }
    }

    // The target function's argument list: literals for constant columns,
    // column references for varying ones. Argument ORDER is preserved.
    let call_args: Vec<String> = (0..ncols)
        .map(|c| match &constant_cols[c] {
            Some(lit) => lit.clone(),
            None => varying_col_names[c].clone().unwrap(),
        })
        .collect();
    let call_args_str = call_args.join(", ");

    let mut sql = String::with_capacity(64 + nrows * (varying_ct * 3 + 3));
    sql.push_str("SELECT ");
    sql.push_str(&extra.target_name);
    sql.push('(');
    sql.push_str(&call_args_str);
    sql.push(')');

    if varying_ct == 0 {
        // All columns are constants — every row's target call is
        // identical, but the ROW COUNT still matters (sum, count, avg,
        // string_agg all depend on it). Emit `nrows` synthetic rows so
        // the target aggregate sees exactly the same cardinality it
        // would if the columns had varied.
        //
        // Reachable most often through window aggregates: a frame with
        // repeated values (a running sum over [20,20,10] visits the
        // (20,20) prefix) collapses to constant-inlining. Emitting one
        // synthetic row would produce `sum(20) FROM (VALUES (1))` = 20
        // instead of the correct 40 — the bug the OVER (...) snapshot
        // test surfaced.
        sql.push_str(" FROM (VALUES ");
        for r in 0..nrows {
            if r > 0 {
                sql.push(',');
            }
            sql.push_str("(1)");
        }
        sql.push(')');
        return sql;
    }

    sql.push_str(" FROM (VALUES ");
    for r in 0..nrows {
        if r > 0 {
            sql.push(',');
        }
        sql.push('(');
        let mut first = true;
        for c in 0..ncols {
            if constant_cols[c].is_some() {
                continue;
            }
            if !first {
                sql.push(',');
            }
            first = false;
            sql.push('?');
        }
        sql.push(')');
    }
    let varying_names: Vec<String> = varying_col_names.into_iter().flatten().collect();
    sql.push_str(") t(");
    sql.push_str(&varying_names.join(", "));
    sql.push(')');
    sql
}

/// If every row's value in `col` is equal, return a SQL literal for it;
/// otherwise return `None` (the caller then uses a `?` placeholder for
/// that column). Handles the "argument must be a constant" case for
/// target aggregates like `string_agg`.
fn constant_literal_for_col(rows: &[Vec<Value>], col: usize, type_code: u8) -> Option<String> {
    if rows.is_empty() {
        return None;
    }
    let first = &rows[0][col];
    for r in 1..rows.len() {
        if !values_equal(first, &rows[r][col]) {
            return None;
        }
    }
    value_to_sql_literal(first, type_code)
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Boolean(x), Value::Boolean(y)) => x == y,
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::BigInt(x), Value::BigInt(y)) => x == y,
        (Value::Double(x), Value::Double(y)) => x == y,
        (Value::Text(x), Value::Text(y)) => x == y,
        (Value::Blob(x), Value::Blob(y)) => x == y,
        _ => false,
    }
}

/// Render a `Value` as a SQL literal. Used only for constant-inlining;
/// only handles the types the delegating wrapper supports today.
fn value_to_sql_literal(v: &Value, type_code: u8) -> Option<String> {
    match (type_code, v) {
        (_, Value::Null) => Some("NULL".to_string()),
        (T_BOOLEAN, Value::Boolean(b)) => Some(if *b { "TRUE" } else { "FALSE" }.to_string()),
        (T_INTEGER, Value::Int(x)) => Some(x.to_string()),
        (T_BIGINT, Value::BigInt(x)) => Some(x.to_string()),
        (T_DOUBLE, Value::Double(x)) => Some(x.to_string()),
        (T_VARCHAR, Value::Text(s)) => Some(format!("'{}'", s.replace('\'', "''"))),
        (T_BLOB, Value::Blob(bytes)) => {
            // BLOB literals: `'\x01\x02'::BLOB`. Escape every byte as
            // `\xHH`. Only used for constants — a per-row varying BLOB
            // would need placeholder binding anyway.
            let mut out = String::from("'");
            for b in bytes {
                out.push_str(&format!("\\x{b:02X}"));
            }
            out.push_str("'::BLOB");
            Some(out)
        }
        _ => None,
    }
}

pub unsafe extern "C" fn destroy(state: *mut ffi::duckdb_aggregate_state, count: ffi::idx_t) {
    for i in 0..count as usize {
        let slot = *state.add(i) as *mut *mut DelegatingAggState;
        if slot.is_null() {
            continue;
        }
        let ptr = *slot;
        if !ptr.is_null() {
            drop(Box::from_raw(ptr));
        }
    }
}

pub unsafe extern "C" fn extra_destroy(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut DelegatingAggExtra));
    }
}

// -- helpers ------------------------------------------------------------------

unsafe fn write_result_null(result: ffi::duckdb_vector, idx: usize) {
    let validity = ffi::duckdb_vector_get_validity(result) as *mut u64;
    if validity.is_null() {
        return;
    }
    let word = validity.add(idx / 64);
    let bit = 1u64 << (idx % 64);
    *word &= !bit;
}

unsafe fn set_valid(result: ffi::duckdb_vector, idx: usize) {
    let validity = ffi::duckdb_vector_get_validity(result) as *mut u64;
    if validity.is_null() {
        return;
    }
    let word = validity.add(idx / 64);
    let bit = 1u64 << (idx % 64);
    *word |= bit;
}

// -- public registration API --------------------------------------------------

/// Register a real C-API `AggregateFunction` under `alias_name` that
/// delegates to `target_name`. Users can then call `alias_name(...)`
/// with any modifier and DuckDB's binder handles them; delegation happens
/// per-group inside `finalize`.
///
/// # Safety
/// `raw_con` must be a valid, live `duckdb_connection`. `sibling` must be
/// a duckdb-rs Connection to the SAME database.
pub unsafe fn register_delegating_aggregate(
    raw_con: ffi::duckdb_connection,
    alias_name: &str,
    target_name: &str,
    arg_types: Vec<u8>,
    return_type: u8,
    sibling: Arc<Mutex<Connection>>,
) -> Result<(), String> {
    if !crate::catalog::is_safe_identifier(alias_name)
        || !crate::catalog::is_safe_identifier(target_name)
    {
        return Err(format!(
            "delegating aggregate: identifiers must match [A-Za-z0-9_]+ \
             (alias='{alias_name}', target='{target_name}')"
        ));
    }

    let func = ffi::duckdb_create_aggregate_function();
    let cname = std::ffi::CString::new(alias_name).map_err(|e| e.to_string())?;
    ffi::duckdb_aggregate_function_set_name(func, cname.as_ptr());

    for &code in &arg_types {
        let mut arg = ffi::duckdb_create_logical_type(duckdb_type_of(code));
        ffi::duckdb_aggregate_function_add_parameter(func, arg);
        ffi::duckdb_destroy_logical_type(&mut arg);
    }
    let mut ret = ffi::duckdb_create_logical_type(duckdb_type_of(return_type));
    ffi::duckdb_aggregate_function_set_return_type(func, ret);
    ffi::duckdb_destroy_logical_type(&mut ret);

    let extra = Box::into_raw(Box::new(DelegatingAggExtra {
        target_name: target_name.to_string(),
        arg_types,
        return_type,
        con: sibling,
    })) as *mut c_void;
    ffi::duckdb_aggregate_function_set_extra_info(func, extra, Some(extra_destroy));

    ffi::duckdb_aggregate_function_set_functions(
        func,
        Some(state_size),
        Some(init),
        Some(update),
        Some(combine),
        Some(finalize),
    );
    ffi::duckdb_aggregate_function_set_destructor(func, Some(destroy));

    let rc = ffi::duckdb_register_aggregate_function(raw_con, func);
    let mut func_mut = func;
    ffi::duckdb_destroy_aggregate_function(&mut func_mut);
    if rc != ffi::DuckDBSuccess {
        return Err(format!(
            "duckdb_register_aggregate_function('{alias_name}') failed"
        ));
    }
    Ok(())
}
