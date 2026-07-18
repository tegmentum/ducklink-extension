//! Delegating aggregate wrappers — the C-API-only path to full aggregate
//! transparency for community-native aliases.
//!
//! Ducklink registers a REAL `AggregateFunction` via the DuckDB C API under
//! ducklink's chosen name (e.g. `crypto.hash_agg`), whose implementation
//! accumulates input values into a per-group state and then, at
//! `finalize`, runs a nested SQL query invoking the community aggregate on
//! the accumulated values.
//!
//! Because ducklink's wrapper IS a real aggregate to DuckDB's binder, every
//! aggregate modifier (`DISTINCT`, `FILTER`, `ORDER BY`, `OVER`) works
//! transparently: the binder deduplicates / filters / orders / windows the
//! input rows BEFORE our `update` callback sees them, so our accumulator
//! ends up holding exactly the values DuckDB has determined the target
//! aggregate should see. `finalize` then invokes the community aggregate
//! on that pre-processed input via a nested query on a sibling connection.
//!
//! # Prototype scope (this file)
//!
//! This first cut hardwires the wrapper to a single signature: one
//! `BIGINT` input, one `BIGINT` output, delegating to a built-in target
//! aggregate (`sum` by default). Enough to prove the delegation mechanism
//! end-to-end against all four modifier shapes without needing an
//! INSTALL FROM community; generalization to arbitrary community-aggregate
//! signatures follows once the prototype is green.

use std::os::raw::c_void;
use std::sync::{Arc, Mutex};

use duckdb::ffi;
use duckdb::Connection;

/// Per-registration metadata retrieved from every callback via
/// `duckdb_aggregate_function_get_extra_info`.
///
/// The sibling `Connection` is held in an `Arc<Mutex<...>>` so `finalize`
/// (which may run concurrently across groups on the same aggregate
/// instance) serialises access to the underlying DuckDB connection while
/// keeping the accumulator lock-free per group.
pub struct DelegatingAggExtra {
    /// Bare name of the community aggregate ducklink is delegating to
    /// (e.g. `"sum"`, `"crypto_hash_agg"`). Spliced into the nested
    /// `finalize` query verbatim; must have been validated by the caller
    /// via `crate::catalog::is_safe_identifier`.
    pub target_name: String,
    /// A sibling connection on the same DuckDB database, opened once at
    /// registration and reused for every `finalize` nested query. Avoids
    /// contention on the runtime's main connection.
    pub con: Arc<Mutex<Connection>>,
}

/// Per-group accumulator state. `AtomicUsize` isn't required here —
/// DuckDB serialises per-group state access — so a plain `Vec` is fine.
///
/// The state stores raw `i64` values in the prototype; when the wrapper
/// generalises to arbitrary types, this becomes a `Vec<duckdb::types::Value>`
/// with per-column columnar storage.
#[derive(Default)]
pub struct DelegatingAggState {
    pub values: Vec<i64>,
}

/// Size of the per-group state slot DuckDB should allocate. The slot
/// itself holds a `*mut DelegatingAggState`; the actual state is
/// heap-allocated once and pointed to by that slot.
pub unsafe extern "C" fn state_size(_info: ffi::duckdb_function_info) -> ffi::idx_t {
    std::mem::size_of::<*mut DelegatingAggState>() as ffi::idx_t
}

/// Called once per group before its first `update`. Allocates a fresh
/// state on the heap and writes a pointer into the DuckDB-owned slot.
pub unsafe extern "C" fn init(_info: ffi::duckdb_function_info, state: ffi::duckdb_aggregate_state) {
    let slot = state as *mut *mut DelegatingAggState;
    *slot = Box::into_raw(Box::new(DelegatingAggState::default()));
}

/// Called for a batch of input rows. Each row's state pointer is at
/// `*states.add(row_index)`; multiple rows may point at the SAME state
/// (grouped aggregation). Copies the input column's raw `i64` values
/// into each row's per-group `Vec`.
pub unsafe extern "C" fn update(
    _info: ffi::duckdb_function_info,
    input: ffi::duckdb_data_chunk,
    states: *mut ffi::duckdb_aggregate_state,
) {
    let n = ffi::duckdb_data_chunk_get_size(input) as usize;
    if n == 0 {
        return;
    }
    // Prototype signature: one BIGINT column at position 0.
    let vec = ffi::duckdb_data_chunk_get_vector(input, 0);
    let data = ffi::duckdb_vector_get_data(vec) as *const i64;
    let validity = ffi::duckdb_vector_get_validity(vec) as *const u64;
    for r in 0..n {
        // Skip NULLs — mirrors the way DuckDB's own SUM behaves.
        if !validity.is_null() {
            let word = *validity.add(r / 64);
            if (word >> (r % 64)) & 1 == 0 {
                continue;
            }
        }
        let value = *data.add(r);
        // The slot at `states.add(r)` is a `duckdb_aggregate_state`
        // (opaque pointer). DuckDB allocated `state_size` bytes at that
        // address; our `init` wrote a `*mut DelegatingAggState` into
        // those bytes. Reach the state via one indirection.
        let slot = *states.add(r) as *mut *mut DelegatingAggState;
        if slot.is_null() {
            // Some C-API calling conventions (sorted-aggregate,
            // window-framework) pass a states array with sparse entries
            // where only certain rows carry a valid state pointer.
            // Skip rather than crash; the wrapper's prototype scope
            // targets the flat / grouped / distinct / filtered paths.
            continue;
        }
        let state_ptr = *slot;
        (*state_ptr).values.push(value);
    }
}

/// Merge one state into another. Called when DuckDB parallelises the
/// aggregation and needs to reduce partial results. For our list-of-values
/// accumulator, this is just an extend.
pub unsafe extern "C" fn combine(
    _info: ffi::duckdb_function_info,
    source: *mut ffi::duckdb_aggregate_state,
    target: *mut ffi::duckdb_aggregate_state,
    count: ffi::idx_t,
) {
    for i in 0..count as usize {
        let src_slot = *source.add(i) as *mut *mut DelegatingAggState;
        let tgt_slot = *target.add(i) as *mut *mut DelegatingAggState;
        let src = *src_slot;
        let tgt = *tgt_slot;
        let src_values = std::mem::take(&mut (*src).values);
        (*tgt).values.extend(src_values);
    }
}

/// Produce one output row per group. This is where the delegation
/// happens: for each group, we invoke the target community aggregate on
/// the accumulated values via a nested SQL query on the sibling connection.
///
/// Prototype: hardwired to invoke `SELECT <target>(unnest(?::BIGINT[]))`
/// on each group's `Vec<i64>`. Result assumed to be a `BIGINT`.
pub unsafe extern "C" fn finalize(
    info: ffi::duckdb_function_info,
    source: *mut ffi::duckdb_aggregate_state,
    result: ffi::duckdb_vector,
    count: ffi::idx_t,
    offset: ffi::idx_t,
) {
    let extra = &*(ffi::duckdb_aggregate_function_get_extra_info(info) as *const DelegatingAggExtra);
    let out_data = ffi::duckdb_vector_get_data(result) as *mut i64;
    // Every extension writing to a vector should also touch the validity
    // mask; DuckDB defaults new vectors' validity to all-invalid.
    ffi::duckdb_vector_ensure_validity_writable(result);
    let out_validity = ffi::duckdb_vector_get_validity(result) as *mut u64;

    for i in 0..count as usize {
        let slot = *source.add(i) as *mut *mut DelegatingAggState;
        if slot.is_null() {
            continue;
        }
        let state_ptr = *slot;
        let state = &mut *state_ptr;
        let out_idx = offset as usize + i;

        if state.values.is_empty() {
            // Empty group → NULL (matches SUM semantics).
            set_bit(out_validity, out_idx, false);
            continue;
        }

        // Build a nested query: SELECT <target>(v) FROM (VALUES (10),(20),...) t(v).
        // Values are i64 — validated on the way in — so string
        // interpolation is safe. This dodges duckdb-rs's current
        // limitation ("binding List parameters is not yet supported"); a
        // future duckdb-rs version could let us switch to a bound
        // BIGINT[] parameter without touching the outer contract.
        let values_clause: String = state
            .values
            .iter()
            .map(|v| format!("({v})"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT {t}(v) FROM (VALUES {values_clause}) x(v)",
            t = extra.target_name
        );

        let con = extra.con.lock().unwrap_or_else(|e| e.into_inner());
        let query_result: Result<i64, _> = con.query_row(&sql, [], |r| r.get(0));
        match query_result {
            Ok(v) => {
                *out_data.add(out_idx) = v;
                set_bit(out_validity, out_idx, true);
            }
            Err(e) => {
                eprintln!("[ducklink] delegating aggregate '{}' finalize failed: {e}", extra.target_name);
                set_bit(out_validity, out_idx, false);
            }
        }
    }
}

/// Free the per-group state's heap allocation. Signature matches
/// `duckdb_aggregate_destroy_t`.
pub unsafe extern "C" fn destroy(
    state: *mut ffi::duckdb_aggregate_state,
    count: ffi::idx_t,
) {
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

/// Free the boxed extra info when the aggregate function is unregistered.
pub unsafe extern "C" fn extra_destroy(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut DelegatingAggExtra));
    }
}

fn set_bit(mask: *mut u64, idx: usize, value: bool) {
    if mask.is_null() {
        return;
    }
    unsafe {
        let word = mask.add(idx / 64);
        let bit = 1u64 << (idx % 64);
        if value {
            *word |= bit;
        } else {
            *word &= !bit;
        }
    }
}

/// Register `<alias_schema>.<alias_name>(BIGINT) -> BIGINT` as a real
/// aggregate delegating to `target_name(BIGINT) -> BIGINT`.
///
/// Prototype signature only. In the generalised form the caller supplies
/// arg / return types from `duckdb_functions()`, and this fn builds the
/// C-API metadata to match.
///
/// # Safety
/// `raw_con` must be a valid, live `duckdb_connection`. `sibling` must be
/// a valid duckdb-rs Connection to the SAME database.
pub unsafe fn register_bigint_delegating_aggregate(
    raw_con: ffi::duckdb_connection,
    alias_schema: Option<&str>,
    alias_name: &str,
    target_name: &str,
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
    if let Some(s) = alias_schema {
        if !crate::catalog::is_safe_identifier(s) {
            return Err(format!("delegating aggregate: schema='{s}' unsafe"));
        }
    }

    let func = ffi::duckdb_create_aggregate_function();
    // Aggregate name — `set_schema` in DuckDB C API is limited; instead
    // for the prototype we register a fully-qualified name. If a schema
    // is present it's spliced into `set_name` via a `schema.name` form;
    // DuckDB accepts that in some builds. If not, follow-up work moves
    // to `duckdb_aggregate_function_set_schema` when we generalise.
    let bare = std::ffi::CString::new(alias_name).map_err(|e| e.to_string())?;
    ffi::duckdb_aggregate_function_set_name(func, bare.as_ptr());

    let mut arg = ffi::duckdb_create_logical_type(ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT);
    ffi::duckdb_aggregate_function_add_parameter(func, arg);
    ffi::duckdb_destroy_logical_type(&mut arg);
    let mut ret = ffi::duckdb_create_logical_type(ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT);
    ffi::duckdb_aggregate_function_set_return_type(func, ret);
    ffi::duckdb_destroy_logical_type(&mut ret);

    let extra = Box::into_raw(Box::new(DelegatingAggExtra {
        target_name: target_name.to_string(),
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
