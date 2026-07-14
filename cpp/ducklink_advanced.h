//===----------------------------------------------------------------------===//
// ducklink_advanced.h
//
// C ABI between the native advanced-tier C++ shim (cpp/ducklink_*.cpp, compiled
// against DuckDB's INTERNAL headers) and the extension's Rust side
// (src/advanced.rs), which routes each call to the embedded wasmtime engine and
// on to the owning component's parser / optimizer / table-stream dispatch.
//
// Mirrors the wasm core's bridge headers (wasm_optimizer_bridge.h /
// wasm_table_stream_bridge.h) but for the NATIVE loadable extension: the
// registration entrypoints are CALLED FROM Rust (with the duckdb_database the
// loader handed us); the `_try_rewrite` / `ducklink_ts_*` functions are
// IMPLEMENTED IN Rust and called from the C++ shim.
//===----------------------------------------------------------------------===//
#ifndef DUCKLINK_ADVANCED_H
#define DUCKLINK_ADVANCED_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

//===----------------------------------------------------------------------===//
// build-model probe (cpp/ducklink_advanced.cpp)
//===----------------------------------------------------------------------===//

// Proves the C++ shim links + is reachable from Rust and that the DuckDB
// internal C++ ABI resolves at load: dereferences the database to its DBConfig.
// Returns the configured maximum_threads (>= 0) on success, or a negative code
// if `db` could not be cast to a DatabaseInstance.
int32_t ducklink_advanced_probe(void *db);

//===----------------------------------------------------------------------===//
// registration entrypoints (C++; called from Rust at LOAD)
//===----------------------------------------------------------------------===//

// Install the component-driven ParserExtension on `db`. Idempotent (a
// process-wide guard avoids stacking duplicates). Returns 0 on success.
int32_t ducklink_register_parser(void *db);

// Install the component-driven OptimizerExtension on `db`. Idempotent. 0 on ok.
int32_t ducklink_register_optimizer(void *db);

// Register a streaming + FILTER-PUSHDOWN TableFunction named `name` (engine
// callback `handle`). `arg_type_codes` is a comma-joined list of duckdb_type
// codes for the positional args (may be empty). `cols_spec` is a '\n'-joined
// list of `name\t<duckdb_type_code>` lines for the emitted columns. Idempotent
// per (db, name). Returns 0 on success.
int32_t ducklink_register_filterable_table_function(void *db, const char *name, uint32_t handle,
                                                    const char *arg_type_codes,
                                                    const char *cols_spec);

//===----------------------------------------------------------------------===//
// Rust-implemented bridge fns the C++ shim calls
//===----------------------------------------------------------------------===//

// PARSER: offer the rejected statement `sql` to every declared component parser.
// Returns a malloc'd rewrite-SQL C string (free via ducklink_adv_free) if one
// claims it, or NULL if none do.
char *ducklink_parser_try_rewrite(const char *sql);

// OPTIMIZER: offer the flattened plan to every declared component rule.
// `plan_json` is the neutral plan-shape array
// (`[{"id":N,"op":"X","parent":P,"table":"T"?}, ...]`, same shape the wasm core
// ships); `query` is the source SQL (may be empty). Returns a malloc'd
// rewrite-SQL C string, or NULL if no rule rewrote it.
char *ducklink_optimizer_try_rewrite(const char *plan_json, const char *query);

// Free a C string returned by the `_try_rewrite` bridges / ducklink_ts_*.
void ducklink_adv_free(char *ptr);

// Sentinel prefix `ducklink_parser_try_rewrite` returns for a `LOAD WASM '<arg>'`
// statement (the rest of the string is the argument). The parser plan path
// recognizes it and calls `ducklink_load_wasm` with the live context db, rather
// than executing the returned string as SQL. Lock-step with LOAD_WASM_SENTINEL
// in src/advanced.rs.
#define DUCKLINK_LOAD_WASM_SENTINEL "\001ducklink:load-wasm\001"

// LOAD WASM: load the component named/at `path` into the live database `db`
// (a duckdb_database wrapping the parser's ClientContext) and register its
// functions. On success writes a malloc'd summary into *out_summary (free via
// ducklink_adv_free) and returns 0; on error writes the message and returns != 0.
int32_t ducklink_load_wasm(void *db, const char *path, char **out_summary);

// Sentinel prefix for a `LOAD NATIVE '<name>'` statement. Same pipeline as
// LOAD WASM but the plan path calls `ducklink_load_native` instead. Lock-step
// with LOAD_NATIVE_SENTINEL in src/advanced.rs.
#define DUCKLINK_LOAD_NATIVE_SENTINEL "\001ducklink:load-native\001"

// LOAD NATIVE: install (download + sha256-verify + cache) a native
// `.duckdb_extension` for the running host's platform + DuckDB version, then
// invoke DuckDB's LOAD on the resulting absolute path. Does NOT flip
// allow_unsigned_extensions — the user makes that trust decision themselves.
// On success writes a malloc'd summary into *out_summary (free via
// ducklink_adv_free) and returns 0; on error writes a human-readable message
// (including remediation for the common unsigned-signature case) and returns != 0.
int32_t ducklink_load_native(void *db, const char *name, char **out_summary);

//===----------------------------------------------------------------------===//
// catalog-alias shim (cpp/ducklink_alias.cpp) — community-native transparency
//===----------------------------------------------------------------------===//

// Register `existing_name` (a scalar / aggregate / table function already in
// the system catalog) under `new_name`, so DuckDB's binder resolves both to
// the same underlying function set. Ducklink's community-native branch calls
// this after `INSTALL <ext> FROM community; LOAD <ext>;` to present community's
// functions under ducklink's chosen names — aggregate delegation stays
// transparent under DISTINCT / FILTER / ORDER BY / window because the alias
// IS a real AggregateFunctionCatalogEntry, not a scalar-macro wrap.
//
// `conn` is a raw `duckdb_connection` (the shim casts it to the internal
// `ConnectionWrapper` to reach ClientContext). Returns:
//    1 = aggregate aliased
//    2 = scalar aliased
//    3 = table function aliased
//   -1 = null argument
//   -2 = invalid connection handle
//   -3 = no function of any kind found under `existing_name`
//   -4 = C++ exception (message in *out_err)
//   -5 = unknown exception (message in *out_err)
// On error, `*out_err` receives a malloc'd C string (free via
// ducklink_adv_free); on success, `*out_err` is left NULL.
int32_t ducklink_alias_function(void *conn, const char *existing_name,
                                const char *new_name, char **out_err);

//===----------------------------------------------------------------------===//
// table-stream bridge (filter pushdown) — mirrors wasm_table_stream_bridge.h
//===----------------------------------------------------------------------===//

// Compare-op codes, mirroring table-stream's `filter-op` enum order.
#define DUCKLINK_TS_OP_EQ 0
#define DUCKLINK_TS_OP_NE 1
#define DUCKLINK_TS_OP_LT 2
#define DUCKLINK_TS_OP_LE 3
#define DUCKLINK_TS_OP_GT 4
#define DUCKLINK_TS_OP_GE 5
#define DUCKLINK_TS_OP_IS_IN 6
#define DUCKLINK_TS_OP_IS_NULL 7
#define DUCKLINK_TS_OP_IS_NOT_NULL 8

// Value-type tags for a tagged constant (filter operand or bound argument).
#define DUCKLINK_TS_VAL_NONE 0
#define DUCKLINK_TS_VAL_BOOLEAN 1
#define DUCKLINK_TS_VAL_INT64 2
#define DUCKLINK_TS_VAL_FLOAT64 3
#define DUCKLINK_TS_VAL_TEXT 4

// A tagged scalar value crossing the C ABI (bound arg, or a filter operand).
typedef struct DucklinkTsValue {
	uint8_t value_type; // DUCKLINK_TS_VAL_*
	int64_t i64;        // INT64 / BOOLEAN (0/1)
	double f64;         // FLOAT64
	const char *text;   // TEXT (NUL-terminated, borrowed)
} DucklinkTsValue;

// One pushed-down predicate; `column` indexes the EMITTED (post-projection)
// schema. A scalar comparator carries one value; is-null / is-not-null carry
// zero; is-in carries N.
typedef struct DucklinkTsFilter {
	uint32_t column;
	uint8_t op; // DUCKLINK_TS_OP_*
	const DucklinkTsValue *values;
	uint32_t nvalues;
} DucklinkTsFilter;

// Open a streaming cursor for table fn `handle` with bound `args`, `projection`
// (real column indices in emit order; nproj==0 => all), and conjunctive
// `filters`. Returns a cursor handle, or 0 on error (ducklink_ts_last_error).
uint32_t ducklink_ts_open(uint32_t handle, const DucklinkTsValue *args, uint32_t nargs,
                          const uint32_t *projection, uint32_t nproj,
                          const DucklinkTsFilter *filters, uint32_t nfilt);

// Pull the next batch into `chunk` (a `duckdb_data_chunk` raw handle). Returns
// true if rows were written, false at EOF (chunk size set 0) or on error
// (ducklink_ts_last_error set).
bool ducklink_ts_fill(uint32_t handle, uint32_t cursor, void *chunk);

// Close + free a streaming cursor.
void ducklink_ts_close(uint32_t handle, uint32_t cursor);

// Most recent table-stream bridge error (owned by Rust; valid until the next
// bridge call). Empty C string when none.
const char *ducklink_ts_last_error(void);

#ifdef __cplusplus
}
#endif

#endif // DUCKLINK_ADVANCED_H
