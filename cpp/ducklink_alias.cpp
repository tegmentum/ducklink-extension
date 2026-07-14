//===----------------------------------------------------------------------===//
// ducklink_alias.cpp
//
// Advanced-tier catalog-alias shim: register an existing scalar / aggregate /
// table function catalog entry under a NEW name, so DuckDB's binder sees a
// real CatalogEntry (not a wrapping macro) at the alias. This is what makes
// aggregate delegation transparent — `DISTINCT`, `FILTER (WHERE …)`, `ORDER
// BY`, and window-context (`OVER (…)`) all work through the alias because it
// IS an aggregate to DuckDB, not a scalar macro composing list()+list_aggregate.
//
// The community-native branch of ducklink calls this after INSTALL + LOAD to
// present community's functions under ducklink's chosen names. Scalar / table
// aliases could equivalently be done via `CREATE OR REPLACE MACRO`, but going
// through the catalog for all three kinds keeps behaviour uniform (identical
// planner treatment) and lets one code path serve every function kind.
//
// Loaded internal C++ symbols (Catalog::GetSystemCatalog, Catalog::GetEntry,
// Catalog::CreateFunction, FunctionSet<T>, CreateAggregateFunctionInfo /
// CreateScalarFunctionInfo / CreateTableFunctionInfo, ConnectionWrapper, ...)
// are left UNDEFINED in the shim and resolved at LOAD against the host DuckDB
// process (v1.5.4), which exports all of them. Version drift is handled by
// the same advanced-tier version guard as the parser/optimizer shims.
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb.h"

#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "ducklink_advanced.h"

#include <cstdlib>
#include <cstring>
#include <exception>
#include <string>

namespace {

// Duplicate `s` into a malloc'd C string the caller frees with
// `ducklink_adv_free`. Returns NULL on OOM (caller treats as "no error message").
char *dup_c_str(const std::string &s) {
	char *p = static_cast<char *>(std::malloc(s.size() + 1));
	if (!p) {
		return nullptr;
	}
	std::memcpy(p, s.data(), s.size());
	p[s.size()] = '\0';
	return p;
}

// Copy every overload in `src` into a fresh, renamed derived FunctionSet.
// `SetT` must be the concrete leaf type (AggregateFunctionSet /
// ScalarFunctionSet / TableFunctionSet) — CreateAggregateFunctionInfo &c.
// take the leaf type by value, not the base FunctionSet<T> template, so we
// have to construct the leaf here. `FunT` is the corresponding function type
// (AggregateFunction / ScalarFunction / TableFunction). The per-function
// `name` field is also updated so any binder-produced error message refers
// to the alias rather than community's original.
template <class SetT, class FunT>
SetT rename_set(const duckdb::FunctionSet<FunT> &src, const std::string &new_name) {
	SetT out(new_name);
	for (const FunT &fn : src.functions) {
		FunT copy = fn;
		copy.name = new_name;
		out.AddFunction(std::move(copy));
	}
	return out;
}

} // namespace

extern "C" int32_t ducklink_alias_function(
    void *conn,
    const char *existing_name,
    const char *new_name,
    char **out_err
) {
	using namespace duckdb;

	if (out_err) {
		*out_err = nullptr;
	}
	if (!conn || !existing_name || !new_name) {
		if (out_err) {
			*out_err = dup_c_str("ducklink_alias_function: null argument");
		}
		return -1;
	}

	try {
		// `duckdb_connection` reinterprets straight to `duckdb::Connection *`
		// (matches every other C API TU under `src/main/capi/`, e.g.
		// `prepared-c.cpp`), so no wrapper indirection here.
		auto connection = reinterpret_cast<Connection *>(conn);
		if (!connection || !connection->context) {
			if (out_err) {
				*out_err = dup_c_str("ducklink_alias_function: invalid connection handle");
			}
			return -2;
		}
		auto &context = *connection->context;
		const std::string schema = DEFAULT_SCHEMA;
		const std::string ex_name = existing_name;
		const std::string new_nm = new_name;

		// `Catalog::CreateFunction(ClientContext&, ...)` requires an active
		// transaction on the context. In an auto-commit connection with no
		// query mid-flight there isn't one, so we open a fresh transaction
		// around the registration. Idempotent-friendly: if a caller already
		// wrapped us in a transaction we still commit (nested begins are a
		// no-op for our REPLACE_ON_CONFLICT semantics).
		const bool had_auto_commit = connection->IsAutoCommit();
		connection->BeginTransaction();

		auto &catalog = Catalog::GetSystemCatalog(context);

		// Prefer aggregate → scalar → table. The three catalog spaces are
		// disjoint by DuckDB convention (an extension registers a name in
		// exactly one of them), so this order just tests which space the
		// name lives in.
		int32_t rc = 0;
		try {
			if (auto entry = catalog.GetEntry(context, CatalogType::AGGREGATE_FUNCTION_ENTRY,
			                                  schema, ex_name, OnEntryNotFound::RETURN_NULL)) {
				auto &afe = entry->Cast<AggregateFunctionCatalogEntry>();
				CreateAggregateFunctionInfo info(
				    rename_set<AggregateFunctionSet, AggregateFunction>(afe.functions, new_nm));
				info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
				info.schema = schema;
				info.name = new_nm;
				catalog.CreateFunction(context, info);
				rc = 1;
			} else if (auto entry = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY,
			                                         schema, ex_name, OnEntryNotFound::RETURN_NULL)) {
				auto &sfe = entry->Cast<ScalarFunctionCatalogEntry>();
				CreateScalarFunctionInfo info(
				    rename_set<ScalarFunctionSet, ScalarFunction>(sfe.functions, new_nm));
				info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
				info.schema = schema;
				info.name = new_nm;
				catalog.CreateFunction(context, info);
				rc = 2;
			} else if (auto entry = catalog.GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY,
			                                        schema, ex_name, OnEntryNotFound::RETURN_NULL)) {
				auto &tfe = entry->Cast<TableFunctionCatalogEntry>();
				CreateTableFunctionInfo info(
				    rename_set<TableFunctionSet, TableFunction>(tfe.functions, new_nm));
				info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
				info.schema = schema;
				info.name = new_nm;
				catalog.CreateFunction(context, info);
				rc = 3;
			}
		} catch (...) {
			// Roll back before rethrowing so the connection isn't left with
			// a dangling transaction after an error.
			try { connection->Rollback(); } catch (...) {}
			if (had_auto_commit) connection->SetAutoCommit(true);
			throw;
		}

		connection->Commit();
		if (had_auto_commit) connection->SetAutoCommit(true);

		if (rc > 0) {
			return rc;
		}

		std::string msg = "ducklink_alias_function: no scalar/aggregate/table function named '";
		msg += ex_name;
		msg += "' in system catalog";
		if (out_err) {
			*out_err = dup_c_str(msg);
		}
		return -3;
	} catch (const std::exception &e) {
		if (out_err) {
			std::string msg = "ducklink_alias_function: ";
			msg += e.what();
			*out_err = dup_c_str(msg);
		}
		return -4;
	} catch (...) {
		if (out_err) {
			*out_err = dup_c_str("ducklink_alias_function: unknown exception");
		}
		return -5;
	}
}
