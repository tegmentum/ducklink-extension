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
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
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
    const char *source_schema,
    const char *existing_name,
    const char *target_schema,
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
		// Schema arguments default to "main" when NULL — that matches the
		// pre-namespace behaviour so existing callers that pass NULL keep
		// working. `source_schema` is where we LOOK UP `existing_name`;
		// `target_schema` is where we REGISTER the alias `new_name`.
		const std::string src_schema = source_schema ? source_schema : DEFAULT_SCHEMA;
		const std::string tgt_schema = target_schema ? target_schema : DEFAULT_SCHEMA;
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

		// Route lookup vs registration to the right CATALOG:
		//   - LOOKUP for `existing_name` walks the SYSTEM catalog, because
		//     community extensions and every ducklink-loaded function
		//     register into `system.main` via `ExtensionLoader::RegisterFunction`.
		//   - REGISTRATION for `new_name` lands in the USER'S DEFAULT catalog
		//     (usually `memory` for an in-memory DB), so two-part references
		//     like `crypto.hash(x)` bind naturally without a `system.` prefix.
		//     A user's `information_schema` scan then also picks up the alias.
		auto &lookup_catalog = Catalog::GetSystemCatalog(context);
		const std::string &default_db = DatabaseManager::GetDefaultDatabase(context);
		auto &target_catalog = Catalog::GetCatalog(context, default_db);

		// `DuckSchemaEntry::AddEntryInternal` refuses to accept new entries
		// unless the surrounding `MetaTransaction` has this database
		// flagged as modified (see duck_schema_entry.cpp:110 — the check
		// exempts temp and system catalogs but errors on `memory`, which
		// is our target). Flag it explicitly before `CreateSchema` or
		// `CreateFunction` gets called.
		auto &target_attached = target_catalog.GetAttached();
		MetaTransaction::Get(context).ModifyDatabase(target_attached, DatabaseModificationType());

		// Ensure the target schema exists before we try to register into it.
		// A missing schema would make `CreateFunction` throw with a catalog
		// error; creating it up-front is the shim's CREATE-SCHEMA-IF-NOT-EXISTS
		// so callers don't have to think about it. `internal = false`: DuckDB
		// refuses to create internal entries in a non-system catalog
		// ("internal entries can only be created in the system catalog"),
		// and we're targeting the user's default catalog so the alias shows
		// up in `information_schema` alongside user-defined functions.
		if (tgt_schema != DEFAULT_SCHEMA) {
			CreateSchemaInfo schema_info;
			schema_info.catalog = default_db;
			schema_info.schema = tgt_schema;
			schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
			schema_info.internal = false;
			target_catalog.CreateSchema(context, schema_info);
		}

		// Prefer aggregate → scalar → table. The three catalog spaces are
		// disjoint by DuckDB convention (an extension registers a name in
		// exactly one of them), so this order just tests which space the
		// name lives in.
		int32_t rc = 0;
		try {
			// DuckDB's `Catalog::GetEntry(context, CatalogType, schema, name,
			// RETURN_NULL)` is NOT strict on the type filter in the "main"
			// schema: probing `AGGREGATE_FUNCTION_ENTRY` for a scalar-registered
			// name (e.g. `crypto_hash`) still returns the scalar entry, and
			// `Cast<AggregateFunctionCatalogEntry>` on that is a
			// `reinterpret_cast` that SIGSEGVs the moment aggregate-only
			// fields are touched. Probe each kind but only accept the entry
			// when its actual `type` matches the kind we asked for; treat any
			// mismatched-type return like a null.
			auto probe = [&](CatalogType want) -> optional_ptr<CatalogEntry> {
				try {
					auto e = lookup_catalog.GetEntry(context, want, src_schema, ex_name,
					                                 OnEntryNotFound::RETURN_NULL);
					if (e && e->type == want) {
						return e;
					}
					// Community's functions live in `system.main`; user
					// tables and their schemas live in the user's default
					// catalog. If the caller pointed us at a non-main source
					// schema, also try the target catalog — that's where
					// `DUCKLINK PREFIX c: crypto` (source == crypto, a
					// ducklink-owned schema in the default catalog) needs to
					// find its entries.
					if (src_schema != DEFAULT_SCHEMA) {
						auto e2 = target_catalog.GetEntry(context, want, src_schema, ex_name,
						                                  OnEntryNotFound::RETURN_NULL);
						if (e2 && e2->type == want) {
							return e2;
						}
					}
					return nullptr;
				} catch (...) {
					return nullptr;
				}
			};
			auto agg_entry = probe(CatalogType::AGGREGATE_FUNCTION_ENTRY);
			auto sc_entry  = agg_entry ? optional_ptr<CatalogEntry>(nullptr)
			                           : probe(CatalogType::SCALAR_FUNCTION_ENTRY);
			auto tbl_entry = (agg_entry || sc_entry)
			                     ? optional_ptr<CatalogEntry>(nullptr)
			                     : probe(CatalogType::TABLE_FUNCTION_ENTRY);

			if (agg_entry) {
				auto &afe = agg_entry->Cast<AggregateFunctionCatalogEntry>();
				CreateAggregateFunctionInfo info(
				    rename_set<AggregateFunctionSet, AggregateFunction>(afe.functions, new_nm));
				info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
				info.schema = tgt_schema;
				info.name = new_nm;
				info.catalog = default_db;
				// The CreateFunctionInfo constructors flip `internal = true`
				// so DuckDB registers built-in-style, system-only entries.
				// We're targeting the user's default catalog, so clear it —
				// aliases behave like user-defined functions.
				info.internal = false;
				target_catalog.CreateFunction(context, info);
				rc = 1;
			} else if (sc_entry) {
				auto &sfe = sc_entry->Cast<ScalarFunctionCatalogEntry>();
				CreateScalarFunctionInfo info(
				    rename_set<ScalarFunctionSet, ScalarFunction>(sfe.functions, new_nm));
				info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
				info.schema = tgt_schema;
				info.name = new_nm;
				info.catalog = default_db;
				// The CreateFunctionInfo constructors flip `internal = true`
				// so DuckDB registers built-in-style, system-only entries.
				// We're targeting the user's default catalog, so clear it —
				// aliases behave like user-defined functions.
				info.internal = false;
				target_catalog.CreateFunction(context, info);
				rc = 2;
			} else if (tbl_entry) {
				auto &tfe = tbl_entry->Cast<TableFunctionCatalogEntry>();
				CreateTableFunctionInfo info(
				    rename_set<TableFunctionSet, TableFunction>(tfe.functions, new_nm));
				info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
				info.schema = tgt_schema;
				info.name = new_nm;
				info.catalog = default_db;
				// The CreateFunctionInfo constructors flip `internal = true`
				// so DuckDB registers built-in-style, system-only entries.
				// We're targeting the user's default catalog, so clear it —
				// aliases behave like user-defined functions.
				info.internal = false;
				target_catalog.CreateFunction(context, info);
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
		msg += "' in schema '";
		msg += src_schema;
		msg += "'";
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
