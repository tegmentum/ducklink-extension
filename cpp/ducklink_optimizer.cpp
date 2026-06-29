//===----------------------------------------------------------------------===//
// ducklink_optimizer.cpp
//
// The advanced-tier OPTIMIZER shim: a component-driven OptimizerExtension. It
//   1. flattens the bound logical plan into a NEUTRAL JSON descriptor (operator
//      type names + parent links + the table name for GETs -- NOT a by-value
//      LogicalOperator tree; the WIT recursion wall),
//   2. offers it to every declared optimizer rule via the Rust bridge
//      `ducklink_optimizer_try_rewrite` (-> the components' optimizer-dispatch
//      .call-optimize),
//   3. if a rule returns a string->SQL REWRITE (the rewrite-query directive),
//      re-plans that SQL with a fresh Parser+Planner and replaces the plan.
//
// A direct port of the wasm core's wasm_component_optimizer.cpp to the native
// loadable extension. The plan crosses the boundary as JSON text; nothing
// DuckDB-internal leaks by value.
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb.h"

#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "ducklink_advanced.h"

#include <cstdio>
#include <string>

namespace duckdb {
namespace {

void JsonEscape(const string &in, std::string &out) {
	for (char c : in) {
		if (c == '"' || c == '\\') {
			out += '\\';
			out += c;
		} else if (c == '\n') {
			out += "\\n";
		} else {
			out += c;
		}
	}
}

void FlattenPlan(LogicalOperator &op, int &next_id, int parent, std::string &out, bool &first) {
	int my_id = next_id++;
	if (!first) {
		out += ",";
	}
	first = false;
	out += "{\"id\":" + std::to_string(my_id) + ",\"op\":\"";
	JsonEscape(op.GetName(), out);
	out += "\",\"parent\":" + std::to_string(parent);
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto table = get.GetTable();
		if (table) {
			out += ",\"table\":\"";
			JsonEscape(table->name, out);
			out += "\"";
		}
	}
	out += "}";
	for (auto &child : op.children) {
		FlattenPlan(*child, next_id, my_id, out, first);
	}
}

//! The component-driven optimizer rule.
class DucklinkComponentOptimizer : public OptimizerExtension {
public:
	DucklinkComponentOptimizer() {
		optimize_function = Optimize;
	}
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		if (!plan) {
			return;
		}
		// 1. flatten the plan to neutral JSON.
		std::string json = "[";
		int next_id = 0;
		bool first = true;
		FlattenPlan(*plan, next_id, -1, json, first);
		json += "]";

		// 2. offer it to the declared component rules.
		char *rewrite = ducklink_optimizer_try_rewrite(json.c_str(), "");
		if (!rewrite) {
			return; // no rule claimed it
		}
		std::string sql(rewrite);
		ducklink_adv_free(rewrite);
		if (sql.empty()) {
			return;
		}

		// 3. re-plan the rewrite SQL and replace the plan in place.
		try {
			Parser parser;
			parser.ParseQuery(sql);
			if (parser.statements.empty()) {
				return;
			}
			Planner planner(input.context);
			planner.CreatePlan(std::move(parser.statements[0]));
			if (planner.plan) {
				plan = std::move(planner.plan);
			}
		} catch (std::exception &e) {
			fprintf(stderr, "[ducklink_optimizer] rewrite re-plan failed (%s); keeping original plan\n",
			        e.what());
		}
	}
};

} // namespace
} // namespace duckdb

//! Install the component-driven optimizer rule on `db`. Idempotent.
extern "C" int32_t ducklink_register_optimizer(void *db) {
	using namespace duckdb;
	if (!db) {
		return 1;
	}
	static bool registered = false;
	if (registered) {
		return 0;
	}
	try {
		auto wrapper = reinterpret_cast<DatabaseWrapper *>(db);
		if (!wrapper || !wrapper->database) {
			return 1;
		}
		auto &instance = *wrapper->database->instance;
		auto &config = DBConfig::GetConfig(instance);
		OptimizerExtension::Register(config, DucklinkComponentOptimizer());
		registered = true;
		return 0;
	} catch (const std::exception &e) {
		fprintf(stderr, "ducklink_register_optimizer failed: %s\n", e.what());
		return 1;
	} catch (...) {
		return 1;
	}
}
