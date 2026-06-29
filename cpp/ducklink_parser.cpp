//===----------------------------------------------------------------------===//
// ducklink_parser.cpp
//
// The advanced-tier PARSER shim: a DuckDB ParserExtension that, when the
// built-in parser rejects a statement, offers the statement text to the loaded
// components' parser-dispatch.call-parse (through the Rust bridge
// `ducklink_parser_try_rewrite`). A component that claims it returns a
// string->SQL rewrite; the shim's plan_function runs that rewrite on a fresh
// connection and streams the result, so `LOAD ggsql; VISUALIZE SELECT ...`
// becomes the rewritten chart query.
//
// Only the rewrite STRING crosses the WIT boundary (the by-value-safe parser
// path) — no DuckDB AST leaves the process. This mirrors the wasm core's
// execute-level parser interception, but as a real ParserExtension because the
// native loadable extension does not own DuckDB's execute loop.
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb.h"

#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/exception.hpp"

#include "ducklink_advanced.h"

#include <cstdio>
#include <string>

namespace duckdb {
namespace {

//! Parse data carried from parse_function to plan_function: the component's
//! string->SQL rewrite.
struct DucklinkParseData : public ParserExtensionParseData {
	explicit DucklinkParseData(string rewrite_p) : rewrite(std::move(rewrite_p)) {
	}
	string rewrite;
	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq<DucklinkParseData>(rewrite);
	}
	string ToString() const override {
		return rewrite;
	}
};

//! Bind data for the exec table function: the buffered rewrite result.
struct DucklinkExecBind : public TableFunctionData {
	vector<unique_ptr<DataChunk>> chunks;
	vector<LogicalType> types;
	vector<string> names;
};

struct DucklinkExecGlobal : public GlobalTableFunctionState {
	idx_t cursor = 0;
	idx_t MaxThreads() const override {
		return 1;
	}
};

//! Run the rewrite SQL (passed as the single VARCHAR parameter) on a fresh
//! connection, buffer the result, and declare its schema. Executing the rewrite
//! here keeps the ParserExtension's plan a plain scan over the buffered rows.
static unique_ptr<FunctionData> DucklinkExecBindFn(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto rewrite = input.inputs[0].GetValue<string>();
	auto result = make_uniq<DucklinkExecBind>();

	Connection con(*context.db);
	auto query_result = con.Query(rewrite);
	if (query_result->HasError()) {
		throw InvalidInputException("ducklink parser rewrite failed: %s", query_result->GetError());
	}
	result->types = query_result->types;
	result->names = query_result->names;
	while (true) {
		auto chunk = query_result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		result->chunks.push_back(std::move(chunk));
	}
	return_types = result->types;
	names = result->names;
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DucklinkExecInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<DucklinkExecGlobal>();
}

static void DucklinkExecFunc(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<DucklinkExecBind>();
	auto &gstate = data.global_state->Cast<DucklinkExecGlobal>();
	if (gstate.cursor >= bind.chunks.size()) {
		output.SetCardinality(0);
		return;
	}
	output.Reference(*bind.chunks[gstate.cursor]);
	gstate.cursor++;
}

//! parse_function: offer the rejected statement to the component parsers.
static ParserExtensionParseResult DucklinkParse(ParserExtensionInfo *, const string &query) {
	char *rewrite = ducklink_parser_try_rewrite(query.c_str());
	if (!rewrite) {
		return ParserExtensionParseResult(); // no component claimed it -> original error
	}
	string sql(rewrite);
	ducklink_adv_free(rewrite);
	if (sql.empty()) {
		return ParserExtensionParseResult();
	}
	return ParserExtensionParseResult(
	    unique_ptr<ParserExtensionParseData>(make_uniq<DucklinkParseData>(std::move(sql))));
}

//! plan_function: run the rewrite via the exec table function as a scan.
static ParserExtensionPlanResult DucklinkPlan(ParserExtensionInfo *, ClientContext &,
                                              unique_ptr<ParserExtensionParseData> parse_data) {
	auto &data = static_cast<DucklinkParseData &>(*parse_data);
	ParserExtensionPlanResult result;
	result.function = TableFunction("ducklink_parser_exec", {LogicalType::VARCHAR}, DucklinkExecFunc,
	                                DucklinkExecBindFn, DucklinkExecInit);
	result.parameters.push_back(Value(data.rewrite));
	result.requires_valid_transaction = false;
	result.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

class DucklinkParserExtension : public ParserExtension {
public:
	DucklinkParserExtension() {
		parse_function = DucklinkParse;
		plan_function = DucklinkPlan;
	}
};

} // namespace
} // namespace duckdb

//! Install the component-driven ParserExtension on `db`. Idempotent: a
//! process-wide guard avoids stacking duplicate extensions across LOADs.
extern "C" int32_t ducklink_register_parser(void *db) {
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
		ParserExtension::Register(config, DucklinkParserExtension());
		registered = true;
		return 0;
	} catch (const std::exception &e) {
		fprintf(stderr, "ducklink_register_parser failed: %s\n", e.what());
		return 1;
	} catch (...) {
		return 1;
	}
}
