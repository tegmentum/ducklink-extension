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
#include "duckdb/parser/parser.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/helper.hpp"

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
//!
//! `LOAD WASM` special case: when the parser bridge returns the
//! DUCKLINK_LOAD_WASM_SENTINEL marker, the "rewrite" is not SQL — it carries the
//! component argument. We load the component into the LIVE database (the parser's
//! own `context.db`, wrapped as a `duckdb_database`) via `ducklink_load_wasm`,
//! registering its functions on the connection the statement runs on, and emit a
//! single `summary` row. This is what makes runtime loading work in the real
//! loadable host, where the init-captured handle cannot be re-connected later.
static unique_ptr<FunctionData> DucklinkExecBindFn(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto rewrite = input.inputs[0].GetValue<string>();
	auto result = make_uniq<DucklinkExecBind>();

	const string wasm_sentinel = DUCKLINK_LOAD_WASM_SENTINEL;
	const string native_sentinel = DUCKLINK_LOAD_NATIVE_SENTINEL;
	const string prefix_sentinel = DUCKLINK_PREFIX_SENTINEL;
	auto matches = [&](const string &prefix) { return rewrite.rfind(prefix, 0) == 0; };

	if (matches(prefix_sentinel)) {
		// DUCKLINK PREFIX payload is `{alias}\t{namespace}` — the tab is
		// illegal in identifiers so a split is unambiguous.
		string payload = rewrite.substr(prefix_sentinel.size());
		auto tab = payload.find('\t');
		if (tab == string::npos) {
			throw InvalidInputException("DUCKLINK PREFIX: malformed payload");
		}
		string alias = payload.substr(0, tab);
		string ns = payload.substr(tab + 1);
		DatabaseWrapper wrapper;
		wrapper.database = make_shared_ptr<DuckDB>(*context.db);
		char *summary = nullptr;
		int32_t rc = ducklink_prefix(reinterpret_cast<void *>(&wrapper), alias.c_str(),
		                             ns.c_str(), &summary);
		string msg = summary ? string(summary) : string("DUCKLINK PREFIX: no summary");
		if (summary) {
			ducklink_adv_free(summary);
		}
		if (rc != 0) {
			throw InvalidInputException("%s", msg);
		}
		result->types = {LogicalType::VARCHAR};
		result->names = {"summary"};
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(Allocator::DefaultAllocator(), result->types);
		chunk->SetValue(0, 0, Value(msg));
		chunk->SetCardinality(1);
		result->chunks.push_back(std::move(chunk));
		return_types = result->types;
		names = result->names;
		return std::move(result);
	}

	if (matches(wasm_sentinel) || matches(native_sentinel)) {
		const bool is_native = matches(native_sentinel);
		const string &sentinel = is_native ? native_sentinel : wasm_sentinel;
		const char *tag = is_native ? "LOAD NATIVE" : "LOAD WASM";
		string arg = rewrite.substr(sentinel.size());
		// Wrap the parser's own database instance as a stable-C duckdb_database so
		// the Rust loader registers on the SAME database this statement runs on.
		DatabaseWrapper wrapper;
		wrapper.database = make_shared_ptr<DuckDB>(*context.db);
		char *summary = nullptr;
		int32_t rc = is_native
		                 ? ducklink_load_native(reinterpret_cast<void *>(&wrapper), arg.c_str(), &summary)
		                 : ducklink_load_wasm(reinterpret_cast<void *>(&wrapper), arg.c_str(), &summary);
		string msg = summary ? string(summary) : string(tag) + ": no summary";
		if (summary) {
			ducklink_adv_free(summary);
		}
		if (rc != 0) {
			throw InvalidInputException("%s", msg);
		}
		result->types = {LogicalType::VARCHAR};
		result->names = {"summary"};
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(Allocator::DefaultAllocator(), result->types);
		chunk->SetValue(0, 0, Value(msg));
		chunk->SetCardinality(1);
		result->chunks.push_back(std::move(chunk));
		return_types = result->types;
		names = result->names;
		return std::move(result);
	}

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

//! parser_override: called BEFORE DuckDB's built-in parser sees the query.
//! Runs the Rust rewriter over `query`; if it finds ducklink's colon-syntax
//! (`c:hash(x)`), rewrites to `c.hash(x)` and hands the result to DuckDB's
//! own `Parser::ParseQuery`. When no rewrite happens, we return an empty
//! ParserOverrideResult so the built-in parser takes over unchanged.
//!
//! If the rewritten SQL somehow fails to parse (should never happen — we
//! only substitute `:` for `.`, both valid in a schema qualifier), we
//! fall through to the built-in parser with the ORIGINAL text so the user
//! sees an error message that points at what THEY wrote.
static ParserOverrideResult DucklinkParserOverride(ParserExtensionInfo *, const string &query,
                                                   ParserOptions &options) {
	char *rewritten_c = ducklink_parser_rewrite_colon(query.c_str());
	if (!rewritten_c) {
		return ParserOverrideResult(); // no rewrite -> DISPLAY_ORIGINAL_ERROR
	}
	string rewritten(rewritten_c);
	ducklink_adv_free(rewritten_c);
	try {
		Parser parser(options);
		parser.ParseQuery(rewritten);
		return ParserOverrideResult(std::move(parser.statements));
	} catch (...) {
		return ParserOverrideResult();
	}
}

class DucklinkParserExtension : public ParserExtension {
public:
	DucklinkParserExtension() {
		parse_function = DucklinkParse;
		plan_function = DucklinkPlan;
		parser_override = DucklinkParserOverride;
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

		// Enable the extension parser-override hook. DuckDB defaults to
		// DEFAULT_OVERRIDE which skips every registered `parser_override`
		// (parser.cpp:242); users would otherwise have to
		// `SET allow_parser_override_extension = 'FALLBACK_OVERRIDE'`
		// themselves. FALLBACK_OVERRIDE is safe: if our override returns
		// an empty result (nothing to rewrite), the built-in parser sees
		// the query unchanged. Idempotent — flipping it a second time is
		// a no-op.
		try {
			duckdb::Connection con(*wrapper->database);
			auto result = con.Query(
			    "SET allow_parser_override_extension = 'FALLBACK'");
			if (result->HasError()) {
				fprintf(stderr,
				        "[ducklink] failed to enable parser_override: %s\n",
				        result->GetError().c_str());
			}
		} catch (const std::exception &e) {
			fprintf(stderr, "[ducklink] enabling parser_override threw: %s\n",
			        e.what());
		}

		registered = true;
		return 0;
	} catch (const std::exception &e) {
		fprintf(stderr, "ducklink_register_parser failed: %s\n", e.what());
		return 1;
	} catch (...) {
		return 1;
	}
}
