//===----------------------------------------------------------------------===//
// ducklink_table_stream.cpp
//
// The advanced-tier TABLE-FUNCTION FILTER-PUSHDOWN shim: a streaming DuckDB
// TableFunction (filter_pushdown = true) for a component that declared a
// filterable table function via the additive table-stream marker. A port of the
// wasm core's wasm_table_stream.cpp to the native loadable extension.
//
// The SQL WHERE's pushed TableFilter set is mapped to the neutral, by-value-safe
// filter descriptor (column index + op + constant) and driven to the owning
// component's table-stream-dispatch.call-table-open-filtered through the Rust
// bridge (ducklink_ts_open / fill / close). A component that honors the filters
// prunes rows at the source; one that ignores them stays correct (DuckDB
// re-checks above the scan).
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb.h"

#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/common/exception.hpp"

#include "ducklink_advanced.h"

#include <cstdio>
#include <functional>
#include <string>
#include <vector>

namespace duckdb {
namespace {

//! reg-bridge type code (mirrors src/reg_duckdb.rs `type_code`) -> LogicalType.
static LogicalType DucklinkTsTypeCodeToLogical(uint32_t code) {
	switch (code) {
	case 0:
		return LogicalType::BIGINT;
	case 1:
		return LogicalType::UBIGINT;
	case 2:
		return LogicalType::DOUBLE;
	case 3:
		return LogicalType::BOOLEAN;
	case 4:
		return LogicalType::VARCHAR;
	case 5:
		return LogicalType::BLOB;
	case 6:
		return LogicalType::TINYINT;
	case 7:
		return LogicalType::SMALLINT;
	case 8:
		return LogicalType::INTEGER;
	case 9:
		return LogicalType::UTINYINT;
	case 10:
		return LogicalType::USMALLINT;
	case 11:
		return LogicalType::UINTEGER;
	case 12:
		return LogicalType::FLOAT;
	case 13:
		return LogicalType::TIMESTAMP;
	case 14:
		return LogicalType::DATE;
	case 15:
		return LogicalType::TIME;
	case 16:
		return LogicalType::TIMESTAMP_TZ;
	case 17:
		return LogicalType::DECIMAL(18, 3);
	case 18:
		return LogicalType::INTERVAL;
	case 19:
		return LogicalType::UUID;
	default:
		return LogicalType::VARCHAR;
	}
}

static std::string DucklinkTsLastError() {
	const char *msg = ducklink_ts_last_error();
	return msg ? std::string(msg) : std::string("unknown ducklink table-stream error");
}

static vector<std::string> SplitLines(const char *raw) {
	vector<std::string> out;
	if (!raw) {
		return out;
	}
	std::string s(raw);
	if (s.empty()) {
		return out;
	}
	size_t start = 0;
	while (true) {
		size_t pos = s.find('\n', start);
		if (pos == std::string::npos) {
			out.push_back(s.substr(start));
			break;
		}
		out.push_back(s.substr(start, pos - start));
		start = pos + 1;
	}
	return out;
}

static vector<std::string> SplitComma(const char *raw) {
	vector<std::string> out;
	if (!raw) {
		return out;
	}
	std::string s(raw);
	if (s.empty()) {
		return out;
	}
	size_t start = 0;
	while (true) {
		size_t pos = s.find(',', start);
		if (pos == std::string::npos) {
			out.push_back(s.substr(start));
			break;
		}
		out.push_back(s.substr(start, pos - start));
		start = pos + 1;
	}
	return out;
}

//! Map a DuckDB comparison ExpressionType to a bridge ts-op code.
static bool MapCompareOp(ExpressionType type, uint8_t &out_op) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		out_op = DUCKLINK_TS_OP_EQ;
		return true;
	case ExpressionType::COMPARE_NOTEQUAL:
		out_op = DUCKLINK_TS_OP_NE;
		return true;
	case ExpressionType::COMPARE_LESSTHAN:
		out_op = DUCKLINK_TS_OP_LT;
		return true;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		out_op = DUCKLINK_TS_OP_LE;
		return true;
	case ExpressionType::COMPARE_GREATERTHAN:
		out_op = DUCKLINK_TS_OP_GT;
		return true;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		out_op = DUCKLINK_TS_OP_GE;
		return true;
	default:
		return false;
	}
}

//! Fill a DucklinkTsValue from a DuckDB Value; `text_storage` keeps any VARCHAR
//! alive for the open call. Returns false on a type we don't ship.
static bool FillValue(const Value &constant, DucklinkTsValue &out, std::string &text_storage) {
	out.value_type = DUCKLINK_TS_VAL_NONE;
	out.i64 = 0;
	out.f64 = 0.0;
	out.text = nullptr;
	if (constant.IsNull()) {
		return false;
	}
	switch (constant.type().id()) {
	case LogicalTypeId::BOOLEAN:
		out.value_type = DUCKLINK_TS_VAL_BOOLEAN;
		out.i64 = BooleanValue::Get(constant) ? 1 : 0;
		return true;
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		out.value_type = DUCKLINK_TS_VAL_INT64;
		out.i64 = constant.GetValue<int64_t>();
		return true;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		out.value_type = DUCKLINK_TS_VAL_FLOAT64;
		out.f64 = constant.GetValue<double>();
		return true;
	case LogicalTypeId::VARCHAR:
		text_storage = StringValue::Get(constant);
		out.value_type = DUCKLINK_TS_VAL_TEXT;
		out.text = text_storage.c_str();
		return true;
	default:
		return false;
	}
}

//! Stashed on the TableFunction: the engine callback handle + emitted schema.
struct DucklinkTsInfo : public TableFunctionInfo {
	uint32_t handle = 0;
	vector<string> names;
	vector<LogicalType> types;
};

//! One bound argument value, owned (text kept alive through init's open call).
struct DucklinkTsArg {
	uint8_t value_type = DUCKLINK_TS_VAL_NONE;
	int64_t i64 = 0;
	double f64 = 0.0;
	std::string text;
};

struct DucklinkTsBindData : public TableFunctionData {
	uint32_t handle = 0;
	vector<string> names;
	vector<LogicalType> types;
	vector<DucklinkTsArg> args;
};

struct DucklinkTsGlobalState : public GlobalTableFunctionState {
	uint32_t handle = 0;
	uint32_t cursor = 0;
	bool finished = false;

	~DucklinkTsGlobalState() override {
		if (cursor != 0) {
			ducklink_ts_close(handle, cursor);
			cursor = 0;
		}
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

//! Resolve a projected position (index INTO column_ids) to the real column index,
//! skipping virtual/rowid columns.
static bool ResolveColumn(const vector<column_t> &column_ids, idx_t projected_pos,
                          uint32_t &out_real_column) {
	if (projected_pos >= column_ids.size()) {
		return false;
	}
	column_t cid = column_ids[projected_pos];
	if (cid == COLUMN_IDENTIFIER_ROW_ID) {
		return false;
	}
	out_real_column = static_cast<uint32_t>(cid);
	return true;
}

static unique_ptr<FunctionData> DucklinkTsBind(ClientContext &, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto &info = input.info->Cast<DucklinkTsInfo>();
	for (idx_t i = 0; i < info.names.size(); i++) {
		names.push_back(info.names[i]);
		return_types.push_back(info.types[i]);
	}
	auto result = make_uniq<DucklinkTsBindData>();
	result->handle = info.handle;
	result->names = info.names;
	result->types = info.types;
	for (auto &val : input.inputs) {
		DucklinkTsArg a;
		std::string text;
		DucklinkTsValue tagged;
		if (FillValue(val, tagged, text)) {
			a.value_type = tagged.value_type;
			a.i64 = tagged.i64;
			a.f64 = tagged.f64;
			a.text = text;
		}
		result->args.push_back(std::move(a));
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DucklinkTsInitGlobal(ClientContext &,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DucklinkTsBindData>();
	auto state = make_uniq<DucklinkTsGlobalState>();
	state->handle = bind_data.handle;

	// Bound argument values -> tagged C values.
	vector<DucklinkTsValue> args;
	args.reserve(bind_data.args.size());
	for (auto &a : bind_data.args) {
		DucklinkTsValue v;
		v.value_type = a.value_type;
		v.i64 = a.i64;
		v.f64 = a.f64;
		v.text = (a.value_type == DUCKLINK_TS_VAL_TEXT) ? a.text.c_str() : nullptr;
		args.push_back(v);
	}

	// Projection: column_ids in emit order -> real column indices.
	vector<uint32_t> projection;
	projection.reserve(input.column_ids.size());
	for (idx_t i = 0; i < input.column_ids.size(); i++) {
		uint32_t real_col;
		if (ResolveColumn(input.column_ids, i, real_col)) {
			projection.push_back(real_col);
		}
	}

	// Filters: input.filters maps (index INTO column_ids) -> TableFilter. Recurse
	// through CONJUNCTION_AND so every AND-ed clause is pushed (DuckDB removes the
	// above-scan filter for pushed predicates), and translate IN / null checks.
	struct TsClause {
		uint32_t column = 0;
		uint8_t op = DUCKLINK_TS_OP_EQ;
		vector<DucklinkTsValue> operands;
		vector<std::string> texts;
	};
	vector<TsClause> clauses;

	std::function<void(uint32_t, const TableFilter &)> collect =
	    [&](uint32_t real_col, const TableFilter &tf) {
		    switch (tf.filter_type) {
		    case TableFilterType::CONSTANT_COMPARISON: {
			    auto &cf = tf.Cast<ConstantFilter>();
			    TsClause c;
			    c.column = real_col;
			    if (!MapCompareOp(cf.comparison_type, c.op)) {
				    return;
			    }
			    c.texts.resize(1);
			    DucklinkTsValue v;
			    if (!FillValue(cf.constant, v, c.texts[0])) {
				    return;
			    }
			    c.operands.push_back(v);
			    clauses.push_back(std::move(c));
			    break;
		    }
		    case TableFilterType::IS_NULL: {
			    TsClause c;
			    c.column = real_col;
			    c.op = DUCKLINK_TS_OP_IS_NULL;
			    clauses.push_back(std::move(c));
			    break;
		    }
		    case TableFilterType::IS_NOT_NULL: {
			    TsClause c;
			    c.column = real_col;
			    c.op = DUCKLINK_TS_OP_IS_NOT_NULL;
			    clauses.push_back(std::move(c));
			    break;
		    }
		    case TableFilterType::CONJUNCTION_AND: {
			    auto &conj = tf.Cast<ConjunctionAndFilter>();
			    for (auto &child : conj.child_filters) {
				    collect(real_col, *child);
			    }
			    break;
		    }
		    case TableFilterType::IN_FILTER: {
			    auto &inf = tf.Cast<InFilter>();
			    TsClause c;
			    c.column = real_col;
			    c.op = DUCKLINK_TS_OP_IS_IN;
			    c.texts.resize(inf.values.size());
			    bool ok = true;
			    idx_t i = 0;
			    for (auto &val : inf.values) {
				    DucklinkTsValue v;
				    if (!FillValue(val, v, c.texts[i])) {
					    ok = false;
					    break;
				    }
				    c.operands.push_back(v);
				    i++;
			    }
			    if (ok && !c.operands.empty()) {
				    clauses.push_back(std::move(c));
			    }
			    break;
		    }
		    default:
			    break;
		    }
	    };

	if (input.filters) {
		for (auto &entry : input.filters->filters) {
			idx_t projected_pos = entry.first;
			uint32_t real_col;
			if (!ResolveColumn(input.column_ids, projected_pos, real_col)) {
				continue;
			}
			collect(real_col, *entry.second);
		}
	}

	// Flatten clauses into a stable operand pool + filter array.
	idx_t total_operands = 0;
	for (auto &c : clauses) {
		total_operands += c.operands.size();
	}
	vector<DucklinkTsValue> operand_pool;
	operand_pool.reserve(total_operands);
	vector<DucklinkTsFilter> filters;
	filters.reserve(clauses.size());
	for (auto &c : clauses) {
		DucklinkTsFilter f;
		f.column = c.column;
		f.op = c.op;
		f.nvalues = static_cast<uint32_t>(c.operands.size());
		f.values = c.operands.empty() ? nullptr : operand_pool.data() + operand_pool.size();
		for (idx_t i = 0; i < c.operands.size(); i++) {
			DucklinkTsValue v = c.operands[i];
			if (v.value_type == DUCKLINK_TS_VAL_TEXT) {
				v.text = c.texts[i].c_str();
			}
			operand_pool.push_back(v);
		}
		filters.push_back(f);
	}

	const DucklinkTsValue *args_ptr = args.empty() ? nullptr : args.data();
	const uint32_t *proj_ptr = projection.empty() ? nullptr : projection.data();
	const DucklinkTsFilter *filt_ptr = filters.empty() ? nullptr : filters.data();

	uint32_t cursor = ducklink_ts_open(bind_data.handle, args_ptr, static_cast<uint32_t>(args.size()),
	                                   proj_ptr, static_cast<uint32_t>(projection.size()), filt_ptr,
	                                   static_cast<uint32_t>(filters.size()));
	if (cursor == 0) {
		throw IOException("ducklink table-stream open failed: %s", DucklinkTsLastError());
	}
	state->cursor = cursor;
	return std::move(state);
}

static void DucklinkTsFunction(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<DucklinkTsGlobalState>();
	if (gstate.finished || gstate.cursor == 0) {
		output.SetCardinality(0);
		return;
	}
	auto chunk_handle = reinterpret_cast<void *>(&output);
	bool has_rows = ducklink_ts_fill(gstate.handle, gstate.cursor, chunk_handle);
	if (!has_rows) {
		const char *err = ducklink_ts_last_error();
		if (err && err[0] != '\0') {
			gstate.finished = true;
			throw IOException("ducklink table-stream fill failed: %s", std::string(err));
		}
		gstate.finished = true;
		output.SetCardinality(0);
	}
}

} // namespace
} // namespace duckdb

//! Register a streaming + filter-pushdown TableFunction. Idempotent per (db, name).
extern "C" int32_t ducklink_register_filterable_table_function(void *db, const char *name, uint32_t handle,
                                                               const char *arg_type_codes,
                                                               const char *cols_spec) {
	using namespace duckdb;
	if (!db || !name) {
		return 1;
	}
	try {
		auto wrapper = reinterpret_cast<DatabaseWrapper *>(db);
		if (!wrapper || !wrapper->database) {
			return 1;
		}
		auto &instance = *wrapper->database->instance;

		vector<LogicalType> arg_types;
		for (auto &code : SplitComma(arg_type_codes)) {
			if (code.empty()) {
				continue;
			}
			arg_types.push_back(DucklinkTsTypeCodeToLogical(static_cast<uint32_t>(std::stoul(code))));
		}

		auto info = make_shared_ptr<DucklinkTsInfo>();
		info->handle = handle;
		for (auto &line : SplitLines(cols_spec)) {
			size_t tab = line.find('\t');
			if (tab == std::string::npos) {
				continue;
			}
			std::string col_name = line.substr(0, tab);
			uint32_t code = static_cast<uint32_t>(std::stoul(line.substr(tab + 1)));
			info->names.push_back(col_name);
			info->types.push_back(DucklinkTsTypeCodeToLogical(code));
		}

		std::string fn_name(name);
		TableFunction tf(fn_name, arg_types, DucklinkTsFunction, DucklinkTsBind, DucklinkTsInitGlobal);
		tf.projection_pushdown = true;
		tf.filter_pushdown = true;
		tf.function_info = info;

		CreateTableFunctionInfo create_info(tf);
		create_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;

		auto &system_catalog = Catalog::GetSystemCatalog(instance);
		auto transaction = CatalogTransaction::GetSystemTransaction(instance);
		system_catalog.CreateTableFunction(transaction, create_info);
		return 0;
	} catch (const std::exception &e) {
		fprintf(stderr, "ducklink_register_filterable_table_function failed: %s\n", e.what());
		return 1;
	} catch (...) {
		return 1;
	}
}
