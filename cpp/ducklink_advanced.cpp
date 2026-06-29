//===----------------------------------------------------------------------===//
// ducklink_advanced.cpp
//
// The build-model probe for the advanced native dispatch tier. Proves a C++ TU
// compiled against DuckDB's INTERNAL headers links into the loadable extension
// and that the internal C++ ABI resolves at LOAD time against the host DuckDB
// (which exports those symbols). The per-tier shims (parser / optimizer /
// table-stream) live in their own TUs and share this build configuration.
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb.h"

#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

#include "ducklink_advanced.h"

extern "C" int32_t ducklink_advanced_probe(void *db) {
	using namespace duckdb;
	if (!db) {
		return -1;
	}
	auto wrapper = reinterpret_cast<DatabaseWrapper *>(db);
	if (!wrapper || !wrapper->database) {
		return -2;
	}
	auto &instance = *wrapper->database->instance;
	auto &config = DBConfig::GetConfig(instance);
	return static_cast<int32_t>(config.options.maximum_threads);
}
