#include "core/glue_log_types.hpp"

#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

constexpr LogLevel GlueScanLogType::LEVEL;

static Value OptionalCount(optional_idx value) {
	return value.IsValid() ? Value::UBIGINT(value.GetIndex()) : Value(LogicalType::UBIGINT);
}

static Value OptionalString(const string &value) {
	return value.empty() ? Value(LogicalType::VARCHAR) : Value(value);
}

GlueScanLogType::GlueScanLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType GlueScanLogType::GetLogType() {
	child_list_t<LogicalType> children = {
	    {"table", LogicalType::VARCHAR},           {"scan_id", LogicalType::UBIGINT},
	    {"event", LogicalType::VARCHAR},           {"partitions", LogicalType::UBIGINT},
	    {"partitions_read", LogicalType::UBIGINT}, {"listings", LogicalType::UBIGINT},
	    {"files", LogicalType::UBIGINT},           {"duration_ms", LogicalType::DOUBLE},
	    {"detail", LogicalType::VARCHAR}};
	return LogicalType::STRUCT(children);
}

string GlueScanLogType::ConstructLogMessage(const GlueScanEvent &event) {
	child_list_t<Value> children = {
	    {"table", Value(event.table)},
	    {"scan_id", Value::UBIGINT(event.scan_id)},
	    {"event", Value(event.event)},
	    {"partitions", OptionalCount(event.partitions)},
	    {"partitions_read", OptionalCount(event.partitions_read)},
	    {"listings", OptionalCount(event.listings)},
	    {"files", OptionalCount(event.files)},
	    {"duration_ms", event.duration_ms < 0 ? Value(LogicalType::DOUBLE) : Value::DOUBLE(event.duration_ms)},
	    {"detail", OptionalString(event.detail)}};
	return Value::STRUCT(std::move(children)).ToString();
}

void RegisterGlueLogTypes(DatabaseInstance &db) {
	auto &log_manager = db.GetLogManager();
	log_manager.RegisterLogType(make_uniq<GlueScanLogType>());
}

// The operation is the X-Amz-Target header, which the HTTP log does not redact
unique_ptr<CreateMacroInfo> GetGlueRequestsMacro() {
	static const DefaultTableMacro macro = {DEFAULT_SCHEMA, "glue_requests", {nullptr}, {{nullptr, nullptr}}, R"(
SELECT timestamp, connection_id, transaction_id, query_id, thread_id,
       request.headers['x-amz-target'] AS operation,
       response.status AS status,
       response.reason AS reason,
       request.start_time AS start_time,
       request.duration_ms AS duration_ms
FROM duckdb_logs_parsed('HTTP')
WHERE starts_with(request.headers['x-amz-target'], 'AWSGlue.')
)"};
	return DefaultTableFunctionGenerator::CreateTableMacroInfo(macro);
}

} // namespace duckdb
