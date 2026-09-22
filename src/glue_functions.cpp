#include "glue_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "glue_api.hpp"
#include "storage/glue_catalog.hpp"

namespace duckdb {

namespace {

struct GlueGetTableResponseBindData : public TableFunctionData {
	GlueTableInfo table;
	string raw_json;
};

struct GlueGetTableResponseState : public GlobalTableFunctionState {
	bool done = false;
};

unique_ptr<FunctionData> GlueGetTableResponseBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto qualified = QualifiedName::Parse(input.inputs[0].GetValue<string>());
	if (qualified.Catalog().empty() || qualified.Schema().empty()) {
		throw BinderException("glue_get_table_response expects a fully qualified table name: "
		                      "'<catalog>.<schema>.<table>', got '%s'",
		                      input.inputs[0].GetValue<string>());
	}
	auto catalog = Catalog::GetCatalogEntry(context, qualified.Catalog());
	if (!catalog) {
		throw BinderException("Catalog '%s' does not exist", qualified.Catalog().GetIdentifierName());
	}
	if (catalog->GetCatalogType() != "glue") {
		throw BinderException("glue_get_table_response only works on tables of a Glue catalog, '%s' is a %s catalog",
		                      qualified.Catalog().GetIdentifierName(), catalog->GetCatalogType());
	}
	auto &glue_catalog = catalog->Cast<GlueCatalog>();

	auto result = make_uniq<GlueGetTableResponseBindData>();
	if (!GlueAPI::GetTable(context, glue_catalog, qualified.Schema().GetIdentifierName(),
	                       qualified.Name().GetIdentifierName(), result->table, &result->raw_json)) {
		throw CatalogException("Table '%s.%s' does not exist in Glue catalog '%s'",
		                       qualified.Schema().GetIdentifierName(), qualified.Name().GetIdentifierName(),
		                       qualified.Catalog().GetIdentifierName());
	}

	auto column_type = LogicalType::LIST(LogicalType::STRUCT(
	    {{"name", LogicalType::VARCHAR}, {"type", LogicalType::VARCHAR}, {"comment", LogicalType::VARCHAR}}));
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	// StorageDescriptor.SortColumns, with the direction resolved. Glue's SortOrder integer is in the 'response' column.
	auto sort_column_type =
	    LogicalType::LIST(LogicalType::STRUCT({{"column", LogicalType::VARCHAR}, {"direction", LogicalType::VARCHAR}}));
	names = {"database_name", "table_name", "table_type",       "glue_table_type", "location",
	         "serde_library", "columns",    "partition_keys",   "bucket_columns",  "number_of_buckets",
	         "sort_columns",  "parameters", "serde_parameters", "response"};
	return_types = {LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                column_type,
	                column_type,
	                LogicalType::LIST(LogicalType::VARCHAR),
	                LogicalType::INTEGER,
	                sort_column_type,
	                map_type,
	                map_type,
	                LogicalType::VARIANT()};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GlueGetTableResponseInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlueGetTableResponseState>();
}

Value ColumnsToValue(const vector<GlueColumn> &columns, const LogicalType &list_type) {
	vector<Value> entries;
	for (auto &column : columns) {
		entries.push_back(
		    Value::STRUCT({{"name", Value(column.name)},
		                   {"type", Value(column.type)},
		                   {"comment", column.comment.empty() ? Value(LogicalType::VARCHAR) : Value(column.comment)}}));
	}
	return Value::LIST(ListType::GetChildType(list_type), std::move(entries));
}

Value StringsToValue(const vector<string> &strings) {
	vector<Value> entries;
	for (auto &entry : strings) {
		entries.emplace_back(entry);
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(entries));
}

Value SortColumnsToValue(const vector<GlueColumn> &sort_columns, const LogicalType &list_type) {
	vector<Value> entries;
	for (auto &sort_column : sort_columns) {
		entries.push_back(Value::STRUCT(
		    {{"column", Value(sort_column.name)}, {"direction", Value(sort_column.DescribeSortOrder())}}));
	}
	return Value::LIST(ListType::GetChildType(list_type), std::move(entries));
}

Value MapToValue(const unordered_map<string, string> &map) {
	vector<Value> keys;
	vector<Value> values;
	for (auto &entry : map) {
		keys.emplace_back(entry.first);
		values.emplace_back(entry.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

void GlueGetTableResponseScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueGetTableResponseState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GlueGetTableResponseBindData>();
	auto &table = bind_data.table;

	output.SetValue(0, 0, Value(table.database_name));
	output.SetValue(1, 0, Value(table.name));
	output.SetValue(2, 0, Value(GlueTableFormatToString(table.GetFormat())));
	output.SetValue(3, 0, Value(table.glue_table_type));
	output.SetValue(4, 0, Value(table.location));
	output.SetValue(5, 0, Value(table.serde_library));
	output.SetValue(6, 0, ColumnsToValue(table.columns, output.data[6].GetType()));
	output.SetValue(7, 0, ColumnsToValue(table.partition_keys, output.data[7].GetType()));
	output.SetValue(8, 0, StringsToValue(table.bucket_columns));
	output.SetValue(9, 0, Value::INTEGER(table.number_of_buckets));
	output.SetValue(10, 0, SortColumnsToValue(table.sort_columns, output.data[10].GetType()));
	output.SetValue(11, 0, MapToValue(table.parameters));
	output.SetValue(12, 0, MapToValue(table.serde_parameters));

	// The complete Glue Table object: JSON as serialized by the AWS SDK, cast to VARIANT
	Vector json(LogicalType::JSON(), 1);
	json.SetValue(0, Value(bind_data.raw_json));
	VectorOperations::Cast(context, json, output.data[13], 1);

	output.SetCardinality(1);
}

} // namespace

TableFunction GetGlueGetTableResponseFunction() {
	TableFunction function("glue_get_table_response", {LogicalType::VARCHAR}, GlueGetTableResponseScan,
	                       GlueGetTableResponseBind, GlueGetTableResponseInit);
	return function;
}

} // namespace duckdb
