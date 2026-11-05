#include "functions/glue_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"

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
	auto name = ResolveGlueTableName(context, "glue_get_table_response", input.inputs[0].GetValue<string>());
	auto &catalog = Catalog::GetCatalog(context, name.Catalog()).Cast<GlueCatalog>();
	auto result = make_uniq<GlueGetTableResponseBindData>();
	auto database_name = name.Schema().GetIdentifierName();
	auto table_name = name.Name().GetIdentifierName();
	// Always fetches: the 'response' column carries the raw reply, which only a fresh call has. The fetched
	// table warms the metadata cache for the binds that follow.
	GlueTableInfo info;
	if (!GlueAPI::GetTable(context, catalog, database_name, table_name, info, &result->raw_json)) {
		throw CatalogException("Table '%s.%s' does not exist in Glue catalog '%s'", database_name, table_name,
		                       name.Catalog().GetIdentifierName());
	}
	result->table = info;
	// Of() is null in a bare statement (nothing started the catalog's transaction); Get() starts it
	auto &cache = *GlueTransaction::Get(context, catalog).cache;
	auto fetched = make_shared_ptr<const GlueTableInfo>(std::move(info));
	cache.GetTable(database_name, table_name, [&fetched]() { return fetched; });

	auto column_type = GlueColumnsType();
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	names = {"database_name", "table_name",     "table_type", "glue_table_type",  "location", "serde_library",
	         "columns",       "partition_keys", "parameters", "serde_parameters", "response"};
	return_types = {LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                column_type,
	                column_type,
	                map_type,
	                map_type,
	                LogicalType::VARIANT()};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GlueGetTableResponseInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlueGetTableResponseState>();
}

void GlueGetTableResponseScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueGetTableResponseState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GlueGetTableResponseBindData>();
	auto &table = bind_data.table;

	output.data[0].Append(Value(table.schema_name));
	output.data[1].Append(Value(table.name));
	output.data[2].Append(Value(table.GetFormat().ToString()));
	output.data[3].Append(Value(table.glue_table_type));
	output.data[4].Append(Value(table.location));
	output.data[5].Append(Value(table.serde_library));
	output.data[6].Append(GlueColumnsToValue(table.columns));
	output.data[7].Append(GlueColumnsToValue(table.partition_keys));
	output.data[8].Append(GlueParametersToValue(table.parameters));
	output.data[9].Append(GlueParametersToValue(table.serde_parameters));

	// The complete Glue Table object: JSON as serialized by the AWS SDK, cast to VARIANT
	Vector json(LogicalType::JSON(), 1);
	json.SetValue(0, Value(bind_data.raw_json));
	VectorOperations::Cast(context, json, output.data[10], 1);

	output.CheckCardinality(1);
}

struct GlueGetDatabaseResponseBindData : public TableFunctionData {
	GlueDatabaseInfo database;
	string raw_json;
};

unique_ptr<FunctionData> GlueGetDatabaseResponseBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto components = QualifiedName::ParseComponents(input.inputs[0].GetValue<string>());
	if (components.size() == 1) {
		// an unqualified database name: resolve it the way a query would (search path, default catalog)
		auto &schema = Catalog::GetSchema(context, Identifier(), components[0]);
		components = {schema.ParentCatalog().GetName(), Identifier(schema.name)};
	}
	if (components.size() != 2) {
		throw BinderException("glue_get_database_response expects a database name: '<catalog>.<database>' or "
		                      "'<database>', got '%s'",
		                      input.inputs[0].GetValue<string>());
	}
	auto &catalog_name = components[0];
	auto database_name = components[1].GetIdentifierName();
	auto catalog = Catalog::GetCatalogEntry(context, catalog_name);
	if (!catalog) {
		throw BinderException("Catalog '%s' does not exist", catalog_name.GetIdentifierName());
	}
	if (catalog->GetCatalogType() != "glue") {
		throw BinderException("glue_get_database_response only works on a Glue catalog, '%s' is a %s catalog",
		                      catalog_name.GetIdentifierName(), catalog->GetCatalogType());
	}
	auto result = make_uniq<GlueGetDatabaseResponseBindData>();
	if (!GlueAPI::GetDatabase(context, catalog->Cast<GlueCatalog>(), database_name, result->database,
	                          &result->raw_json)) {
		throw CatalogException("Database '%s' does not exist in Glue catalog '%s'", database_name,
		                       catalog_name.GetIdentifierName());
	}
	names = {"database_name", "description", "location_uri", "parameters", "response"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR), LogicalType::VARIANT()};
	return std::move(result);
}

void GlueGetDatabaseResponseScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueGetTableResponseState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GlueGetDatabaseResponseBindData>();
	auto &database = bind_data.database;
	auto optional_string = [](const string &value) {
		return value.empty() ? Value(LogicalType::VARCHAR) : Value(value);
	};

	output.data[0].Append(Value(database.name));
	output.data[1].Append(optional_string(database.description));
	output.data[2].Append(optional_string(database.location_uri));
	output.data[3].Append(GlueParametersToValue(database.parameters));

	// The complete Glue Database object: JSON as serialized by the AWS SDK, cast to VARIANT
	Vector json(LogicalType::JSON(), 1);
	json.SetValue(0, Value(bind_data.raw_json));
	VectorOperations::Cast(context, json, output.data[4], 1);

	output.CheckCardinality(1);
}

} // namespace

LogicalType GlueColumnsType() {
	return LogicalType::LIST(LogicalType::STRUCT(
	    {{"name", LogicalType::VARCHAR}, {"type", LogicalType::VARCHAR}, {"comment", LogicalType::VARCHAR}}));
}

Value GlueColumnsToValue(const vector<GlueColumn> &columns) {
	vector<Value> entries;
	for (auto &column : columns) {
		entries.push_back(
		    Value::STRUCT({{"name", Value(column.name)},
		                   {"type", Value(column.type)},
		                   {"comment", column.comment.empty() ? Value(LogicalType::VARCHAR) : Value(column.comment)}}));
	}
	return Value::LIST(ListType::GetChildType(GlueColumnsType()), std::move(entries));
}

Value GlueParametersToValue(const unordered_map<string, string> &parameters) {
	vector<Value> keys;
	vector<Value> values;
	for (auto &entry : parameters) {
		keys.emplace_back(entry.first);
		values.emplace_back(entry.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

QualifiedName ResolveGlueTableName(ClientContext &context, const string &function_name, const string &table_name) {
	auto qualified = QualifiedName::Parse(table_name);
	if (qualified.Catalog().empty() || qualified.Schema().empty()) {
		// a partially qualified name: resolve it the way a query would (search path, default catalog)
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, qualified);
		auto &entry = Catalog::GetEntry(context, lookup);
		qualified = QualifiedName(entry.ParentCatalog().GetName(), Identifier(entry.ParentSchema().name),
		                          Identifier(entry.name));
	}
	auto catalog = Catalog::GetCatalogEntry(context, qualified.Catalog());
	if (!catalog) {
		throw BinderException("Catalog '%s' does not exist", qualified.Catalog().GetIdentifierName());
	}
	if (catalog->GetCatalogType() != "glue") {
		throw BinderException("%s only works on tables of a Glue catalog, '%s' is a %s catalog", function_name,
		                      qualified.Catalog().GetIdentifierName(), catalog->GetCatalogType());
	}
	return qualified;
}

TableFunction GetGlueGetDatabaseResponseFunction() {
	TableFunction function("glue_get_database_response", {LogicalType::VARCHAR}, GlueGetDatabaseResponseScan,
	                       GlueGetDatabaseResponseBind, GlueGetTableResponseInit);
	return function;
}

TableFunction GetGlueGetTableResponseFunction() {
	TableFunction function("glue_get_table_response", {LogicalType::VARCHAR}, GlueGetTableResponseScan,
	                       GlueGetTableResponseBind, GlueGetTableResponseInit);
	return function;
}

} // namespace duckdb
