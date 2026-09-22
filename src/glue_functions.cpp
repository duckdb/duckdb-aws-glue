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
	output.SetValue(8, 0, MapToValue(table.parameters));
	output.SetValue(9, 0, MapToValue(table.serde_parameters));

	// The complete Glue Table object: JSON as serialized by the AWS SDK, cast to VARIANT
	Vector json(LogicalType::JSON(), 1);
	json.SetValue(0, Value(bind_data.raw_json));
	VectorOperations::Cast(context, json, output.data[10], 1);

	output.SetCardinality(1);
}

//! Whether a glue_* function is going to change the Glue catalog. Required rather than defaulted, so that adding a
//! function forces the question to be answered: a mutating one must be refused on a READ_ONLY attachment, and DuckDB
//! cannot do that for us. A table function is bound and executed like a scan, so the binder's read-only check --
//! which does stop INSERT / CREATE / ALTER / DROP -- never sees these at all.
enum class GlueWriteIntent { READS, WRITES };

struct GlueDatabaseTarget {
	GlueCatalog *catalog = nullptr;
	string catalog_name;
	string database_name;
};

//! Resolve '<catalog>.<database>' to an attached Glue catalog, refusing a mutating call on a READ_ONLY attachment.
//! The database-level counterpart of ResolveGlueTable in glue_partition_functions.cpp.
GlueDatabaseTarget ResolveGlueDatabase(ClientContext &context, const string &function_name, const Value &name,
                                       GlueWriteIntent intent) {
	auto input = name.GetValue<string>();
	auto components = QualifiedName::ParseComponents(input);
	if (components.size() != 2) {
		throw BinderException("%s expects a qualified database name: '<catalog>.<database>', got '%s'", function_name,
		                      input);
	}
	auto catalog = Catalog::GetCatalogEntry(context, components[0]);
	if (!catalog) {
		throw BinderException("Catalog '%s' does not exist", components[0].GetIdentifierName());
	}
	if (catalog->GetCatalogType() != "glue") {
		throw BinderException("%s only works on databases of a Glue catalog, '%s' is a %s catalog", function_name,
		                      components[0].GetIdentifierName(), catalog->GetCatalogType());
	}
	GlueDatabaseTarget result;
	result.catalog = &catalog->Cast<GlueCatalog>();
	result.catalog_name = components[0].GetIdentifierName();
	result.database_name = components[1].GetIdentifierName();
	// Refuse before touching Glue at all, so a rejected call has no partial effect. Same exception type and phrasing
	// as DuckDB's own read-only refusal (client_context.cpp), so a statement and a glue_* call fail the same way.
	if (intent == GlueWriteIntent::WRITES && result.catalog->access_mode == AccessMode::READ_ONLY) {
		throw InvalidInputException("Cannot execute %s on database \"%s\" which is attached in read-only mode!",
		                            function_name, result.catalog_name);
	}
	return result;
}

//! Read an option list given as a struct ({comment: 'x', owner: 'y'}, like glue_add_partition's partition spec) or as
//! a MAP (which is what the glue_hive_ddl grammar builds from CREATE SCHEMA ... WITH (...)). Values are rendered as
//! VARCHAR, since every Glue database field is a string.
case_insensitive_map_t<string> ParseOptionList(const string &function_name, const Value &options) {
	case_insensitive_map_t<string> result;
	if (options.IsNull()) {
		return result;
	}
	auto type_id = options.type().id();
	if (type_id == LogicalTypeId::STRUCT) {
		auto &types = StructType::GetChildTypes(options.type());
		auto &values = StructValue::GetChildren(options);
		for (idx_t i = 0; i < types.size(); i++) {
			auto name = types[i].first.GetIdentifierName();
			if (result.find(name) != result.end()) {
				throw BinderException("%s: option '%s' is given twice", function_name, name);
			}
			if (values[i].IsNull()) {
				throw BinderException("%s: NULL is not a valid value for option '%s'", function_name, name);
			}
			result[name] = values[i].DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
		}
		return result;
	}
	if (type_id == LogicalTypeId::MAP) {
		for (auto &entry : MapValue::GetChildren(options)) {
			auto &pair = StructValue::GetChildren(entry);
			if (pair[0].IsNull()) {
				throw BinderException("%s: an option name may not be NULL", function_name);
			}
			auto name = pair[0].DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
			if (result.find(name) != result.end()) {
				throw BinderException("%s: option '%s' is given twice", function_name, name);
			}
			if (pair[1].IsNull()) {
				throw BinderException("%s: NULL is not a valid value for option '%s'", function_name, name);
			}
			result[name] = pair[1].DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
		}
		return result;
	}
	throw BinderException("%s expects the options as a struct ({comment: '...'}) or a MAP, got %s", function_name,
	                      options.type().ToString());
}

struct GlueGetDatabaseResponseBindData : public TableFunctionData {
	GlueDatabaseInfo database;
	string raw_json;
};

struct GlueGetDatabaseResponseState : public GlobalTableFunctionState {
	bool done = false;
};

unique_ptr<FunctionData> GlueGetDatabaseResponseBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto target = ResolveGlueDatabase(context, "glue_get_database_response", input.inputs[0], GlueWriteIntent::READS);

	auto result = make_uniq<GlueGetDatabaseResponseBindData>();
	if (!GlueAPI::GetDatabase(context, *target.catalog, target.database_name, result->database, &result->raw_json)) {
		throw CatalogException("Database '%s' does not exist in Glue catalog '%s'", target.database_name,
		                       target.catalog_name);
	}

	names = {"database_name", "comment", "location", "parameters", "response"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR), LogicalType::VARIANT()};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GlueGetDatabaseResponseInit(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	return make_uniq<GlueGetDatabaseResponseState>();
}

void GlueGetDatabaseResponseScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueGetDatabaseResponseState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GlueGetDatabaseResponseBindData>();
	auto &database = bind_data.database;

	output.SetValue(0, 0, Value(database.name));
	output.SetValue(1, 0, Value(database.description));
	output.SetValue(2, 0, Value(database.location_uri));
	output.SetValue(3, 0, MapToValue(database.parameters));

	// The complete Glue Database object: JSON as serialized by the AWS SDK, cast to VARIANT
	Vector json(LogicalType::JSON(), 1);
	json.SetValue(0, Value(bind_data.raw_json));
	VectorOperations::Cast(context, json, output.data[4], 1);

	output.SetCardinality(1);
}

struct GlueCreateDatabaseBindData : public TableFunctionData {
	GlueDatabaseTarget target;
	GlueDatabaseInfo database;
	bool if_not_exists = false;
};

struct GlueDatabaseChangeState : public GlobalTableFunctionState {
	bool done = false;
};

unique_ptr<FunctionData> GlueCreateDatabaseBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlueCreateDatabaseBindData>();
	result->target = ResolveGlueDatabase(context, "glue_create_database", input.inputs[0], GlueWriteIntent::WRITES);
	case_insensitive_map_t<string> options;
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "if_not_exists") {
			result->if_not_exists = option.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else if (name == "options") {
			options = ParseOptionList("glue_create_database", option.second);
		}
	}
	result->database =
	    result->target.catalog->BuildDatabaseInfo(result->target.database_name, options, "glue_create_database");
	names = {"database_name", "created"};
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GlueDatabaseChangeInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlueDatabaseChangeState>();
}

void GlueCreateDatabaseScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueDatabaseChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GlueCreateDatabaseBindData>();
	auto &catalog = *bind_data.target.catalog;
	auto &database_name = bind_data.target.database_name;

	bool created = true;
	if (bind_data.if_not_exists) {
		GlueDatabaseInfo existing;
		if (GlueAPI::GetDatabase(context, catalog, database_name, existing)) {
			created = false;
		}
	}
	if (created) {
		// throws a CatalogException when it already exists and if_not_exists was not given
		GlueAPI::CreateDatabase(context, catalog, bind_data.database);
	}
	// keep the schema cache in step, the way CREATE SCHEMA does: without this a listing that already ran would not
	// show the new database for the life of the attachment
	GlueDatabaseInfo stored;
	if (GlueAPI::GetDatabase(context, catalog, database_name, stored)) {
		catalog.GetSchemas().CreateEntry(catalog.GetSchemas().CreateSchemaEntry(stored));
	}

	output.SetValue(0, 0, Value(database_name));
	output.SetValue(1, 0, Value::BOOLEAN(created));
	output.SetCardinality(1);
}

struct GlueSetDatabasePropertiesBindData : public TableFunctionData {
	GlueDatabaseTarget target;
	case_insensitive_map_t<string> properties;
};

unique_ptr<FunctionData> GlueSetDatabasePropertiesBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlueSetDatabasePropertiesBindData>();
	result->target =
	    ResolveGlueDatabase(context, "glue_set_database_properties", input.inputs[0], GlueWriteIntent::WRITES);
	result->properties = ParseOptionList("glue_set_database_properties", input.inputs[1]);
	if (result->properties.empty()) {
		throw BinderException("glue_set_database_properties: at least one property is required");
	}
	names = {"database_name", "properties_set"};
	return_types = {LogicalType::VARCHAR, LogicalType::BIGINT};
	return std::move(result);
}

void GlueSetDatabasePropertiesScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueDatabaseChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GlueSetDatabasePropertiesBindData>();
	// SET DBPROPERTIES merges, so 'comment' is handled here too rather than being silently dropped as a property
	unordered_map<string, string> properties;
	string description;
	bool has_description = false;
	for (auto &property : bind_data.properties) {
		if (StringUtil::CIEquals(property.first, "comment")) {
			description = property.second;
			has_description = true;
			continue;
		}
		properties[property.first] = property.second;
	}
	if (!properties.empty()) {
		GlueAPI::SetDatabaseProperties(context, *bind_data.target.catalog, bind_data.target.database_name, properties);
	}
	if (has_description) {
		GlueAPI::SetDatabaseDescription(context, *bind_data.target.catalog, bind_data.target.database_name,
		                                description);
	}

	output.SetValue(0, 0, Value(bind_data.target.database_name));
	output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(bind_data.properties.size())));
	output.SetCardinality(1);
}

} // namespace

TableFunction GetGlueGetTableResponseFunction() {
	TableFunction function("glue_get_table_response", {LogicalType::VARCHAR}, GlueGetTableResponseScan,
	                       GlueGetTableResponseBind, GlueGetTableResponseInit);
	return function;
}

TableFunction GetGlueGetDatabaseResponseFunction() {
	TableFunction function("glue_get_database_response", {LogicalType::VARCHAR}, GlueGetDatabaseResponseScan,
	                       GlueGetDatabaseResponseBind, GlueGetDatabaseResponseInit);
	return function;
}

TableFunction GetGlueCreateDatabaseFunction() {
	TableFunction function("glue_create_database", {LogicalType::VARCHAR}, GlueCreateDatabaseScan,
	                       GlueCreateDatabaseBind, GlueDatabaseChangeInit);
	function.named_parameters["options"] = LogicalType::ANY;
	function.named_parameters["if_not_exists"] = LogicalType::BOOLEAN;
	return function;
}

TableFunction GetGlueSetDatabasePropertiesFunction() {
	TableFunction function("glue_set_database_properties", {LogicalType::VARCHAR, LogicalType::ANY},
	                       GlueSetDatabasePropertiesScan, GlueSetDatabasePropertiesBind, GlueDatabaseChangeInit);
	return function;
}

} // namespace duckdb
