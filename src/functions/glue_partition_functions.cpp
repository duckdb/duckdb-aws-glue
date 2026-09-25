#include "functions/glue_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "api/glue_api.hpp"
#include "core/glue_types.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_table.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Shared: resolving the table and the partition spec
//===--------------------------------------------------------------------===//
struct GluePartitionTarget {
	GlueCatalog *catalog = nullptr;
	GlueTableInfo table;

	string TableName() const {
		return table.database_name + "." + table.name;
	}
};

GluePartitionTarget ResolveGlueTable(ClientContext &context, const string &function_name, const Value &table_name,
                                     bool require_partitions = true) {
	auto qualified = QualifiedName::Parse(table_name.GetValue<string>());
	if (qualified.Catalog().empty() || qualified.Schema().empty()) {
		// a partially qualified name: resolve it the way a query would (search path, default catalog)
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, qualified);
		auto &entry = Catalog::GetEntry(context, lookup);
		if (entry.ParentCatalog().GetCatalogType() != "glue") {
			throw BinderException("%s only works on tables of a Glue catalog, '%s' is a table of a %s catalog",
			                      function_name, table_name.GetValue<string>(), entry.ParentCatalog().GetCatalogType());
		}
		auto &glue_table = entry.Cast<GlueTable>();
		qualified = QualifiedName(entry.ParentCatalog().GetName(), Identifier(glue_table.table_info.database_name),
		                          Identifier(glue_table.table_info.name));
	}
	auto catalog = Catalog::GetCatalogEntry(context, qualified.Catalog());
	if (!catalog) {
		throw BinderException("Catalog '%s' does not exist", qualified.Catalog().GetIdentifierName());
	}
	if (catalog->GetCatalogType() != "glue") {
		throw BinderException("%s only works on tables of a Glue catalog, '%s' is a %s catalog", function_name,
		                      qualified.Catalog().GetIdentifierName(), catalog->GetCatalogType());
	}
	GluePartitionTarget result;
	result.catalog = &catalog->Cast<GlueCatalog>();
	if (!GlueAPI::GetTable(context, *result.catalog, qualified.Schema().GetIdentifierName(),
	                       qualified.Name().GetIdentifierName(), result.table)) {
		throw CatalogException("Table '%s.%s' does not exist in Glue catalog '%s'",
		                       qualified.Schema().GetIdentifierName(), qualified.Name().GetIdentifierName(),
		                       qualified.Catalog().GetIdentifierName());
	}
	if (result.table.GetFormat() != GlueTableFormat::HIVE) {
		throw NotImplementedException("%s only works on Hive tables, '%s' is a %s table", function_name,
		                              result.TableName(), result.table.GetFormatName());
	}
	if (require_partitions && result.table.partition_keys.empty()) {
		throw InvalidInputException("Table '%s' is not partitioned", result.TableName());
	}
	return result;
}

//! Turn a partition spec {key: value, ...} into the partition values (as strings) in partition key order. The spec
//! must name every partition key and nothing else.
vector<string> ParsePartitionSpec(const string &function_name, const GluePartitionTarget &target, const Value &spec) {
	auto &keys = target.table.partition_keys;
	vector<string> key_names;
	for (auto &key : keys) {
		key_names.push_back(key.name);
	}
	auto describe_keys = StringUtil::Join(key_names, ", ");
	if (spec.type().id() != LogicalTypeId::STRUCT || spec.IsNull()) {
		throw BinderException("%s expects the partition as a struct of the partition keys of table '%s': {%s}",
		                      function_name, target.TableName(), describe_keys);
	}
	auto &spec_types = StructType::GetChildTypes(spec.type());
	auto &spec_values = StructValue::GetChildren(spec);
	vector<string> result(keys.size());
	vector<bool> seen(keys.size(), false);
	for (idx_t i = 0; i < spec_types.size(); i++) {
		auto &name = spec_types[i].first.GetIdentifierName();
		idx_t key_index = DConstants::INVALID_INDEX;
		for (idx_t k = 0; k < keys.size(); k++) {
			if (StringUtil::CIEquals(keys[k].name, name)) {
				key_index = k;
				break;
			}
		}
		if (key_index == DConstants::INVALID_INDEX) {
			throw BinderException("'%s' is not a partition key of table '%s', the partition keys are: %s", name,
			                      target.TableName(), describe_keys);
		}
		if (seen[key_index]) {
			throw BinderException("Partition key '%s' is given twice", keys[key_index].name);
		}
		seen[key_index] = true;
		auto &value = spec_values[i];
		if (value.IsNull()) {
			result[key_index] = HivePartitioning::DEFAULT_PARTITION_NAME;
			continue;
		}
		// store the value the way the partition key type would render it
		auto key_type = GlueTypes::ToLogicalType(keys[key_index].type);
		result[key_index] = value.DefaultCastAs(key_type).ToString();
	}
	for (idx_t k = 0; k < keys.size(); k++) {
		if (!seen[k]) {
			throw BinderException("%s: the partition must name every partition key of table '%s' (%s), '%s' is missing",
			                      function_name, target.TableName(), describe_keys, keys[k].name);
		}
	}
	return result;
}

//! Turn (key, value) pairs into the partition values in partition key order; NULL values (empty optional) become
//! the default partition
vector<string> ParsePartitionPairs(const string &function_name, const GluePartitionTarget &target,
                                   const vector<pair<string, optional<string>>> &pairs) {
	auto &keys = target.table.partition_keys;
	vector<string> key_names;
	for (auto &key : keys) {
		key_names.push_back(key.name);
	}
	auto describe_keys = StringUtil::Join(key_names, ", ");
	vector<string> result(keys.size());
	vector<bool> seen(keys.size(), false);
	for (auto &pair : pairs) {
		idx_t key_index = DConstants::INVALID_INDEX;
		for (idx_t k = 0; k < keys.size(); k++) {
			if (StringUtil::CIEquals(keys[k].name, pair.first)) {
				key_index = k;
				break;
			}
		}
		if (key_index == DConstants::INVALID_INDEX) {
			throw BinderException("'%s' is not a partition key of table '%s', the partition keys are: %s", pair.first,
			                      target.TableName(), describe_keys);
		}
		if (seen[key_index]) {
			throw BinderException("Partition key '%s' is given twice", keys[key_index].name);
		}
		seen[key_index] = true;
		if (!pair.second) {
			result[key_index] = HivePartitioning::DEFAULT_PARTITION_NAME;
			continue;
		}
		auto key_type = GlueTypes::ToLogicalType(keys[key_index].type);
		result[key_index] = Value(*pair.second).DefaultCastAs(key_type).ToString();
	}
	for (idx_t k = 0; k < keys.size(); k++) {
		if (!seen[k]) {
			throw BinderException("%s: the partition must name every partition key of table '%s' (%s), '%s' is missing",
			                      function_name, target.TableName(), describe_keys, keys[k].name);
		}
	}
	return result;
}

//! The location a partition gets when none is given: <table location>/<key>=<value>/...
string DefaultPartitionLocation(const GluePartitionTarget &target, const vector<string> &values) {
	auto location = target.table.location;
	StringUtil::RTrim(location, "/");
	if (location.empty()) {
		throw InvalidInputException("Table '%s' has no location in Glue, provide the partition location explicitly",
		                            target.TableName());
	}
	auto &keys = target.table.partition_keys;
	for (idx_t i = 0; i < keys.size(); i++) {
		location += "/" + HivePartitioning::Escape(keys[i].name) + "=";
		if (values[i] == HivePartitioning::DEFAULT_PARTITION_NAME) {
			location += values[i];
		} else {
			location += HivePartitioning::EscapeValue(values[i]);
		}
	}
	return location;
}

Value PartitionValueToValue(ClientContext &context, const string &str_value, const LogicalType &type) {
	if (str_value == HivePartitioning::DEFAULT_PARTITION_NAME) {
		return Value(type);
	}
	Value value(str_value);
	if (type.id() == LogicalTypeId::VARCHAR) {
		return value;
	}
	auto cast = value.TryCastAs(context, type);
	if (!cast) {
		return Value(type);
	}
	return value;
}

//! Bind data for the functions that change one partition and report one row when executed
struct GluePartitionChangeBindData : public TableFunctionData {
	GluePartitionTarget target;
	vector<string> values;
	vector<string> new_values;
	string location;
	bool if_not_exists = false;
	bool if_exists = false;
};

struct GluePartitionChangeState : public GlobalTableFunctionState {
	bool done = false;
};

unique_ptr<GlobalTableFunctionState> GluePartitionChangeInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GluePartitionChangeState>();
}

//===--------------------------------------------------------------------===//
// glue_partitions
//===--------------------------------------------------------------------===//
struct GluePartitionsBindData : public TableFunctionData {
	GluePartitionTarget target;
	vector<LogicalType> key_types;
	vector<GluePartitionInfo> partitions;
};

struct GluePartitionsState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

unique_ptr<FunctionData> GluePartitionsBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GluePartitionsBindData>();
	result->target = ResolveGlueTable(context, "glue_partitions", input.inputs[0]);
	auto &table = result->target.table;
	for (auto &key : table.partition_keys) {
		auto type = GlueTypes::ToLogicalType(key.type);
		result->key_types.push_back(type);
		names.emplace_back(key.name);
		return_types.push_back(type);
	}
	names.emplace_back("location");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("parameters");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	result->partitions = GlueAPI::GetPartitions(context, *result->target.catalog, table.database_name, table.name);
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GluePartitionsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GluePartitionsState>();
}

void GluePartitionsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<GluePartitionsBindData>();
	auto &state = data.global_state->Cast<GluePartitionsState>();
	auto &keys = bind_data.target.table.partition_keys;
	idx_t count = 0;
	while (state.offset < bind_data.partitions.size() && count < STANDARD_VECTOR_SIZE) {
		auto &partition = bind_data.partitions[state.offset++];
		for (idx_t k = 0; k < keys.size(); k++) {
			Value value = k < partition.values.size()
			                  ? PartitionValueToValue(context, partition.values[k], bind_data.key_types[k])
			                  : Value(bind_data.key_types[k]);
			output.data[k].Append(value);
		}
		output.data[keys.size()].Append(Value(partition.location));
		vector<Value> parameter_keys;
		vector<Value> parameter_values;
		for (auto &parameter : partition.parameters) {
			parameter_keys.emplace_back(parameter.first);
			parameter_values.emplace_back(parameter.second);
		}
		output.data[keys.size() + 1].Append(Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR,
		                                               std::move(parameter_keys), std::move(parameter_values)));
		count++;
	}
}

//===--------------------------------------------------------------------===//
// glue_add_partition
//===--------------------------------------------------------------------===//
unique_ptr<FunctionData> GlueAddPartitionBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GluePartitionChangeBindData>();
	result->target = ResolveGlueTable(context, "glue_add_partition", input.inputs[0]);
	result->values = ParsePartitionSpec("glue_add_partition", result->target, input.inputs[1]);
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "location") {
			result->location = option.second.GetValue<string>();
			StringUtil::RTrim(result->location, "/");
			if (result->location.empty()) {
				throw BinderException("glue_add_partition: 'location' must not be empty");
			}
		} else if (name == "if_not_exists") {
			result->if_not_exists = option.second.GetValue<bool>();
		}
	}
	if (result->location.empty()) {
		result->location = DefaultPartitionLocation(result->target, result->values);
	}
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

void GlueAddPartitionScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GluePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GluePartitionChangeBindData>();
	auto &table = bind_data.target.table;
	GluePartitionInput partition;
	partition.values = bind_data.values;
	partition.location = bind_data.location;
	GlueAPI::CreatePartition(context, *bind_data.target.catalog, table.database_name, table.name, partition,
	                         bind_data.if_not_exists);
	output.SetValue(0, 0, Value(bind_data.location));
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// glue_drop_partition
//===--------------------------------------------------------------------===//
unique_ptr<FunctionData> GlueDropPartitionBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GluePartitionChangeBindData>();
	result->target = ResolveGlueTable(context, "glue_drop_partition", input.inputs[0]);
	result->values = ParsePartitionSpec("glue_drop_partition", result->target, input.inputs[1]);
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "if_exists") {
			result->if_exists = option.second.GetValue<bool>();
		}
	}
	names = {"dropped"};
	return_types = {LogicalType::BOOLEAN};
	return std::move(result);
}

void GlueDropPartitionScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GluePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GluePartitionChangeBindData>();
	auto &table = bind_data.target.table;
	auto dropped =
	    GlueAPI::DeletePartition(context, *bind_data.target.catalog, table.database_name, table.name, bind_data.values);
	if (!dropped && !bind_data.if_exists) {
		throw CatalogException("Partition [%s] does not exist in Glue table '%s'",
		                       StringUtil::Join(bind_data.values, ", "), bind_data.target.TableName());
	}
	output.SetValue(0, 0, Value::BOOLEAN(dropped));
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// glue_rename_partition
//===--------------------------------------------------------------------===//
unique_ptr<FunctionData> GlueRenamePartitionBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GluePartitionChangeBindData>();
	result->target = ResolveGlueTable(context, "glue_rename_partition", input.inputs[0]);
	result->values = ParsePartitionSpec("glue_rename_partition", result->target, input.inputs[1]);
	result->new_values = ParsePartitionSpec("glue_rename_partition", result->target, input.inputs[2]);
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

void GlueRenamePartitionScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GluePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GluePartitionChangeBindData>();
	auto &table = bind_data.target.table;
	auto &catalog = *bind_data.target.catalog;
	GlueAPI::RenamePartition(context, catalog, table.database_name, table.name, bind_data.values, bind_data.new_values);
	GluePartitionInfo renamed;
	string location;
	if (GlueAPI::GetPartition(context, catalog, table.database_name, table.name, bind_data.new_values, renamed)) {
		location = renamed.location;
	}
	output.SetValue(0, 0, Value(location));
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// glue_set_partition_location / glue_set_table_location
//===--------------------------------------------------------------------===//
string ParseLocation(const string &function_name, const Value &location) {
	if (location.IsNull()) {
		throw BinderException("%s: the location must not be NULL", function_name);
	}
	auto result = location.GetValue<string>();
	StringUtil::RTrim(result, "/");
	if (result.empty()) {
		throw BinderException("%s: the location must not be empty", function_name);
	}
	if (!StringUtil::StartsWith(StringUtil::Lower(result), "s3://") &&
	    !StringUtil::StartsWith(StringUtil::Lower(result), "s3a://")) {
		throw BinderException("%s: the location must be an S3 location (s3://...), got '%s'", function_name, result);
	}
	return result;
}

unique_ptr<FunctionData> GlueSetPartitionLocationBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GluePartitionChangeBindData>();
	result->target = ResolveGlueTable(context, "glue_set_partition_location", input.inputs[0]);
	result->values = ParsePartitionSpec("glue_set_partition_location", result->target, input.inputs[1]);
	result->location = ParseLocation("glue_set_partition_location", input.inputs[2]);
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

void GlueSetPartitionLocationScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GluePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GluePartitionChangeBindData>();
	auto &table = bind_data.target.table;
	GlueAPI::SetPartitionLocation(context, *bind_data.target.catalog, table.database_name, table.name, bind_data.values,
	                              bind_data.location);
	output.SetValue(0, 0, Value(bind_data.location));
	output.SetCardinality(1);
}

unique_ptr<FunctionData> GlueSetTableLocationBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GluePartitionChangeBindData>();
	result->target = ResolveGlueTable(context, "glue_set_table_location", input.inputs[0], false);
	result->location = ParseLocation("glue_set_table_location", input.inputs[1]);
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

void GlueSetTableLocationScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GluePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GluePartitionChangeBindData>();
	auto &table = bind_data.target.table;
	GlueAPI::SetTableLocation(context, *bind_data.target.catalog, table.database_name, table.name, bind_data.location);
	output.SetValue(0, 0, Value(bind_data.location));
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// glue_alter_table: the SQL partition DDL, several actions in one statement
//===--------------------------------------------------------------------===//
enum class GlueAlterAction : uint8_t { ADD, DROP, RENAME, SET_PARTITION_LOCATION, SET_TABLE_LOCATION };

struct GlueAlterStep {
	GlueAlterAction action;
	bool if_not_exists = false;
	bool if_exists = false;
	vector<string> values;
	vector<string> new_values;
	string location;
};

struct GlueAlterTableBindData : public TableFunctionData {
	GluePartitionTarget target;
	vector<GlueAlterStep> steps;
};

//! glue_alter_table reports one row per action and an ALTER TABLE may carry more actions than a DataChunk
//! holds, so the rows are collected once when the actions are applied and then paged out. Its own state
//! rather than GluePartitionChangeState, which the single-row functions share and which needs no offset.
struct GlueAlterTableState : public GlobalTableFunctionState {
	struct EmittedRow {
		Value action;
		Value partition;
		Value location;
	};
	vector<EmittedRow> rows;
	idx_t offset = 0;
	//! the Glue calls happen on the first scan only; the later ones just hand out the remaining rows
	bool applied = false;
};

unique_ptr<GlobalTableFunctionState> GlueAlterTableInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlueAlterTableState>();
}

//! A struct field by name (case-insensitive), NULL if the struct has no such field
Value GetStructField(const Value &value, const string &field_name) {
	auto &types = StructType::GetChildTypes(value.type());
	auto &children = StructValue::GetChildren(value);
	for (idx_t i = 0; i < types.size(); i++) {
		if (StringUtil::CIEquals(types[i].first.GetIdentifierName(), field_name)) {
			return children[i];
		}
	}
	return Value();
}

//! A partition given as a list of {key, value} structs (value NULL for the default partition)
vector<string> ParsePartitionPairsValue(const string &function_name, const GluePartitionTarget &target,
                                        const Value &pairs) {
	if (pairs.IsNull() || pairs.type().id() != LogicalTypeId::LIST) {
		throw BinderException("%s: a partition must be given as a list of {key, value} structs", function_name);
	}
	vector<pair<string, optional<string>>> result;
	for (auto &entry : ListValue::GetChildren(pairs)) {
		if (entry.IsNull() || entry.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("%s: a partition must be given as a list of {key, value} structs", function_name);
		}
		auto key = GetStructField(entry, "key");
		auto value = GetStructField(entry, "value");
		if (key.IsNull()) {
			throw BinderException("%s: a partition entry needs a key", function_name);
		}
		optional<string> value_string;
		if (!value.IsNull()) {
			value_string = value.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
		}
		result.emplace_back(key.GetValue<string>(), std::move(value_string));
	}
	return ParsePartitionPairs(function_name, target, result);
}

unique_ptr<FunctionData> GlueAlterTableBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlueAlterTableBindData>();
	auto &actions = input.inputs[1];
	if (actions.IsNull() || actions.type().id() != LogicalTypeId::LIST) {
		throw BinderException("glue_alter_table expects a list of actions");
	}
	// table-level actions do not need a partitioned table
	bool needs_partitions = false;
	for (auto &action : ListValue::GetChildren(actions)) {
		auto name = StringUtil::Lower(GetStructField(action, "action").ToString());
		if (name != "set_table_location") {
			needs_partitions = true;
		}
	}
	result->target = ResolveGlueTable(context, "glue_alter_table", input.inputs[0], needs_partitions);
	for (auto &action : ListValue::GetChildren(actions)) {
		if (action.IsNull() || action.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("glue_alter_table: every action must be a struct");
		}
		GlueAlterStep step;
		auto name = StringUtil::Lower(GetStructField(action, "action").ToString());
		auto if_not_exists = GetStructField(action, "if_not_exists");
		auto if_exists = GetStructField(action, "if_exists");
		auto partition = GetStructField(action, "partition");
		auto new_partition = GetStructField(action, "new_partition");
		auto location = GetStructField(action, "location");
		step.if_not_exists = !if_not_exists.IsNull() && if_not_exists.GetValue<bool>();
		step.if_exists = !if_exists.IsNull() && if_exists.GetValue<bool>();
		if (name == "add") {
			step.action = GlueAlterAction::ADD;
			step.values = ParsePartitionPairsValue("ALTER TABLE ADD PARTITION", result->target, partition);
			step.location = location.IsNull() ? DefaultPartitionLocation(result->target, step.values)
			                                  : ParseLocation("ALTER TABLE ADD PARTITION", location);
		} else if (name == "drop") {
			step.action = GlueAlterAction::DROP;
			step.values = ParsePartitionPairsValue("ALTER TABLE DROP PARTITION", result->target, partition);
		} else if (name == "rename") {
			step.action = GlueAlterAction::RENAME;
			step.values = ParsePartitionPairsValue("ALTER TABLE RENAME PARTITION", result->target, partition);
			step.new_values = ParsePartitionPairsValue("ALTER TABLE RENAME PARTITION", result->target, new_partition);
		} else if (name == "set_partition_location") {
			step.action = GlueAlterAction::SET_PARTITION_LOCATION;
			step.values = ParsePartitionPairsValue("ALTER TABLE PARTITION SET LOCATION", result->target, partition);
			step.location = ParseLocation("ALTER TABLE PARTITION SET LOCATION", location);
		} else if (name == "set_table_location") {
			step.action = GlueAlterAction::SET_TABLE_LOCATION;
			step.location = ParseLocation("ALTER TABLE SET LOCATION", location);
		} else {
			throw BinderException("glue_alter_table: unknown action '%s'", name);
		}
		result->steps.push_back(std::move(step));
	}
	names = {"action", "partition", "location"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(result);
}

string DescribeAction(GlueAlterAction action) {
	switch (action) {
	case GlueAlterAction::ADD:
		return "add partition";
	case GlueAlterAction::DROP:
		return "drop partition";
	case GlueAlterAction::RENAME:
		return "rename partition";
	case GlueAlterAction::SET_PARTITION_LOCATION:
		return "set partition location";
	case GlueAlterAction::SET_TABLE_LOCATION:
		return "set table location";
	}
	return "unknown";
}

//! Apply every action, once, and record the row each one reports. Kept separate from emitting them because
//! there can be more actions than a DataChunk holds, so the rows are handed out over several scan calls
//! while the Glue calls must happen exactly once.
void GlueAlterTableApply(ClientContext &context, TableFunctionInput &data, GlueAlterTableState &state) {
	auto &bind_data = data.bind_data->Cast<GlueAlterTableBindData>();
	auto &catalog = *bind_data.target.catalog;
	auto &table = bind_data.target.table;
	auto table_name = bind_data.target.TableName();

	// Glue has no transactions: check everything that can fail against the catalog first, so that a statement
	// which fails half way registers or removes nothing
	for (auto &step : bind_data.steps) {
		GluePartitionInfo existing;
		switch (step.action) {
		case GlueAlterAction::ADD:
			if (!step.if_not_exists &&
			    GlueAPI::GetPartition(context, catalog, table.database_name, table.name, step.values, existing)) {
				throw CatalogException("Partition [%s] already exists in Glue table '%s'",
				                       StringUtil::Join(step.values, ", "), table_name);
			}
			break;
		case GlueAlterAction::DROP:
		case GlueAlterAction::SET_PARTITION_LOCATION:
			if (!step.if_exists &&
			    !GlueAPI::GetPartition(context, catalog, table.database_name, table.name, step.values, existing)) {
				throw CatalogException("Partition [%s] does not exist in Glue table '%s'",
				                       StringUtil::Join(step.values, ", "), table_name);
			}
			break;
		case GlueAlterAction::RENAME:
			if (!GlueAPI::GetPartition(context, catalog, table.database_name, table.name, step.values, existing)) {
				throw CatalogException("Partition [%s] does not exist in Glue table '%s'",
				                       StringUtil::Join(step.values, ", "), table_name);
			}
			if (GlueAPI::GetPartition(context, catalog, table.database_name, table.name, step.new_values, existing)) {
				throw CatalogException("Partition [%s] already exists in Glue table '%s'",
				                       StringUtil::Join(step.new_values, ", "), table_name);
			}
			break;
		case GlueAlterAction::SET_TABLE_LOCATION:
			break;
		}
	}

	// consecutive adds go out as one BatchCreatePartition call
	auto emit = [&](const GlueAlterStep &step) {
		GlueAlterTableState::EmittedRow row;
		row.action = Value(DescribeAction(step.action));
		row.partition = step.values.empty() ? Value(LogicalType::VARCHAR) : Value(StringUtil::Join(step.values, ", "));
		row.location = step.location.empty() ? Value(LogicalType::VARCHAR) : Value(step.location);
		state.rows.push_back(std::move(row));
	};
	vector<GluePartitionInput> pending_adds;
	auto flush_adds = [&]() {
		if (pending_adds.empty()) {
			return;
		}
		GlueAPI::BatchCreatePartitions(context, catalog, table.database_name, table.name, pending_adds);
		pending_adds.clear();
	};
	for (auto &step : bind_data.steps) {
		if (step.action != GlueAlterAction::ADD) {
			flush_adds();
		}
		switch (step.action) {
		case GlueAlterAction::ADD: {
			GluePartitionInput partition;
			partition.values = step.values;
			partition.location = step.location;
			pending_adds.push_back(std::move(partition));
			break;
		}
		case GlueAlterAction::DROP:
			GlueAPI::DeletePartition(context, catalog, table.database_name, table.name, step.values);
			break;
		case GlueAlterAction::RENAME:
			GlueAPI::RenamePartition(context, catalog, table.database_name, table.name, step.values, step.new_values);
			break;
		case GlueAlterAction::SET_PARTITION_LOCATION:
			GlueAPI::SetPartitionLocation(context, catalog, table.database_name, table.name, step.values,
			                              step.location);
			break;
		case GlueAlterAction::SET_TABLE_LOCATION:
			GlueAPI::SetTableLocation(context, catalog, table.database_name, table.name, step.location);
			break;
		}
		emit(step);
	}
	flush_adds();
}

void GlueAlterTableScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueAlterTableState>();
	if (!state.applied) {
		state.applied = true;
		GlueAlterTableApply(context, data, state);
	}
	// one row per action, so there can be more rows than a chunk holds: page them out
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset++];
		output.data[0].Append(row.action);
		output.data[1].Append(row.partition);
		output.data[2].Append(row.location);
		count++;
	}
}

} // namespace

TableFunction GetGluePartitionsFunction() {
	TableFunction function("glue_partitions", {LogicalType::VARCHAR}, GluePartitionsScan, GluePartitionsBind,
	                       GluePartitionsInit);
	return function;
}

TableFunction GetGlueAddPartitionFunction() {
	TableFunction function("glue_add_partition", {LogicalType::VARCHAR, LogicalType::ANY}, GlueAddPartitionScan,
	                       GlueAddPartitionBind, GluePartitionChangeInit);
	function.named_parameters["location"] = LogicalType::VARCHAR;
	function.named_parameters["if_not_exists"] = LogicalType::BOOLEAN;
	return function;
}

TableFunction GetGlueDropPartitionFunction() {
	TableFunction function("glue_drop_partition", {LogicalType::VARCHAR, LogicalType::ANY}, GlueDropPartitionScan,
	                       GlueDropPartitionBind, GluePartitionChangeInit);
	function.named_parameters["if_exists"] = LogicalType::BOOLEAN;
	return function;
}

TableFunction GetGlueRenamePartitionFunction() {
	TableFunction function("glue_rename_partition", {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY},
	                       GlueRenamePartitionScan, GlueRenamePartitionBind, GluePartitionChangeInit);
	return function;
}

TableFunction GetGlueSetPartitionLocationFunction() {
	TableFunction function("glue_set_partition_location",
	                       {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::VARCHAR}, GlueSetPartitionLocationScan,
	                       GlueSetPartitionLocationBind, GluePartitionChangeInit);
	return function;
}

TableFunction GetGlueSetTableLocationFunction() {
	TableFunction function("glue_set_table_location", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                       GlueSetTableLocationScan, GlueSetTableLocationBind, GluePartitionChangeInit);
	return function;
}

TableFunction GetGlueAlterTableFunction() {
	TableFunction function("glue_alter_table", {LogicalType::VARCHAR, LogicalType::ANY}, GlueAlterTableScan,
	                       GlueAlterTableBind, GlueAlterTableInit);
	return function;
}

} // namespace duckdb
