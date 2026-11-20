#include "functions/glue_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_result.hpp"

#include "api/glue_api.hpp"
#include "core/glue_types.hpp"
#include "core/hive_location.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

namespace {

void ThrowIsView(const string &function_name, const string &table_name, bool require_partitions) {
	if (require_partitions) {
		throw BinderException("%s: Glue view '%s' has no partitions", function_name, table_name);
	}
	throw BinderException("%s: '%s' is a Glue view, not a table", function_name, table_name);
}

} // namespace

GluePartitionTarget ResolveGlueTable(ClientContext &context, const string &function_name, const Value &table_name,
                                     bool require_partitions) {
	auto name = ResolveGlueTableName(context, function_name, table_name.GetValue<string>());
	GluePartitionTarget result;
	result.catalog = &Catalog::GetCatalog(context, name.Catalog()).Cast<GlueCatalog>();
	if (!GlueAPI::GetTable(context, *result.catalog, name.Schema().GetIdentifierName(), name.Name().GetIdentifierName(),
	                       result.table)) {
		throw CatalogException("Table '%s.%s' does not exist in Glue catalog '%s'", name.Schema().GetIdentifierName(),
		                       name.Name().GetIdentifierName(), name.Catalog().GetIdentifierName());
	}
	if (result.table.IsView()) {
		ThrowIsView(function_name, result.TableName(), require_partitions);
	}
	auto format = result.table.GetFormat();
	if (!format.IsHive()) {
		throw NotImplementedException("%s only works on Hive tables, '%s' is a %s table", function_name,
		                              result.TableName(), format.Describe(result.table));
	}
	if (require_partitions && result.table.partition_keys.empty()) {
		throw InvalidInputException("Table '%s' is not partitioned", result.TableName());
	}
	return result;
}

Value PartitionListingValue(ClientContext &context, const string &key, const string &str_value,
                            const LogicalType &type) {
	try {
		return GlueTypes::PartitionValue(context, key, str_value, type);
	} catch (std::exception &) {
		return Value(type);
	}
}

vector<string> ParsePartitionPairs(const string &function_name, const GluePartitionTarget &target,
                                   const vector<pair<string, Value>> &pairs) {
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
		if (pair.second.IsNull()) {
			result[key_index] = HivePartitioning::DEFAULT_PARTITION_NAME;
			continue;
		}
		// store the value the way the partition key type would render it
		auto key_type = GlueTypes::ToLogicalType(keys[key_index].type);
		result[key_index] = pair.second.DefaultCastAs(key_type).ToString();
	}
	for (idx_t k = 0; k < keys.size(); k++) {
		if (!seen[k]) {
			throw BinderException("%s: the partition must name every partition key of table '%s' (%s), '%s' is missing",
			                      function_name, target.TableName(), describe_keys, keys[k].name);
		}
	}
	return result;
}

vector<string> ParsePartitionSpec(const string &function_name, const GluePartitionTarget &target, const Value &spec) {
	if (spec.type().id() != LogicalTypeId::STRUCT || spec.IsNull()) {
		vector<string> key_names;
		for (auto &key : target.table.partition_keys) {
			key_names.push_back(key.name);
		}
		throw BinderException("%s expects the partition as a struct of the partition keys of table '%s': {%s}",
		                      function_name, target.TableName(), StringUtil::Join(key_names, ", "));
	}
	auto &spec_types = StructType::GetChildTypes(spec.type());
	auto &spec_values = StructValue::GetChildren(spec);
	vector<pair<string, Value>> pairs;
	for (idx_t i = 0; i < spec_types.size(); i++) {
		pairs.emplace_back(spec_types[i].first.GetIdentifierName(), spec_values[i]);
	}
	return ParsePartitionPairs(function_name, target, pairs);
}

namespace {

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
	// the strings as Glue stores them: 'abc' on an INT key is NULL above but 'abc' here
	names.emplace_back("partition_values");
	// numbered like duplicate result columns (location_1) when a partition key has the name; the keys keep their names
	auto deduplicated = names;
	QueryResult::DeduplicateColumns(deduplicated);
	for (idx_t i = table.partition_keys.size(); i < names.size(); i++) {
		names[i] = deduplicated[i];
	}
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
	auto load_partitions = [&]() {
		return make_shared_ptr<const vector<GluePartitionInfo>>(
		    GlueAPI::GetPartitions(context, *result->target.catalog, table.schema_name, table.name));
	};
	auto &cache = *GlueTransaction::Get(context, *result->target.catalog).cache;
	result->partitions = *cache.GetPartitions(table.schema_name, table.name, load_partitions);
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
			Value value =
			    k < partition.values.size()
			        ? PartitionListingValue(context, keys[k].name, partition.values[k], bind_data.key_types[k])
			        : Value(bind_data.key_types[k]);
			output.data[k].Append(value);
		}
		output.data[keys.size()].Append(Value(partition.location));
		auto &raw_column = output.data[keys.size() + 1];
		vector<Value> raw_values;
		raw_values.reserve(partition.values.size());
		for (auto &str_value : partition.values) {
			raw_values.emplace_back(str_value);
		}
		raw_column.Append(Value::LIST(ListType::GetChildType(raw_column.GetType()), std::move(raw_values)));
		count++;
	}
}

//===--------------------------------------------------------------------===//
// glue_replace_columns: replace the data columns of a Hive table
//===--------------------------------------------------------------------===//
struct GlueReplaceColumnsBindData : public TableFunctionData {
	GluePartitionTarget target;
	vector<GlueColumn> columns;

	//! The checks in bind are against the table as it is then: a prepared statement binds again on every execution
	bool SupportStatementCache() const override {
		return false;
	}
};

//! A struct value whose fields are all strings (or NULL), e.g. {id: 'BIGINT'}
static bool IsStringStruct(const Value &value) {
	if (value.IsNull() || value.type().id() != LogicalTypeId::STRUCT) {
		return false;
	}
	for (auto &child : StructType::GetChildTypes(value.type())) {
		if (child.second.id() != LogicalTypeId::VARCHAR && child.second.id() != LogicalTypeId::SQLNULL) {
			return false;
		}
	}
	return true;
}

unique_ptr<FunctionData> GlueReplaceColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlueReplaceColumnsBindData>();
	result->target = ResolveGlueTable(context, "glue_replace_columns", input.inputs[0], false);
	GlueCatalog::ThrowIfInExplicitTransaction(context, "glue_replace_columns");
	auto &table = result->target.table;
	auto &columns = input.inputs[1];
	if (!IsStringStruct(columns) || StructType::GetChildCount(columns.type()) == 0) {
		throw BinderException("glue_replace_columns: the columns must be a struct of column name to type, e.g. "
		                      "{id: 'BIGINT', name: 'VARCHAR'}");
	}
	optional_ptr<const Value> comments;
	// as in Hive, the comments of the columns that stay are dropped unless keep_comments is set
	bool keep_comments = false;
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "comments") {
			comments = option.second;
		} else if (name == "keep_comments") {
			keep_comments = !option.second.IsNull() && option.second.GetValue<bool>();
		}
	}
	if (comments && !IsStringStruct(*comments)) {
		throw BinderException("glue_replace_columns: 'comments' must be a struct of column name to comment");
	}
	case_insensitive_map_t<idx_t> current;
	for (idx_t i = 0; i < table.columns.size(); i++) {
		current[table.columns[i].name] = i;
	}
	auto &column_names = StructType::GetChildTypes(columns.type());
	auto &column_types = StructValue::GetChildren(columns);
	// csv files are read by position, the other formats by name; other SerDes are refused
	bool by_position = table.GetFileFormat() == HiveFileFormat::CSV;
	if (by_position && column_names.size() != table.columns.size()) {
		throw BinderException("glue_replace_columns: table '%s' is stored as csv, which is read by position, so it "
		                      "must keep its %d columns",
		                      result->target.TableName(), table.columns.size());
	}
	case_insensitive_map_t<idx_t> given;
	for (idx_t i = 0; i < column_names.size(); i++) {
		GlueColumn column;
		column.name = column_names[i].first.GetIdentifierName();
		if (column_types[i].IsNull()) {
			throw BinderException("glue_replace_columns: no type given for column '%s'", column.name);
		}
		column.type =
		    GlueTypes::FromLogicalType(TransformStringToLogicalType(column_types[i].GetValue<string>(), context));
		// the type as it reads back from Glue; a type that can not be read back fails here, before Glue is changed
		auto type = GlueTypes::ToLogicalType(column.type);
		// a column that stays must keep a type its existing files can be read with, as for ALTER COLUMN TYPE
		optional_ptr<const GlueColumn> previous;
		if (by_position) {
			previous = &table.columns[i];
		} else {
			auto existing = current.find(column.name);
			if (existing != current.end()) {
				previous = &table.columns[existing->second];
			}
		}
		if (previous) {
			// a column matched by name keeps its stored name: the json reader matches field names case-sensitively
			if (!by_position) {
				column.name = previous->name;
			}
			if (keep_comments) {
				column.comment = previous->comment;
			}
			auto from = GlueTypes::ToLogicalType(previous->type);
			if (!GlueSchemaEntry::IsAllowedHiveTypeChange(from, type)) {
				throw BinderException("glue_replace_columns: can not change column '%s' of table '%s' from %s to %s: "
				                      "existing files keep their types, only widening changes are supported",
				                      previous->name, result->target.TableName(), from.ToString(), type.ToString());
			}
		}
		for (auto &key : table.partition_keys) {
			if (StringUtil::CIEquals(key.name, column.name)) {
				throw BinderException(
				    "glue_replace_columns: '%s' is a partition key of table '%s', the columns are the "
				    "data columns only",
				    column.name, result->target.TableName());
			}
		}
		given[column.name] = result->columns.size();
		result->columns.push_back(std::move(column));
	}
	if (comments) {
		auto &comment_names = StructType::GetChildTypes(comments->type());
		auto &comment_values = StructValue::GetChildren(*comments);
		for (idx_t i = 0; i < comment_names.size(); i++) {
			auto &name = comment_names[i].first.GetIdentifierName();
			auto column = given.find(name);
			if (column == given.end()) {
				throw BinderException("glue_replace_columns: 'comments' names '%s', which is not one of the columns",
				                      name);
			}
			// NULL removes the comment
			result->columns[column->second].comment =
			    comment_values[i].IsNull() ? string() : comment_values[i].GetValue<string>();
		}
	}
	auto require_kept = [&](const string &column) {
		if (given.find(column) == given.end()) {
			throw BinderException("glue_replace_columns: table '%s' is %s, the columns must keep '%s'",
			                      result->target.TableName(), table.DescribeBucketing(), column);
		}
	};
	for (auto &column : table.bucket_columns) {
		require_kept(column);
	}
	for (auto &column : table.sort_columns) {
		require_kept(column.name);
	}
	names = {"columns"};
	return_types = {LogicalType::LIST(LogicalType::STRUCT(
	    {{"name", LogicalType::VARCHAR}, {"type", LogicalType::VARCHAR}, {"comment", LogicalType::VARCHAR}}))};
	return std::move(result);
}

void GlueReplaceColumnsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GluePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GlueReplaceColumnsBindData>();
	auto &catalog = *bind_data.target.catalog;
	auto &table = bind_data.target.table;
	// keeps the refreshed entry alive while its columns are read below
	Transaction::Get(context, catalog);
	GlueAPI::UpdateTableColumns(context, catalog, table, bind_data.columns);
	auto schema = catalog.GetSchemas().GetEntry(context, table.schema_name);
	if (!schema) {
		throw CatalogException("Table \"%s\" was altered but its Glue database could not be found afterwards",
		                       bind_data.target.TableName());
	}
	auto &updated = schema->Cast<GlueSchemaEntry>().RefreshTable(context, table.name);
	vector<Value> columns;
	for (auto &column : updated.table_info.columns) {
		columns.push_back(
		    Value::STRUCT({{"name", Value(column.name)},
		                   {"type", Value(column.type)},
		                   {"comment", column.comment.empty() ? Value(LogicalType::VARCHAR) : Value(column.comment)}}));
	}
	output.data[0].Append(Value::LIST(ListType::GetChildType(output.data[0].GetType()), std::move(columns)));
	output.CheckCardinality(1);
}

//===--------------------------------------------------------------------===//
// glue_get_partition_response
//===--------------------------------------------------------------------===//
struct GlueGetPartitionResponseBindData : public TableFunctionData {
	GluePartitionTarget target;
	GluePartitionDetails partition;
	string raw_json;
};

unique_ptr<FunctionData> GlueGetPartitionResponseBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlueGetPartitionResponseBindData>();
	result->target = ResolveGlueTable(context, "glue_get_partition_response", input.inputs[0]);
	auto values = ParsePartitionSpec("glue_get_partition_response", result->target, input.inputs[1]);
	auto &table = result->target.table;
	if (!GlueAPI::GetPartition(context, *result->target.catalog, table.schema_name, table.name, values,
	                           result->partition, &result->raw_json)) {
		throw CatalogException("Partition [%s] does not exist in Glue table '%s'", StringUtil::Join(values, ", "),
		                       result->target.TableName());
	}
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	names = {"database_name", "table_name", "partition_values", "location", "serde_library",
	         "columns",       "parameters", "serde_parameters", "response"};
	return_types = {LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::VARCHAR),
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                GlueColumnsType(),
	                map_type,
	                map_type,
	                LogicalType::VARIANT()};
	return std::move(result);
}

void GlueGetPartitionResponseScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GluePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GlueGetPartitionResponseBindData>();
	auto &table = bind_data.target.table;
	auto &partition = bind_data.partition;
	vector<Value> values;
	for (auto &value : partition.values) {
		values.emplace_back(value);
	}

	output.data[0].Append(Value(table.schema_name));
	output.data[1].Append(Value(table.name));
	output.data[2].Append(Value::LIST(LogicalType::VARCHAR, std::move(values)));
	output.data[3].Append(Value(partition.location));
	output.data[4].Append(Value(partition.serde_library));
	output.data[5].Append(GlueColumnsToValue(partition.partition_columns));
	output.data[6].Append(GlueParametersToValue(partition.parameters));
	output.data[7].Append(GlueParametersToValue(partition.serde_parameters));

	// The complete Glue Partition object: JSON as serialized by the AWS SDK, cast to VARIANT
	Vector json(LogicalType::JSON(), 1);
	json.SetValue(0, Value(bind_data.raw_json));
	VectorOperations::Cast(context, json, output.data[8], 1);

	output.CheckCardinality(1);
}

} // namespace

TableFunction GetGlueGetPartitionResponseFunction() {
	TableFunction function("glue_get_partition_response", {LogicalType::VARCHAR, LogicalType::ANY},
	                       GlueGetPartitionResponseScan, GlueGetPartitionResponseBind, GluePartitionChangeInit);
	return function;
}

TableFunction GetGluePartitionsFunction() {
	TableFunction function("glue_partitions", {LogicalType::VARCHAR}, GluePartitionsScan, GluePartitionsBind,
	                       GluePartitionsInit);
	return function;
}

TableFunction GetGlueReplaceColumnsFunction() {
	TableFunction function("glue_replace_columns", {LogicalType::VARCHAR, LogicalType::ANY}, GlueReplaceColumnsScan,
	                       GlueReplaceColumnsBind, GluePartitionChangeInit);
	function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("comments", LogicalType::ANY);
		options.Add("keep_comments", LogicalType::BOOLEAN);
	});
	return function;
}

} // namespace duckdb
