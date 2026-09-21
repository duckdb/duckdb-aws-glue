#include "glue_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "storage/hive_multi_file_reader.hpp"

namespace duckdb {

namespace {

//! The partition location when none is given: <root>/<key>=<value>/...
string DefaultPartitionLocation(const HiveScanInfo &info, const vector<string> &values) {
	auto location = info.root_location;
	StringUtil::RTrim(location, "/");
	for (idx_t i = 0; i < info.partition_keys.size(); i++) {
		location += "/" + HivePartitioning::Escape(info.partition_keys[i]) + "=";
		if (values[i] == HivePartitioning::DEFAULT_PARTITION_NAME) {
			location += values[i];
		} else {
			location += HivePartitioning::EscapeValue(values[i]);
		}
	}
	return location;
}

void ParseSchema(ClientContext &context, HiveScanInfo &info, const Value &schema) {
	if (schema.IsNull() || schema.type().id() != LogicalTypeId::STRUCT) {
		throw BinderException("hive_scan: 'schema' must be a struct of column name to type, e.g. "
		                      "{id: 'INTEGER', dt: 'VARCHAR'}");
	}
	auto &names = StructType::GetChildTypes(schema.type());
	auto &values = StructValue::GetChildren(schema);
	for (idx_t i = 0; i < names.size(); i++) {
		if (values[i].IsNull()) {
			throw BinderException("hive_scan: no type given for column '%s'", names[i].first.GetIdentifierName());
		}
		info.names.push_back(names[i].first);
		info.types.push_back(TransformStringToLogicalType(values[i].GetValue<string>(), context));
	}
	if (info.names.empty()) {
		throw BinderException("hive_scan: 'schema' must name at least one column");
	}
}

void ParsePartitionKeys(HiveScanInfo &info, const Value &keys) {
	if (keys.IsNull() || keys.type().id() != LogicalTypeId::LIST) {
		throw BinderException("hive_scan: 'partition_keys' must be a list of column names");
	}
	for (auto &key : ListValue::GetChildren(keys)) {
		info.partition_keys.push_back(key.GetValue<string>());
	}
}

void ParsePartitions(HiveScanInfo &info, const Value &partitions) {
	if (partitions.IsNull() || partitions.type().id() != LogicalTypeId::LIST) {
		throw BinderException("hive_scan: 'partitions' must be a list of structs, one per partition: the partition "
		                      "columns with their values and optionally 'location'");
	}
	for (auto &partition_value : ListValue::GetChildren(partitions)) {
		if (partition_value.IsNull() || partition_value.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("hive_scan: every entry of 'partitions' must be a struct");
		}
		auto &fields = StructType::GetChildTypes(partition_value.type());
		auto &values = StructValue::GetChildren(partition_value);
		// the partition keys are the fields of the first partition (other than 'location') unless given explicitly
		if (info.partition_keys.empty()) {
			for (auto &field : fields) {
				if (!StringUtil::CIEquals(field.first.GetIdentifierName(), "location")) {
					info.partition_keys.push_back(field.first.GetIdentifierName());
				}
			}
		}
		GluePartitionInfo partition;
		partition.values.resize(info.partition_keys.size());
		vector<bool> seen(info.partition_keys.size(), false);
		for (idx_t i = 0; i < fields.size(); i++) {
			auto &field_name = fields[i].first.GetIdentifierName();
			if (StringUtil::CIEquals(field_name, "location")) {
				if (!values[i].IsNull()) {
					partition.location = values[i].GetValue<string>();
					StringUtil::RTrim(partition.location, "/");
				}
				continue;
			}
			auto key_index = info.GetPartitionKeyIndex(field_name);
			if (key_index == DConstants::INVALID_INDEX) {
				throw BinderException("hive_scan: '%s' is not a partition key, the partition keys are: %s", field_name,
				                      StringUtil::Join(info.partition_keys, ", "));
			}
			seen[key_index] = true;
			partition.values[key_index] =
			    values[i].IsNull() ? string(HivePartitioning::DEFAULT_PARTITION_NAME) : values[i].ToString();
		}
		for (idx_t k = 0; k < info.partition_keys.size(); k++) {
			if (!seen[k]) {
				throw BinderException("hive_scan: partition %d has no value for partition key '%s'",
				                      info.partitions.size() + 1, info.partition_keys[k]);
			}
		}
		if (partition.location.empty()) {
			partition.location = DefaultPartitionLocation(info, partition.values);
		}
		info.partitions.push_back(std::move(partition));
	}
}

unique_ptr<FunctionData> HiveScanBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto scan_info = make_shared_ptr<HiveScanInfo>();
	scan_info->root_location = input.inputs[0].GetValue<string>();
	StringUtil::RTrim(scan_info->root_location, "/");
	scan_info->table_name = scan_info->root_location;
	if (scan_info->root_location.empty()) {
		throw BinderException("hive_scan: the root location must not be empty");
	}

	scan_info->file_format = HiveFileFormat::PARQUET;
	optional_ptr<const Value> schema;
	optional_ptr<const Value> partition_keys;
	optional_ptr<const Value> partitions;
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "schema") {
			schema = option.second;
		} else if (name == "partition_keys") {
			partition_keys = option.second;
		} else if (name == "partitions") {
			partitions = option.second;
		} else if (name == "format") {
			scan_info->file_format = HiveFileFormatFromString(option.second.GetValue<string>());
		} else if (name == "delim") {
			scan_info->delimiter = option.second.GetValue<string>();
		} else if (name == "quote") {
			scan_info->quote = option.second.GetValue<string>();
		} else if (name == "escape") {
			scan_info->escape = option.second.GetValue<string>();
		} else if (name == "header") {
			scan_info->header = option.second.GetValue<bool>();
		}
	}
	if (!schema) {
		throw BinderException("hive_scan requires the table schema: hive_scan('s3://...', schema := {id: 'INTEGER', "
		                      "dt: 'VARCHAR'})");
	}
	ParseSchema(context, *scan_info, *schema);
	if (partition_keys) {
		ParsePartitionKeys(*scan_info, *partition_keys);
	}
	if (partitions) {
		ParsePartitions(*scan_info, *partitions);
	}
	// 'partition_keys' with no 'partitions' argument at all reads nothing: declaring the keys makes the table
	// partitioned, and a partitioned table reads only the partitions it is given. That combination is a caller who
	// meant to pass partitions, so say so instead of returning an empty result.
	//
	// 'partitions := []' is a different statement -- the table has no registered partition -- and stays a legal empty
	// scan, matching a Glue table whose partitions have not been registered: unregistered data is not visible.
	if (partition_keys && !partitions) {
		throw BinderException("hive_scan: 'partition_keys' was given without 'partitions', which would read no rows. "
		                      "Pass the partitions to read, e.g. partitions := [{%s: '...'}]; pass partitions := [] "
		                      "for a table that has no registered partition",
		                      scan_info->partition_keys.empty() ? "dt" : scan_info->partition_keys[0]);
	}
	for (auto &key : scan_info->partition_keys) {
		bool found = false;
		for (auto &column : scan_info->names) {
			if (StringUtil::CIEquals(column.GetIdentifierName(), key)) {
				found = true;
				break;
			}
		}
		if (!found) {
			throw BinderException("hive_scan: partition key '%s' is not a column of the schema", key);
		}
	}

	names = scan_info->names;
	return_types = scan_info->types;
	unique_ptr<FunctionData> bind_data;
	BindHiveScan(context, std::move(scan_info), bind_data);
	return bind_data;
}

} // namespace

TableFunction GetHiveScanFunction(DatabaseInstance &db) {
	// hive_scan is read_parquet with its own bind: the schema and partitions come from the arguments instead of from a
	// Glue table, and the bind picks the reader for the format (read_parquet, read_csv, read_json)
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &catalog_schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto catalog_entry = catalog_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "read_parquet");
	if (!catalog_entry) {
		throw MissingExtensionException("hive_scan requires the parquet extension");
	}
	auto &function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	shared_ptr<const TableFunction> base;
	for (idx_t i = 0; i < function_set.functions.Size(); i++) {
		auto candidate = function_set.functions.GetFunctionByOffset(i);
		if (candidate->arguments.size() == 1 && candidate->arguments[0].id() == LogicalTypeId::LIST) {
			base = candidate;
		}
	}
	if (!base) {
		throw InternalException("read_parquet has no list overload to base hive_scan on");
	}
	TableFunction function = *base;
	function.name = "hive_scan";
	function.arguments = {LogicalType::VARCHAR};
	function.named_parameters.clear();
	function.named_parameters["schema"] = LogicalType::ANY;
	function.named_parameters["partition_keys"] = LogicalType::LIST(LogicalType::VARCHAR);
	function.named_parameters["partitions"] = LogicalType::ANY;
	function.named_parameters["format"] = LogicalType::VARCHAR;
	function.named_parameters["delim"] = LogicalType::VARCHAR;
	function.named_parameters["quote"] = LogicalType::VARCHAR;
	function.named_parameters["escape"] = LogicalType::VARCHAR;
	function.named_parameters["header"] = LogicalType::BOOLEAN;
	function.bind = HiveScanBind;
	function.bind_replace = nullptr;
	function.get_multi_file_reader = HiveMultiFileReader::CreateInstance;
	function.function_info = nullptr;
	// the bind data holds a reader with state that a serialized plan can not carry
	function.serialize = nullptr;
	function.deserialize = nullptr;
	return function;
}

} // namespace duckdb
