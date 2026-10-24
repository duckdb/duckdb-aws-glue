#include "functions/glue_functions.hpp"
#include "functions/hive_scan_call.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "planning/hive_multi_file_reader.hpp"

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

//! The data files of a 'files' argument: paths, or structs of a 'filename' and the options to open the file with
vector<OpenFileInfo> ParseFiles(const Value &files, const string &argument) {
	if (files.type().id() != LogicalTypeId::LIST) {
		throw BinderException("hive_scan: %s must be a list of files, e.g. ['s3://bucket/path/data_0.parquet']",
		                      argument);
	}
	vector<OpenFileInfo> result;
	for (auto &file : ListValue::GetChildren(files)) {
		result.push_back(OpenFileInfo::FromValue(file, Identifier("hive_scan")));
	}
	return result;
}

bool IsReservedPartitionField(const string &field_name) {
	return StringUtil::CIEquals(field_name, "location") || StringUtil::CIEquals(field_name, "files");
}

void ParsePartitions(HiveScanInfo &info, const Value &partitions) {
	if (partitions.IsNull() || partitions.type().id() != LogicalTypeId::LIST) {
		throw BinderException("hive_scan: 'partitions' must be a list of structs, one per partition: the partition "
		                      "columns with their values and optionally 'location' and 'files'");
	}
	vector<GluePartitionInfo> result;
	vector<vector<OpenFileInfo>> partition_files;
	idx_t partitions_with_files = 0;
	for (auto &partition_value : ListValue::GetChildren(partitions)) {
		if (partition_value.IsNull() || partition_value.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("hive_scan: every entry of 'partitions' must be a struct");
		}
		auto &fields = StructType::GetChildTypes(partition_value.type());
		auto &values = StructValue::GetChildren(partition_value);
		// the partition keys are the first partition's fields other than 'location' and 'files', unless given
		if (info.partition_keys.empty()) {
			for (auto &field : fields) {
				if (!IsReservedPartitionField(field.first.GetIdentifierName())) {
					info.partition_keys.push_back(field.first.GetIdentifierName());
				}
			}
		}
		GluePartitionInfo partition;
		partition.values.resize(info.partition_keys.size());
		vector<bool> seen(info.partition_keys.size(), false);
		vector<OpenFileInfo> files;
		bool has_files = false;
		for (idx_t i = 0; i < fields.size(); i++) {
			auto &field_name = fields[i].first.GetIdentifierName();
			if (StringUtil::CIEquals(field_name, "location")) {
				if (!values[i].IsNull()) {
					partition.location = values[i].GetValue<string>();
					StringUtil::RTrim(partition.location, "/");
				}
				continue;
			}
			if (StringUtil::CIEquals(field_name, "files")) {
				if (!values[i].IsNull()) {
					files = ParseFiles(values[i], StringUtil::Format("'files' of partition %d", result.size() + 1));
					has_files = true;
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
				throw BinderException("hive_scan: partition %d has no value for partition key '%s'", result.size() + 1,
				                      info.partition_keys[k]);
			}
		}
		if (partition.location.empty()) {
			partition.location = DefaultPartitionLocation(info, partition.values);
		}
		if (has_files) {
			partitions_with_files++;
		}
		partition_files.push_back(std::move(files));
		result.push_back(std::move(partition));
	}
	if (partitions_with_files > 0 && partitions_with_files < result.size()) {
		throw BinderException("hive_scan: either every partition gives its 'files' or none does, but %d of the %d "
		                      "partitions do",
		                      partitions_with_files, result.size());
	}
	if (partitions_with_files > 0) {
		info.explicit_files = true;
		info.partition_files = std::move(partition_files);
	}
	info.SetPartitions(std::move(result));
}

//! A file given twice would be read twice
void VerifyFilesGivenOnce(const HiveScanInfo &info) {
	unordered_set<string> paths;
	auto verify = [&](const OpenFileInfo &file) {
		if (!paths.insert(file.path).second) {
			throw BinderException("hive_scan: the file '%s' is given more than once", file.path);
		}
	};
	for (auto &file : info.root_files) {
		verify(file);
	}
	for (auto &files : info.partition_files) {
		for (auto &file : files) {
			verify(file);
		}
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
	optional_ptr<const Value> files;
	bool header_given = false;
	bool skip_given = false;
	bool null_for_invalid_data_given = false;
	bool parallel_given = false;
	// a reading argument added here is restated by HiveScanGetCall too
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "schema") {
			schema = option.second;
		} else if (name == "partition_keys") {
			partition_keys = option.second;
		} else if (name == "partitions") {
			partitions = option.second;
		} else if (name == "files") {
			files = option.second;
		} else if (option.second.IsNull()) {
			throw BinderException("hive_scan: '%s' must not be NULL", name);
		} else if (name == "format") {
			scan_info->file_format = HiveFileFormatFromString(option.second.GetValue<string>());
		} else if (name == "delim") {
			scan_info->csv_options.delimiter = option.second.GetValue<string>();
		} else if (name == "quote") {
			scan_info->csv_options.quote = option.second.GetValue<string>();
		} else if (name == "escape") {
			scan_info->csv_options.escape = option.second.GetValue<string>();
		} else if (name == "header") {
			scan_info->csv_options.skip_lines = option.second.GetValue<bool>() ? 1 : 0;
			header_given = true;
		} else if (name == "skip") {
			auto skip = option.second.GetValue<int64_t>();
			if (skip < 0) {
				throw BinderException("hive_scan: 'skip' must not be negative, but is %d", skip);
			}
			scan_info->csv_options.skip_lines = NumericCast<idx_t>(skip);
			skip_given = true;
		} else if (name == "nullstr") {
			scan_info->csv_options.null_string = option.second.GetValue<string>();
		} else if (name == "serde_fields") {
			scan_info->csv_options.serde_fields = option.second.GetValue<bool>();
		} else if (name == "null_for_invalid_data") {
			scan_info->csv_options.null_for_invalid_data = option.second.GetValue<bool>();
			null_for_invalid_data_given = true;
		} else if (name == "parallel") {
			scan_info->csv_options.parallel = option.second.GetValue<bool>();
			parallel_given = true;
		} else if (name == "ignore_errors") {
			scan_info->json_options.ignore_errors = option.second.GetValue<bool>();
		}
	}
	if (header_given && skip_given) {
		throw BinderException("hive_scan: give either 'header' or 'skip' (the number of header lines), not both");
	}
	if (null_for_invalid_data_given && !scan_info->csv_options.serde_fields) {
		throw BinderException("hive_scan: 'null_for_invalid_data' needs serde_fields := true: with read_csv rules a "
		                      "field that does not parse as its column type fails the query");
	}
	if (!parallel_given) {
		scan_info->csv_options.UseParallelSetting(context);
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
	if (partition_keys && !partitions) {
		throw BinderException("hive_scan: 'partition_keys' was given without 'partitions', which would read no rows. "
		                      "Pass the partitions to read, e.g. partitions := [{%s: '...'}]; pass partitions := [] "
		                      "for a table that has no registered partition",
		                      scan_info->partition_keys.empty() ? "dt" : scan_info->partition_keys[0]);
	}
	if (files && !files->IsNull()) {
		if (partitions || !scan_info->partition_keys.empty()) {
			throw BinderException("hive_scan: 'files' is for a table without partition keys; give the files of a "
			                      "partitioned table per partition, e.g. partitions := [{dt: '...', files: [...]}]");
		}
		scan_info->root_files = ParseFiles(*files, "'files'");
		scan_info->explicit_files = true;
	}
	VerifyFilesGivenOnce(*scan_info);
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
	// the scan runs as its format's reader, like the scan of a Glue table: hive_scan itself is read_parquet, whose
	// pushdown capabilities are not those of the other formats
	input.table_function = BoundTableFunction(BindHiveScan(context, std::move(scan_info), bind_data));
	return bind_data;
}

unique_ptr<FunctionData> HiveScanListBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<Identifier> &names) {
	throw BinderException("hive_scan takes a single root location, not a list of files");
}

//! A scan is a Hive scan of this build when BindHiveScan gave its reader the HiveMultiFileReader
optional_ptr<const HiveScanInfo> HiveScanInfoOf(const LogicalGet &get) {
	if (get.function.get_multi_file_reader != HiveMultiFileReader::CreateInstance || !get.bind_data) {
		return nullptr;
	}
	auto &bind_data = get.bind_data->Cast<MultiFileBindData>();
	if (!bind_data.multi_file_reader) {
		return nullptr;
	}
	return &bind_data.multi_file_reader->Cast<HiveMultiFileReader>().ScanInfo();
}

} // namespace

optional<HiveScanCall> HiveScanGetCall(const LogicalGet &get) {
	auto info = HiveScanInfoOf(get);
	if (!info) {
		return nullopt;
	}
	HiveScanCall call;
	call.root = info->root_location;
	child_list_t<Value> schema;
	for (idx_t i = 0; i < info->names.size(); i++) {
		schema.emplace_back(info->names[i], Value(info->types[i].ToString()));
	}
	call.arguments["schema"] = Value::STRUCT(std::move(schema));
	call.arguments["format"] = Value(HiveFileFormatToString(info->file_format));
	if (!info->partition_keys.empty()) {
		vector<Value> keys;
		for (auto &key : info->partition_keys) {
			keys.emplace_back(key);
		}
		call.arguments["partition_keys"] = Value::LIST(LogicalType::VARCHAR, std::move(keys));
		call.partition_keys = info->partition_keys;
	}
	switch (info->file_format) {
	case HiveFileFormat::CSV: {
		auto &csv = info->csv_options;
		call.arguments["delim"] = Value(csv.delimiter);
		call.arguments["quote"] = Value(csv.quote);
		call.arguments["escape"] = Value(csv.escape);
		call.arguments["skip"] = Value::BIGINT(NumericCast<int64_t>(csv.skip_lines));
		call.arguments["nullstr"] = Value(csv.null_string);
		call.arguments["serde_fields"] = Value::BOOLEAN(csv.serde_fields);
		if (csv.serde_fields) {
			call.arguments["null_for_invalid_data"] = Value::BOOLEAN(csv.null_for_invalid_data);
		}
		// as bound: hive_csv_parallel was resolved in the session that planned the scan
		call.arguments["parallel"] = Value::BOOLEAN(csv.parallel);
		break;
	}
	case HiveFileFormat::JSON:
		call.arguments["ignore_errors"] = Value::BOOLEAN(info->json_options.ignore_errors);
		break;
	case HiveFileFormat::PARQUET:
	case HiveFileFormat::AVRO:
		break;
	}
	return call;
}

optional<HiveScanFilePartition> HiveScanGetFilePartition(const LogicalGet &get, const string &path) {
	auto info = HiveScanInfoOf(get);
	if (!info || info->partition_keys.empty()) {
		return nullopt;
	}
	auto partition = info->TryGetPartitionOfFile(path);
	if (!partition) {
		return nullopt;
	}
	HiveScanFilePartition result;
	result.values = partition->values;
	result.location = partition->location;
	return result;
}

TableFunctionSet GetHiveScanFunction(DatabaseInstance &db) {
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
		auto &signature = candidate->GetSignature();
		if (signature.GetParameterCount() >= 1 && signature.GetParameter(0).GetType().id() == LogicalTypeId::LIST) {
			base = candidate;
		}
	}
	if (!base) {
		throw InternalException("read_parquet has no list overload to base hive_scan on");
	}
	TableFunction function = *base;
	function.name = "hive_scan";
	FunctionSignature signature;
	signature.SetReturnType(base->GetReturnType());
	signature.AddParameter(LogicalType::VARCHAR);
	signature.WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("schema", LogicalType::ANY);
		options.Add("partition_keys", LogicalType::LIST(LogicalType::VARCHAR));
		options.Add("partitions", LogicalType::ANY);
		options.Add("files", LogicalType::ANY);
		options.Add("format", LogicalType::VARCHAR);
		options.Add("delim", LogicalType::VARCHAR);
		options.Add("quote", LogicalType::VARCHAR);
		options.Add("escape", LogicalType::VARCHAR);
		options.Add("header", LogicalType::BOOLEAN);
		options.Add("skip", LogicalType::BIGINT);
		options.Add("nullstr", LogicalType::VARCHAR);
		options.Add("serde_fields", LogicalType::BOOLEAN);
		options.Add("null_for_invalid_data", LogicalType::BOOLEAN);
		options.Add("parallel", LogicalType::BOOLEAN);
		options.Add("ignore_errors", LogicalType::BOOLEAN);
	});
	function.GetSignature() = std::move(signature);
	function.bind = HiveScanBind;
	function.bind_replace = nullptr;
	function.get_multi_file_reader = HiveMultiFileReader::CreateInstance;
	function.function_info = nullptr;
	// the bind replaces this function with the format's reader, which serializes the scan
	function.serialize = nullptr;
	function.deserialize = nullptr;

	// a Hive scan, of a Glue table or a hive_scan() call, is its format's reader named hive_scan: a serialized one
	// records the reader's list argument, so deserializing it resolves to this overload, which binds the scan again
	TableFunction list_function = *base;
	list_function.name = "hive_scan";
	list_function.bind = HiveScanListBind;
	list_function.bind_replace = nullptr;
	SetHiveScanSerialization(list_function);

	TableFunctionSet set("hive_scan");
	set.AddFunction(std::move(function));
	set.AddFunction(std::move(list_function));
	return set;
}

} // namespace duckdb
