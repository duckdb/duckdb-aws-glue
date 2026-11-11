#include "functions/glue_repair_table.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_result.hpp"

#include "api/glue_api.hpp"
#include "api/glue_api_util.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_file_credentials.hpp"
#include "catalog/glue_table.hpp"
#include "core/glue_types.hpp"
#include "functions/glue_functions.hpp"
#include "planning/hive_multi_file_reader.hpp"

namespace duckdb {

namespace {

struct GlueRepairTableBindData : public TableFunctionData {
	GluePartitionTarget target;
	vector<LogicalType> key_types;
	string root;

	//! The partitions are created from the table definition fetched at bind, so a prepared statement binds every time
	bool SupportStatementCache() const override {
		return false;
	}
};

struct GlueRepairTableState : public GlobalTableFunctionState {
	vector<GluePartitionInput> added;
	idx_t offset = 0;
	bool applied = false;
};

//! The repair lists the table's files by path, without the credentials its scans read with
// TODO: support repairing a table with credentials, by listing its files with them
void ThrowIfReadWithFileCredentials(ClientContext &context, const Value &table_name,
                                    const GluePartitionTarget &target) {
	if (!GlueFileCredentialsCallback::Find(context)) {
		return;
	}
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
	                       ResolveGlueTableName(context, "glue_repair_table", table_name.GetValue<string>()));
	auto entry = Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		return;
	}
	if (!GlueFileCredentialsCallback::Get(context, entry->Cast<GlueTable>()).empty()) {
		throw NotImplementedException("glue_repair_table of Glue table '%s', which is read with file credentials, is "
		                              "not supported",
		                              target.TableName());
	}
}

unique_ptr<FunctionData> GlueRepairTableBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlueRepairTableBindData>();
	result->target = ResolveGlueTable(context, "glue_repair_table", input.inputs[0]);
	GlueCatalog::ThrowIfInExplicitTransaction(context, "glue_repair_table");
	auto &table = result->target.table;
	// the partitions are read from the directory layout Hive writes; another engine's InputFormat lays files out its
	// way
	auto file_format = table.GetFileFormat();
	if (!table.GetFormat().IsWritable(table, file_format)) {
		throw NotImplementedException("glue_repair_table of Glue table '%s' is not supported: its InputFormat '%s' is "
		                              "not one Hive uses for %s tables, so DuckDB does not know how the files below "
		                              "the table location are laid out",
		                              result->target.TableName(), table.input_format,
		                              HiveFileFormatToString(file_format));
	}
	ThrowIfReadWithFileCredentials(context, input.inputs[0], result->target);
	result->root = result->target.table.location;
	StringUtil::RTrim(result->root, "/");
	if (result->root.empty()) {
		throw InvalidInputException("Table '%s' has no location in Glue", result->target.TableName());
	}
	for (auto &key : result->target.table.partition_keys) {
		auto type = GlueTypes::ToLogicalType(key.type);
		result->key_types.push_back(type);
		names.emplace_back(key.name);
		return_types.push_back(type);
	}
	names.emplace_back("location");
	auto deduplicated = names;
	QueryResult::DeduplicateColumns(deduplicated);
	names.back() = deduplicated.back();
	return_types.emplace_back(LogicalType::VARCHAR);
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GlueRepairTableInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlueRepairTableState>();
}

//! 'dt=2024-01-01/country=US/part' -> values [2024-01-01, US], partition_directory 'dt=2024-01-01/country=US'
bool ReadDiscoveredPartition(ClientContext &context, const GlueRepairTableBindData &bind_data,
                             const string &relative_directory, vector<string> &values, string &partition_directory) {
	auto &keys = bind_data.target.table.partition_keys;
	auto components = StringUtil::Split(relative_directory, '/');
	if (components.size() < keys.size()) {
		return false;
	}
	values.clear();
	partition_directory.clear();
	for (idx_t k = 0; k < keys.size(); k++) {
		auto &component = components[k];
		auto equals = component.find('=');
		if (equals == string::npos) {
			return false;
		}
		try {
			if (!StringUtil::CIEquals(HivePartitioning::Unescape(component.substr(0, equals)), keys[k].name)) {
				return false;
			}
			auto value = HivePartitioning::Unescape(component.substr(equals + 1));
			auto typed = GlueTypes::PartitionValue(context, keys[k].name, value, bind_data.key_types[k]);
			// stored as glue_add_partition stores it: year=07 -> '7'
			values.push_back(typed.IsNull() ? value : typed.ToString());
		} catch (std::exception &) {
			// not UTF-8 once unescaped, or not a value of the key's type
			return false;
		}
		if (k > 0) {
			partition_directory += "/";
		}
		partition_directory += component;
	}
	return true;
}

//! Calls 'callback' for every file below 'directory' as it is listed, keeping none of them: S3 lists the whole tree,
//! a local file system one directory at a time
void ListFilesBelow(FileSystem &fs, const string &directory,
                    const std::function<void(const OpenFileInfo &)> &callback) {
	vector<string> directories {directory};
	while (!directories.empty()) {
		auto current = std::move(directories.back());
		directories.pop_back();
		fs.ListFiles(current, [&](OpenFileInfo &info) {
			info.path = fs.JoinPath(current, info.path);
			if (FileSystem::IsDirectory(info)) {
				directories.push_back(std::move(info.path));
			} else {
				callback(info);
			}
		});
	}
}

void GlueRepairTableApply(ClientContext &context, TableFunctionInput &data, GlueRepairTableState &state) {
	auto &bind_data = data.bind_data->Cast<GlueRepairTableBindData>();
	auto &catalog = *bind_data.target.catalog;
	auto &table = bind_data.target.table;
	CheckWritable(catalog, "BatchCreatePartition");

	set<vector<string>> existing_values;
	unordered_set<string> existing_locations;
	for (auto &partition : GlueAPI::GetPartitions(context, catalog, table.schema_name, table.name)) {
		existing_values.insert(partition.values);
		auto location = partition.location;
		StringUtil::RTrim(location, "/");
		existing_locations.insert(std::move(location));
	}

	auto root_prefix = bind_data.root + "/";
	unordered_set<string> seen_directories;
	// values -> location: the first directory found for the values wins
	map<vector<string>, string> discovered;
	// never from a listing kept by glue_cache_listings (it misses files added after it), and not kept: a table can
	// hold more files than fit in memory
	ListFilesBelow(FileSystem::GetFileSystem(context), bind_data.root, [&](const OpenFileInfo &file) {
		if (!IsDataFile(file, root_prefix)) {
			return;
		}
		auto separator = file.path.find_last_of('/');
		if (separator == string::npos || separator < root_prefix.size()) {
			return;
		}
		auto directory = file.path.substr(0, separator);
		if (!seen_directories.insert(directory).second) {
			return;
		}
		auto relative = directory.substr(root_prefix.size());
		vector<string> values;
		string partition_directory;
		if (!ReadDiscoveredPartition(context, bind_data, relative, values, partition_directory)) {
			return;
		}
		auto location = root_prefix + partition_directory;
		if (existing_values.count(values) || existing_locations.count(location)) {
			return;
		}
		discovered.emplace(std::move(values), std::move(location));
	});

	for (auto &entry : discovered) {
		GluePartitionInput partition;
		partition.values = entry.first;
		partition.location = entry.second;
		state.added.push_back(std::move(partition));
	}
	if (state.added.empty()) {
		return;
	}
	GlueAPI::BatchCreatePartitions(context, catalog, table, state.added);
}

void GlueRepairTableScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<GlueRepairTableBindData>();
	auto &state = data.global_state->Cast<GlueRepairTableState>();
	if (!state.applied) {
		state.applied = true;
		GlueRepairTableApply(context, data, state);
	}
	auto &keys = bind_data.target.table.partition_keys;
	idx_t count = 0;
	while (state.offset < state.added.size() && count < STANDARD_VECTOR_SIZE) {
		auto &partition = state.added[state.offset++];
		for (idx_t k = 0; k < keys.size(); k++) {
			output.data[k].Append(
			    PartitionListingValue(context, keys[k].name, partition.values[k], bind_data.key_types[k]));
		}
		output.data[keys.size()].Append(Value(partition.location));
		count++;
	}
}

} // namespace

TableFunction GetGlueRepairTableFunction() {
	TableFunction function("glue_repair_table", {LogicalType::VARCHAR}, GlueRepairTableScan, GlueRepairTableBind,
	                       GlueRepairTableInit);
	return function;
}

} // namespace duckdb
