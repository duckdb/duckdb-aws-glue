#include "planning/hive_multi_file_reader.hpp"
#include "planning/hive_query_cache.hpp"
#include "planning/stats/hive_stats.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "core/glue_types.hpp"

namespace duckdb {

//! The scan info of the hive scan being bound on this thread. The reader is created by the bound function's own bind
//! (read_parquet, read_csv, read_json) through get_multi_file_reader, which only sees the TableFunction, and the
//! function_info slot of the JSON reader is taken by its multi-file wrapper: so the scan info is handed over here.
static thread_local shared_ptr<HiveScanInfo> *current_scan_info = nullptr;

struct HiveScanInfoScope {
	explicit HiveScanInfoScope(shared_ptr<HiveScanInfo> &info) {
		current_scan_info = &info;
	}
	~HiveScanInfoScope() {
		current_scan_info = nullptr;
	}
};

//===--------------------------------------------------------------------===//
// HiveScanInfo
//===--------------------------------------------------------------------===//
idx_t HiveScanInfo::GetPartitionKeyIndex(const string &name) const {
	for (idx_t i = 0; i < partition_keys.size(); i++) {
		if (StringUtil::CIEquals(partition_keys[i], name)) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

shared_ptr<const vector<GluePartitionInfo>> HiveScanInfo::Partitions(ClientContext &context) const {
	annotated_lock_guard<annotated_mutex> guard(partitions_lock);
	if (!partitions) {
		if (partition_keys.empty() || catalog_name.empty()) {
			// unpartitioned, or a hive_scan that was given none
			partitions = make_shared_ptr<const vector<GluePartitionInfo>>();
		} else {
			partitions = GetTablePartitions(context, *this);
		}
	}
	return partitions;
}

bool HiveScanInfo::PartitionsLoaded() const {
	annotated_lock_guard<annotated_mutex> guard(partitions_lock);
	return partitions != nullptr;
}

void HiveScanInfo::SetPartitions(vector<GluePartitionInfo> partitions_p) {
	annotated_lock_guard<annotated_mutex> guard(partitions_lock);
	D_ASSERT(!partitions);
	partitions = make_shared_ptr<const vector<GluePartitionInfo>>(std::move(partitions_p));
}

idx_t HiveScanInfo::GetPartitionIndexOfFile(const string &path) const {
	lock_guard<mutex> guard(file_partitions_lock);
	auto entry = file_partitions.find(path);
	if (entry == file_partitions.end()) {
		throw InternalException("Hive scan of '%s.%s': data file '%s' does not belong to any Glue partition",
		                        database_name, table_name, path);
	}
	return entry->second;
}

const GluePartitionInfo &HiveScanInfo::GetPartitionOfFile(const string &path) const {
	auto partition_index = GetPartitionIndexOfFile(path);
	annotated_lock_guard<annotated_mutex> guard(partitions_lock);
	// a file is attributed to a partition only after the partitions are loaded, and they are never replaced
	D_ASSERT(partitions);
	return (*partitions)[partition_index];
}

string HiveScanInfo::Describe() const {
	if (database_name.empty()) {
		return table_name;
	}
	return database_name + "." + table_name;
}

//! Whether a path below a location is hidden: Hive skips _* and .* files and directories
static bool IsHiddenPath(const string &relative_path) {
	for (auto &component : StringUtil::Split(relative_path, '/')) {
		if (component.empty() || component[0] == '_' || component[0] == '.') {
			return true;
		}
	}
	return false;
}

//! The data files below 'location', at any depth (like read_parquet on a directory)
static void ListDataFiles(ClientContext &context, const string &location, vector<OpenFileInfo> &files) {
	auto directory = location;
	StringUtil::RTrim(directory, "/");
	if (directory.empty()) {
		return;
	}
	auto prefix = directory + "/";
	for (auto &file : HiveQueryCache::Get(context)->GetDirectoryListing(context, directory)->GetAllFiles()) {
		if (!StringUtil::StartsWith(file.path, prefix) || IsHiddenPath(file.path.substr(prefix.size()))) {
			continue;
		}
		files.push_back(file);
	}
}

//===--------------------------------------------------------------------===//
// HiveMultiFileList
//===--------------------------------------------------------------------===//
HiveMultiFileList::HiveMultiFileList(ClientContext &context, shared_ptr<HiveScanInfo> scan_info_p,
                                     vector<idx_t> partition_indexes_p)
    : LazyMultiFileList(&context), client_context(context), scan_info(std::move(scan_info_p)),
      partition_indexes(make_shared_ptr<const vector<idx_t>>(std::move(partition_indexes_p))) {
}

HiveMultiFileList::HiveMultiFileList(ClientContext &context, shared_ptr<HiveScanInfo> scan_info_p)
    : LazyMultiFileList(&context), client_context(context), scan_info(std::move(scan_info_p)) {
}

shared_ptr<const vector<idx_t>> HiveMultiFileList::PartitionIndexes() const {
	annotated_lock_guard<annotated_mutex> guard(indexes_lock);
	if (!partition_indexes) {
		auto partitions = scan_info->Partitions(client_context);
		vector<idx_t> all;
		for (idx_t i = 0; i < partitions->size(); i++) {
			all.push_back(i);
		}
		partition_indexes = make_shared_ptr<const vector<idx_t>>(std::move(all));
	}
	return partition_indexes;
}

void HiveMultiFileList::PlanListings() const {
	if (planned) {
		return;
	}
	planned = true;
	if (scan_info->partition_keys.empty()) {
		listing_jobs.push_back(ListingJob {true, {}});
		return;
	}
	// the partitions below the table root can be listed together
	auto root = scan_info->root_location;
	StringUtil::RTrim(root, "/");
	vector<idx_t> below_root;
	vector<idx_t> elsewhere;
	auto partitions = scan_info->Partitions(client_context);
	auto indexes = PartitionIndexes();
	for (auto partition_index : *indexes) {
		auto location = (*partitions)[partition_index].location;
		StringUtil::RTrim(location, "/");
		if (!root.empty() && location.size() > root.size() + 1 && StringUtil::StartsWith(location, root + "/")) {
			below_root.push_back(partition_index);
		} else {
			elsewhere.push_back(partition_index);
		}
	}
	idx_t threshold = 10;
	Value setting;
	if (client_context.TryGetCurrentSetting("hive_partition_listing_threshold", setting) && !setting.IsNull()) {
		threshold = setting.GetValue<idx_t>();
	}
	if (!below_root.empty() && below_root.size() >= threshold) {
		listing_jobs.push_back(ListingJob {true, std::move(below_root)});
	} else {
		for (auto partition_index : below_root) {
			listing_jobs.push_back(ListingJob {false, {partition_index}});
		}
	}
	for (auto partition_index : elsewhere) {
		listing_jobs.push_back(ListingJob {false, {partition_index}});
	}
}

void HiveMultiFileList::BuildPartitionLocations() const {
	if (partition_locations_built) {
		return;
	}
	partition_locations_built = true;
	auto partitions = scan_info->Partitions(client_context);
	for (idx_t i = 0; i < partitions->size(); i++) {
		auto location = (*partitions)[i].location;
		StringUtil::RTrim(location, "/");
		if (location.empty()) {
			continue;
		}
		// two partitions on the same location: the first wins, as in AddFile
		partition_by_location.emplace(std::move(location), i);
	}
}

optional_idx HiveMultiFileList::OwningPartition(const string &file_path, idx_t min_directory_size) const {
	auto separator = file_path.find_last_of('/');
	if (separator == string::npos) {
		return optional_idx();
	}
	auto directory = file_path.substr(0, separator);
	while (directory.size() >= min_directory_size) {
		auto entry = partition_by_location.find(directory);
		if (entry != partition_by_location.end()) {
			return optional_idx(entry->second);
		}
		auto parent = directory.find_last_of('/');
		if (parent == string::npos) {
			break;
		}
		directory = directory.substr(0, parent);
	}
	return optional_idx();
}

void HiveMultiFileList::ListPartition(idx_t partition_index) const {
	auto partitions = scan_info->Partitions(client_context);
	auto &partition = (*partitions)[partition_index];
	if (partition.values.size() != scan_info->partition_keys.size()) {
		throw InvalidInputException("Partition [%s] of Hive table '%s' has %d values but the table has %d partition "
		                            "keys",
		                            StringUtil::Join(partition.values, ", "), scan_info->Describe(),
		                            partition.values.size(), scan_info->partition_keys.size());
	}
	auto location = partition.location;
	StringUtil::RTrim(location, "/");
	vector<OpenFileInfo> partition_files;
	ListDataFiles(client_context, location, partition_files);
	BuildPartitionLocations();
	lock_guard<mutex> guard(scan_info->file_partitions_lock);
	for (auto &file : partition_files) {
		// skip files of another partition registered at a location nested inside this one
		auto owner = OwningPartition(file.path, location.size());
		if (owner.IsValid() && owner.GetIndex() != partition_index) {
			continue;
		}
		AddFile(std::move(file), partition_index);
	}
}

void HiveMultiFileList::AddFile(OpenFileInfo file, idx_t partition_index) const {
	// Deduplicated per list: the scan info is shared with the copies of this list (the late materialization
	// optimizer copies the bind data) and with the list before partition pruning, which all list the same files
	if (!listed_files.insert(file.path).second) {
		return;
	}
	scan_info->file_partitions[file.path] = partition_index;
	expanded_files.push_back(std::move(file));
}

void HiveMultiFileList::ListRoot(const vector<idx_t> &partitions) const {
	auto root = scan_info->root_location;
	StringUtil::RTrim(root, "/");
	if (root.empty()) {
		throw InvalidInputException("Hive table '%s' has no location", scan_info->Describe());
	}
	// one recursive listing of the root: on S3 a flat ListObjectsV2 over the prefix, 1000 keys per request, continued
	// from the pages the sample fetched
	auto files = HiveQueryCache::Get(client_context)->GetDirectoryListing(client_context, root)->GetAllFiles();
	unordered_set<idx_t> reading;
	auto all = scan_info->Partitions(client_context);
	for (auto partition_index : partitions) {
		auto &partition = (*all)[partition_index];
		if (partition.values.size() != scan_info->partition_keys.size()) {
			throw InvalidInputException("Partition [%s] of Hive table '%s' has %d values but the table has %d "
			                            "partition keys",
			                            StringUtil::Join(partition.values, ", "), scan_info->Describe(),
			                            partition.values.size(), scan_info->partition_keys.size());
		}
		reading.insert(partition_index);
	}
	BuildPartitionLocations();
	auto root_prefix = root + "/";
	lock_guard<mutex> guard(scan_info->file_partitions_lock);
	for (auto &file : files) {
		if (!StringUtil::StartsWith(file.path, root_prefix) || IsHiddenPath(file.path.substr(root_prefix.size()))) {
			continue;
		}
		auto partition_index = OwningPartition(file.path, root.size() + 1);
		if (!partition_index.IsValid() || !reading.count(partition_index.GetIndex())) {
			continue;
		}
		AddFile(std::move(file), partition_index.GetIndex());
	}
}

bool HiveMultiFileList::ExpandNextPath() const {
	// called with the list's lock held; runs one listing per call
	PlanListings();
	if (next_job >= listing_jobs.size()) {
		return false;
	}
	auto &job = listing_jobs[next_job++];
	if (scan_info->partition_keys.empty()) {
		if (scan_info->root_location.empty()) {
			throw InvalidInputException("Hive table '%s' has no location", scan_info->Describe());
		}
		ListDataFiles(client_context, scan_info->root_location, expanded_files);
		return true;
	}
	if (job.root) {
		ListRoot(job.partitions);
	} else {
		ListPartition(job.partitions[0]);
	}
	return true;
}

FileExpandResult HiveMultiFileList::GetExpandResult() const {
	// Until everything is listed the answer is a guess, and listing now would defeat the point of listing lazily:
	// the schema comes from the table, so nothing at bind time needs a file
	lock_guard<mutex> lck(lock);
	if (!all_files_expanded) {
		return FileExpandResult::MULTIPLE_FILES;
	}
	if (expanded_files.size() > 1) {
		return FileExpandResult::MULTIPLE_FILES;
	}
	return expanded_files.size() == 1 ? FileExpandResult::SINGLE_FILE : FileExpandResult::NO_FILES;
}

MultiFileCount HiveMultiFileList::GetFileCount(idx_t min_exact_count) const {
	// The optimizer asks for a rough file count to estimate the cardinality. Listing the partitions for that would
	// make planning (and EXPLAIN) pay for the listing; answer with what is known and at least one file per partition
	// still to list.
	lock_guard<mutex> lck(lock);
	// An exact count is answered exactly: the progress calculation keeps asking for one more file until everything
	// is expanded, and never terminates on an estimate. Planning asks with no minimum (see HiveScanCardinality).
	while (!all_files_expanded && expanded_files.size() < min_exact_count) {
		if (client_context.IsInterrupted()) {
			throw InterruptException();
		}
		if (!ExpandNextPath()) {
			all_files_expanded = true;
		}
	}
	if (all_files_expanded) {
		return MultiFileCount(expanded_files.size(), FileExpansionType::ALL_FILES_EXPANDED);
	}
	PlanListings();
	idx_t remaining = 0;
	for (idx_t i = next_job; i < listing_jobs.size(); i++) {
		remaining += listing_jobs[i].root ? MaxValue<idx_t>(listing_jobs[i].partitions.size(), 1) : 1;
	}
	return MultiFileCount(expanded_files.size() + remaining, FileExpansionType::NOT_ALL_FILES_KNOWN);
}

vector<OpenFileInfo> HiveMultiFileList::ListSampleDirectory() const {
	lock_guard<mutex> lck(lock);
	PlanListings();
	vector<OpenFileInfo> files;
	if (scan_info->partition_keys.empty()) {
		ListDataFiles(client_context, scan_info->root_location, files);
		return files;
	}
	if (listing_jobs.empty()) {
		return files;
	}
	if (listing_jobs[0].root) {
		return SampleRootPartitions(listing_jobs[0].partitions);
	}
	auto partition_index = listing_jobs[0].partitions[0];
	auto location = (*scan_info->Partitions(client_context))[partition_index].location;
	StringUtil::RTrim(location, "/");
	vector<OpenFileInfo> listed;
	ListDataFiles(client_context, location, listed);
	// as ListPartition keeps them: not the files of a partition registered at a location nested inside this one
	BuildPartitionLocations();
	lock_guard<mutex> guard(scan_info->file_partitions_lock);
	for (auto &file : listed) {
		auto owner = OwningPartition(file.path, location.size());
		if (owner.IsValid() && owner.GetIndex() != partition_index) {
			continue;
		}
		scan_info->file_partitions[file.path] = partition_index;
		files.push_back(std::move(file));
	}
	return files;
}

vector<OpenFileInfo> HiveMultiFileList::SampleRootPartitions(const vector<idx_t> &partitions) const {
	auto root = scan_info->root_location;
	StringUtil::RTrim(root, "/");
	auto root_prefix = root + "/";
	auto listing = HiveQueryCache::Get(client_context)->GetDirectoryListing(client_context, root);
	unordered_set<idx_t> reading(partitions.begin(), partitions.end());
	BuildPartitionLocations();
	optional_idx sampled;
	string sampled_prefix;
	bool in_sampled = false;
	bool listed_all = false;
	optional_idx last_partition;
	idx_t last_partition_start = 0;
	vector<OpenFileInfo> files;
	vector<idx_t> file_partitions;
	MultiFileListScanData scan;
	listing->InitializeScan(scan);
	// the pages fetched so far, and more only to find a partition the scan reads or while it may continue
	scan.scan_type = MultiFileListScanType::FETCH_IF_AVAILABLE;
	OpenFileInfo file;
	while (true) {
		if (!listing->Scan(scan, file)) {
			if (scan.scan_type == MultiFileListScanType::ALWAYS_FETCH) {
				listed_all = true;
				break;
			}
			if (sampled.IsValid() && !in_sampled) {
				break;
			}
			scan.scan_type = MultiFileListScanType::ALWAYS_FETCH;
			continue;
		}
		if (!StringUtil::StartsWith(file.path, root_prefix) || IsHiddenPath(file.path.substr(root_prefix.size()))) {
			continue;
		}
		in_sampled = sampled.IsValid() && StringUtil::StartsWith(file.path, sampled_prefix);
		if (scan.scan_type == MultiFileListScanType::ALWAYS_FETCH && sampled.IsValid() && !in_sampled) {
			// pages come in key order, so the sampled partition's files are all seen
			break;
		}
		auto owner = OwningPartition(file.path, root_prefix.size());
		if (!owner.IsValid()) {
			continue;
		}
		if (owner != last_partition) {
			last_partition = owner;
			last_partition_start = files.size();
		}
		if (!sampled.IsValid() && reading.count(owner.GetIndex())) {
			sampled = owner;
			sampled_prefix = (*scan_info->Partitions(client_context))[owner.GetIndex()].location;
			StringUtil::RTrim(sampled_prefix, "/");
			sampled_prefix += "/";
			in_sampled = true;
		}
		files.push_back(file);
		file_partitions.push_back(owner.GetIndex());
	}
	if (!listed_all && last_partition.IsValid() && last_partition != sampled) {
		// the pages fetched may end inside the last partition
		files.resize(last_partition_start);
	}
	lock_guard<mutex> guard(scan_info->file_partitions_lock);
	for (idx_t i = 0; i < files.size(); i++) {
		scan_info->file_partitions[files[i].path] = file_partitions[i];
	}
	return files;
}

vector<OpenFileInfo> HiveMultiFileList::GetDisplayFileList(optional_idx max_files) const {
	bool expanded;
	{
		lock_guard<mutex> lck(lock);
		expanded = all_files_expanded;
	}
	if (expanded) {
		// the base lists the files, which takes the lock: it must not be held here
		return LazyMultiFileList::GetDisplayFileList(max_files);
	}
	// Not listed yet (bind, EXPLAIN): show what is known without loading the partitions or listing
	vector<OpenFileInfo> result;
	if (scan_info->partition_keys.empty() || !scan_info->PartitionsLoaded()) {
		result.emplace_back(scan_info->root_location);
		return result;
	}
	auto partitions = scan_info->Partitions(client_context);
	auto indexes = PartitionIndexes();
	for (auto partition_index : *indexes) {
		if (max_files.IsValid() && result.size() >= max_files.GetIndex()) {
			break;
		}
		result.emplace_back((*partitions)[partition_index].location);
	}
	return result;
}

unique_ptr<MultiFileList> HiveMultiFileList::Copy() const {
	shared_ptr<const vector<idx_t>> indexes;
	{
		annotated_lock_guard<annotated_mutex> guard(indexes_lock);
		indexes = partition_indexes;
	}
	if (!indexes) {
		return make_uniq<HiveMultiFileList>(client_context, scan_info);
	}
	return make_uniq<HiveMultiFileList>(client_context, scan_info, *indexes);
}

//===--------------------------------------------------------------------===//
// Binding
//===--------------------------------------------------------------------===//
static const TableFunction &GetListReadFunction(ClientContext &context, const string &function_name,
                                                const HiveScanInfo &scan_info) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &catalog_schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto catalog_entry = catalog_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, Identifier(function_name));
	if (!catalog_entry) {
		throw MissingExtensionException("Reading Hive table '%s' requires %s, which is not available",
		                                scan_info.Describe(), function_name);
	}
	auto &function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	return *function_set.functions.GetFunctionByArguments(context, {LogicalType::LIST(LogicalType::VARCHAR)});
}

//! The full description of the scan: CommonSubplanOptimizer compares it to decide whether sub-plans are equal, and a
//! deserialized plan binds the same scan again from it
static void HiveScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                              const BoundTableFunction &function) {
	auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
	auto &list = bind_data.file_list->Cast<HiveMultiFileList>();
	auto &info = list.ScanInfo();
	serializer.WriteProperty(103, "location", info.root_location);
	// by value: an index into Glue's partition list means nothing elsewhere. Partitions are loaded lazily; the
	// optimizer only serializes scans of statements that plan one, which load them anyway (GetFileCount, pruning).
	vector<vector<string>> partitions;
	vector<string> locations;
	auto all = info.Partitions(list.Context());
	auto indexes = list.PartitionIndexes();
	for (auto index : *indexes) {
		partitions.push_back((*all)[index].values);
		locations.push_back((*all)[index].location);
	}
	serializer.WriteProperty(104, "partitions", partitions);
	serializer.WriteProperty(105, "partition_locations", locations);
	serializer.WriteProperty(106, "types", bind_data.types);
	serializer.WriteProperty(107, "names", bind_data.names);
	serializer.WriteProperty(108, "format", HiveFileFormatToString(info.file_format));
	serializer.WriteProperty(109, "partition_keys", info.partition_keys);
	serializer.WriteProperty(110, "columns", info.names);
	serializer.WriteProperty(111, "column_types", info.types);
	serializer.WriteProperty(112, "delimiter", info.delimiter);
	serializer.WriteProperty(113, "quote", info.quote);
	serializer.WriteProperty(114, "escape", info.escape);
	serializer.WriteProperty(115, "header", info.header);
}

//! Bind the scan again from what HiveScanSerialize wrote: the partitions it holds are the ones the plan reads, those
//! that filters already pruned are not brought back
static unique_ptr<FunctionData> HiveScanDeserialize(Deserializer &deserializer, BoundTableFunction &function) {
	auto &context = deserializer.Get<ClientContext &>();
	auto info = make_shared_ptr<HiveScanInfo>();
	info->root_location = deserializer.ReadProperty<string>(103, "location");
	info->table_name = info->root_location;
	auto partitions = deserializer.ReadProperty<vector<vector<string>>>(104, "partitions");
	auto locations = deserializer.ReadProperty<vector<string>>(105, "partition_locations");
	deserializer.ReadProperty<vector<LogicalType>>(106, "types");
	deserializer.ReadProperty<vector<Identifier>>(107, "names");
	info->file_format = HiveFileFormatFromString(deserializer.ReadProperty<string>(108, "format"));
	info->partition_keys = deserializer.ReadProperty<vector<string>>(109, "partition_keys");
	info->names = deserializer.ReadProperty<vector<Identifier>>(110, "columns");
	info->types = deserializer.ReadProperty<vector<LogicalType>>(111, "column_types");
	info->delimiter = deserializer.ReadProperty<string>(112, "delimiter");
	info->quote = deserializer.ReadProperty<string>(113, "quote");
	info->escape = deserializer.ReadProperty<string>(114, "escape");
	info->header = deserializer.ReadProperty<bool>(115, "header");
	if (partitions.size() != locations.size()) {
		throw SerializationException("Hive scan of \"%s\": %d partitions but %d partition locations", info->table_name,
		                             partitions.size(), locations.size());
	}
	vector<GluePartitionInfo> scan_partitions;
	for (idx_t i = 0; i < partitions.size(); i++) {
		scan_partitions.push_back({std::move(partitions[i]), std::move(locations[i])});
	}
	// the partitions the plan read, not Glue's current ones: a deserialized scan does not ask Glue again
	info->SetPartitions(std::move(scan_partitions));
	unique_ptr<FunctionData> bind_data;
	function = BoundTableFunction(BindHiveScan(context, std::move(info), bind_data));
	return bind_data;
}

void SetHiveScanSerialization(TableFunction &function) {
	function.SetSerializeCallback(HiveScanSerialize);
	function.SetDeserializeCallback(HiveScanDeserialize);
}

static BindInfo GlueHiveBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &multi_file_data = bind_data->Cast<MultiFileBindData>();
	auto &info = multi_file_data.multi_file_reader->Cast<HiveMultiFileReader>().ScanInfo();
	auto result = info.format_bind_info ? info.format_bind_info(bind_data) : BindInfo(ScanType::EXTERNAL);
	result.table = info.table;
	return result;
}

TableFunction BindHiveScan(ClientContext &context, shared_ptr<HiveScanInfo> scan_info,
                           unique_ptr<FunctionData> &bind_data) {
	// the reader for the file format; the data columns (everything but the partition keys) are what the files hold
	child_list_t<Value> data_columns;
	for (idx_t i = 0; i < scan_info->names.size(); i++) {
		if (scan_info->GetPartitionKeyIndex(scan_info->names[i].GetIdentifierName()) == DConstants::INVALID_INDEX) {
			data_columns.emplace_back(scan_info->names[i], Value(scan_info->types[i].ToString()));
		}
	}
	named_argument_map_t param_map;
	string function_name;
	switch (scan_info->file_format) {
	case HiveFileFormat::PARQUET:
		function_name = "read_parquet";
		break;
	case HiveFileFormat::CSV:
		// Hive CSV files carry no schema: the columns are given, by position, and the dialect is the one the table
		// describes - there is nothing left for the sniffer to find, so every file is opened without sniffing
		function_name = "read_csv";
		param_map["columns"] = Value::STRUCT(data_columns);
		param_map["auto_detect"] = Value::BOOLEAN(false);
		param_map["header"] = Value::BOOLEAN(scan_info->header);
		param_map["delim"] = Value(scan_info->delimiter);
		param_map["quote"] = Value(scan_info->quote);
		param_map["escape"] = Value(scan_info->escape);
		// a quoted empty field is an empty string, not NULL (Hive reads it that way, and DuckDB writes it for one)
		param_map["allow_quoted_nulls"] = Value::BOOLEAN(false);
		break;
	case HiveFileFormat::JSON:
		// one JSON object per line, keys matched to the columns by name
		ExtensionHelper::AutoLoadExtension(context, "json");
		function_name = "read_json";
		param_map["columns"] = Value::STRUCT(data_columns);
		param_map["format"] = Value("newline_delimited");
		break;
	case HiveFileFormat::AVRO:
		// the files carry their schema, columns are matched by name like parquet
		ExtensionHelper::AutoLoadExtension(context, "avro");
		function_name = "read_avro";
		break;
	}
	auto scan_function = GetListReadFunction(context, function_name, *scan_info);
	// with the HiveMultiFileReader: the table's schema and partition values, not the files'
	scan_function.get_multi_file_reader = HiveMultiFileReader::CreateInstance;
	// the format reader serializes its file list, which would expand this lazy list (listing S3) while the
	// common-subplan optimizer computes plan signatures
	SetHiveScanSerialization(scan_function);
	// partition columns are answered from the partition values; the format keeps every other column
	scan_info->format_statistics = scan_function.statistics_extended;
	scan_function.statistics_extended = HivePartitionStatistics;
	// and the row count comes from a sampled file rather than the format's constant
	scan_function.cardinality = HiveScanCardinality;
	scan_info->format_bind_info = scan_function.get_bind_info;
	scan_function.get_bind_info = GlueHiveBindInfo;

	vector<LogicalType> return_types;
	vector<Identifier> names;
	// the path argument is not used: CreateFileList builds the file list from the partitions
	TableFunctionRef empty_ref;
	vector<Value> inputs = {Value::LIST(LogicalType::VARCHAR, {Value(scan_info->root_location)})};
	// the JSON reader's multi-file wrapper reads its wrapped function from the bind input's info
	scan_function.GetSignature().FillNamedDefaults(context, param_map);
	BoundTableFunction bound_function(scan_function);
	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, scan_function.function_info.get(),
	                                  nullptr, bound_function, empty_ref);
	HiveScanInfoScope scope(scan_info);
	bind_data = scan_function.bind(context, bind_input, return_types, names);
	scan_function.SetName("hive_scan");
	return scan_function;
}

//===--------------------------------------------------------------------===//
// HiveMultiFileReader
//===--------------------------------------------------------------------===//
HiveMultiFileReader::HiveMultiFileReader(shared_ptr<HiveScanInfo> scan_info_p) : scan_info(std::move(scan_info_p)) {
}

unique_ptr<MultiFileReader> HiveMultiFileReader::CreateInstance(const BoundTableFunction &table) {
	shared_ptr<HiveScanInfo> info;
	if (current_scan_info) {
		info = *current_scan_info;
	}
	auto result = make_uniq<HiveMultiFileReader>(std::move(info));
	result->function_name = table.GetDefinition()->name;
	return std::move(result);
}

unique_ptr<MultiFileReader> HiveMultiFileReader::Copy() const {
	auto result = make_uniq<HiveMultiFileReader>(scan_info);
	result->function_name = function_name;
	return std::move(result);
}

const HiveScanInfo &HiveMultiFileReader::ScanInfo() const {
	if (!scan_info) {
		throw InternalException("HiveMultiFileReader used without a HiveScanInfo (a hive scan can not be restored "
		                        "from a serialized plan)");
	}
	return *scan_info;
}

shared_ptr<MultiFileList> HiveMultiFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                              const FileGlobInput &glob_input) {
	// every partition; pruning narrows the list before anything is loaded or listed
	return make_shared_ptr<HiveMultiFileList>(context, scan_info);
}

bool HiveMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                               vector<Identifier> &names, MultiFileReaderBindData &bind_data) {
	// The schema is the one Glue defines, not the schema of the first file: files are mapped to it by column name
	auto &info = ScanInfo();
	return_types = info.types;
	names = info.names;
	bind_data.schema.clear();
	for (idx_t i = 0; i < info.names.size(); i++) {
		// columns a file does not have are filled in per file in FinalizeBind
		bind_data.schema.push_back(MultiFileColumnDefinition::CreateFromNameAndType(info.names[i], info.types[i]));
	}
	bind_data.mapping = MultiFileColumnMappingMode::BY_NAME;
	return true;
}

void HiveMultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files,
                                      vector<LogicalType> &return_types, vector<Identifier> &names,
                                      MultiFileReaderBindData &bind_data) {
	// partition values come from Glue, never from the directory names
	options.auto_detect_hive_partitioning = false;
	options.hive_partitioning = false;
	options.union_by_name = false;
	MultiFileReader::BindOptions(options, files, return_types, names, bind_data);
}

//===--------------------------------------------------------------------===//
// Partition pruning
//===--------------------------------------------------------------------===//
struct PartitionKeyProjection {
	idx_t projected_column_index;
	idx_t partition_key_index;
};

//! Replace references to partition columns of the scanned table by the partition's values
static void ReplacePartitionColumnRefs(ClientContext &context, unique_ptr<Expression> &expr, TableIndex table_index,
                                       const vector<PartitionKeyProjection> &projections, const HiveScanInfo &info,
                                       const GluePartitionInfo &partition) {
	if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		if (colref.Binding().table_index != table_index) {
			return;
		}
		auto column_index = colref.Binding().column_index.GetIndex();
		for (auto &projection : projections) {
			if (projection.projected_column_index != column_index) {
				continue;
			}
			auto &key = info.partition_keys[projection.partition_key_index];
			auto &partition_value = partition.values[projection.partition_key_index];
			auto value = GlueTypes::PartitionValue(context, key, partition_value, colref.GetReturnType());
			expr = make_uniq<BoundConstantExpression>(std::move(value));
			return;
		}
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		ReplacePartitionColumnRefs(context, child, table_index, projections, info, partition);
	});
}

//! The partitions among 'candidates' that no filter rules out, each filter evaluated with the partition's values in
//! place of its partition columns. A filter that needs data columns is skipped. 'decided_filters' are those true of
//! every partition kept.
static vector<idx_t> PartitionsToRead(ClientContext &context, const HiveScanInfo &info,
                                      const vector<GluePartitionInfo> &partitions, const vector<idx_t> &candidates,
                                      TableIndex table_index, const vector<PartitionKeyProjection> &projections,
                                      const vector<unique_ptr<Expression>> &filters,
                                      unordered_set<idx_t> &pruning_filters, unordered_set<idx_t> &decided_filters) {
	vector<idx_t> kept;
	vector<idx_t> true_in_kept(filters.size(), 0);
	for (auto partition_index : candidates) {
		auto &partition = partitions[partition_index];
		bool keep = true;
		vector<idx_t> true_here;
		for (idx_t filter_index = 0; filter_index < filters.size(); filter_index++) {
			auto filter_copy = filters[filter_index]->Copy();
			ReplacePartitionColumnRefs(context, filter_copy, table_index, projections, info, partition);
			Value result;
			if (!filter_copy->IsScalar() || !filter_copy->IsFoldable() ||
			    !ExpressionExecutor::TryEvaluateScalar(context, *filter_copy, result)) {
				// needs the data columns, can not decide here
				continue;
			}
			if (result.IsNull() || !result.GetValue<bool>()) {
				keep = false;
				pruning_filters.insert(filter_index);
				break;
			}
			true_here.push_back(filter_index);
		}
		if (keep) {
			kept.push_back(partition_index);
			for (auto filter_index : true_here) {
				true_in_kept[filter_index]++;
			}
		}
	}
	for (idx_t filter_index = 0; filter_index < filters.size(); filter_index++) {
		if (!kept.empty() && true_in_kept[filter_index] == kept.size()) {
			decided_filters.insert(filter_index);
		}
	}
	return kept;
}

//! Which projected columns ('column_ids' into 'column_names') are partition keys
static vector<PartitionKeyProjection> PartitionKeyProjections(const HiveScanInfo &info,
                                                              const vector<column_t> &column_ids,
                                                              const vector<Identifier> &column_names) {
	vector<PartitionKeyProjection> projections;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		if (IsVirtualColumn(column_ids[i])) {
			continue;
		}
		auto key_index = info.GetPartitionKeyIndex(column_names[column_ids[i]].GetIdentifierName());
		if (key_index != DConstants::INVALID_INDEX) {
			projections.push_back({i, key_index});
		}
	}
	return projections;
}

static void AddPruningFiltersToExtraInfo(ExtraOperatorInfo &extra_info, const vector<unique_ptr<Expression>> &filters,
                                         const unordered_set<idx_t> &pruning_filters) {
	for (idx_t filter_index = 0; filter_index < filters.size(); filter_index++) {
		if (pruning_filters.find(filter_index) == pruning_filters.end()) {
			continue;
		}
		if (!extra_info.file_filters.empty()) {
			extra_info.file_filters += " AND ";
		}
		extra_info.file_filters += filters[filter_index]->ToString();
	}
}

unique_ptr<MultiFileList> HiveMultiFileReader::ComplexFilterPushdown(ClientContext &context, MultiFileList &files,
                                                                     const MultiFileOptions &options,
                                                                     MultiFilePushdownInfo &pushdown_info,
                                                                     vector<unique_ptr<Expression>> &filters) {
	auto &info = ScanInfo();
	if (info.partition_keys.empty() || filters.empty()) {
		return nullptr;
	}
	auto projections = PartitionKeyProjections(info, pushdown_info.column_ids, pushdown_info.column_names);
	if (projections.empty()) {
		return nullptr;
	}
	// A filter that can be evaluated with the partition values alone decides whether the partition is read at all,
	// before its directory is listed. Such a filter is then true of every row read and is removed, as DuckDB's own
	// hive partitioning does: kept, the optimizer would cost it a second time (a range filter keeps a fifth of the
	// rows the pruning already narrowed down).
	// first use of the partitions: loaded here, with the filters known
	auto &hive_list = files.Cast<HiveMultiFileList>();
	auto candidates = hive_list.PartitionIndexes();
	auto partitions = info.Partitions(context);
	unordered_set<idx_t> pruning_filters;
	unordered_set<idx_t> decided_filters;
	auto kept = PartitionsToRead(context, info, *partitions, *candidates, pushdown_info.table_index, projections,
	                             filters, pruning_filters, decided_filters);
	AddPruningFiltersToExtraInfo(pushdown_info.extra_info, filters, pruning_filters);
	vector<unique_ptr<Expression>> remaining_filters;
	for (idx_t filter_index = 0; filter_index < filters.size(); filter_index++) {
		if (decided_filters.find(filter_index) == decided_filters.end()) {
			remaining_filters.push_back(std::move(filters[filter_index]));
		}
	}
	filters = std::move(remaining_filters);
	// reported as files in EXPLAIN, but these are partitions: nothing has been listed yet
	pushdown_info.extra_info.total_files = candidates->size();
	pushdown_info.extra_info.filtered_files = kept.size();
	if (kept.size() == candidates->size()) {
		return nullptr;
	}
	return make_uniq<HiveMultiFileList>(context, scan_info, std::move(kept));
}

unique_ptr<MultiFileList> HiveMultiFileList::DynamicFilterPushdown(MultiFileDynamicPushdownInfo &info) const {
	auto &scan = *scan_info;
	if (scan.partition_keys.empty() || !info.filters.HasFilters()) {
		return nullptr;
	}
	struct PartitionKeyFilter {
		const ExpressionFilter &filter;
		idx_t partition_key_index;
		const LogicalType &type;
	};
	vector<PartitionKeyFilter> key_filters;
	for (auto &entry : info.filters) {
		auto primary_index = info.column_indexes[entry.GetIndex()].GetPrimaryIndex();
		if (IsVirtualColumn(primary_index)) {
			continue;
		}
		auto key_index = scan.GetPartitionKeyIndex(info.column_names[primary_index].GetIdentifierName());
		if (key_index == DConstants::INVALID_INDEX) {
			continue;
		}
		auto &filter =
		    ExpressionFilter::GetExpressionFilter(entry.Filter(), "HiveMultiFileList::DynamicFilterPushdown");
		key_filters.push_back({filter, key_index, info.column_types[primary_index]});
	}
	if (key_filters.empty()) {
		return nullptr;
	}
	// join filters are optional filters, which fold to true as expressions: evaluate the filter they wrap instead
	auto candidates = PartitionIndexes();
	auto partitions = scan.Partitions(info.context);
	vector<idx_t> kept;
	for (auto partition_index : *candidates) {
		auto &partition = (*partitions)[partition_index];
		bool keep = true;
		for (auto &key_filter : key_filters) {
			auto &key = scan.partition_keys[key_filter.partition_key_index];
			auto value = GlueTypes::PartitionValue(info.context, key, partition.values[key_filter.partition_key_index],
			                                       key_filter.type);
			if (!key_filter.filter.EvaluateWithConstant(info.context, value)) {
				keep = false;
				break;
			}
		}
		if (keep) {
			kept.push_back(partition_index);
		}
	}
	if (kept.size() == candidates->size()) {
		return nullptr;
	}
	return make_uniq<HiveMultiFileList>(info.context, scan_info, std::move(kept));
}

//===--------------------------------------------------------------------===//
// Per file
//===--------------------------------------------------------------------===//
void HiveMultiFileReader::FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
                                       const MultiFileReaderBindData &options,
                                       const vector<MultiFileColumnDefinition> &global_columns,
                                       const vector<ColumnIndex> &global_column_ids, ClientContext &context,
                                       optional_ptr<MultiFileReaderGlobalState> global_state) {
	// The first constant registered for a column wins, so the partition values go in before the base runs: with a
	// union schema (the JSON reader's default) the base would otherwise fill every column a file lacks, partition
	// columns included, with NULL.
	auto &info = ScanInfo();
	case_insensitive_set_t local_names;
	for (auto &local_column : reader_data.reader->GetColumns()) {
		local_names.insert(local_column.name.GetIdentifierName());
	}
	optional_ptr<const GluePartitionInfo> partition;
	if (!info.partition_keys.empty()) {
		partition = &info.GetPartitionOfFile(reader_data.reader->GetFileName());
	}
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto &column_id = global_column_ids[i];
		if (column_id.IsVirtualColumn()) {
			continue;
		}
		auto &global_column = global_columns[column_id.GetPrimaryIndex()];
		auto &name = global_column.name.GetIdentifierName();
		auto key_index = info.GetPartitionKeyIndex(name);
		if (key_index != DConstants::INVALID_INDEX) {
			// a partition column is a constant: the value Glue stores for the file's partition
			auto &key = info.partition_keys[key_index];
			auto value = GlueTypes::PartitionValue(context, key, partition->values[key_index], global_column.type);
			reader_data.constant_map.Add(MultiFileGlobalIndex(i), std::move(value));
			continue;
		}
		if (local_names.find(name) == local_names.end()) {
			// a data column the file does not have (added to the table after the file was written) reads as NULL
			auto &type = column_id.HasType() ? column_id.GetScanType() : global_column.type;
			reader_data.constant_map.Add(MultiFileGlobalIndex(i), Value(type));
		}
	}
	// the filename / file_index virtual columns
	MultiFileReader::FinalizeBind(reader_data, file_options, options, global_columns, global_column_ids, context,
	                              global_state);
}

} // namespace duckdb
