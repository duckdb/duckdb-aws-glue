#include "planning/hive_multi_file_reader.hpp"
#include "planning/hive_query_cache.hpp"
#include "planning/stats/hive_stats.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "duckdb/common/path.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "catalog/glue_table.hpp"
#include "core/glue_types.hpp"
#include "functions/hive_reader_call.hpp"

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
	auto partition_index = FindPartitionIndexOfFile(path);
	if (!partition_index.IsValid()) {
		throw InternalException("Hive scan of '%s.%s': data file '%s' does not belong to any Glue partition",
		                        database_name, table_name, path);
	}
	return partition_index.GetIndex();
}

optional_idx HiveScanInfo::FindPartitionIndexOfFile(const string &path) const {
	lock_guard<mutex> guard(file_partitions_lock);
	auto entry = file_partitions.find(path);
	if (entry == file_partitions.end()) {
		return optional_idx();
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

static bool SameCSVDialect(const HiveCSVOptions &a, const HiveCSVOptions &b) {
	return a.delimiter == b.delimiter && a.quote == b.quote && a.escape == b.escape && a.skip_lines == b.skip_lines &&
	       a.null_string == b.null_string && a.serde_fields == b.serde_fields &&
	       a.null_for_invalid_data == b.null_for_invalid_data && a.false_unless_true == b.false_unless_true;
}

ErrorData HiveScanInfo::PartitionStorageMismatch(const GluePartitionInfo &partition) const {
	if (!this->table) {
		return ErrorData();
	}
	auto &table = this->table->Cast<GlueTable>().table_info;
	auto &input_format = partition.input_format.empty() ? table.input_format : partition.input_format;
	auto &serde_library = partition.serde_library.empty() ? table.serde_library : partition.serde_library;
	if (StringUtil::CIEquals(input_format, table.input_format) &&
	    StringUtil::CIEquals(serde_library, table.serde_library) &&
	    (partition.serde_library.empty() || partition.serde_parameters == table.serde_parameters)) {
		return ErrorData();
	}
	auto partition_storage = table;
	partition_storage.input_format = input_format;
	partition_storage.serde_library = serde_library;
	if (!partition.serde_library.empty()) {
		partition_storage.serde_parameters = partition.serde_parameters;
	}
	string stored_as;
	auto format = GlueTableFormat::Detect(partition_storage);
	if (!format.IsHive()) {
		stored_as = format.Describe(partition_storage);
	} else {
		try {
			auto partition_format = partition_storage.GetFileFormat();
			if (IsTextFileFormat(partition_format)) {
				partition_storage.CheckTextSerdeSupported(partition_format);
			}
			if (partition_format != file_format) {
				stored_as = HiveFileFormatToString(partition_format);
			} else if (partition_format == HiveFileFormat::CSV &&
			           !SameCSVDialect(partition_storage.GetCSVOptions(), csv_options)) {
				stored_as = "csv with another dialect";
			} else if (partition_format == HiveFileFormat::JSON &&
			           partition_storage.GetJSONOptions().ignore_errors != json_options.ignore_errors) {
				stored_as = "json with other SerDe properties";
			}
		} catch (NotImplementedException &) {
			stored_as = "an unsupported format";
		}
	}
	// the names differ but read alike, e.g. the older parquet.hive.* classes of a Parquet table
	if (stored_as.empty()) {
		return ErrorData();
	}
	vector<string> spec;
	for (idx_t i = 0; i < partition_keys.size() && i < partition.values.size(); i++) {
		spec.push_back(partition_keys[i] + "=" + partition.values[i]);
	}
	return ErrorData(
	    ExceptionType::NOT_IMPLEMENTED,
	    StringUtil::Format("Partition [%s] of Hive table '%s' is stored as %s (InputFormat '%s', SerDe '%s'), but "
	                       "the table as %s (SerDe '%s'): partitions stored in a format other than the table's can "
	                       "not be read yet",
	                       StringUtil::Join(spec, ", "), Describe(), stored_as, input_format, serde_library,
	                       HiveFileFormatToString(file_format), table.serde_library));
}

void HiveScanInfo::VerifyConsistentPartitionSerDe(const GluePartitionInfo &partition) const {
	auto mismatch = PartitionStorageMismatch(partition);
	if (mismatch.HasError()) {
		mismatch.Throw();
	}
}

string HiveScanInfo::Describe() const {
	if (database_name.empty()) {
		return table_name;
	}
	return database_name + "." + table_name;
}

//! Whether a listed object below 'prefix' (the location, with its trailing slash) is a data file. Hive skips _* and .*
//! files and directories; EMR and S3 tools mark a directory with a <name>_$folder$ object next to it (an object, never
//! a directory, so only the last component is checked); and a zero-byte object of any name holds no rows and is not a
//! file of any format the reader opens
bool IsDataFile(const OpenFileInfo &file, const string &prefix) {
	if (!StringUtil::StartsWith(file.path, prefix)) {
		return false;
	}
	auto components = StringUtil::Split(file.path.substr(prefix.size()), '/');
	for (auto &component : components) {
		if (component.empty() || component[0] == '_' || component[0] == '.') {
			return false;
		}
	}
	if (components.empty() || StringUtil::EndsWith(components.back(), "_$folder$")) {
		return false;
	}
	auto size = ListedFileSize(file);
	return !size.IsValid() || size.GetIndex() > 0;
}

//! Glue tables only: a hive_scan root has no catalog, and no write through the extension clears its listing
static bool CacheListings(ClientContext &context, const HiveScanInfo &scan_info) {
	if (scan_info.catalog_name.empty()) {
		return false;
	}
	Value enabled;
	return context.TryGetCurrentSetting("glue_cache_listings", enabled) && !enabled.IsNull() &&
	       BooleanValue::Get(enabled);
}

//! The data files below 'location', at any depth (like read_parquet on a directory)
static void ListDataFiles(ClientContext &context, bool cached, const string &location, vector<OpenFileInfo> &files) {
	auto directory = location;
	StringUtil::RTrim(directory, "/");
	if (directory.empty()) {
		return;
	}
	auto prefix = directory + "/";
	for (auto &file : HiveQueryCache::Get(context)->GetDirectoryListing(context, directory, cached)->GetAllFiles()) {
		if (!IsDataFile(file, prefix)) {
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
	ListDataFiles(client_context, CacheListings(client_context, *scan_info), location, partition_files);
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
	auto files = HiveQueryCache::Get(client_context)
	                 ->GetDirectoryListing(client_context, root, CacheListings(client_context, *scan_info))
	                 ->GetAllFiles();
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
		if (!IsDataFile(file, root_prefix)) {
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
		ListDataFiles(client_context, CacheListings(client_context, *scan_info), scan_info->root_location,
		              expanded_files);
		return true;
	}
	auto partitions = scan_info->Partitions(client_context);
	for (auto partition_index : job.partitions) {
		scan_info->VerifyConsistentPartitionSerDe((*partitions)[partition_index]);
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
		ListDataFiles(client_context, CacheListings(client_context, *scan_info), scan_info->root_location, files);
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
	ListDataFiles(client_context, CacheListings(client_context, *scan_info), location, listed);
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
	auto listing = HiveQueryCache::Get(client_context)
	                   ->GetDirectoryListing(client_context, root, CacheListings(client_context, *scan_info));
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
		if (!IsDataFile(file, root_prefix)) {
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
	serializer.WriteProperty(112, "csv_options", info.csv_options);
	serializer.WriteProperty(113, "json_options", info.json_options);
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
	info->csv_options = deserializer.ReadProperty<HiveCSVOptions>(112, "csv_options");
	info->json_options = deserializer.ReadProperty<HiveJSONOptions>(113, "json_options");
	if (partitions.size() != locations.size()) {
		throw SerializationException("Hive scan of \"%s\": %d partitions but %d partition locations", info->table_name,
		                             partitions.size(), locations.size());
	}
	vector<GluePartitionInfo> partition_infos;
	for (idx_t i = 0; i < partitions.size(); i++) {
		GluePartitionInfo partition;
		partition.values = std::move(partitions[i]);
		partition.location = std::move(locations[i]);
		partition_infos.push_back(std::move(partition));
	}
	info->SetPartitions(std::move(partition_infos));
	unique_ptr<FunctionData> bind_data;
	function = BoundTableFunction(BindHiveScan(context, std::move(info), bind_data));
	return bind_data;
}

void SetHiveScanSerialization(TableFunction &function) {
	function.SetSerializeCallback(HiveScanSerialize);
	function.SetDeserializeCallback(HiveScanDeserialize);
}

static optional_ptr<TableCatalogEntry> HiveScanGetTableEntry(optional_ptr<const FunctionData> bind_data) {
	auto &multi_file_data = bind_data->Cast<MultiFileBindData>();
	return multi_file_data.multi_file_reader->Cast<HiveMultiFileReader>().ScanInfo().table;
}

static virtual_column_map_t HiveScanGetVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
	auto &info = bind_data.multi_file_reader->Cast<HiveMultiFileReader>().ScanInfo();
	virtual_column_map_t result;
	if (info.format_virtual_columns) {
		result = info.format_virtual_columns(context, bind_data_p);
	}
	HiveMultiFileReader::AddMetadataColumns(result);
	// the column mapper looks the types of the virtual columns up here
	HiveMultiFileReader::AddMetadataColumns(bind_data.virtual_columns);
	return result;
}

//! Whether a column of the table, or a field nested in one, is declared binary (BLOB)
static bool HasBinaryColumn(const HiveScanInfo &scan_info) {
	for (auto &type : scan_info.types) {
		if (TypeVisitor::Contains(type, LogicalTypeId::BLOB)) {
			return true;
		}
	}
	return false;
}

static void HiveTextComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                          vector<unique_ptr<Expression>> &filters) {
	auto &multi_file_data = bind_data->Cast<MultiFileBindData>();
	auto &scan_info = multi_file_data.multi_file_reader->Cast<HiveMultiFileReader>().ScanInfo();
	if (scan_info.format_pushdown_complex_filter) {
		scan_info.format_pushdown_complex_filter(context, get, bind_data, filters);
	}
	// Occupy table_filters with an always-true filter: FilterPushdown then keeps every filter in the plan instead
	// of converting it into table filters the text readers cannot apply (see InitializeReader). The join filters
	// are unaffected: they attach to the scan at run time and prune the file list through the partition values.
	if (!filters.empty() && !get.table_filters.HasFilters() && !get.GetColumnIds().empty()) {
		auto &type = get.GetColumnType(get.GetColumnIds()[0]);
		auto always_true = make_uniq<BoundConjunctionExpression>(
		    ExpressionType::CONJUNCTION_OR,
		    ExpressionFilter::CreateNullCheckExpression(make_uniq<BoundReferenceExpression>(type, 0ULL),
		                                                ExpressionType::OPERATOR_IS_NULL),
		    ExpressionFilter::CreateNullCheckExpression(make_uniq<BoundReferenceExpression>(type, 0ULL),
		                                                ExpressionType::OPERATOR_IS_NOT_NULL));
		get.table_filters.PushFilter(ProjectionIndex(0), make_uniq<ExpressionFilter>(std::move(always_true)));
	}
}

//! The standard reader of a file format, which BindHiveScan wraps and HiveScanGetReaderCall names
static string ReaderFunctionName(HiveFileFormat format) {
	switch (format) {
	case HiveFileFormat::PARQUET:
		return "read_parquet";
	case HiveFileFormat::CSV:
		return "read_csv";
	case HiveFileFormat::JSON:
		return "read_json";
	case HiveFileFormat::AVRO:
		return "read_avro";
	}
	throw InternalException("Unknown Hive file format");
}

static TableFunctionToSQLResult HiveScanToSQL(ClientContext &context, const LogicalGet &get);

TableFunction BindHiveScan(ClientContext &context, shared_ptr<HiveScanInfo> scan_info,
                           unique_ptr<FunctionData> &bind_data) {
	// the reader for the file format; the data columns (everything but the partition keys) are what the files hold
	child_list_t<Value> data_columns;
	for (idx_t i = 0; i < scan_info->names.size(); i++) {
		if (scan_info->GetPartitionKeyIndex(scan_info->names[i].GetIdentifierName()) == DConstants::INVALID_INDEX) {
			// with SerDe fields every column is read as text; InitializeReader converts it to the column's type
			auto type = scan_info->csv_options.serde_fields ? LogicalType::VARCHAR : scan_info->types[i];
			data_columns.emplace_back(scan_info->names[i], Value(type.ToString()));
		}
	}
	named_argument_map_t param_map;
	auto function_name = ReaderFunctionName(scan_info->file_format);
	switch (scan_info->file_format) {
	case HiveFileFormat::PARQUET:
		// the Glue type decides how a BYTE_ARRAY column without a STRING annotation reads, not the session setting.
		// A table without a binary column has the reader read such a column as text (it verifies the UTF-8); with a
		// binary column the reader keeps the bytes, and InitializeReader decodes the ones Glue declares string
		param_map["binary_as_string"] = Value::BOOLEAN(!HasBinaryColumn(*scan_info));
		// writers leave NaN out of a float column's min/max, so without a nan_count the bounds do not cover NaN
		param_map["can_have_nan"] = Value::BOOLEAN(true);
		break;
	case HiveFileFormat::CSV:
		// Hive CSV files carry no schema: the columns are given, by position, and the dialect is the one the table
		// describes - there is nothing left for the sniffer to find, so every file is opened without sniffing
		param_map["columns"] = Value::STRUCT(data_columns);
		param_map["auto_detect"] = Value::BOOLEAN(false);
		param_map["header"] = Value::BOOLEAN(false);
		param_map["skip"] = Value::BIGINT(NumericCast<int64_t>(scan_info->csv_options.skip_lines));
		param_map["delim"] = Value(scan_info->csv_options.delimiter);
		param_map["quote"] = Value(scan_info->csv_options.quote);
		param_map["escape"] = Value(scan_info->csv_options.escape);
		param_map["nullstr"] = Value(scan_info->csv_options.null_string);
		// a quoted empty field is an empty string, not NULL (Hive reads it that way, and DuckDB writes it for one)
		param_map["allow_quoted_nulls"] = Value::BOOLEAN(false);
		if (scan_info->csv_options.serde_fields) {
			// as the SerDes do: missing trailing fields are NULL and the fields beyond the last column are ignored
			param_map["null_padding"] = Value::BOOLEAN(true);
			param_map["strict_mode"] = Value::BOOLEAN(false);
			if (!scan_info->csv_options.quote.empty()) {
				// the parallel scanner refuses null_padding together with a quoted line break (hive_csv_parallel)
				Value parallel;
				if (context.TryGetCurrentSetting("hive_csv_parallel", parallel) && !parallel.IsNull() &&
				    !BooleanValue::Get(parallel)) {
					param_map["parallel"] = Value::BOOLEAN(false);
				}
			}
		}
		break;
	case HiveFileFormat::JSON:
		// one JSON object per line, keys matched to the columns by name
		ExtensionHelper::AutoLoadExtension(context, "json");
		param_map["columns"] = Value::STRUCT(data_columns);
		param_map["format"] = Value("newline_delimited");
		if (scan_info->json_options.ignore_errors) {
			param_map["ignore_errors"] = Value::BOOLEAN(true);
		}
		break;
	case HiveFileFormat::AVRO:
		// the files carry their schema, columns are matched by name like parquet
		ExtensionHelper::AutoLoadExtension(context, "avro");
		break;
	}
	auto scan_function = GetListReadFunction(context, function_name, *scan_info);
	if (scan_info->csv_options.serde_fields) {
		// read_csv would convert the text itself, strictly: have the column mapping cast it, so InitializeReader can
		// make the casts TRY_CASTs
		auto &info = scan_function.function_info->Cast<TableFunctionMultiFileInfo>();
		auto settings = info.settings;
		settings.supports_cast_map = false;
		scan_function.function_info = make_shared_ptr<TableFunctionMultiFileInfo>(info.function, std::move(settings));
	}
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
	scan_function.get_table_entry = HiveScanGetTableEntry;
	// EXPLAIN (SQL) shows the scan as the standard reader calls that read its files
	scan_function.to_sql = HiveScanToSQL;
	scan_info->format_virtual_columns = scan_function.get_virtual_columns;
	scan_function.get_virtual_columns = HiveScanGetVirtualColumns;
	// the files are mapped onto the table's types (Bind), which a cast's type pushed into the reader would not change
	scan_function.projection_expression_pushdown = nullptr;
	if (IsTextFileFormat(scan_info->file_format)) {
		// join filters prune partitions; every other filter stays in the plan (HiveTextComplexFilterPushdown), and
		// the text readers never evaluate table filters (see InitializeReader)
		scan_info->format_pushdown_complex_filter = scan_function.pushdown_complex_filter;
		scan_function.pushdown_complex_filter = HiveTextComplexFilterPushdown;
		scan_function.filter_pushdown = true;
		scan_function.filter_prune = false;
	}

	vector<LogicalType> return_types;
	vector<Identifier> names;
	// the path argument is not used: CreateFileList builds the file list from the partitions
	TableFunctionRef empty_ref;
	vector<Value> inputs = {Value::LIST(LogicalType::VARCHAR, {Value(scan_info->root_location)})};
	scan_info->reader_parameters = param_map;
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
constexpr column_t HiveMultiFileReader::COLUMN_IDENTIFIER_PATH;
constexpr column_t HiveMultiFileReader::COLUMN_IDENTIFIER_FILE_SIZE;
constexpr column_t HiveMultiFileReader::COLUMN_IDENTIFIER_FILE_MODIFIED_TIME;

void HiveMultiFileReader::AddMetadataColumns(virtual_column_map_t &result) {
	result.emplace(COLUMN_IDENTIFIER_PATH, TableColumn("$path", LogicalType::VARCHAR));
	result.emplace(COLUMN_IDENTIFIER_FILE_SIZE, TableColumn("$file_size", LogicalType::BIGINT));
	result.emplace(COLUMN_IDENTIFIER_FILE_MODIFIED_TIME, TableColumn("$file_modified_time", LogicalType::TIMESTAMP_TZ));
}

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

//! Keep a filter the pruning took out of the plan for a SQL export of the scan, over the scan's columns
//! (ExtraOperatorInfo::FILE_FILTER_TABLE_INDEX), as DuckDB's own hive partitioning does
static void RetainFileFilter(MultiFilePushdownInfo &pushdown_info, unique_ptr<Expression> filter) {
	auto &retained = pushdown_info.extra_info.file_filter_expressions;
	if (!retained) {
		return;
	}
	bool representable = true;
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    filter, [&](BoundColumnRefExpression &column, unique_ptr<Expression> &) {
		    auto binding = column.Binding();
		    if (column.Depth() != 0 || binding.table_index != pushdown_info.table_index ||
		        binding.column_index.GetIndex() >= pushdown_info.column_indexes.size()) {
			    representable = false;
			    return;
		    }
		    auto &index = pushdown_info.column_indexes[binding.column_index.GetIndex()];
		    if (index.IsVirtualColumn() || index.IsPushdownExtract() || index.HasType()) {
			    representable = false;
			    return;
		    }
		    column.BindingMutable() = ColumnBinding(TableIndex(ExtraOperatorInfo::FILE_FILTER_TABLE_INDEX),
		                                            ProjectionIndex(index.GetPrimaryIndex()));
	    });
	if (!representable) {
		retained.reset();
		return;
	}
	retained->push_back(std::move(filter));
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
	if (!pushdown_info.extra_info.total_files.IsValid()) {
		pushdown_info.extra_info.file_filter_expressions = vector<unique_ptr<Expression>>();
	}
	vector<unique_ptr<Expression>> remaining_filters;
	for (idx_t filter_index = 0; filter_index < filters.size(); filter_index++) {
		if (decided_filters.find(filter_index) == decided_filters.end()) {
			remaining_filters.push_back(std::move(filters[filter_index]));
		} else {
			RetainFileFilter(pushdown_info, std::move(filters[filter_index]));
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
//! The size and modification time the listing reported for the file, asked of the file system when it did not
static FileMetadata DataFileMetadata(ClientContext &context, const OpenFileInfo &file) {
	FileMetadata result;
	if (file.extended_info) {
		auto &options = file.extended_info->options;
		auto size = options.find("file_size");
		if (size != options.end() && !size->second.IsNull()) {
			result.file_size = size->second.GetValue<int64_t>();
		}
		auto last_modified = options.find("last_modified");
		if (last_modified != options.end() && !last_modified->second.IsNull()) {
			result.last_modification_time = last_modified->second.GetValue<timestamp_t>();
		}
	}
	if (result.file_size < 0 || !result.last_modification_time.IsFinite()) {
		auto &fs = FileSystem::GetFileSystem(context);
		auto handle = fs.OpenFile(file, FileFlags::FILE_FLAGS_READ);
		auto stats = handle->Stats();
		if (result.file_size < 0) {
			result.file_size = stats.file_size;
		}
		if (!result.last_modification_time.IsFinite()) {
			result.last_modification_time = stats.last_modification_time;
		}
	}
	return result;
}

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
	auto &reader = *reader_data.reader;
	bool file_metadata_read = false;
	FileMetadata file_metadata;
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto &column_id = global_column_ids[i];
		if (column_id.IsVirtualColumn()) {
			auto virtual_id = column_id.GetPrimaryIndex();
			if (virtual_id == COLUMN_IDENTIFIER_PATH) {
				reader_data.constant_map.Add(MultiFileGlobalIndex(i), Value(reader.GetFileName()));
			} else if (virtual_id == COLUMN_IDENTIFIER_FILE_SIZE ||
			           virtual_id == COLUMN_IDENTIFIER_FILE_MODIFIED_TIME) {
				if (!file_metadata_read) {
					file_metadata = DataFileMetadata(context, reader.file);
					file_metadata_read = true;
				}
				Value value;
				if (virtual_id == COLUMN_IDENTIFIER_FILE_SIZE) {
					value = file_metadata.file_size < 0 ? Value(LogicalType::BIGINT)
					                                    : Value::BIGINT(file_metadata.file_size);
				} else {
					auto &last_modified = file_metadata.last_modification_time;
					value = last_modified.IsFinite() ? Value::TIMESTAMPTZ(timestamp_tz_t(last_modified))
					                                 : Value(LogicalType::TIMESTAMP_TZ);
				}
				reader_data.constant_map.Add(MultiFileGlobalIndex(i), std::move(value));
			}
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

//! Make the cast of a field's text to its column's type a TRY_CAST
static void MakeTryCast(ClientContext &context, unique_ptr<Expression> &expr) {
	if (!expr || !BoundCastExpression::IsCast(*expr)) {
		return;
	}
	auto &cast = expr->Cast<BoundFunctionExpression>();
	if (BoundCastExpression::IsTryCast(cast) ||
	    BoundCastExpression::Child(cast).GetReturnType().id() != LogicalTypeId::VARCHAR) {
		return;
	}
	auto target_type = cast.GetReturnType();
	auto child = std::move(BoundCastExpression::ChildMutable(cast));
	expr = BoundCastExpression::AddCastToType(context, std::move(child), target_type, true);
}

//! A parquet BYTE_ARRAY column without a STRING annotation (Impala, older Hive and parquet-mr writers) reads as a
//! BLOB when the table has a binary column (binary_as_string = false, BindHiveScan), while Glue declares this column
//! 'string'. The reader's cast of that BLOB to VARCHAR would escape every non-ASCII byte (\xC3\xBC for ü); the bytes
//! are the text, so decode() them as UTF-8 instead, as Hive and Trino do, which fails when they are not UTF-8. Only a
//! column's own cast is replaced: the same bytes inside a LIST, STRUCT or MAP are converted by the reader's nested
//! cast and keep the escaped form.
static void DecodeBlobAsText(ClientContext &context, unique_ptr<Expression> &expr) {
	if (!expr || !BoundCastExpression::IsCast(*expr)) {
		return;
	}
	// the whole type, not the id: a cast to an alias of VARCHAR such as JSON must keep its own conversion
	auto &cast = expr->Cast<BoundFunctionExpression>();
	if (cast.GetReturnType() != LogicalType::VARCHAR ||
	    BoundCastExpression::Child(cast).GetReturnType() != LogicalType::BLOB) {
		return;
	}
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(BoundCastExpression::ChildMutable(cast)));
	FunctionBinder function_binder(context);
	expr = function_binder.BindScalarFunction(DEFAULT_SCHEMA, "decode", std::move(children));
}

//! Make the cast of a field's text to BOOLEAN lower(text) = 'true', as OpenCSVSerde reads a boolean
static void MakeFalseUnlessTrue(ClientContext &context, unique_ptr<Expression> &expr) {
	if (!expr || !BoundCastExpression::IsCast(*expr) || expr->GetReturnType().id() != LogicalTypeId::BOOLEAN) {
		return;
	}
	auto &cast = expr->Cast<BoundFunctionExpression>();
	if (BoundCastExpression::Child(cast).GetReturnType().id() != LogicalTypeId::VARCHAR) {
		return;
	}
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(BoundCastExpression::ChildMutable(cast)));
	FunctionBinder function_binder(context);
	ErrorData error;
	auto lower = function_binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier("lower"),
	                                                std::move(children), error);
	if (!lower) {
		error.Throw();
	}
	expr = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, std::move(lower),
	                                         make_uniq<BoundConstantExpression>(Value("true")));
}

ReaderInitializeType HiveMultiFileReader::InitializeReader(MultiFileReaderData &reader_data,
                                                           const MultiFileBindData &bind_data,
                                                           const vector<MultiFileColumnDefinition> &global_columns,
                                                           const vector<ColumnIndex> &global_column_ids,
                                                           optional_ptr<TableFilterSet> table_filters,
                                                           ClientContext &context, MultiFileGlobalState &gstate) {
	if (IsTextFileFormat(ScanInfo().file_format)) {
		// partition-column filters were applied by pruning the file list; nothing reaches read_csv/read_json
		table_filters = nullptr;
	}
	auto result = MultiFileReader::InitializeReader(reader_data, bind_data, global_columns, global_column_ids,
	                                                table_filters, context, gstate);
	if (ScanInfo().file_format == HiveFileFormat::PARQUET) {
		// without a binary column the reader reads unannotated BYTE_ARRAY columns as text itself (binary_as_string)
		if (HasBinaryColumn(ScanInfo())) {
			for (auto &expr : reader_data.expressions) {
				DecodeBlobAsText(context, expr);
			}
			// a filter pushed into the reader evaluates the column's conversion there, on a copy of the expression
			for (auto &entry : reader_data.reader->expression_map) {
				DecodeBlobAsText(context, entry.second.expression);
			}
		}
		return result;
	}
	if (!ScanInfo().csv_options.serde_fields) {
		return result;
	}
	if (!reader_data.reader->expression_map.empty()) {
		throw InternalException("Hive text table '%s': a filter was pushed into read_csv", ScanInfo().Describe());
	}
	auto &csv_options = ScanInfo().csv_options;
	for (auto &expr : reader_data.expressions) {
		if (!expr) {
			continue;
		}
		switch (csv_options.FieldConversion(expr->GetReturnType())) {
		case HiveFieldConversion::NONE:
		case HiveFieldConversion::CAST:
			break;
		case HiveFieldConversion::TRY_CAST:
			MakeTryCast(context, expr);
			break;
		case HiveFieldConversion::FALSE_UNLESS_TRUE:
			MakeFalseUnlessTrue(context, expr);
			break;
		}
	}
	return result;
}

//===--------------------------------------------------------------------===//
// The scan as a standard reader call (functions/hive_reader_call.hpp)
//===--------------------------------------------------------------------===//
//! The scan info of a Hive scan this build of the extension bound: its multi-file reader is this build's
static optional_ptr<const HiveScanInfo> BoundHiveScanInfo(const LogicalGet &get) {
	if (get.function.GetName() != "hive_scan" || !get.bind_data) {
		return nullptr;
	}
	auto bind_data = dynamic_cast<const MultiFileBindData *>(get.bind_data.get());
	if (!bind_data || !bind_data->multi_file_reader) {
		return nullptr;
	}
	auto reader = dynamic_cast<const HiveMultiFileReader *>(bind_data->multi_file_reader.get());
	if (!reader) {
		return nullptr;
	}
	return &reader->ScanInfo();
}

//! The `schema` option over the data columns, keyed by name: every file is mapped onto them by name, a column a file
//! lacks reads as NULL and one of another type is cast, as HiveMultiFileReader maps a file onto the table's columns
static Value DataColumnsSchema(const HiveScanInfo &info, const MultiFileBindData &bind_data) {
	child_list_t<LogicalType> entry_fields;
	entry_fields.emplace_back("name", LogicalType::VARCHAR);
	entry_fields.emplace_back("type", LogicalType::VARCHAR);
	entry_fields.emplace_back("default_value", LogicalType::VARCHAR);
	vector<Value> keys;
	vector<Value> entries;
	for (idx_t i = 0; i < bind_data.names.size() && i < bind_data.types.size(); i++) {
		auto &name = bind_data.names[i].GetIdentifierName();
		if (info.GetPartitionKeyIndex(name) != DConstants::INVALID_INDEX) {
			continue;
		}
		child_list_t<Value> entry;
		entry.emplace_back("name", Value(name));
		entry.emplace_back("type", Value(bind_data.types[i].ToString()));
		entry.emplace_back("default_value", Value(LogicalType::VARCHAR));
		keys.emplace_back(name);
		entries.push_back(Value::STRUCT(std::move(entry)));
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::STRUCT(std::move(entry_fields)), std::move(keys),
	                  std::move(entries));
}

//! Whether the scan reads its column 'index' as the table declares it. The optimizer can push the type of a cast over a
//! column into a csv scan (type_pushdown) and take the cast out of the plan, which no reader call can say
static bool ScanTypeIsTableType(const HiveScanInfo &info, const MultiFileBindData &bind_data, idx_t index) {
	for (idx_t i = 0; i < info.names.size() && i < info.types.size(); i++) {
		if (StringUtil::CIEquals(info.names[i].GetIdentifierName(), bind_data.names[index].GetIdentifierName())) {
			return info.types[i] == bind_data.types[index];
		}
	}
	return false;
}

//! A csv field read as text, barred (__internal_barrier) so that the caller's optimizer does not push its conversion
//! into read_csv (type_pushdown), which would convert the text itself, and strictly
static unique_ptr<ParsedExpression> BarredField(const Identifier &name) {
	vector<unique_ptr<ParsedExpression>> field;
	field.push_back(make_uniq<ColumnRefExpression>(name));
	return make_uniq<FunctionExpression>(Identifier("__internal_barrier"), std::move(field));
}

//! A data column of the scan, of the table's type, as an expression over the reader's column: the conversion of a field
//! read as text that InitializeReader gives the scan (HiveCSVOptions::FieldConversion)
static unique_ptr<ParsedExpression> ReaderColumn(const HiveScanInfo &info, const Identifier &name,
                                                 const LogicalType &type) {
	if (info.file_format != HiveFileFormat::CSV || !info.csv_options.serde_fields) {
		return make_uniq<ColumnRefExpression>(name);
	}
	unique_ptr<ParsedExpression> column;
	switch (info.csv_options.FieldConversion(type)) {
	case HiveFieldConversion::NONE:
		return make_uniq<ColumnRefExpression>(name);
	case HiveFieldConversion::CAST:
		column = make_uniq<CastExpression>(type, BarredField(name), false);
		break;
	case HiveFieldConversion::TRY_CAST:
		column = make_uniq<CastExpression>(type, BarredField(name), true);
		break;
	case HiveFieldConversion::FALSE_UNLESS_TRUE: {
		vector<unique_ptr<ParsedExpression>> lower;
		lower.push_back(BarredField(name));
		column = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
		                                         make_uniq<FunctionExpression>(Identifier("lower"), std::move(lower)),
		                                         ConstantExpression::FromValue(Value("true")));
		break;
	}
	}
	column->SetAlias(name);
	return column;
}

optional<HiveReaderCall> HiveScanGetReaderCall(const LogicalGet &get) {
	auto info = BoundHiveScanInfo(get);
	if (!info || (info->file_format == HiveFileFormat::PARQUET && HasBinaryColumn(*info))) {
		return nullopt;
	}
	auto &bind_data = get.bind_data->Cast<MultiFileBindData>();
	HiveReaderCall call;
	call.function = ReaderFunctionName(info->file_format);
	for (auto &parameter : info->reader_parameters) {
		call.parameters[parameter.first] = parameter.second;
	}
	if (info->file_format == HiveFileFormat::PARQUET || info->file_format == HiveFileFormat::AVRO) {
		call.parameters["schema"] = DataColumnsSchema(*info, bind_data);
	}
	call.parameters["hive_partitioning"] = Value::BOOLEAN(false);
	for (idx_t i = 0; i < bind_data.names.size() && i < bind_data.types.size(); i++) {
		if (!ScanTypeIsTableType(*info, bind_data, i)) {
			return nullopt;
		}
		if (info->GetPartitionKeyIndex(bind_data.names[i].GetIdentifierName()) != DConstants::INVALID_INDEX) {
			continue;
		}
		call.columns.push_back(ReaderColumn(*info, bind_data.names[i], bind_data.types[i]));
	}
	return std::move(call);
}

optional<vector<unique_ptr<ParsedExpression>>> HiveScanGetFilePartitionValues(const LogicalGet &get,
                                                                              const string &path) {
	auto info = BoundHiveScanInfo(get);
	if (!info || info->partition_keys.empty()) {
		return nullopt;
	}
	// the listing spells a path as the file system does; the caller may spell it another way (s3://b//x for s3://b/x)
	auto listed_path = path;
	if (!info->FindPartitionIndexOfFile(listed_path).IsValid()) {
		listed_path = Path::Normalize(path);
		if (!info->FindPartitionIndexOfFile(listed_path).IsValid()) {
			return nullopt;
		}
	}
	auto &partition = info->GetPartitionOfFile(listed_path);
	auto &bind_data = get.bind_data->Cast<MultiFileBindData>();
	vector<unique_ptr<ParsedExpression>> values;
	for (idx_t key = 0; key < info->partition_keys.size() && key < partition.values.size(); key++) {
		for (idx_t i = 0; i < bind_data.names.size() && i < bind_data.types.size(); i++) {
			if (!StringUtil::CIEquals(bind_data.names[i].GetIdentifierName(), info->partition_keys[key])) {
				continue;
			}
			if (!ScanTypeIsTableType(*info, bind_data, i)) {
				return nullopt;
			}
			auto &text = partition.values[key];
			auto &type = bind_data.types[i];
			// GlueTypes::PartitionValue, the cast done by the caller
			unique_ptr<ParsedExpression> value;
			if (GlueTypes::PartitionValueIsNull(text, type)) {
				value = make_uniq<CastExpression>(type, ConstantExpression::FromValue(Value()));
			} else if (type.id() == LogicalTypeId::VARCHAR) {
				value = ConstantExpression::FromValue(Value(text));
			} else {
				value = make_uniq<CastExpression>(type, ConstantExpression::FromValue(Value(text)));
			}
			value->SetAlias(bind_data.names[i]);
			values.push_back(std::move(value));
			break;
		}
	}
	if (values.size() != info->partition_keys.size()) {
		return nullopt;
	}
	return std::move(values);
}

//! A constant spelled as the SQL export spells one, with its type
static unique_ptr<ParsedExpression> ExportedConstant(const Value &value) {
	auto exported = BoundExpressionSQLExporter::Export(BoundConstantExpression(value), {});
	if (exported.HasError()) {
		return nullptr;
	}
	return std::move(exported.GetValue());
}

//! The reader call over 'files', its named parameters in name order
static unique_ptr<TableRef> ReaderCallRef(const HiveReaderCall &call, const vector<string> &files) {
	vector<Value> file_values;
	for (auto &file : files) {
		file_values.emplace_back(file);
	}
	vector<unique_ptr<ParsedExpression>> arguments;
	auto file_list = ExportedConstant(Value::LIST(LogicalType::VARCHAR, std::move(file_values)));
	if (!file_list) {
		return nullptr;
	}
	arguments.push_back(std::move(file_list));
	vector<Identifier> names;
	for (auto &parameter : call.parameters) {
		names.push_back(parameter.first);
	}
	std::sort(names.begin(), names.end(), [](const Identifier &left, const Identifier &right) {
		return left.GetIdentifierName() < right.GetIdentifierName();
	});
	for (auto &name : names) {
		auto value = ExportedConstant(call.parameters.find(name)->second);
		if (!value) {
			return nullptr;
		}
		arguments.push_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
		                                                    make_uniq<ColumnRefExpression>(name), std::move(value)));
	}
	auto function = make_uniq<TableFunctionRef>();
	function->function = SQLExportHelpers::SystemFunction(Identifier(call.function), std::move(arguments));
	return std::move(function);
}

//! A scan without reader calls: a Glue table is read by name, as an export without to_sql reads it
static TableFunctionToSQLResult ReadTableByName(const LogicalGet &get, string reason) {
	auto table = get.GetTable();
	if (!table) {
		return {nullptr, std::move(reason)};
	}
	auto reference = make_uniq<BaseTableRef>();
	reference->SetQualifiedName(table->schema.GetQualifiedName(table->name));
	return {std::move(reference), {}};
}

//! The scan as the reader calls that read its files, one per partition, UNION ALLed, each selecting every column of the
//! scan in order (a partition key as the partition's value). Without files, a relation of the columns with no rows.
static TableFunctionToSQLResult HiveScanToSQL(ClientContext &, const LogicalGet &get) {
	auto info = BoundHiveScanInfo(get);
	if (!info) {
		return {nullptr, "hive_scan"};
	}
	for (auto &index : get.GetColumnIds()) {
		if (index.IsVirtualColumn() && !index.IsEmptyColumn()) {
			return ReadTableByName(get, "virtual_column");
		}
	}
	auto call = HiveScanGetReaderCall(get);
	if (!call) {
		return ReadTableByName(get, "reader_call");
	}
	auto &bind_data = get.bind_data->Cast<MultiFileBindData>();
	// the files of each partition, the partitions in the order the scan lists them
	vector<vector<string>> partition_files;
	unordered_map<idx_t, idx_t> read_of_partition;
	for (auto &file : bind_data.file_list->GetAllFiles()) {
		idx_t read = 0;
		if (!info->partition_keys.empty()) {
			auto partition_index = info->FindPartitionIndexOfFile(file.path);
			if (!partition_index.IsValid()) {
				return ReadTableByName(get, "partition_values");
			}
			read = read_of_partition.emplace(partition_index.GetIndex(), partition_files.size()).first->second;
		}
		if (read == partition_files.size()) {
			partition_files.emplace_back();
		}
		partition_files[read].push_back(file.path);
	}
	vector<unique_ptr<QueryNode>> reads;
	for (auto &files : partition_files) {
		optional<vector<unique_ptr<ParsedExpression>>> partition_values;
		if (!info->partition_keys.empty()) {
			partition_values = HiveScanGetFilePartitionValues(get, files[0]);
			if (!partition_values) {
				return ReadTableByName(get, "partition_values");
			}
		}
		auto read = make_uniq<SelectNode>();
		idx_t data_column = 0;
		for (auto &name : bind_data.names) {
			auto key_index = info->GetPartitionKeyIndex(name.GetIdentifierName());
			if (key_index == DConstants::INVALID_INDEX) {
				D_ASSERT(data_column < call->columns.size());
				read->select_list.push_back(call->columns[data_column++]->Copy());
			} else {
				read->select_list.push_back(std::move((*partition_values)[key_index]));
			}
		}
		read->from_table = ReaderCallRef(*call, files);
		if (!read->from_table) {
			return ReadTableByName(get, "reader_parameter");
		}
		reads.push_back(std::move(read));
	}
	if (reads.empty()) {
		auto empty = make_uniq<SelectNode>();
		for (idx_t i = 0; i < bind_data.names.size() && i < bind_data.types.size(); i++) {
			auto column = ExportedConstant(Value(bind_data.types[i]));
			if (!column) {
				return ReadTableByName(get, "column_type");
			}
			column->SetAlias(bind_data.names[i]);
			empty->select_list.push_back(std::move(column));
		}
		empty->from_table = make_uniq<EmptyTableRef>();
		empty->where_clause = ConstantExpression::FromValue(Value::BOOLEAN(false));
		reads.push_back(std::move(empty));
	}
	auto statement = make_uniq<SelectStatement>();
	if (reads.size() == 1) {
		statement->node = std::move(reads[0]);
	} else {
		auto union_all = make_uniq<SetOperationNode>();
		union_all->setop_type = SetOperationType::UNION;
		union_all->setop_all = true;
		union_all->children = std::move(reads);
		statement->node = std::move(union_all);
	}
	return {make_uniq<SubqueryRef>(std::move(statement)), {}};
}

} // namespace duckdb
