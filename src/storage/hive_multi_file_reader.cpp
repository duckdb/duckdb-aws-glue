#include "storage/hive_multi_file_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "glue_types.hpp"

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

const GluePartitionInfo &HiveScanInfo::GetPartitionOfFile(const string &path) const {
	lock_guard<mutex> guard(file_partitions_lock);
	auto entry = file_partitions.find(path);
	if (entry == file_partitions.end()) {
		throw InternalException("Hive scan of '%s.%s': data file '%s' does not belong to any Glue partition",
		                        database_name, table_name, path);
	}
	return partitions[entry->second];
}

string HiveScanInfo::Describe() const {
	if (database_name.empty()) {
		return table_name;
	}
	return database_name + "." + table_name;
}

//! Whether a path component below the table root is data (Hive skips _* and .* files and directories)
static bool IsHiddenComponent(const string &name) {
	return name.empty() || name[0] == '_' || name[0] == '.';
}

//! The data files below 'location', at any depth.
//!
//! A location is a prefix, not a single directory level: DuckDB's own read_parquet descends into
//! subdirectories when it is given a directory, and Glue stores nothing but the prefix, so a file in a
//! nested directory under a partition belongs to that partition. Listing only one level made the answer
//! depend on hive_partition_listing_threshold, because ListRoot below is recursive.
//!
//! Recursing means subdirectories a single level listing never saw are now in scope, so the hidden test
//! applies to every component below 'location' rather than only the file name: a file in _temporary/ or
//! .hive-staging/ is no more data than a _SUCCESS file is.
//!
//! This serves both the per-partition listing and the whole table location of an UNPARTITIONED table,
//! so unpartitioned tables read nested files as well. That is deliberate: it is what read_parquet does
//! with a directory, and it keeps all three listing paths (unpartitioned root, partitioned root,
//! per-partition) on one rule.
//!
//! Not unified with ListRoot: that one maps each file to the DEEPEST registered partition location it
//! lies under, while this attributes everything below 'location' to the one partition being listed. The
//! two agree unless one partition's location is nested inside another's, in which case a file in the
//! inner location is labelled with the outer partition's values here and the inner partition's values
//! there.
static void ListDataFiles(FileSystem &fs, const string &location, vector<OpenFileInfo> &files) {
	auto directory = location;
	StringUtil::RTrim(directory, "/");
	if (directory.empty()) {
		return;
	}
	auto prefix = directory + "/";
	for (auto &file : fs.GlobFiles(directory + "/**", FileGlobOptions::ALLOW_EMPTY)) {
		if (!StringUtil::StartsWith(file.path, prefix)) {
			continue;
		}
		bool hidden = false;
		for (auto &component : StringUtil::Split(file.path.substr(prefix.size()), '/')) {
			if (IsHiddenComponent(component)) {
				hidden = true;
				break;
			}
		}
		if (hidden) {
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
      partition_indexes(std::move(partition_indexes_p)) {
}

void HiveMultiFileList::PlanListings() const {
	if (planned) {
		return;
	}
	planned = true;
	if (scan_info->partition_keys.empty()) {
		jobs.push_back(ListingJob {true, {}});
		return;
	}
	// the partitions below the table root can be listed together
	auto root = scan_info->root_location;
	StringUtil::RTrim(root, "/");
	vector<idx_t> below_root;
	vector<idx_t> elsewhere;
	for (auto partition_index : partition_indexes) {
		auto location = scan_info->partitions[partition_index].location;
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
		jobs.push_back(ListingJob {true, std::move(below_root)});
	} else {
		for (auto partition_index : below_root) {
			jobs.push_back(ListingJob {false, {partition_index}});
		}
	}
	for (auto partition_index : elsewhere) {
		jobs.push_back(ListingJob {false, {partition_index}});
	}
}

void HiveMultiFileList::BuildPartitionLocations() const {
	if (partition_locations_built) {
		return;
	}
	partition_locations_built = true;
	// Every registered partition, not only the ones this query reads. A file under a partition location belongs to
	// that partition whether or not the partition survived pruning: leaving a pruned partition out would hand its
	// files to the enclosing partition, so the values a row carries would depend on the query's filters.
	for (idx_t i = 0; i < scan_info->partitions.size(); i++) {
		auto location = scan_info->partitions[i].location;
		StringUtil::RTrim(location, "/");
		if (location.empty()) {
			continue;
		}
		// Two partitions on the same location: the first wins, as in AddFile
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

void HiveMultiFileList::ListPartition(FileSystem &fs, idx_t partition_index) const {
	auto &partition = scan_info->partitions[partition_index];
	if (partition.values.size() != scan_info->partition_keys.size()) {
		throw InvalidInputException("Partition [%s] of Hive table '%s' has %d values but the table has %d partition "
		                            "keys",
		                            StringUtil::Join(partition.values, ", "), scan_info->Describe(),
		                            partition.values.size(), scan_info->partition_keys.size());
	}
	auto location = partition.location;
	StringUtil::RTrim(location, "/");
	vector<OpenFileInfo> partition_files;
	ListDataFiles(fs, partition.location, partition_files);
	BuildPartitionLocations();
	lock_guard<mutex> guard(scan_info->file_partitions_lock);
	for (auto &file : partition_files) {
		// Locations may overlap: Glue lets a partition sit inside another partition's prefix, and the recursive
		// listing here then also finds the inner partition's files. They are not this partition's, so attribute by
		// deepest registered location, exactly as ListRoot does, and keep only what belongs here.
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

void HiveMultiFileList::ListRoot(FileSystem &fs, const vector<idx_t> &partitions) const {
	auto root = scan_info->root_location;
	StringUtil::RTrim(root, "/");
	if (root.empty()) {
		throw InvalidInputException("Hive table '%s' has no location", scan_info->Describe());
	}
	// one recursive listing of the root: on S3 a flat ListObjectsV2 over the prefix, 1000 keys per request
	auto files = fs.GlobFiles(root + "/**", FileGlobOptions::ALLOW_EMPTY);
	for (auto partition_index : partitions) {
		auto &partition = scan_info->partitions[partition_index];
		if (partition.values.size() != scan_info->partition_keys.size()) {
			throw InvalidInputException("Partition [%s] of Hive table '%s' has %d values but the table has %d "
			                            "partition keys",
			                            StringUtil::Join(partition.values, ", "), scan_info->Describe(),
			                            partition.values.size(), scan_info->partition_keys.size());
		}
	}
	// Attribution is over EVERY registered location, while only the partitions of this job are read. Matching
	// against the read set alone would give a pruned partition's files to the partition enclosing it, so a row's
	// partition values would change with the query's filters.
	BuildPartitionLocations();
	unordered_set<idx_t> reading;
	for (auto partition_index : partitions) {
		reading.insert(partition_index);
	}
	auto root_prefix = root + "/";
	lock_guard<mutex> guard(scan_info->file_partitions_lock);
	for (auto &file : files) {
		if (!StringUtil::StartsWith(file.path, root_prefix)) {
			continue;
		}
		// hidden files and directories (_SUCCESS, .hive-staging, ...) are not data
		auto relative = file.path.substr(root_prefix.size());
		bool hidden = false;
		for (auto &component : StringUtil::Split(relative, '/')) {
			if (IsHiddenComponent(component)) {
				hidden = true;
				break;
			}
		}
		if (hidden) {
			continue;
		}
		// the deepest registered partition location the file lies in
		auto partition_index = OwningPartition(file.path, root.size() + 1);
		if (!partition_index.IsValid() || !reading.count(partition_index.GetIndex())) {
			// under no registered location, or under one this job is not reading (pruned, or listed on its own
			// because it lies outside the root)
			continue;
		}
		AddFile(std::move(file), partition_index.GetIndex());
	}
}

bool HiveMultiFileList::ExpandNextPath() const {
	// called with the list's lock held; runs one listing per call
	PlanListings();
	if (next_job >= jobs.size()) {
		return false;
	}
	auto &job = jobs[next_job++];
	auto &fs = FileSystem::GetFileSystem(client_context);
	if (scan_info->partition_keys.empty()) {
		if (scan_info->root_location.empty()) {
			throw InvalidInputException("Hive table '%s' has no location", scan_info->Describe());
		}
		ListDataFiles(fs, scan_info->root_location, expanded_files);
		return true;
	}
	if (job.root) {
		ListRoot(fs, job.partitions);
	} else {
		ListPartition(fs, job.partitions[0]);
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
	if (all_files_expanded) {
		return MultiFileCount(expanded_files.size(), FileExpansionType::ALL_FILES_EXPANDED);
	}
	PlanListings();
	idx_t remaining = 0;
	for (idx_t i = next_job; i < jobs.size(); i++) {
		remaining += jobs[i].root ? MaxValue<idx_t>(jobs[i].partitions.size(), 1) : 1;
	}
	return MultiFileCount(expanded_files.size() + remaining, FileExpansionType::NOT_ALL_FILES_KNOWN);
}

optional_idx HiveMultiFileList::EstimateTotalFileCount() const {
	lock_guard<mutex> lck(lock);
	if (all_files_expanded) {
		return expanded_files.size();
	}
	PlanListings();
	// how many partitions the jobs already run covered, and how many are still to come. A root job covers every
	// partition below the table root, a partition job covers one.
	idx_t covered = 0;
	idx_t remaining = 0;
	for (idx_t i = 0; i < jobs.size(); i++) {
		auto partitions = jobs[i].root ? MaxValue<idx_t>(jobs[i].partitions.size(), 1) : 1;
		if (i < next_job) {
			covered += partitions;
		} else {
			remaining += partitions;
		}
	}
	if (covered == 0) {
		// nothing listed yet: there is no observed rate to extrapolate
		return optional_idx();
	}
	// the partitions already listed set the rate for the ones that have not been
	auto files_per_partition = static_cast<double>(expanded_files.size()) / static_cast<double>(covered);
	auto estimate = static_cast<double>(expanded_files.size()) + files_per_partition * static_cast<double>(remaining);
	return MaxValue<idx_t>(static_cast<idx_t>(estimate), 1);
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
	// not listed yet (e.g. EXPLAIN): show the partition directories instead of listing them
	vector<OpenFileInfo> result;
	if (scan_info->partition_keys.empty()) {
		result.emplace_back(scan_info->root_location);
		return result;
	}
	for (auto partition_index : partition_indexes) {
		if (max_files.IsValid() && result.size() >= max_files.GetIndex()) {
			break;
		}
		result.emplace_back(scan_info->partitions[partition_index].location);
	}
	return result;
}

unique_ptr<MultiFileList> HiveMultiFileList::Copy() const {
	return make_uniq<HiveMultiFileList>(client_context, scan_info, partition_indexes);
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

//===--------------------------------------------------------------------===//
// Cardinality sample
//===--------------------------------------------------------------------===//
//! List ONE partition and open ONE of its files, so the scan's cost reflects its data. Glue carries no statistics of
//! any kind (no numRows, no totalSize, and Partition.Parameters is null), so without this the estimate is a constant:
//! a 40-row dimension table and a 200,000-row fact table cost the same, and csv/json/avro are estimated at one row.
//!
//! This is the trick read_parquet already plays -- it globs and binds on the first file, which is what fills in
//! ParquetReadBindData::initial_file_cardinality. We skip that path because Glue gives us the schema, so we take the
//! row count from a file WITHOUT letting the file define the schema: the columns, their order and their types stay
//! Glue's. The reader is asked for one file's worth of rows through the interface, which is the only thing that can
//! read a parquet footer.
//! Rows in one file of a line-oriented format, measured rather than assumed: read a bounded prefix, count its lines,
//! and scale the average line length over the file. csv and newline-delimited json are one row per line. There is no
//! footer to ask, so this is the only way to get a number that responds to the data at all -- and a measured average
//! beats a constant bytes-per-row, which cannot hold across two tables with different columns.
static optional_idx SampleLineOrientedRowsPerFile(ClientContext &context, const OpenFileInfo &file, bool header) {
	static constexpr idx_t SAMPLE_BYTES = 65536;
	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(file.path, FileFlags::FILE_FLAGS_READ);
	if (!handle) {
		return optional_idx();
	}
	auto file_size = static_cast<idx_t>(handle->GetFileSize());
	if (file_size == 0) {
		return optional_idx();
	}
	auto sample_size = MinValue<idx_t>(file_size, SAMPLE_BYTES);
	string buffer(sample_size, '\0');
	handle->Read(reinterpret_cast<void *>(&buffer[0]), sample_size, 0);
	idx_t lines = 0;
	for (idx_t i = 0; i < sample_size; i++) {
		if (buffer[i] == '\n') {
			lines++;
		}
	}
	if (lines == 0) {
		return optional_idx();
	}
	auto bytes_per_line = static_cast<double>(sample_size) / static_cast<double>(lines);
	auto rows = static_cast<idx_t>(static_cast<double>(file_size) / bytes_per_line);
	if (header && rows > 1) {
		rows--;
	}
	return MaxValue<idx_t>(rows, 1);
}

//! Rows in one data file of the scan, measured once and remembered. The file is taken from the scan's own file list,
//! which means the listing it costs is the listing the scan was going to do anyway -- only earlier.
static optional_idx SampleRowsPerFile(ClientContext &context, const MultiFileBindData &bind_data,
                                      const HiveMultiFileList &files, const HiveScanInfo &scan_info) {
	lock_guard<mutex> lck(scan_info.sample_lock);
	if (scan_info.rows_sample_attempted) {
		return scan_info.sampled_rows_per_file;
	}
	scan_info.rows_sample_attempted = true;
	// expands the first listing job only, and the files stay in the list for the scan to read
	auto file = files.GetFirstFile();
	if (file.path.empty()) {
		return optional_idx();
	}
	switch (scan_info.file_format) {
	case HiveFileFormat::PARQUET: {
		// the row count is in the footer, and the format's own reader is the only thing that can read it. Parquet is
		// bound with no named parameters, so a reader built from default options reads the file as the scan will
		auto options = bind_data.interface->InitializeOptions(context, nullptr);
		// a throwaway copy of the bind data: Initialize and FinalizeBindData write the opened file's numbers into it,
		// and the real bind data must not be rewritten behind the running scan
		auto probe = bind_data.Copy();
		auto &probe_data = probe->Cast<MultiFileBindData>();
		auto reader = bind_data.multi_file_reader->CreateReader(context, file, *options, bind_data.file_options,
		                                                        *probe_data.interface);
		if (!reader) {
			return optional_idx();
		}
		probe_data.Initialize(std::move(reader));
		probe_data.interface->FinalizeBindData(probe_data);
		// file_count 1 makes the format report that one file's rows rather than an extrapolation over the list
		auto sampled = probe_data.interface->GetCardinality(context, probe_data, 1);
		if (sampled && sampled->has_estimated_cardinality) {
			scan_info.sampled_rows_per_file = sampled->estimated_cardinality;
		}
		break;
	}
	case HiveFileFormat::CSV:
	case HiveFileFormat::JSON:
		// no reader is built for these: they are bound with a dialect and an explicit column list that fresh options
		// would not reproduce. Counting lines needs none of it
		scan_info.sampled_rows_per_file = SampleLineOrientedRowsPerFile(context, file, scan_info.header);
		break;
	case HiveFileFormat::AVRO:
		// binary, with its own block structure: no bounded way to count rows without an avro reader
		break;
	}
	return scan_info.sampled_rows_per_file;
}

//! Cardinality of a Hive scan, from one sampled file and the files the scan will read. Replaces a constant: without it
//! a 40-row dimension table and a 200,000-row fact table cost the same, and csv/json/avro are estimated at one row.
static unique_ptr<NodeStatistics> HiveScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
	auto hive_list = dynamic_cast<const HiveMultiFileList *>(bind_data.file_list.get());
	if (!hive_list) {
		return nullptr;
	}
	auto &scan_info = hive_list->ScanInfo();
	auto fallback = [&]() -> unique_ptr<NodeStatistics> {
		return scan_info.format_cardinality ? scan_info.format_cardinality(context, bind_data_p) : nullptr;
	};
	optional_idx rows_per_file;
	try {
		rows_per_file = SampleRowsPerFile(context, bind_data, *hive_list, scan_info);
	} catch (std::exception &) {
		// costing must not fail a query: a file that cannot be listed or opened is the scan's problem to report
		return fallback();
	}
	if (!rows_per_file.IsValid()) {
		return fallback();
	}
	// the file count of the partitions this scan reads -- pruning has already happened by the time the cardinality is
	// asked for, so a filtered scan is costed on what it actually reads
	auto file_count = hive_list->EstimateTotalFileCount();
	if (!file_count.IsValid()) {
		return fallback();
	}
	// An estimate, never a max: max_cardinality is a bound the optimizer may rely on, and one file says nothing about
	// the size of the rest.
	return make_uniq<NodeStatistics>(rows_per_file.GetIndex() * file_count.GetIndex());
}

//===--------------------------------------------------------------------===//
// Partition column statistics
//===--------------------------------------------------------------------===//
//! Statistics for a partition column, from the values of the partitions the scan will read. Those values are the
//! complete set of values the column takes, so min/max, the distinct count and has-null are exact and cost no file I/O.
//! DuckDB otherwise declines statistics for a hive column entirely: the file holds the column the partition value
//! overrides, and may type it differently. Any column that is not a partition key is left to the format's own function.
static unique_ptr<BaseStatistics> HivePartitionStatistics(ClientContext &context,
                                                          TableFunctionGetStatisticsInput &input) {
	auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
	auto hive_list = dynamic_cast<const HiveMultiFileList *>(bind_data.file_list.get());
	if (!hive_list) {
		return nullptr;
	}
	auto &info = hive_list->ScanInfo();

	// the column is a partition key only when it is a whole top-level column of the table
	idx_t key_index = DConstants::INVALID_INDEX;
	if (input.column_index.HasPrimaryIndex() && !input.column_index.HasChildren()) {
		auto column_id = input.column_index.GetPrimaryIndex();
		if (column_id < bind_data.columns.size()) {
			key_index = info.GetPartitionKeyIndex(bind_data.columns[column_id].name.GetIdentifierName());
		}
	}
	if (key_index == DConstants::INVALID_INDEX) {
		// a data column, a struct field or a virtual column: what the files hold is not ours to describe
		return info.format_statistics ? info.format_statistics(context, input) : nullptr;
	}

	// the partitions left after pruning: filter pushdown runs before statistics are asked for
	auto &partition_indexes = hive_list->PartitionIndexes();
	if (partition_indexes.empty()) {
		return nullptr;
	}
	auto &type = bind_data.columns[input.column_index.GetPrimaryIndex()].type;
	unique_ptr<BaseStatistics> result;
	value_set_t distinct_values;
	for (auto partition_index : partition_indexes) {
		auto &partition = info.partitions[partition_index];
		if (key_index >= partition.values.size()) {
			// Glue registered the partition with fewer values than the table has keys
			return nullptr;
		}
		Value value;
		try {
			// the value as the scan emits it: the hive NULL sentinel mapped to NULL, converted to the column's declared
			// type. The same call the scan and the pruning use, so none of the three can disagree about a partition
			value =
			    GlueTypes::PartitionValue(context, info.partition_keys[key_index], partition.values[key_index], type);
		} catch (std::exception &) {
			// a value the column's type cannot hold: reading the partition would fail, but planning must not
			return nullptr;
		}
		auto value_stats = BaseStatistics::FromConstant(value);
		if (!result) {
			result = value_stats.ToUnique();
		} else {
			// a union of single values, not an accumulation over rows: expand the bounds and claim nothing more
			result->Merge(value_stats, StatsMergeType::EXPAND_BOUNDS);
		}
		distinct_values.insert(std::move(value));
	}
	// every value of the column is a partition value, so the count is exact rather than estimated
	result->SetDistinctCount(distinct_values.size());
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
	named_parameter_map_t param_map;
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
	// partition columns are answered from the partition values; the format keeps every other column
	scan_info->format_statistics = scan_function.statistics_extended;
	scan_function.statistics_extended = HivePartitionStatistics;
	// and the row count comes from a sampled file rather than the format's constant
	scan_info->format_cardinality = scan_function.cardinality;
	scan_function.cardinality = HiveScanCardinality;

	vector<LogicalType> return_types;
	vector<Identifier> names;
	// the path argument is not used: CreateFileList builds the file list from the partitions
	TableFunctionRef empty_ref;
	vector<Value> inputs = {Value::LIST(LogicalType::VARCHAR, {Value(scan_info->root_location)})};
	// the JSON reader's multi-file wrapper reads its wrapped function from the bind input's info
	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, scan_function.function_info.get(),
	                                  nullptr, scan_function, empty_ref);
	HiveScanInfoScope scope(scan_info);
	bind_data = scan_function.bind(context, bind_input, return_types, names);
	return scan_function;
}

//===--------------------------------------------------------------------===//
// HiveMultiFileReader
//===--------------------------------------------------------------------===//
HiveMultiFileReader::HiveMultiFileReader(shared_ptr<HiveScanInfo> scan_info_p) : scan_info(std::move(scan_info_p)) {
}

unique_ptr<MultiFileReader> HiveMultiFileReader::CreateInstance(const TableFunction &table) {
	shared_ptr<HiveScanInfo> info;
	if (current_scan_info) {
		info = *current_scan_info;
	}
	auto result = make_uniq<HiveMultiFileReader>(std::move(info));
	result->function_name = table.name;
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
	// every partition, listed lazily; ComplexFilterPushdown narrows the partitions before anything is listed
	auto &info = ScanInfo();
	vector<idx_t> partition_indexes;
	for (idx_t i = 0; i < info.partitions.size(); i++) {
		partition_indexes.push_back(i);
	}
	return make_shared_ptr<HiveMultiFileList>(context, scan_info, std::move(partition_indexes));
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
//! Replace references to partition columns of the scanned table by the partition's values
static void ReplacePartitionColumnRefs(ClientContext &context, unique_ptr<Expression> &expr, TableIndex table_index,
                                       const unordered_map<idx_t, idx_t> &projection_to_key, const HiveScanInfo &info,
                                       const GluePartitionInfo &partition) {
	if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		if (colref.Binding().table_index != table_index) {
			return;
		}
		auto entry = projection_to_key.find(colref.Binding().column_index.GetIndex());
		if (entry == projection_to_key.end()) {
			return;
		}
		auto &key = info.partition_keys[entry->second];
		auto value = GlueTypes::PartitionValue(context, key, partition.values[entry->second], colref.GetReturnType());
		expr = make_uniq<BoundConstantExpression>(std::move(value));
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		ReplacePartitionColumnRefs(context, child, table_index, projection_to_key, info, partition);
	});
}

unique_ptr<MultiFileList> HiveMultiFileReader::ComplexFilterPushdown(ClientContext &context, MultiFileList &files,
                                                                     const MultiFileOptions &options,
                                                                     MultiFilePushdownInfo &pushdown_info,
                                                                     vector<unique_ptr<Expression>> &filters) {
	auto &info = ScanInfo();
	if (info.partition_keys.empty() || filters.empty()) {
		return nullptr;
	}
	// which projected columns are partition keys
	unordered_map<idx_t, idx_t> projection_to_key;
	for (idx_t i = 0; i < pushdown_info.column_ids.size(); i++) {
		auto column_id = pushdown_info.column_ids[i];
		if (IsVirtualColumn(column_id)) {
			continue;
		}
		auto key_index = info.GetPartitionKeyIndex(pushdown_info.column_names[column_id].GetIdentifierName());
		if (key_index != DConstants::INVALID_INDEX) {
			projection_to_key[i] = key_index;
		}
	}
	if (projection_to_key.empty()) {
		return nullptr;
	}

	// A filter that can be evaluated with the partition values alone decides whether the partition is read at all,
	// before its directory is listed. The filters themselves are kept: on the rows that remain they are cheap, the
	// partition columns are constants.
	auto &hive_list = files.Cast<HiveMultiFileList>();
	auto &candidates = hive_list.PartitionIndexes();
	vector<idx_t> kept;
	unordered_set<idx_t> pruning_filters;
	for (auto partition_index : candidates) {
		auto &partition = info.partitions[partition_index];
		bool keep = true;
		for (idx_t filter_index = 0; filter_index < filters.size(); filter_index++) {
			auto &filter = filters[filter_index];
			auto filter_copy = filter->Copy();
			ReplacePartitionColumnRefs(context, filter_copy, pushdown_info.table_index, projection_to_key, info,
			                           partition);
			Value result;
			if (!filter_copy->IsScalar() || !filter_copy->IsFoldable() ||
			    !ExpressionExecutor::TryEvaluateScalar(context, *filter_copy, result)) {
				// needs the data columns, can not decide here
				continue;
			}
			if (result.IsNull() || !result.GetValue<bool>()) {
				keep = false;
				if (pruning_filters.insert(filter_index).second) {
					if (!pushdown_info.extra_info.file_filters.empty()) {
						pushdown_info.extra_info.file_filters += " AND ";
					}
					pushdown_info.extra_info.file_filters += filter->ToString();
				}
				break;
			}
		}
		if (keep) {
			kept.push_back(partition_index);
		}
	}
	// reported as files in EXPLAIN, but these are partitions: nothing has been listed yet
	pushdown_info.extra_info.total_files = candidates.size();
	pushdown_info.extra_info.filtered_files = kept.size();
	if (kept.size() == candidates.size()) {
		return nullptr;
	}
	return make_uniq<HiveMultiFileList>(context, scan_info, std::move(kept));
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
