#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/table_function.hpp"

#include "glue_api.hpp"

namespace duckdb {
class FileSystem;

//! Everything a Hive table scan knows before any data file is opened: the table schema as Glue defines it, the
//! partitions Glue lists (values and locations) and the data files of every partition
struct HiveScanInfo : public TableFunctionInfo {
	//! Where the table comes from, for error messages: a Glue table or a hive_scan root
	string database_name;
	string table_name;
	//! The table location: the data files of an unpartitioned table live directly below it, and it is the parent of
	//! the <key>=<value> directories of partitions without an explicit location
	string root_location;
	//! The columns of the table: data columns first, partition keys last
	vector<Identifier> names;
	vector<LogicalType> types;
	//! The file format of the data files
	HiveFileFormat file_format = HiveFileFormat::PARQUET;
	//! CSV only: the dialect and whether every file starts with a header line
	string delimiter = ",";
	string quote = "\"";
	string escape = "\"";
	bool header = false;
	//! The partition keys, in order
	vector<string> partition_keys;
	//! The partitions registered in Glue (empty for an unpartitioned table)
	vector<GluePartitionInfo> partitions;
	//! The partition (index into 'partitions') each listed data file belongs to. Filled in while the file list expands,
	//! which can run concurrently with opening files.
	mutable mutex file_partitions_lock;
	unordered_map<string, idx_t> file_partitions;
	//! The statistics function of the bound file format reader. BindHiveScan wraps it to answer partition columns from
	//! the partition values, and every other column is delegated back to this. Null when the reader has none.
	table_statistics_extended_t format_statistics = nullptr;

	//! The index of a partition key by name, or DConstants::INVALID_INDEX
	idx_t GetPartitionKeyIndex(const string &name) const;
	//! The partition the file at 'path' belongs to
	const GluePartitionInfo &GetPartitionOfFile(const string &path) const;
	//! A description of the table for error messages
	string Describe() const;
};

//! The data files of a Hive table, listed lazily: nothing is listed until the scan asks for files, and the filters on
//! the partition columns are applied to the partition values first (HiveMultiFileReader::ComplexFilterPushdown), so
//! only the partitions a query reads are ever listed. When at least 'hive_partition_listing_threshold' of those
//! partitions live below the table root, the root is listed once (recursively, one request per 1000 keys on S3) and
//! the files are matched to their partitions by prefix; otherwise, and for partitions elsewhere, every partition is
//! one listing of its location. An unpartitioned table is one listing of the root location.
class HiveMultiFileList : public LazyMultiFileList {
public:
	//! 'partition_indexes' are the partitions (indexes into HiveScanInfo::partitions) to read
	HiveMultiFileList(ClientContext &context, shared_ptr<HiveScanInfo> scan_info, vector<idx_t> partition_indexes);

	const vector<idx_t> &PartitionIndexes() const {
		return partition_indexes;
	}
	const HiveScanInfo &ScanInfo() const {
		return *scan_info;
	}
	FileExpandResult GetExpandResult() const override;
	//! Without listing: the number of partitions still to read as a lower bound (NOT_ALL_FILES_KNOWN)
	MultiFileCount GetFileCount(idx_t min_exact_count = 0) const override;
	vector<OpenFileInfo> GetDisplayFileList(optional_idx max_files = optional_idx()) const override;
	unique_ptr<MultiFileList> Copy() const override;

protected:
	bool ExpandNextPath() const override;

private:
	//! A directory listing still to do: the table root (for the partitions below it) or one partition
	struct ListingJob {
		bool root;
		vector<idx_t> partitions;
	};
	//! Decide the listings from the partitions to read (once, under the lock)
	void PlanListings() const;
	void ListRoot(FileSystem &fs, const vector<idx_t> &partitions) const;
	void ListPartition(FileSystem &fs, idx_t partition_index) const;
	//! Index every registered partition location, not only the ones being read: a file belongs to the deepest
	//! location registered for it, and which partitions a query happens to read must not change that (once)
	void BuildPartitionLocations() const;
	//! The partition a listed file belongs to: the deepest registered location the file lies under, walking up from
	//! its directory and stopping below 'min_directory_size'. Invalid when the file is under no registered location.
	optional_idx OwningPartition(const string &file_path, idx_t min_directory_size) const;
	//! Add a listed file of the partition unless this list already has it (two partitions sharing a location: the
	//! file belongs to the first). Called with HiveScanInfo::file_partitions_lock held.
	void AddFile(OpenFileInfo file, idx_t partition_index) const;

private:
	//! The context the list was created in; LazyMultiFileList keeps it as an optional_ptr that is const in const
	//! members
	ClientContext &client_context;
	shared_ptr<HiveScanInfo> scan_info;
	vector<idx_t> partition_indexes;
	mutable bool planned = false;
	mutable vector<ListingJob> jobs;
	//! The next entry of 'jobs' to run
	mutable idx_t next_job = 0;
	//! The paths already in 'expanded_files'
	mutable unordered_set<string> listed_files;
	//! Every registered partition location (trailing '/' trimmed) to its index in HiveScanInfo::partitions, for
	//! deepest-match attribution. Built by BuildPartitionLocations on first use.
	mutable unordered_map<string, idx_t> partition_by_location;
	mutable bool partition_locations_built = false;
};

//! Bind the reader for the file format (read_parquet, read_csv, read_json or read_avro) over the partitions of
//! 'scan_info' with the HiveMultiFileReader. Returns the bound table function and fills in 'bind_data'; the scan
//! produces exactly the columns of 'scan_info'. No file is listed or opened here.
TableFunction BindHiveScan(ClientContext &context, shared_ptr<HiveScanInfo> scan_info,
                           unique_ptr<FunctionData> &bind_data);

//! MultiFileReader for Hive tables registered in Glue. It reads the files Glue's partitions point to (whatever their
//! directory names), binds the schema Glue defines rather than the schema of the first file (a column missing from a
//! file reads as NULL, a differently typed column is cast) and fills the partition columns of every file with the
//! values Glue stores for its partition. Filters on partition columns prune whole partitions before any file is opened.
class HiveMultiFileReader : public MultiFileReader {
public:
	explicit HiveMultiFileReader(shared_ptr<HiveScanInfo> scan_info);

	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table);

	unique_ptr<MultiFileReader> Copy() const override;
	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	          vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;
	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, MultiFileList &files,
	                                                const MultiFileOptions &options, MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) override;
	void FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
	                  const MultiFileReaderBindData &options, const vector<MultiFileColumnDefinition> &global_columns,
	                  const vector<ColumnIndex> &global_column_ids, ClientContext &context,
	                  optional_ptr<MultiFileReaderGlobalState> global_state) override;

private:
	const HiveScanInfo &ScanInfo() const;

private:
	shared_ptr<HiveScanInfo> scan_info;
};

} // namespace duckdb
