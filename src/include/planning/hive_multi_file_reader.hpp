#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/table_function.hpp"

#include "core/glue_info.hpp"

namespace duckdb {

//! Everything a Hive table scan knows before any data file is opened: the table schema as Glue defines it, the
//! partitions Glue lists (values and locations) and the data files of every partition
struct HiveScanInfo : public TableFunctionInfo {
	//! Where the table comes from: a Glue table in the attached catalog catalog_name, or a hive_scan root
	string catalog_name;
	string database_name;
	string table_name;
	//! The Glue catalog table this scan reads, or null when the scan is a hive_scan over a location: reported
	//! through the scan's bind info, as iceberg_scan reports its Iceberg table
	optional_ptr<TableCatalogEntry> table;
	//! The bind info callback of the bound file format reader, which the Hive scan extends with 'table'
	table_function_get_bind_info_t format_bind_info = nullptr;
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
	//! The partition (index into Partitions()) each listed data file belongs to. Filled in while the file list expands,
	//! which can run concurrently with opening files.
	mutable mutex file_partitions_lock;
	unordered_map<string, idx_t> file_partitions;
	//! The statistics function of the bound file format reader. BindHiveScan wraps it to answer partition columns from
	//! the partition values, and every other column is delegated back to this. Null when the reader has none.
	table_statistics_extended_t format_statistics = nullptr;

	//! The index of a partition key by name, or DConstants::INVALID_INDEX
	idx_t GetPartitionKeyIndex(const string &name) const;
	//! The partition (index into 'partitions') the file at 'path' belongs to
	idx_t GetPartitionIndexOfFile(const string &path) const;
	const GluePartitionInfo &GetPartitionOfFile(const string &path) const;
	//! A description of the table for error messages
	string Describe() const;
	//! The partitions of the table: given at bind (hive_scan), or fetched from Glue on first use and shared with every
	//! scan of the table in the query. Never replaced once set, so indexes into it stay valid
	shared_ptr<const vector<GluePartitionInfo>> Partitions(ClientContext &context) const;
	bool PartitionsLoaded() const;
	void SetPartitions(vector<GluePartitionInfo> partitions_p);

private:
	mutable annotated_mutex partitions_lock;
	mutable shared_ptr<const vector<GluePartitionInfo>> partitions DUCKDB_GUARDED_BY(partitions_lock);
};

//! The data files of a Hive table, listed lazily: nothing is listed until the scan asks for files, and the filters on
//! the partition columns are applied to the partition values first (HiveMultiFileReader::ComplexFilterPushdown), so
//! only the partitions a query reads are ever listed. When at least 'hive_partition_listing_threshold' of those
//! partitions live below the table root, the root is listed once (recursively, one request per 1000 keys on S3) and
//! the files are matched to their partitions by prefix; otherwise, and for partitions elsewhere, every partition is
//! one listing of its location. An unpartitioned table is one listing of the root location.
class HiveMultiFileList : public LazyMultiFileList {
public:
	//! 'partition_indexes' are the partitions to read
	HiveMultiFileList(ClientContext &context, shared_ptr<HiveScanInfo> scan_info, vector<idx_t> partition_indexes);
	//! Every partition of the table, resolved when PartitionIndexes() is first asked
	HiveMultiFileList(ClientContext &context, shared_ptr<HiveScanInfo> scan_info);

	//! The partitions to read, as indexes into HiveScanInfo::Partitions()
	shared_ptr<const vector<idx_t>> PartitionIndexes() const;
	const HiveScanInfo &ScanInfo() const {
		return *scan_info;
	}
	ClientContext &Context() const {
		return client_context;
	}
	FileExpandResult GetExpandResult() const override;
	//! Prune on the table filters pushed in when the scan starts
	unique_ptr<MultiFileList> DynamicFilterPushdown(MultiFileDynamicPushdownInfo &info) const override;
	//! Without listing: the number of partitions still to read as a lower bound (NOT_ALL_FILES_KNOWN)
	MultiFileCount GetFileCount(idx_t min_exact_count = 0) const override;
	//! The data files listed to measure the table: the first partition's, or the location of an unpartitioned table.
	//! The listing is kept until the query ends, and any scan listing the same directory in the query takes it from
	//! there. When the scan lists the table root, the files of every partition the root's first page holds completely
	//! instead (fetching more only until it holds one the scan reads), and the scan continues that listing.
	vector<OpenFileInfo> ListSampleDirectory() const;
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
	void ListRoot(const vector<idx_t> &partitions) const;
	void ListPartition(idx_t partition_index) const;
	//! The files of the partitions the fetched pages of the root listing hold completely, fetching more only until
	//! they hold one of 'partitions'
	vector<OpenFileInfo> SampleRootPartitions(const vector<idx_t> &partitions) const;
	//! Index every registered partition location, including pruned ones, so attribution does not depend on filters
	void BuildPartitionLocations() const;
	//! The partition of the deepest registered location containing the file, searching no shorter than
	//! 'min_directory_size'
	optional_idx OwningPartition(const string &file_path, idx_t min_directory_size) const;
	//! Add a listed file of the partition unless this list already has it (two partitions sharing a location: the
	//! file belongs to the first). Called with HiveScanInfo::file_partitions_lock held.
	void AddFile(OpenFileInfo file, idx_t partition_index) const;

private:
	//! The context the list was created in; LazyMultiFileList keeps it as an optional_ptr that is const in const
	//! members
	ClientContext &client_context;
	shared_ptr<HiveScanInfo> scan_info;
	mutable annotated_mutex indexes_lock;
	//! Null for every partition of the table until PartitionIndexes() resolves it
	mutable shared_ptr<const vector<idx_t>> partition_indexes DUCKDB_GUARDED_BY(indexes_lock);
	mutable bool planned = false;
	mutable vector<ListingJob> listing_jobs;
	//! The next entry of 'listing_jobs' to run
	mutable idx_t next_job = 0;
	//! The paths already in 'expanded_files'
	mutable unordered_set<string> listed_files;
	//! Registered partition location (without trailing '/') to its index in HiveScanInfo::Partitions()
	mutable unordered_map<string, idx_t> partition_by_location;
	mutable bool partition_locations_built = false;
};

//! Bind the reader for the file format (read_parquet, read_csv, read_json or read_avro) over the partitions of
//! 'scan_info' with the HiveMultiFileReader. Returns the bound table function and fills in 'bind_data'; the scan
//! produces exactly the columns of 'scan_info'. No file is listed or opened here.
TableFunction BindHiveScan(ClientContext &context, shared_ptr<HiveScanInfo> scan_info,
                           unique_ptr<FunctionData> &bind_data);
//! Give 'function' the Hive scan's plan serialization, which describes the scan in full so that deserializing a plan
//! binds the same scan again
void SetHiveScanSerialization(TableFunction &function);

//! MultiFileReader for Hive tables registered in Glue. It reads the files Glue's partitions point to (whatever their
//! directory names), binds the schema Glue defines rather than the schema of the first file (a column missing from a
//! file reads as NULL, a differently typed column is cast) and fills the partition columns of every file with the
//! values Glue stores for its partition. Filters on partition columns prune whole partitions before any file is opened.
class HiveMultiFileReader : public MultiFileReader {
public:
	explicit HiveMultiFileReader(shared_ptr<HiveScanInfo> scan_info);

	static unique_ptr<MultiFileReader> CreateInstance(const BoundTableFunction &table);

	//! The scan info behind this reader, which the Hive scan's bind info delegates to the format reader and extends
	const HiveScanInfo &ScanInfo() const;

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
	shared_ptr<HiveScanInfo> scan_info;
};

} // namespace duckdb
