#include "planning/hive_stats.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "core/glue_types.hpp"
#include "planning/hive_multi_file_reader.hpp"

namespace duckdb {

//! The size the listing reported for a file, when it reported one. S3 and the local filesystem both carry it, so the
//! bytes of a table are known from the listing alone, without opening anything
static optional_idx ListedFileSize(const OpenFileInfo &file) {
	if (!file.extended_info) {
		return optional_idx();
	}
	idx_t size;
	if (!file.extended_info->TryGetOption("file_size", size)) {
		return optional_idx();
	}
	return size;
}

//===--------------------------------------------------------------------===//
// Per-query cache
//===--------------------------------------------------------------------===//
//! The measurement of one directory of a table
struct HiveTableSample {
	//! The directory was listed; false when the format is not measured or the listing failed
	bool listed = false;
	//! The data files in the directory, their total size, and the smallest and largest of them
	idx_t files = 0;
	optional_idx bytes;
	idx_t min_file_size = 0;
	idx_t max_file_size = 0;
	//! The rows and size of the file measured, the largest
	optional_idx file_rows;
	optional_idx file_bytes;
};

//! The sample of a table, measured once per query for every scan of the table
struct HiveTableSampleEntry {
	annotated_mutex lock;
	//! Null until the first scan of the table measures it
	unique_ptr<HiveTableSample> sample DUCKDB_GUARDED_BY(lock);
};

//! The partitions of a table, fetched once per query for every scan of the table
struct HiveTablePartitionsEntry {
	annotated_mutex lock;
	//! Null until the first scan of the table asks for them
	shared_ptr<const vector<GluePartitionInfo>> partitions DUCKDB_GUARDED_BY(lock);
};

static constexpr const char *HIVE_QUERY_CACHE = "glue_hive_query_cache";

//! What the scans of the running query share per table and directory, dropped when the query ends
class HiveQueryCache : public ClientContextState {
public:
	void QueryEnd(ClientContext &context) override {
		annotated_lock_guard<annotated_mutex> guard(lock);
		samples.clear();
		partitions.clear();
		directory_listings.clear();
	}
	shared_ptr<HiveTableSampleEntry> GetSample(const string &table) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &sample = samples[table];
		if (!sample) {
			sample = make_shared_ptr<HiveTableSampleEntry>();
		}
		return sample;
	}
	shared_ptr<HiveTablePartitionsEntry> GetPartitions(const string &table) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &entry = partitions[table];
		if (!entry) {
			entry = make_shared_ptr<HiveTablePartitionsEntry>();
		}
		return entry;
	}
	shared_ptr<MultiFileList> GetDirectoryListing(ClientContext &context, const string &directory) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &listing = directory_listings[directory];
		if (!listing) {
			auto &fs = FileSystem::GetFileSystem(context);
			listing = shared_ptr<MultiFileList>(fs.GlobFileList(directory + "/**", FileGlobOptions::ALLOW_EMPTY));
		}
		return listing;
	}

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<HiveTableSampleEntry>> samples DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<HiveTablePartitionsEntry>> partitions DUCKDB_GUARDED_BY(lock);
	//! The recursive listing of a directory, fetched page by page as it is read
	unordered_map<string, shared_ptr<MultiFileList>> directory_listings DUCKDB_GUARDED_BY(lock);
};

static shared_ptr<HiveQueryCache> GetQueryCache(ClientContext &context) {
	return context.registered_state->GetOrCreate<HiveQueryCache>(HIVE_QUERY_CACHE);
}

static string TableKey(const HiveScanInfo &info) {
	return info.catalog_name + "." + info.Describe();
}

static string DirectoryKey(const string &location) {
	auto directory = location;
	StringUtil::RTrim(directory, "/");
	return directory;
}

shared_ptr<MultiFileList> GetDirectoryListing(ClientContext &context, const string &directory) {
	return GetQueryCache(context)->GetDirectoryListing(context, DirectoryKey(directory));
}

shared_ptr<const vector<GluePartitionInfo>> GetTablePartitions(ClientContext &context, const HiveScanInfo &info) {
	auto entry = GetQueryCache(context)->GetPartitions(TableKey(info));
	// per table, so the Glue calls for different tables do not wait on each other
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	if (!entry->partitions) {
		auto &catalog = Catalog::GetCatalog(context, Identifier(info.catalog_name)).Cast<GlueCatalog>();
		entry->partitions = make_shared_ptr<const vector<GluePartitionInfo>>(
		    GlueAPI::GetPartitions(context, catalog, info.database_name, info.table_name));
	}
	return entry->partitions;
}

//===--------------------------------------------------------------------===//
// Cardinality sample
//===--------------------------------------------------------------------===//
//! The uncompressed size a gzip file records in its last four bytes (modulo 4 GiB, and of its last member only)
static optional_idx GzipUncompressedSize(FileSystem &fs, const string &path) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	if (!handle) {
		return optional_idx();
	}
	auto compressed_size = static_cast<idx_t>(handle->GetFileSize());
	if (compressed_size < 4) {
		return optional_idx();
	}
	uint8_t trailer[4];
	handle->Read(trailer, sizeof(trailer), compressed_size - sizeof(trailer));
	auto size = idx_t(trailer[0]) | idx_t(trailer[1]) << 8 | idx_t(trailer[2]) << 16 | idx_t(trailer[3]) << 24;
	// text compresses, so a recorded size below the compressed one has wrapped past 4 GiB
	if (size < compressed_size) {
		return optional_idx();
	}
	return size;
}

//! List ONE directory of a table and open ONE of its files, once per query, so every scan's cost reflects the data.
//!
//! This is the trick read_parquet already plays -- it globs and binds on the first file. We skip that path because
//! Glue gives us the schema, so we take the row count from a file.
//! Files in line-oriented formats such as csv and json read a bounded prefix, count its lines,
//! and scale the average line length over the file. csv and newline-delimited json are one row per line. There is no
//! footer to ask.
static optional_idx SampleLineOrientedRowsPerFile(ClientContext &context, const OpenFileInfo &file,
                                                  idx_t header_lines) {
	static constexpr idx_t SAMPLE_BYTES = 65536;
	// zstd records its uncompressed size only optionally, and the scan cannot decompress the others at all
	for (auto extension : {".zst", ".snappy", ".lz4", ".bz2", ".deflate"}) {
		if (StringUtil::EndsWith(file.path, extension)) {
			return optional_idx();
		}
	}
	auto &fs = FileSystem::GetFileSystem(context);
	auto gzip = IsFileCompressed(file.path, FileCompressionType::GZIP);
	optional_idx gzip_size;
	if (gzip) {
		gzip_size = GzipUncompressedSize(fs, file.path);
		if (!gzip_size.IsValid()) {
			return optional_idx();
		}
	}
	auto compression = gzip ? FileCompressionType::GZIP : FileCompressionType::UNCOMPRESSED;
	auto handle = fs.OpenFile(file.path, FileFlags::FILE_FLAGS_READ | compression);
	if (!handle) {
		return optional_idx();
	}
	// sizes and lines are counted uncompressed
	auto file_size = gzip ? gzip_size.GetIndex() : static_cast<idx_t>(handle->GetFileSize());
	if (file_size == 0) {
		return optional_idx();
	}
	string buffer(MinValue<idx_t>(file_size, SAMPLE_BYTES), '\0');
	// sequentially, since a compressed stream cannot be read at an offset
	auto sample_size = static_cast<idx_t>(handle->Read(reinterpret_cast<void *>(&buffer[0]), buffer.size()));
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
	rows = rows > header_lines ? rows - header_lines : 0;
	return MaxValue<idx_t>(rows, 1);
}

//! The rows of one data file: from the footer for parquet, from the lines of a bounded prefix for csv and json
static optional_idx RowsInFile(ClientContext &context, const MultiFileBindData &bind_data, const HiveScanInfo &info,
                               const OpenFileInfo &file) {
	switch (info.file_format) {
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
		auto rows = probe_data.interface->GetCardinality(context, probe_data, 1);
		if (rows && rows->has_estimated_cardinality) {
			return rows->estimated_cardinality;
		}
		return optional_idx();
	}
	case HiveFileFormat::CSV:
	case HiveFileFormat::JSON:
		// no reader is built for these: they are bound with a dialect and an explicit column list that fresh options
		// would not reproduce. Counting lines needs none of it
		return SampleLineOrientedRowsPerFile(context, file, info.csv_options.skip_lines);
	case HiveFileFormat::AVRO:
		return optional_idx();
	}
	return optional_idx();
}

//! Measure the table a scan reads, the first time a scan of it asks in this query. It measures files, which a
//! predicate does not change: the predicate only decides how many partitions a scan reads.
static HiveTableSample MeasureTable(ClientContext &context, const MultiFileBindData &bind_data,
                                    const HiveMultiFileList &files) {
	HiveTableSample sample;
	auto &info = files.ScanInfo();
	if (info.file_format == HiveFileFormat::AVRO) {
		// no bounded way to count avro rows without an avro reader, so there is nothing to list for
		return sample;
	}
	try {
		auto listed = files.ListSampleDirectory();
		sample.listed = true;
		sample.files = listed.size();
		// the largest file carries the least per-file format overhead, so it is the least misleading one to measure
		optional_idx measured;
		idx_t bytes = 0;
		idx_t sized = 0;
		for (idx_t i = 0; i < listed.size(); i++) {
			auto size = ListedFileSize(listed[i]);
			if (!size.IsValid()) {
				continue;
			}
			if (sized == 0 || size.GetIndex() < sample.min_file_size) {
				sample.min_file_size = size.GetIndex();
			}
			if (!measured.IsValid() || size.GetIndex() > sample.max_file_size) {
				sample.max_file_size = size.GetIndex();
				measured = i;
			}
			bytes += size.GetIndex();
			sized++;
		}
		if (sized > 0) {
			// a file the listing gave no size for counts as an average one
			sample.bytes = bytes + (bytes / sized) * (listed.size() - sized);
		}
		if (listed.empty()) {
			return sample;
		}
		auto &file = listed[measured.IsValid() ? measured.GetIndex() : 0];
		sample.file_bytes = ListedFileSize(file);
		sample.file_rows = RowsInFile(context, bind_data, info, file);
	} catch (std::exception &ex) {
		// costing must not fail a query: the scan reports a directory or file it cannot read
		ErrorData error(ex);
		DUCKDB_LOG_WARNING(context, "Could not sample Hive table '%s' for its cardinality: %s", info.Describe(),
		                   error.RawMessage());
	}
	return sample;
}

//! Estimate without listing; the format's own cardinality asks for GetFileCount(500), which lists while planning
static unique_ptr<NodeStatistics> UnsampledCardinality(ClientContext &context, const MultiFileBindData &bind_data) {
	auto count_info = bind_data.file_list->GetFileCount();
	auto estimated_file_count = count_info.count;
	if (count_info.type != FileExpansionType::ALL_FILES_EXPANDED) {
		estimated_file_count *= 2;
	}
	return bind_data.interface->GetCardinality(context, bind_data, estimated_file_count);
}

//! Cardinality of a Hive scan: one directory of the table, measured once per query, scaled by the partitions this
//! scan reads. Replaces a constant: without it a 40-row dimension table and a 200,000-row fact table cost the same,
//! and csv and json are estimated at one row.
unique_ptr<NodeStatistics> HiveScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
	auto &hive_list = bind_data.file_list->Cast<HiveMultiFileList>();
	auto &info = hive_list.ScanInfo();
	// pruning has already happened by the time the cardinality is asked for, so these are the partitions read
	auto partitions = info.partition_keys.empty() ? idx_t(1) : hive_list.PartitionIndexes()->size();
	if (partitions == 0) {
		return make_uniq<NodeStatistics>(0);
	}
	auto listing = GetQueryCache(context)->GetSample(TableKey(info));
	HiveTableSample sample;
	{
		annotated_lock_guard<annotated_mutex> guard(listing->lock);
		if (!listing->sample) {
			listing->sample = make_uniq<HiveTableSample>(MeasureTable(context, bind_data, hive_list));
		}
		// a copy: the sample is a few idx_t, and the estimate below then reads it without the lock
		sample = *listing->sample;
	}
	if (!sample.file_rows.IsValid()) {
		if (sample.listed && sample.files == 0 && info.partition_keys.empty()) {
			// the only directory of the table holds no data file
			return make_uniq<NodeStatistics>(0);
		}
		return UnsampledCardinality(context, bind_data);
	}
	// An estimate, never a max: max_cardinality is a bound the optimizer may rely on, and one directory says nothing
	// about the size of the rest.
	//
	// Files of about one size hold about one number of rows, so the measured file stands in for all of them and the
	// answer is exact when they are equal -- which is the normal shape, one similar file per INSERT. Once they differ
	// by more than a factor of two the count says nothing (10 rows beside 90,000 is two files either way) and only
	// bytes carry the difference.
	if (sample.bytes.IsValid() && sample.file_bytes.IsValid() && sample.file_bytes.GetIndex() > 0 &&
	    sample.max_file_size > sample.min_file_size * 2) {
		auto total_bytes = sample.bytes.GetIndex() * partitions;
		auto rows = static_cast<double>(total_bytes) / static_cast<double>(sample.file_bytes.GetIndex()) *
		            static_cast<double>(sample.file_rows.GetIndex());
		return make_uniq<NodeStatistics>(MaxValue<idx_t>(static_cast<idx_t>(rows), 1));
	}
	return make_uniq<NodeStatistics>(sample.file_rows.GetIndex() * sample.files * partitions);
}

//===--------------------------------------------------------------------===//
// Partition column statistics
//===--------------------------------------------------------------------===//
//! Statistics for a partition column, from the values of the partitions the scan will read. We get the
//! complete set of values the column takes and can derive min/max, the distinct count and has-null.
//! Columns that are not partition keys are left to the format's own function.
unique_ptr<BaseStatistics> HivePartitionStatistics(ClientContext &context, TableFunctionGetStatisticsInput &input) {
	auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
	auto &hive_list = bind_data.file_list->Cast<HiveMultiFileList>();
	auto &info = hive_list.ScanInfo();

	// the column is a partition key only when it is a whole top-level column of the table
	idx_t key_index = DConstants::INVALID_INDEX;
	if (input.column_index.HasPrimaryIndex() && !input.column_index.HasChildren()) {
		auto column_id = input.column_index.GetPrimaryIndex();
		if (column_id < bind_data.columns.size()) {
			key_index = info.GetPartitionKeyIndex(bind_data.columns[column_id].name.GetIdentifierName());
		}
	}
	if (key_index == DConstants::INVALID_INDEX) {
		// a data column, a struct field or a virtual column
		return info.format_statistics ? info.format_statistics(context, input) : nullptr;
	}

	// the partitions left after pruning: filter pushdown runs before statistics are asked for
	auto partition_indexes = hive_list.PartitionIndexes();
	if (partition_indexes->empty()) {
		return nullptr;
	}
	auto &type = bind_data.columns[input.column_index.GetPrimaryIndex()].type;
	unique_ptr<BaseStatistics> result;
	value_set_t distinct_values;
	auto partitions = info.Partitions(context);
	for (auto partition_index : *partition_indexes) {
		auto &partition = (*partitions)[partition_index];
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

} // namespace duckdb
