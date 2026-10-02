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
// Per-query partition listing
//===--------------------------------------------------------------------===//
//! One directory of a table, listed and measured once per query for every scan of the table
struct HiveTablePartitionListing {
	annotated_mutex lock;
	bool attempted DUCKDB_GUARDED_BY(lock) = false;
	//! The directory was listed; false when the format is not measured or the listing failed
	bool listed DUCKDB_GUARDED_BY(lock) = false;
	//! The data files in the directory, their total size, and the smallest and largest of them
	idx_t files DUCKDB_GUARDED_BY(lock) = 0;
	optional_idx bytes DUCKDB_GUARDED_BY(lock);
	idx_t min_file_size DUCKDB_GUARDED_BY(lock) = 0;
	idx_t max_file_size DUCKDB_GUARDED_BY(lock) = 0;
	//! The rows and size of the file measured, the largest
	optional_idx file_rows DUCKDB_GUARDED_BY(lock);
	optional_idx file_bytes DUCKDB_GUARDED_BY(lock);
};

static constexpr const char *HIVE_SAMPLE_CACHE = "glue_hive_sample";

//! The partition listings taken in the running query and the directory listings they took, dropped when the query ends
class HiveSampleCache : public ClientContextState {
public:
	void QueryEnd(ClientContext &context) override {
		annotated_lock_guard<annotated_mutex> guard(lock);
		partition_listings.clear();
		directory_listings.clear();
	}
	shared_ptr<HiveTablePartitionListing> GetPartitionListing(const string &table) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &listing = partition_listings[table];
		if (!listing) {
			listing = make_shared_ptr<HiveTablePartitionListing>();
		}
		return listing;
	}
	void AddDirectoryListing(const string &directory, vector<OpenFileInfo> files) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		directory_listings[directory] = std::move(files);
	}
	//! Append the files a sample listed in 'directory', if one did
	bool GetDirectoryListing(const string &directory, vector<OpenFileInfo> &files) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto entry = directory_listings.find(directory);
		if (entry == directory_listings.end()) {
			return false;
		}
		files.insert(files.end(), entry->second.begin(), entry->second.end());
		return true;
	}

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<HiveTablePartitionListing>> partition_listings DUCKDB_GUARDED_BY(lock);
	unordered_map<string, vector<OpenFileInfo>> directory_listings DUCKDB_GUARDED_BY(lock);
};

static string DirectoryKey(const string &location) {
	auto directory = location;
	StringUtil::RTrim(directory, "/");
	return directory;
}

void AddSampledListing(ClientContext &context, const string &location, const vector<OpenFileInfo> &files) {
	auto cache = context.registered_state->GetOrCreate<HiveSampleCache>(HIVE_SAMPLE_CACHE);
	cache->AddDirectoryListing(DirectoryKey(location), files);
}

bool FindSampledListing(ClientContext &context, const string &location, vector<OpenFileInfo> &files) {
	auto cache = context.registered_state->Get<HiveSampleCache>(HIVE_SAMPLE_CACHE);
	return cache && cache->GetDirectoryListing(DirectoryKey(location), files);
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
static optional_idx SampleLineOrientedRowsPerFile(ClientContext &context, const OpenFileInfo &file, bool header) {
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
	if (header && rows > 1) {
		rows--;
	}
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
		return SampleLineOrientedRowsPerFile(context, file, info.header);
	case HiveFileFormat::AVRO:
		return optional_idx();
	}
	return optional_idx();
}

//! The table of a scan: every scan of it in a query shares one sample
static string SampleKey(const HiveScanInfo &info) {
	return HiveFileFormatToString(info.file_format) + (info.header ? "+header|" : "|") +
	       DirectoryKey(info.root_location) + "|" + info.Describe() + "|" + StringUtil::Join(info.partition_keys, ",");
}

//! Measure the table a scan reads, the first time a scan of it asks in this query. It measures files, which a
//! predicate does not change: the predicate only decides how many partitions a scan reads.
static void MeasureTable(ClientContext &context, const MultiFileBindData &bind_data, const HiveMultiFileList &files,
                         HiveTablePartitionListing &listing) DUCKDB_REQUIRES(listing.lock) {
	if (listing.attempted) {
		return;
	}
	listing.attempted = true;
	auto &info = files.ScanInfo();
	if (info.file_format == HiveFileFormat::AVRO) {
		// no bounded way to count avro rows without an avro reader, so there is nothing to list for
		return;
	}
	try {
		auto listed = files.ListSampleDirectory();
		listing.listed = true;
		listing.files = listed.size();
		// the largest file carries the least per-file format overhead, so it is the least misleading one to measure
		optional_idx measured;
		idx_t bytes = 0;
		idx_t sized = 0;
		for (idx_t i = 0; i < listed.size(); i++) {
			auto size = ListedFileSize(listed[i]);
			if (!size.IsValid()) {
				continue;
			}
			if (sized == 0 || size.GetIndex() < listing.min_file_size) {
				listing.min_file_size = size.GetIndex();
			}
			if (!measured.IsValid() || size.GetIndex() > listing.max_file_size) {
				listing.max_file_size = size.GetIndex();
				measured = i;
			}
			bytes += size.GetIndex();
			sized++;
		}
		if (sized > 0) {
			// a file the listing gave no size for counts as an average one
			listing.bytes = bytes + (bytes / sized) * (listed.size() - sized);
		}
		if (listed.empty()) {
			return;
		}
		auto &file = listed[measured.IsValid() ? measured.GetIndex() : 0];
		listing.file_bytes = ListedFileSize(file);
		listing.file_rows = RowsInFile(context, bind_data, info, file);
	} catch (std::exception &ex) {
		// costing must not fail a query: the scan reports a directory or file it cannot read
		ErrorData error(ex);
		DUCKDB_LOG_WARNING(context, "Could not sample Hive table '%s' for its cardinality: %s", info.Describe(),
		                   error.RawMessage());
	}
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
	auto partitions = info.partition_keys.empty() ? idx_t(1) : hive_list.PartitionIndexes().size();
	if (partitions == 0) {
		return make_uniq<NodeStatistics>(0);
	}
	auto cache = context.registered_state->GetOrCreate<HiveSampleCache>(HIVE_SAMPLE_CACHE);
	auto listing = cache->GetPartitionListing(SampleKey(info));
	annotated_lock_guard<annotated_mutex> guard(listing->lock);
	MeasureTable(context, bind_data, hive_list, *listing);
	if (!listing->file_rows.IsValid()) {
		if (listing->listed && listing->files == 0 && info.partition_keys.empty()) {
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
	if (listing->bytes.IsValid() && listing->file_bytes.IsValid() && listing->file_bytes.GetIndex() > 0 &&
	    listing->max_file_size > listing->min_file_size * 2) {
		auto total_bytes = listing->bytes.GetIndex() * partitions;
		auto rows = static_cast<double>(total_bytes) / static_cast<double>(listing->file_bytes.GetIndex()) *
		            static_cast<double>(listing->file_rows.GetIndex());
		return make_uniq<NodeStatistics>(MaxValue<idx_t>(static_cast<idx_t>(rows), 1));
	}
	return make_uniq<NodeStatistics>(listing->file_rows.GetIndex() * listing->files * partitions);
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
	auto &partition_indexes = hive_list.PartitionIndexes();
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

} // namespace duckdb
