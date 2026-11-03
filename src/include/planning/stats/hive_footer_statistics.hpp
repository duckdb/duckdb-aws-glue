#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_options.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

class HiveMultiFileList;
struct HiveFileFooterEntry;

//! The default and the largest value of hive_footer_statistics_max_files
constexpr idx_t HIVE_FOOTER_STATISTICS_DEFAULT_MAX_FILES = 16;
constexpr idx_t HIVE_FOOTER_STATISTICS_MAX_FILES_LIMIT = 25;

//! The parquet footers of a Hive scan's files, read once per query and shared by every scan of a file
class HiveParquetFooters {
public:
	HiveParquetFooters(ClientContext &context, const MultiFileBindData &bind_data, const HiveMultiFileList &list);

	//! Invalid unless every file is within hive_footer_statistics_max_files and has a footer
	optional_idx Rows();
	//! Null unless every file with rows has the column, as the table's type, in its footer
	unique_ptr<BaseStatistics> Statistics(const MultiFileColumnDefinition &column);
	//! Null when the footer could not be read
	shared_ptr<BaseUnionData> Get(const OpenFileInfo &file);

private:
	//! False when the scan is partitioned or has more files than the bound
	bool ReadAll();
	shared_ptr<HiveFileFooterEntry> Entry(const OpenFileInfo &file) const;

private:
	ClientContext &context;
	const MultiFileBindData &bind_data;
	const HiveMultiFileList &list;
	//! Parsed once from the scan's reader_parameters, shared by every read
	unique_ptr<BaseFileReaderOptions> options;
	MultiFileOptions file_options;
	vector<OpenFileInfo> files;
};

} // namespace duckdb
