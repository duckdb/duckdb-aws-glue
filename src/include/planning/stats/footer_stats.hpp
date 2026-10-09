#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/open_file_info.hpp"

namespace duckdb {
class ClientContext;
struct HiveScanInfo;

//! The most files of an unpartitioned table whose footers are all read
static constexpr idx_t FOOTER_STATISTICS_MAX_FILES = 32;

//! What the parquet footers of all the files of a table say: its rows, and the distinct counts of the columns they
//! bound (the dictionary size the writer recorded, or the width of an integer column's min/max)
struct HiveFooterStatistics {
	idx_t rows = 0;
	case_insensitive_map_t<idx_t> distinct_counts;
};

//! Read the footers of 'files' with parquet_metadata, a few at a time. False when parquet_metadata is not available.
bool ReadFooterStatistics(ClientContext &context, const HiveScanInfo &info, const vector<OpenFileInfo> &files,
                          HiveFooterStatistics &result);

} // namespace duckdb
