#pragma once

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb {

class BaseStatistics;
class BaseUnionData;
struct HiveScanInfo;

//! The default and the largest value of hive_footer_statistics_max_files
constexpr idx_t HIVE_FOOTER_STATISTICS_DEFAULT_MAX_FILES = 16;
constexpr idx_t HIVE_FOOTER_STATISTICS_MAX_FILES_LIMIT = 25;
//! hive_footer_statistics_max_files, capped at its limit
idx_t HiveFooterStatisticsMaxFiles(ClientContext &context);
//! The footers read for the statistics of the table 'info' scans in this query; empty when none were read
vector<shared_ptr<BaseUnionData>> HiveCachedFooters(ClientContext &context, const HiveScanInfo &info);

//! Cardinality of a Hive scan: the files listed for the table, measured once per query, scaled to the partitions read
unique_ptr<NodeStatistics> HiveScanCardinality(ClientContext &context, const FunctionData *bind_data_p);
//! Partition columns are answered from the partition values, every other column by the format's own function
unique_ptr<BaseStatistics> HivePartitionStatistics(ClientContext &context, TableFunctionGetStatisticsInput &input);

} // namespace duckdb
