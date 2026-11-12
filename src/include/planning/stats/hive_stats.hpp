#pragma once

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb {

class BaseStatistics;
struct MultiFileBindData;

//! The default and the largest value of hive_footer_statistics_max_files
constexpr idx_t HIVE_FOOTER_STATISTICS_DEFAULT_MAX_FILES = 16;
constexpr idx_t HIVE_FOOTER_STATISTICS_MAX_FILES_LIMIT = 25;
//! hive_footer_statistics_max_files, capped at its limit: the reader's maximum_files_for_stats
idx_t HiveFooterStatisticsMaxFiles(ClientContext &context);

//! Cardinality of a Hive scan: the files listed for the table, measured once per query, scaled to the partitions read
unique_ptr<NodeStatistics> HiveScanCardinality(ClientContext &context, const FunctionData *bind_data_p);
//! Whether the scan is counted from the footers its reader read at bind, so the table is not sampled
bool HiveScanCountedFromFooters(const MultiFileBindData &bind_data);
//! Measure the sample of the table 'bind_data' scans over every partition, before the optimizer asks for it
void PrefetchTableSample(ClientContext &context, const MultiFileBindData &bind_data);
//! Partition columns are answered from the partition values, every other column by the format's own function
unique_ptr<BaseStatistics> HivePartitionStatistics(ClientContext &context, TableFunctionGetStatisticsInput &input);

} // namespace duckdb
