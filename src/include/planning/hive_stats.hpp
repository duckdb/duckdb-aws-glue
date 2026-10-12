#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb {
class BaseStatistics;
struct GluePartitionInfo;
struct HiveScanInfo;

//! The recursive listing of 'directory' in this query, fetched page by page as it is read: every scan and the sample
//! share it, so a directory is listed once per query
shared_ptr<MultiFileList> GetDirectoryListing(ClientContext &context, const string &directory);
//! The partitions Glue has for the table of 'info'. Within a query, scans of the table that use the same 'expression'
//! share one fetch. An empty 'expression' means all partitions.
shared_ptr<const vector<GluePartitionInfo>> GetTablePartitions(ClientContext &context, const HiveScanInfo &info,
                                                               const string &expression = string());

//! Cardinality of a Hive scan: one directory of the table, measured once per query, scaled by the partitions read
unique_ptr<NodeStatistics> HiveScanCardinality(ClientContext &context, const FunctionData *bind_data_p);
//! Partition columns are answered from the partition values, every other column by the format's own function
unique_ptr<BaseStatistics> HivePartitionStatistics(ClientContext &context, TableFunctionGetStatisticsInput &input);

} // namespace duckdb
