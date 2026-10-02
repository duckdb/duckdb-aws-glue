#pragma once

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb {
class BaseStatistics;

//! Keep the listing of 'location' a sample took until the query ends, so a scan of the same directory reuses it
void AddSampledListing(ClientContext &context, const string &location, const vector<OpenFileInfo> &files);
//! The listing of 'location' taken by a sample in this query, appended to 'files'
bool FindSampledListing(ClientContext &context, const string &location, vector<OpenFileInfo> &files);

//! Cardinality of a Hive scan: one directory of the table, measured once per query, scaled by the partitions read
unique_ptr<NodeStatistics> HiveScanCardinality(ClientContext &context, const FunctionData *bind_data_p);
//! Partition columns are answered from the partition values, every other column by the format's own function
unique_ptr<BaseStatistics> HivePartitionStatistics(ClientContext &context, TableFunctionGetStatisticsInput &input);

} // namespace duckdb
