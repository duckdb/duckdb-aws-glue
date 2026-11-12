#pragma once

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb {

//! The size the listing reported for a file, when it reported one. S3 and the local filesystem both carry it, so the
//! bytes of a table are known from the listing alone, without opening anything
optional_idx ListedFileSize(const OpenFileInfo &file);

class BaseStatistics;

//! Cardinality of a Hive scan: the files listed for the table, measured once per query, scaled to the partitions read
unique_ptr<NodeStatistics> HiveScanCardinality(ClientContext &context, const FunctionData *bind_data_p);
//! Partition columns are answered from the partition values, every other column by the format's own function
unique_ptr<BaseStatistics> HivePartitionStatistics(ClientContext &context, TableFunctionGetStatisticsInput &input);

} // namespace duckdb
