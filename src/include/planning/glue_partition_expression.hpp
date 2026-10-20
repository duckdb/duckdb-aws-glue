#pragma once

#include "duckdb/common/table_index.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
struct HiveScanInfo;

//! A projected column of a Hive scan that is a partition key
struct PartitionKeyProjection {
	idx_t projected_column_index;
	idx_t partition_key_index;
};

//! The filters of a scan written as the Expression of a GetPartitions request, so that Glue returns only the
//! partitions they match. A filter that cannot be written is left out: the expression keeps at least every partition
//! the filters keep, and the filters are still applied to the partition values Glue returns. Empty when no filter can
//! be written. The indexes of the filters the expression covers go in 'used_filters'.
string GluePartitionExpression(const HiveScanInfo &info, TableIndex table_index,
                               const vector<PartitionKeyProjection> &projections,
                               const vector<unique_ptr<Expression>> &filters, unordered_set<idx_t> &used_filters);

} // namespace duckdb
