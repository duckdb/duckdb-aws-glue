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

//! The filters as a GetPartitions Expression matching at least the partitions they keep; empty if none can be written.
//! 'used_filters' gets the indexes of the filters it covers.
string GluePartitionExpression(const HiveScanInfo &info, TableIndex table_index,
                               const vector<PartitionKeyProjection> &projections,
                               const vector<unique_ptr<Expression>> &filters, unordered_set<idx_t> &used_filters);

} // namespace duckdb
