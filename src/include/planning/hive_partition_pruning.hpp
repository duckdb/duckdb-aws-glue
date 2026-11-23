#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ClientContext;
struct GluePartitionInfo;
struct HiveScanInfo;

//! A projected column of a Hive scan that is a partition key
struct PartitionKeyProjection {
	idx_t projected_column_index;
	idx_t partition_key_index;
};

//! What a filter says about one partition, as core's hive partitioning names it for one file
enum class PartitionFilterResult : uint8_t { NOT_EVALUATED, KEEP_FILE, PRUNE_FILE };

//! The partitions among 'candidates' (indexes into 'partitions') that no filter rules out, the filters the partition
//! values alone decide evaluated over chunks of partition values. 'pruning_filters' are the filters that ruled a
//! partition out, 'decided_filters' those true of every partition kept.
vector<idx_t> PartitionsToRead(ClientContext &context, const HiveScanInfo &info,
                               const vector<GluePartitionInfo> &partitions, const vector<idx_t> &candidates,
                               TableIndex table_index, const vector<PartitionKeyProjection> &projections,
                               const vector<unique_ptr<Expression>> &filters, unordered_set<idx_t> &pruning_filters,
                               unordered_set<idx_t> &decided_filters);

} // namespace duckdb
