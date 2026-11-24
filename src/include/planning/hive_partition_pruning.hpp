#pragma once

#include "duckdb/common/array_ptr.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/expression.hpp"

#include <functional>

namespace duckdb {
class BoundColumnRefExpression;
class ClientContext;
class DataChunk;
class ExpressionExecutor;
struct GluePartitionInfo;
struct HiveScanInfo;

//! A projected column of a Hive scan that is a partition key
struct PartitionKeyProjection {
	idx_t projected_column_index;
	idx_t partition_key_index;
};

//! Decides which Glue partitions of a Hive scan are read, from the filters the partition values alone decide. The
//! filters are evaluated over chunks of partition values, as core's hive partitioning does for files.
class HivePartitionPruner {
public:
	HivePartitionPruner(ClientContext &context, const HiveScanInfo &info, const vector<GluePartitionInfo> &partitions,
	                    TableIndex table_index, const vector<PartitionKeyProjection> &projections);

	//! The partitions among 'candidates' (indexes into the partitions) that no filter rules out. 'pruning_filters' are
	//! the filters that ruled a partition out, 'decided_filters' those true of every partition kept.
	vector<idx_t> PartitionsToRead(const vector<idx_t> &candidates, const vector<unique_ptr<Expression>> &filters,
	                               unordered_set<idx_t> &pruning_filters, unordered_set<idx_t> &decided_filters) const;

private:
	//! What a filter says about one partition, as core's hive partitioning names it for one file
	enum class PartitionFilterResult : uint8_t;
	struct PreparedPartitionFilter;
	//! The expression a partition column reference is replaced by
	using PartitionColumnReplacement =
	    std::function<unique_ptr<Expression>(const PartitionKeyProjection &, const BoundColumnRefExpression &)>;

	void ReplacePartitionColumnRefs(unique_ptr<Expression> &expr, const PartitionColumnReplacement &replacement) const;
	bool DecidableByPartitionValues(const Expression &filter) const;
	PreparedPartitionFilter PreparePartitionFilter(const Expression &filter, idx_t filter_index) const;
	PartitionFilterResult EvaluatePartitionFilterAlone(const Expression &filter, idx_t partition_index) const;
	vector<PartitionFilterResult> EvaluatePartitionFilter(const PreparedPartitionFilter &prepared,
	                                                      const Expression &filter,
	                                                      const vector<idx_t> &partition_indexes) const;
	void EvaluatePartitions(const PreparedPartitionFilter &prepared, const Expression &filter,
	                        const vector<idx_t> &partition_indexes, ExpressionExecutor &executor, DataChunk &chunk,
	                        array_ptr<const idx_t> rows, vector<PartitionFilterResult> &results) const;

	ClientContext &context;
	const HiveScanInfo &info;
	const vector<GluePartitionInfo> &partitions;
	TableIndex table_index;
	const vector<PartitionKeyProjection> &projections;
};

} // namespace duckdb
