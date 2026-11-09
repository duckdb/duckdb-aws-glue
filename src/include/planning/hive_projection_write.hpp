#pragma once

#include "core/partition_projection.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! The PARTITION_PATH of a write into a projected table: the partition directory of a row's partition values, relative
//! to writer->CopyTarget(). 'types' are the copy's column types and 'partition_columns' the partition keys' positions
//! in them, in key order
unique_ptr<Expression> ProjectedPartitionPath(shared_ptr<PartitionProjectionWriter> writer,
                                              const vector<LogicalType> &types, const vector<idx_t> &partition_columns);

} // namespace duckdb
