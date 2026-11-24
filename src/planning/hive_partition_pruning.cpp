#include "planning/hive_partition_pruning.hpp"

#include "core/glue_info.hpp"
#include "core/glue_types.hpp"
#include "planning/hive_multi_file_reader.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

// PreparePartitionFilter, LoadPartitionColumn, EvaluatePartitionFilterChunk and EvaluatePartitionFilter mirror the
// helpers of the same names in core's duckdb/src/common/hive_partitioning.cpp, which are not exported: keep them in
// sync. They differ in where the values come from (Glue partitions, not file paths), Glue's NULL values, and a single
// partition the filter throws for, which is decided by EvaluatePartitionFilterAlone.

enum class HivePartitionPruner::PartitionFilterResult : uint8_t { NOT_EVALUATED, KEEP_FILE, PRUNE_FILE };

//! A filter the partition values decide, rewritten as core's hive partitioning does: every partition column it reads
//! is a VARCHAR column of a chunk, cast to the column's type inside the filter
struct HivePartitionPruner::PreparedPartitionFilter {
	//! The index of the filter in the pushed-down filters
	idx_t filter_index;
	unique_ptr<Expression> expr;
	//! For each chunk column: the partition key it holds, and the type the filter reads it as
	vector<idx_t> key_indexes;
	vector<LogicalType> types;

	static bool EvaluatePartitionFilterChunk(ExpressionExecutor &executor, DataChunk &chunk,
	                                         array_ptr<const idx_t> rows, vector<PartitionFilterResult> &results);
};

HivePartitionPruner::HivePartitionPruner(ClientContext &context, const HiveScanInfo &info,
                                         const vector<GluePartitionInfo> &partitions, TableIndex table_index,
                                         const vector<PartitionKeyProjection> &projections)
    : context(context), info(info), partitions(partitions), table_index(table_index), projections(projections) {
}

//! Replace references to partition columns of the scanned table by the expression 'replacement' gives for them
void HivePartitionPruner::ReplacePartitionColumnRefs(unique_ptr<Expression> &expr,
                                                     const PartitionColumnReplacement &replacement) const {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &ref) {
		    if (colref.Binding().table_index != table_index) {
			    return;
		    }
		    auto column_index = colref.Binding().column_index.GetIndex();
		    for (auto &projection : projections) {
			    if (projection.projected_column_index == column_index) {
				    ref = replacement(projection, colref);
				    return;
			    }
		    }
	    });
}

//! Whether the partition values alone can decide 'filter': with its partition columns replaced, it folds to a
//! constant. That depends on the shape of the filter, not on the values, so it is asked once per filter.
bool HivePartitionPruner::DecidableByPartitionValues(const Expression &filter) const {
	auto probe = filter.Copy();
	ReplacePartitionColumnRefs(probe, [](const PartitionKeyProjection &, const BoundColumnRefExpression &colref) {
		return make_uniq<BoundConstantExpression>(Value(colref.GetReturnType()));
	});
	return probe->IsScalar() && probe->IsFoldable();
}

//! Mirrors core's PreparePartitionFilter
HivePartitionPruner::PreparedPartitionFilter HivePartitionPruner::PreparePartitionFilter(const Expression &filter,
                                                                                         idx_t filter_index) const {
	PreparedPartitionFilter result;
	result.filter_index = filter_index;
	result.expr = filter.Copy();
	ReplacePartitionColumnRefs(
	    result.expr, [&](const PartitionKeyProjection &projection, const BoundColumnRefExpression &colref) {
		    idx_t slot;
		    for (slot = 0; slot < result.key_indexes.size(); slot++) {
			    if (result.key_indexes[slot] == projection.partition_key_index) {
				    break;
			    }
		    }
		    if (slot == result.key_indexes.size()) {
			    result.key_indexes.push_back(projection.partition_key_index);
			    result.types.push_back(colref.GetReturnType());
		    }
		    // the partition values are loaded as strings and cast as part of the filter
		    auto reference = make_uniq<BoundReferenceExpression>(LogicalType::VARCHAR, NumericCast<storage_t>(slot));
		    return BoundCastExpression::AddCastToType(context, std::move(reference), colref.GetReturnType());
	    });
	return result;
}

//! Loads the values Glue stores for a partition key of the given partitions into the result vector: verbatim, and NULL
//! where GlueTypes::PartitionValue reads NULL. Mirrors core's LoadPartitionColumn.
static void LoadPartitionColumn(idx_t key_index, const LogicalType &type, const vector<GluePartitionInfo> &partitions,
                                const vector<idx_t> &partition_indexes, array_ptr<const idx_t> rows, Vector &result) {
	auto count = rows.size();
	Vector strings(LogicalType::VARCHAR, count);
	{
		auto writer = FlatVector::Writer<string_t>(strings, count);
		for (auto &row : rows) {
			auto &value = partitions[partition_indexes[row]].values[key_index];
			if (GlueTypes::PartitionValueIsNull(value, type)) {
				writer.WriteNull();
				continue;
			}
			writer.WriteValue(string_t(value));
		}
	}
	result.Reference(strings);
}

//! Evaluates the filter over a chunk of partitions - returns false (leaving the partitions unevaluated) if it throws.
//! Mirrors core's EvaluatePartitionFilterChunk, and also rethrows an interrupt.
bool HivePartitionPruner::PreparedPartitionFilter::EvaluatePartitionFilterChunk(
    ExpressionExecutor &executor, DataChunk &chunk, array_ptr<const idx_t> rows,
    vector<PartitionFilterResult> &results) {
	SelectionVector true_sel(chunk.size());
	idx_t true_count;
	try {
		true_count = executor.SelectExpression(chunk, true_sel);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() == ExceptionType::INTERNAL || error.Type() == ExceptionType::INTERRUPT) {
			throw;
		}
		return false;
	}
	for (auto &row : rows) {
		results[row] = PartitionFilterResult::PRUNE_FILE;
	}
	for (idx_t i = 0; i < true_count; i++) {
		results[rows[true_sel.get_index(i)]] = PartitionFilterResult::KEEP_FILE;
	}
	return true;
}

//! The filter for one partition, with the partition's values folded into a copy of it: what pruning did for every
//! partition before batching. A value its column's type can not hold fails the query here; a filter that can not be
//! folded for the partition is left to the scan.
HivePartitionPruner::PartitionFilterResult
HivePartitionPruner::EvaluatePartitionFilterAlone(const Expression &filter, idx_t partition_index) const {
	auto &partition = partitions[partition_index];
	auto copy = filter.Copy();
	ReplacePartitionColumnRefs(copy,
	                           [&](const PartitionKeyProjection &projection, const BoundColumnRefExpression &colref) {
		                           auto &key = info.partition_keys[projection.partition_key_index];
		                           auto &value = partition.values[projection.partition_key_index];
		                           return make_uniq<BoundConstantExpression>(
		                               GlueTypes::PartitionValue(context, key, value, colref.GetReturnType()));
	                           });
	Value result;
	if (!ExpressionExecutor::TryEvaluateScalar(context, *copy, result)) {
		return PartitionFilterResult::NOT_EVALUATED;
	}
	return result.IsNull() || !result.GetValue<bool>() ? PartitionFilterResult::PRUNE_FILE
	                                                   : PartitionFilterResult::KEEP_FILE;
}

//! The filter for the partitions 'rows' (positions in 'partition_indexes'), loaded into 'chunk'. If it throws, they are
//! split in halves so the other partitions can still be pruned, and a single partition is decided alone.
void HivePartitionPruner::EvaluatePartitions(const PreparedPartitionFilter &prepared, const Expression &filter,
                                             const vector<idx_t> &partition_indexes, ExpressionExecutor &executor,
                                             DataChunk &chunk, array_ptr<const idx_t> rows,
                                             vector<PartitionFilterResult> &results) const {
	for (idx_t column = 0; column < prepared.key_indexes.size(); column++) {
		LoadPartitionColumn(prepared.key_indexes[column], prepared.types[column], partitions, partition_indexes, rows,
		                    chunk.data[column]);
	}
	chunk.SetChildCardinality(rows.size());
	if (PreparedPartitionFilter::EvaluatePartitionFilterChunk(executor, chunk, rows, results)) {
		return;
	}
	if (rows.size() == 1) {
		results[rows[0]] = EvaluatePartitionFilterAlone(filter, partition_indexes[rows[0]]);
		return;
	}
	auto half = rows.size() / 2;
	EvaluatePartitions(prepared, filter, partition_indexes, executor, chunk, array_ptr<const idx_t>(rows.data(), half),
	                   results);
	EvaluatePartitions(prepared, filter, partition_indexes, executor, chunk,
	                   array_ptr<const idx_t>(rows.data() + half, rows.size() - half), results);
}

//! The filter for each of the partitions 'partition_indexes', a chunk at a time. Mirrors core's
//! EvaluatePartitionFilter.
vector<HivePartitionPruner::PartitionFilterResult>
HivePartitionPruner::EvaluatePartitionFilter(const PreparedPartitionFilter &prepared, const Expression &filter,
                                             const vector<idx_t> &partition_indexes) const {
	vector<PartitionFilterResult> results(partition_indexes.size(), PartitionFilterResult::NOT_EVALUATED);
	vector<idx_t> rows(partition_indexes.size());
	for (idx_t row = 0; row < rows.size(); row++) {
		rows[row] = row;
	}
	ExpressionExecutor executor(context, *prepared.expr);
	DataChunk chunk;
	chunk.InitializeEmpty(vector<LogicalType>(prepared.key_indexes.size(), LogicalType::VARCHAR));
	for (idx_t offset = 0; offset < rows.size(); offset += STANDARD_VECTOR_SIZE) {
		auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, rows.size() - offset);
		EvaluatePartitions(prepared, filter, partition_indexes, executor, chunk,
		                   array_ptr<const idx_t>(rows.data() + offset, count), results);
	}
	return results;
}

vector<idx_t> HivePartitionPruner::PartitionsToRead(const vector<idx_t> &candidates,
                                                    const vector<unique_ptr<Expression>> &filters,
                                                    unordered_set<idx_t> &pruning_filters,
                                                    unordered_set<idx_t> &decided_filters) const {
	vector<PreparedPartitionFilter> decidable_filters;
	for (idx_t filter_index = 0; filter_index < filters.size(); filter_index++) {
		if (DecidableByPartitionValues(*filters[filter_index])) {
			decidable_filters.push_back(PreparePartitionFilter(*filters[filter_index], filter_index));
		}
	}
	if (decidable_filters.empty()) {
		return candidates;
	}
	// The filters in order, each over the partitions the ones before it kept: the first that is FALSE (or NULL) for a
	// partition prunes it and is the one reported, and a filter that could not be evaluated for a partition leaves it
	// to the scan
	auto partitions_to_read = candidates;
	// per decidable filter, the partitions it was left to the scan for
	vector<vector<idx_t>> not_evaluated(decidable_filters.size());
	for (idx_t i = 0; i < decidable_filters.size() && !partitions_to_read.empty(); i++) {
		auto &prepared = decidable_filters[i];
		auto results = EvaluatePartitionFilter(prepared, *filters[prepared.filter_index], partitions_to_read);
		vector<idx_t> still_read;
		for (idx_t row = 0; row < partitions_to_read.size(); row++) {
			switch (results[row]) {
			case PartitionFilterResult::PRUNE_FILE:
				pruning_filters.insert(prepared.filter_index);
				break;
			case PartitionFilterResult::NOT_EVALUATED:
				not_evaluated[i].push_back(partitions_to_read[row]);
				still_read.push_back(partitions_to_read[row]);
				break;
			case PartitionFilterResult::KEEP_FILE:
				still_read.push_back(partitions_to_read[row]);
				break;
			}
		}
		partitions_to_read = std::move(still_read);
	}
	if (partitions_to_read.empty()) {
		return partitions_to_read;
	}
	// a filter is true of every partition read unless it was left to the scan for one of them
	unordered_set<idx_t> read(partitions_to_read.begin(), partitions_to_read.end());
	for (idx_t i = 0; i < decidable_filters.size(); i++) {
		bool true_for_all_read = true;
		for (auto partition_index : not_evaluated[i]) {
			if (read.count(partition_index)) {
				true_for_all_read = false;
				break;
			}
		}
		if (true_for_all_read) {
			decided_filters.insert(decidable_filters[i].filter_index);
		}
	}
	return partitions_to_read;
}

} // namespace duckdb
