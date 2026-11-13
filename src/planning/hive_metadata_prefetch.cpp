#include "planning/hive_metadata_prefetch.hpp"

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "planning/hive_multi_file_reader.hpp"
#include "planning/hive_query_cache.hpp"
#include "planning/stats/hive_stats.hpp"

namespace duckdb {

static constexpr const char *HIVE_METADATA_PREFETCH = "glue_hive_metadata_prefetch";

namespace {

//! The prefetch tasks of the running statement, stopped when it ends: they use its transaction
class HiveMetadataPrefetchState : public ClientContextState {
public:
	TaskExecutor &Executor(ClientContext &context) {
		if (!executor) {
			executor = make_uniq<TaskExecutor>(context, TaskSchedulerType::ASYNC);
		}
		return *executor;
	}
	bool Stopping() const {
		return stopping;
	}

	void StatementEnd(ClientContext &context) override {
		if (!executor) {
			return;
		}
		stopping = true;
		executor->CancelAndDrain();
		executor.reset();
		stopping = false;
	}

private:
	unique_ptr<TaskExecutor> executor;
	atomic<bool> stopping {false};
};

//! Fetches a table's partitions and then, with 'sample', measures its sample over every partition
class PrefetchTableTask : public BaseExecutorTask {
public:
	PrefetchTableTask(TaskExecutor &executor, ClientContext &context, unique_ptr<FunctionData> bind_data_p, bool sample)
	    : BaseExecutorTask(executor), context(context), bind_data(std::move(bind_data_p)), sample(sample) {
	}

	void ExecuteTask() override {
		// freed when the task ends: the task itself may be destroyed after the statement ended
		auto owned_bind_data = std::move(bind_data);
		auto &data = owned_bind_data->Cast<MultiFileBindData>();
		data.file_list->Cast<HiveMultiFileList>().ScanInfo().Partitions(context);
		if (sample && !executor.IsCancelled()) {
			PrefetchTableSample(context, data);
		}
	}
	string TaskType() const override {
		return "PrefetchTableTask";
	}

private:
	ClientContext &context;
	unique_ptr<FunctionData> bind_data;
	bool sample;
};

//! A Glue table of the plan with a copy of one scan's bind data, as the optimizer rewrites the plan meanwhile
struct PrefetchTable {
	unique_ptr<FunctionData> bind_data;
	//! A filter may prune a scan of the table to some partitions, which the scan then lists and measures itself
	bool filtered = false;
};

struct PrefetchPlan {
	unordered_map<string, idx_t> table_by_key;
	vector<PrefetchTable> tables;
	//! The partition key columns the scans produce, and the table of each
	column_binding_map_t<idx_t> partition_columns;
	//! The columns of each CTE, which its CTE scans produce
	unordered_map<TableIndex, vector<ColumnBinding>> cte_columns;
};

//! 'to' produces the partition key 'from' reads, if 'from' reads one
void PassOnPartitionColumn(PrefetchPlan &plan, const ColumnBinding &from, const ColumnBinding &to) {
	auto partition_column = plan.partition_columns.find(from);
	if (partition_column != plan.partition_columns.end()) {
		auto table = partition_column->second;
		plan.partition_columns[to] = table;
	}
}

void CollectHiveScans(LogicalOperator &op, PrefetchPlan &plan) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		plan.cte_columns[op.Cast<LogicalMaterializedCTE>().table_index] = op.children[0]->GetColumnBindings();
	}
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto info = HiveScanInfo::TryGet(get);
		// Glue tables only: a hive_scan of a location has its partitions already, and a table counted from its footers
		// is unpartitioned and not sampled
		if (info && !info->catalog_name.empty() &&
		    !HiveScanCountedFromFooters(get.bind_data->Cast<MultiFileBindData>())) {
			auto key = HiveQueryCache::TableKey(info->catalog_name, info->database_name, info->table_name);
			auto entry = plan.table_by_key.emplace(key, plan.tables.size());
			if (entry.second) {
				plan.tables.emplace_back();
				plan.tables.back().bind_data = get.bind_data->Copy();
			}
			auto &column_ids = get.GetColumnIds();
			for (idx_t i = 0; i < column_ids.size(); i++) {
				if (column_ids[i].IsRowIdColumn() || column_ids[i].GetPrimaryIndex() >= get.names.size()) {
					continue;
				}
				auto &name = get.names[column_ids[i].GetPrimaryIndex()].GetIdentifierName();
				if (info->GetPartitionKeyIndex(name) != DConstants::INVALID_INDEX) {
					plan.partition_columns[ColumnBinding(get.table_index, ProjectionIndex(i))] = entry.first->second;
				}
			}
		}
	}
	for (auto &child : op.children) {
		CollectHiveScans(*child, plan);
	}
	// a filter above a subquery or view reads the partition key through a projection, above a CTE through a CTE scan
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			auto &expression = *projection.expressions[i];
			if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
				PassOnPartitionColumn(plan, expression.Cast<BoundColumnRefExpression>().Binding(),
				                      ColumnBinding(projection.table_index, ProjectionIndex(i)));
			}
		}
	} else if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cte_scan = op.Cast<LogicalCTERef>();
		auto cte = plan.cte_columns.find(cte_scan.cte_index);
		for (idx_t i = 0; cte != plan.cte_columns.end() && i < cte->second.size(); i++) {
			PassOnPartitionColumn(plan, cte->second[i], ColumnBinding(cte_scan.table_index, ProjectionIndex(i)));
		}
	}
}

//! A conjunct over the columns of one scan that reads a partition key may prune the table's partitions
void MarkPartitionFilter(const Expression &expression, PrefetchPlan &plan) {
	if (expression.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		for (auto &child : expression.Cast<BoundConjunctionExpression>().GetChildren()) {
			MarkPartitionFilter(*child, plan);
		}
		return;
	}
	vector<ColumnBinding> columns;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expression, [&](const BoundColumnRefExpression &column) { columns.push_back(column.Binding()); });
	optional_idx table;
	for (auto &column : columns) {
		if (column.table_index != columns[0].table_index) {
			return;
		}
		auto partition_column = plan.partition_columns.find(column);
		if (partition_column != plan.partition_columns.end()) {
			table = partition_column->second;
		}
	}
	if (table.IsValid()) {
		plan.tables[table.GetIndex()].filtered = true;
	}
}

void MarkPartitionFilters(LogicalOperator &op, PrefetchPlan &plan) {
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		for (auto &expression : op.expressions) {
			MarkPartitionFilter(*expression, plan);
		}
	}
	for (auto &child : op.children) {
		MarkPartitionFilters(*child, plan);
	}
}

} // namespace

HiveMetadataPrefetch::HiveMetadataPrefetch() {
	pre_optimize_function = PreOptimize;
}

bool HiveMetadataPrefetch::Stopping(ClientContext &context) {
	auto state = context.registered_state->Get<HiveMetadataPrefetchState>(HIVE_METADATA_PREFETCH);
	return state && state->Stopping();
}

void HiveMetadataPrefetch::PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto &context = input.context;
	Value enabled;
	if (context.TryGetCurrentSetting("glue_prefetch_scan_metadata", enabled) && !enabled.IsNull() &&
	    !enabled.GetValue<bool>()) {
		return;
	}
	// without async threads nothing would run the tasks before the statement ends
	if (TaskScheduler::GetScheduler(context).NumberOfAsyncThreads() == 0) {
		return;
	}
	// the tasks are stopped when the statement ends; a plan made outside of one (ExtractPlan) has none
	if (!context.transaction.HasActiveTransaction() || context.transaction.GetActiveQuery() == MAXIMUM_QUERY_ID) {
		return;
	}
	PrefetchPlan prefetch;
	CollectHiveScans(*plan, prefetch);
	if (prefetch.tables.empty()) {
		return;
	}
	MarkPartitionFilters(*plan, prefetch);
	auto &executor =
	    context.registered_state->GetOrCreate<HiveMetadataPrefetchState>(HIVE_METADATA_PREFETCH)->Executor(context);
	vector<unique_ptr<BaseExecutorTask>> tasks;
	for (auto &table : prefetch.tables) {
		tasks.push_back(make_uniq<PrefetchTableTask>(executor, context, std::move(table.bind_data), !table.filtered));
	}
	// not waited for: a scan that gets the cache entry's lock first fetches for itself, and one that needs the metadata
	// while its task runs waits on that lock
	executor.ScheduleTasks(std::move(tasks));
}

} // namespace duckdb
