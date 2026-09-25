#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

namespace duckdb {
class GlueTable;

//! Writes rows into a Hive table registered in Glue: a COPY into the table location (one <key=value> directory level
//! per partition key, or the location of an existing partition), then the new partition directories are registered in
//! Glue.
//! Statistics per partition are also written, but only for HIVE tables that have the parquet format
class GlueHiveInsert : public PhysicalOperator {
public:
	GlueHiveInsert(PhysicalPlan &physical_plan, LogicalOperator &op, GlueTable &table, bool discard);

	//! INSERT INTO <hive table>
	static PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                                    GlueTable &table, optional_ptr<PhysicalOperator> plan);
	//! CREATE TABLE <hive table> AS <query>: creates the table in Glue, then inserts the query result
	static PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
	                                           LogicalCreateTable &op, PhysicalOperator &plan);

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	static PhysicalOperator &PlanWrite(ClientContext &context, PhysicalPlanGenerator &planner, LogicalOperator &op,
	                                   GlueTable &table, PhysicalOperator &plan, const vector<Identifier> &names,
	                                   const vector<LogicalType> &types);

public:
	//! The table written to
	GlueTable &table;
	//! CREATE TABLE IF NOT EXISTS ... AS on an existing table: consume the input and write nothing
	bool discard;
	//! Record the numRows, numFiles and totalSize of the written files in Glue (parquet only)
	bool collect_statistics = false;
	//! The directories of the existing partitions not laid out as <key>=<value>, with their Glue values
	unordered_map<string, vector<string>> partition_directories;
};

} // namespace duckdb
