#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

#include "glue_api.hpp"

namespace duckdb {
class GlueTable;

//! What the plan learned about where this table's partitions live, handed to the operator so Finalize does not have
//! to re-derive it from the written paths (and does not have to trust the cached table_info).
struct GlueHiveWriteInfo {
	//! The table as Glue had it at plan time. Finalize must not use GlueTable::table_info, which may be older: the
	//! location or the partition keys can have changed since the catalog entry was built.
	GlueTableInfo table_info;
	//! The locations of the partitions already registered in Glue, trailing '/' trimmed. A file written into one of
	//! these belongs to a partition that already exists, so it must not be registered again -- and its directory is
	//! not a <key>=<value> path, so the partition values cannot be parsed back out of it.
	unordered_set<string> registered_locations;
};

//! Writes rows into a Hive table registered in Glue: a COPY into the table location (by default one <key=value>
//! directory level per partition key, but a partition registered at another location is written there), then the
//! new partition directories are registered in Glue.
class GlueHiveInsert : public PhysicalOperator {
public:
	GlueHiveInsert(PhysicalPlan &physical_plan, LogicalOperator &op, GlueTable &table, bool discard,
	               GlueHiveWriteInfo write_info = GlueHiveWriteInfo());

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
	//! Where this table's partitions live, as of plan time
	GlueHiveWriteInfo write_info;
};

} // namespace duckdb
