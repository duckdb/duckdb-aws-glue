#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "core/glue_info.hpp"

namespace duckdb {
class GlueCatalog;
class GlueSchemaEntry;
class GlueTable;

//! Writes rows into a Hive table registered in Glue: a COPY into the table location (one <key=value> directory level
//! per partition key, or the location of an existing partition), then the new partition directories are registered in
//! Glue.
class GlueHiveInsert : public PhysicalOperator {
public:
	GlueHiveInsert(PhysicalPlan &physical_plan, LogicalOperator &op, GlueCatalog &catalog, GlueTableInfo table_info);

	//! INSERT INTO <hive table>
	static PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                                    GlueTable &table, const GlueTableInfo &table_info,
	                                    optional_ptr<PhysicalOperator> plan);
	//! CREATE TABLE <hive table> AS <query>: the copy creates the table in Glue when it starts, then writes the result
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
	//! Plan the COPY child of 'insert': INSERT passes the existing table (whose partitions may live at their own
	//! locations), CTAS the schema and the bound statement of the table the copy creates when it starts
	static void PlanWrite(ClientContext &context, PhysicalPlanGenerator &planner, LogicalOperator &op,
	                      GlueHiveInsert &insert, optional_ptr<GlueTable> existing_table,
	                      const GlueTableInfo &table_info, PhysicalOperator &plan, const vector<Identifier> &names,
	                      const vector<LogicalType> &types, optional_ptr<GlueSchemaEntry> create_schema,
	                      unique_ptr<BoundCreateTableInfo> create_info);

public:
	//! The target catalog (for partition registration after the file write).
	GlueCatalog &catalog;
	//! The one resolved table definition used by both CTAS catalog creation and file-writer planning.
	GlueTableInfo table_info;
	//! The directories of existing partitions not laid out as <key=value>, with their Glue values.
	unordered_map<string, vector<string>> partition_directories;
};

} // namespace duckdb
