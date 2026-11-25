#pragma once

#include "duckdb/common/map.hpp"
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
struct GlueHiveInsertGlobalState;

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
	//! Adds the files of a write whose partitions are registered to the glue_written_files temporary table
	void RecordWrittenFiles(ClientContext &context, const vector<string> &files) const;
	//! Throws when a partition registration skipped as existing is registered at another location than its files
	void CheckExistingPartitionLocations(ClientContext &context,
	                                     const vector<GluePartitionInput> &already_existing) const;
	//! Adds the statistics of the written files to the table's. A sum past 64 bits, or a failure, removes them
	//! instead: the rows are in the table by now, so a failure is logged.
	void AddTableStatistics(ClientContext &context, const vector<GlueBasicStatistics> &files) const;
	//! Records the statistics of the files written to each of the 'registered' partitions, which were created unless
	//! 'already_existing' (GlueAPI::AddPartitionStatistics). A failure is logged, and removes them from those
	//! partitions.
	void AddPartitionStatistics(ClientContext &context, const GlueHiveInsertGlobalState &state,
	                            const vector<GluePartitionInput> &registered,
	                            const vector<GluePartitionInput> &already_existing) const;
	//! An INSERT's created partitions may lie in a directory a dropped partition left its files in: lists each
	//! (up to max_partition_listings) and unsets the statistics of those holding other data files, or of all of them
	//! when there are more. 'written' are the files written, by directory.
	void CheckNewPartitionDirectories(ClientContext &context, vector<GluePartitionStatistics> &partitions,
	                                  const unordered_map<string, vector<string>> &written) const;
	//! Removes numRows, numFiles and totalSize from the table, or from 'partitions'; a failure is logged
	void RemoveStatistics(ClientContext &context, const vector<vector<string>> &partitions) const;

public:
	//! The target catalog (for partition registration after the file write).
	GlueCatalog &catalog;
	//! The one resolved table definition used by both CTAS catalog creation and file-writer planning.
	GlueTableInfo table_info;
	//! Where each partition that existed when the INSERT was planned is registered, by its Glue values.
	map<vector<string>, string> planned_locations;
	//! The directories of existing partitions not laid out as <key=value>, with their Glue values.
	unordered_map<string, vector<string>> partition_directories;
	//! Names the files of this write, and its entry in the Glue transaction
	string write_id;
	//! Whether the copy reports the rows and size of each file it wrote (parquet), kept as numRows, numFiles and
	//! totalSize
	bool collect_statistics = false;
	//! CREATE TABLE AS, whose table and partitions start out without data files
	bool is_ctas = false;
	//! glue_write_statistics_max_listings when planned
	idx_t max_partition_listings = 100;
	//! With collect_statistics: the columns of the copy's filename, count and file_size_bytes
	idx_t filename_column = 0;
	idx_t count_column = 0;
	idx_t file_size_column = 0;
};

} // namespace duckdb
