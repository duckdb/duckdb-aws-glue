#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "core/glue_info.hpp"

namespace duckdb {
class LogicalInsert;
class PhysicalOperator;
class PhysicalPlanGenerator;

//! Glue metadata, with local scan and write support for Hive tables.
class GlueTable : public TableCatalogEntry {
public:
	GlueTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, GlueTableInfo table_info);

public:
	const ColumnList &GetColumns() const override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan);
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;

	//! Called by duckdb_tables().sql / ToSQL(). On a Hive table we can round-trip, the sql column
	//! renders as a full CREATE TABLE ... WITH (...). Otherwise it renders the "Not supported:
	//! ..." reason inline, so a bulk catalog scan never breaks on a single bad table.
	unique_ptr<CreateInfo> GetInfo() const override;

public:
	//! The table definition as returned by Glue when the entry was created
	GlueTableInfo table_info;

private:
	TableFunction GetHiveScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data);

private:
	ColumnList columns;
};

} // namespace duckdb
