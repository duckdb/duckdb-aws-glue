#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "core/glue_info.hpp"

namespace duckdb {
class GlueCatalog;

//! A table registered in the Glue Data Catalog. Only Hive (Glue native) tables can be scanned and written; tables
//! of other formats (Iceberg, Delta, ...) are listed with the columns Glue reports but can not be read.
class GlueTable : public TableCatalogEntry {
public:
	GlueTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, GlueTableInfo table_info);

public:
	const ColumnList &GetColumns() const override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	//! filename: the location of the data file a row was read from, as for read_parquet and hive_scan, and the
	//! metadata columns of Trino's Hive connector ("$path", "$file_size", ...)
	virtual_column_map_t GetVirtualColumns() const override;
	//! None: the files of a Hive table have no row identifier
	vector<column_t> GetRowIdColumns() const override;

	//! Called by duckdb_tables().sql / ToSQL(). On a Hive table we can round-trip, the sql column
	//! renders as a full CREATE TABLE ... WITH (...). Otherwise it renders the "Not supported:
	//! ..." reason inline, so a bulk catalog scan never breaks on a single bad table.
	unique_ptr<CreateInfo> GetInfo() const override;

private:
	//! Scan a Hive table with read_parquet over the files of the partitions Glue lists (or the table location for an
	//! unpartitioned table), using the HiveMultiFileReader for Glue's schema and partition values
	TableFunction GetHiveScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data);

public:
	//! The table definition as returned by Glue when the entry was created
	GlueTableInfo table_info;

private:
	//! The columns of the table: data columns first, partition keys last
	ColumnList columns;
};

} // namespace duckdb
