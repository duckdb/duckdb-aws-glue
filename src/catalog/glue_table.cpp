#include "catalog/glue_table.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "functions/glue_functions.hpp"
#include "core/glue_types.hpp"
#include "api/glue_api.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "planning/hive_multi_file_reader.hpp"

namespace duckdb {

GlueTable::GlueTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, GlueTableInfo table_info_p)
    : TableCatalogEntry(catalog, schema, info), table_info(std::move(table_info_p)), columns(info.columns.Copy()) {
	this->internal = false;
}

const ColumnList &GlueTable::GetColumns() const {
	return columns;
}

unique_ptr<BaseStatistics> GlueTable::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableStorageInfo GlueTable::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	return result;
}

virtual_column_map_t GlueTable::GetVirtualColumns() const {
	virtual_column_map_t result;
	result.insert(
	    make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR)));
	HiveMultiFileReader::AddMetadataColumns(result);
	return result;
}

vector<column_t> GlueTable::GetRowIdColumns() const {
	return vector<column_t>();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
TableFunction GlueTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// only Hive (Glue native) tables can be read
	auto format = table_info.GetFormat();
	switch (format.format) {
	case GlueFormat::HIVE:
		if (table_info.IsPartitionProjected()) {
			// Athena derives the partitions from the projection.* parameters and ignores the registered ones, which
			// such a table usually has none of: reading the registered partitions would silently return no or wrong
			// rows
			throw NotImplementedException("Glue table '%s.%s' uses Athena partition projection (projection.enabled), "
			                              "which is not supported yet: Athena derives its partitions from the "
			                              "projection.* parameters and ignores the ones registered in Glue, so a scan "
			                              "of the registered ones would read no rows or the wrong ones. Set "
			                              "projection.enabled to false and register the partitions to read it",
			                              table_info.database_name, table_info.name);
		}
		return GetHiveScanFunction(context, bind_data);
	default:
		throw NotImplementedException("Glue table '%s.%s' has table format %s, which can not be read yet; use "
		                              "glue_get_table_response() to inspect its definition",
		                              table_info.database_name, table_info.name, format.Describe(table_info));
	}
}

//===--------------------------------------------------------------------===//
// Hive scan
//===--------------------------------------------------------------------===//
TableFunction GlueTable::GetHiveScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// The scan produces the columns this entry was planned with: data columns first, partition keys last. The SerDe
	// decides the file format (throws for unsupported SerDes).
	auto scan_info = make_shared_ptr<HiveScanInfo>();
	scan_info->catalog_name = catalog.GetName().GetIdentifierName();
	scan_info->database_name = table_info.database_name;
	scan_info->table_name = table_info.name;
	scan_info->table = this;
	scan_info->root_location = table_info.location;
	scan_info->file_format = table_info.GetFileFormat();
	if (IsTextFileFormat(scan_info->file_format)) {
		table_info.CheckTextSerdeSupported(scan_info->file_format);
	}
	if (scan_info->file_format == HiveFileFormat::CSV) {
		scan_info->csv_options = table_info.GetCSVOptions();
	}
	if (scan_info->file_format == HiveFileFormat::JSON) {
		scan_info->json_options = table_info.GetJSONOptions();
		for (auto &property : scan_info->json_options.unsupported_properties) {
			DUCKDB_LOG_WARNING(context, "Glue table '%s.%s': SerDe property '%s' is not applied when reading",
			                   table_info.database_name, table_info.name, property);
		}
	}
	for (auto &column : GetColumns().Logical()) {
		scan_info->names.push_back(column.Name());
		scan_info->types.push_back(column.Type());
	}
	for (auto &key : table_info.partition_keys) {
		scan_info->partition_keys.push_back(key.name);
	}
	// the partitions are fetched from Glue on first use (HiveScanInfo::Partitions), not at bind
	return BindHiveScan(context, std::move(scan_info), bind_data);
}

} // namespace duckdb
