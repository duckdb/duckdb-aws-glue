#include "storage/glue_table.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "glue_functions.hpp"
#include "glue_types.hpp"
#include "storage/glue_catalog.hpp"
#include "storage/glue_schema_entry.hpp"
#include "storage/glue_transaction.hpp"
#include "storage/hive_multi_file_reader.hpp"

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

GlueTableInfo GlueTable::RefreshTableInfo(ClientContext &context) const {
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	auto load = [&]() -> shared_ptr<const GlueTableInfo> {
		GlueTableInfo info;
		if (!GlueAPI::GetTable(context, glue_catalog, table_info.database_name, table_info.name, info)) {
			return nullptr;
		}
		return make_shared_ptr<const GlueTableInfo>(std::move(info));
	};
	auto cache = GlueTransactionCache::Of(context, catalog);
	auto result = cache ? cache->GetTable(table_info.database_name, table_info.name, load) : load();
	if (!result) {
		throw CatalogException("Glue table '%s.%s' no longer exists", table_info.database_name, table_info.name);
	}
	return *result;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
TableFunction GlueTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// Ask Glue what kind of table this is right before scanning: only Hive (Glue native) tables can be read
	auto latest_info = RefreshTableInfo(context);
	switch (latest_info.GetFormat()) {
	case GlueTableFormat::HIVE:
		return GetHiveScanFunction(context, bind_data, latest_info);
	default:
		throw NotImplementedException("Scan from table with type %s", latest_info.GetFormatName());
	}
}

//===--------------------------------------------------------------------===//
// Hive scan
//===--------------------------------------------------------------------===//
TableFunction GlueTable::GetHiveScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                             const GlueTableInfo &latest_info) {
	// The scan produces the columns this entry was planned with: data columns first, partition keys last. The SerDe
	// decides the file format (throws for unsupported SerDes).
	auto scan_info = make_shared_ptr<HiveScanInfo>();
	scan_info->database_name = latest_info.database_name;
	scan_info->table_name = latest_info.name;
	scan_info->root_location = latest_info.location;
	scan_info->file_format = latest_info.GetFileFormat();
	scan_info->delimiter = latest_info.GetFieldDelimiter();
	scan_info->quote = latest_info.GetQuoteCharacter();
	scan_info->escape = latest_info.GetEscapeCharacter();
	scan_info->header = latest_info.HasHeader();
	for (auto &column : GetColumns().Logical()) {
		scan_info->names.push_back(column.Name());
		scan_info->types.push_back(column.Type());
	}
	for (auto &key : table_info.partition_keys) {
		scan_info->partition_keys.push_back(key.name);
	}
	// the partitions as registered in Glue, each with its own location
	if (!scan_info->partition_keys.empty()) {
		auto &glue_catalog = catalog.Cast<GlueCatalog>();
		auto load = [&]() {
			return make_shared_ptr<const vector<GluePartitionInfo>>(
			    GlueAPI::GetPartitions(context, glue_catalog, latest_info.database_name, latest_info.name));
		};
		auto cache = GlueTransactionCache::Of(context, catalog);
		scan_info->partitions =
		    *(cache ? cache->GetPartitions(latest_info.database_name, latest_info.name, load) : load());
	}
	return BindHiveScan(context, std::move(scan_info), bind_data);
}

} // namespace duckdb
