#include "catalog/glue_table.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "functions/glue_functions.hpp"
#include "core/glue_types.hpp"
#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
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
	return result;
}

vector<column_t> GlueTable::GetRowIdColumns() const {
	return vector<column_t>();
}

GlueTableInfo GlueTable::RefreshTableInfo(ClientContext &context) const {
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	GlueTableInfo result;
	if (!GlueAPI::GetTable(context, glue_catalog, table_info.database_name, table_info.name, result)) {
		throw CatalogException("Glue table '%s.%s' no longer exists", table_info.database_name, table_info.name);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
//! Bind data for the unsupported-format stub scan: the format name for the error message
struct GlueUnsupportedScanData : public FunctionData {
	string format_name;
	explicit GlueUnsupportedScanData(string format_name_p) : format_name(std::move(format_name_p)) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<GlueUnsupportedScanData>(format_name);
	}
	bool Equals(const FunctionData &other) const override {
		return format_name == other.Cast<GlueUnsupportedScanData>().format_name;
	}
};

//! Carries column schema and format name into the unsupported-format stub scan
struct GlueUnsupportedScanInfo : public TableFunctionInfo {
	string format_name;
	vector<LogicalType> column_types;
	vector<Identifier> column_names;
};

static unique_ptr<FunctionData> GlueUnsupportedBind(ClientContext &, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &info = input.info->Cast<GlueUnsupportedScanInfo>();
	return_types = info.column_types;
	names = info.column_names;
	return make_uniq<GlueUnsupportedScanData>(info.format_name);
}

static void GlueUnsupportedScan(ClientContext &, TableFunctionInput &input, DataChunk &) {
	string format = "non-Hive";
	if (input.bind_data) {
		format = input.bind_data->Cast<GlueUnsupportedScanData>().format_name;
	}
	throw NotImplementedException("Scanning %s tables from a Glue catalog is not yet supported; "
	                              "use glue_get_table_response() to inspect the table definition",
	                              format);
}

static TableFunction GlueUnsupportedStub(const GlueTableInfo &info) {
	auto fn_info = make_shared_ptr<GlueUnsupportedScanInfo>();
	fn_info->format_name = info.GetFormatName();
	for (auto &col : info.columns) {
		fn_info->column_types.push_back(GlueTypes::ToLogicalType(col.type));
		fn_info->column_names.emplace_back(col.name);
	}
	for (auto &key : info.partition_keys) {
		fn_info->column_types.push_back(GlueTypes::ToLogicalType(key.type));
		fn_info->column_names.emplace_back(key.name);
	}
	TableFunction stub("glue_unsupported_scan", {}, GlueUnsupportedScan, GlueUnsupportedBind);
	stub.function_info = std::move(fn_info);
	return stub;
}

TableFunction GlueTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// Ask Glue what kind of table this is right before scanning: only Hive (Glue native) tables can be read
	auto latest_info = RefreshTableInfo(context);
	switch (latest_info.GetFormat()) {
	case GlueTableFormat::HIVE: {
		// an unsupported SerDe (e.g. ORC) gets the stub too, so DESCRIBE works
		HiveFileFormat file_format;
		if (!latest_info.TryGetFileFormat(file_format)) {
			return GlueUnsupportedStub(latest_info);
		}
		return GetHiveScanFunction(context, bind_data, latest_info);
	}
	default:
		return GlueUnsupportedStub(latest_info);
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
	scan_info->catalog_name = catalog.GetName().GetIdentifierName();
	scan_info->database_name = latest_info.database_name;
	scan_info->table_name = latest_info.name;
	scan_info->table = this;
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
	// the partitions are fetched from Glue on first use (HiveScanInfo::Partitions), not at bind
	return BindHiveScan(context, std::move(scan_info), bind_data);
}

} // namespace duckdb
