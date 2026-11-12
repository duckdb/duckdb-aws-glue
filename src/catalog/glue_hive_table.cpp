#include "catalog/glue_table.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "planning/glue_hive_insert.hpp"
#include "catalog/glue_file_credentials.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/logging/logger.hpp"
#include "planning/hive_multi_file_reader.hpp"
#include "planning/hive_query_cache.hpp"

namespace duckdb {
namespace {

GlueTable &GetHiveTableForDML(ClientContext &context, GlueTable &table, const char *statement,
                              GlueTableInfo &table_info) {
	table_info = table.schema.Cast<GlueSchemaEntry>().DefinitionForWrite(context, table);
	auto format = table_info.GetFormat();
	if (!format.IsHive()) {
		throw NotImplementedException("%s on Glue table '%s' with type %s is not supported, only Hive tables can be "
		                              "written",
		                              statement, table.name.GetIdentifierName(), format.Describe(table_info));
	}
	// an InputFormat of another engine over a Parquet or text SerDe is a layout DuckDB does not know
	auto file_format = table_info.GetFileFormat();
	if (!format.IsWritable(table_info, file_format)) {
		throw NotImplementedException("%s into Glue table '%s' is not supported: its InputFormat '%s' is not one Hive "
		                              "uses for %s tables, so DuckDB does not know how the files below the table "
		                              "location are laid out",
		                              statement, table.name.GetIdentifierName(), table_info.input_format,
		                              HiveFileFormatToString(file_format));
	}
	// the partitions an INSERT registers are ignored by Athena, and the files may belong under a location template
	if (table_info.IsPartitionProjected()) {
		throw NotImplementedException("%s into Glue table '%s' is not supported: the table uses Athena partition "
		                              "projection (projection.enabled), so the partitions it would register are "
		                              "ignored by Athena. Set projection.enabled to false and register the partitions "
		                              "to write it",
		                              statement, table.name.GetIdentifierName());
	}
	// Hive and Spark bucket with different hash functions and file names; neither is implemented
	if (table_info.IsBucketed()) {
		throw NotImplementedException("%s into Glue table '%s' is not supported: the table is %s, and DuckDB does not "
		                              "write a bucketed layout.",
		                              statement, table.name.GetIdentifierName(), table_info.DescribeBucketing());
	}
	// Athena writes JSON only with the Hive JsonSerDe; OpenX properties (mapping.*, dots.in.keys) would not find the
	// keys DuckDB writes
	if (table_info.GetSerDe() == HiveSerDe::OPENX_JSON) {
		throw NotImplementedException("%s into Glue table '%s' is not supported: the table uses the OpenX JSON SerDe "
		                              "'%s', DuckDB writes JSON tables only with the Hive JsonSerDe "
		                              "(org.apache.hive.hcatalog.data.JsonSerDe)",
		                              statement, table.name.GetIdentifierName(), table_info.serde_library);
	}
	return table;
}

} // namespace

PhysicalOperator &GlueTable::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                        optional_ptr<PhysicalOperator> plan) {
	auto format = table_info.GetFormat();
	if (!format.IsHive()) {
		throw NotImplementedException("INSERT on Glue table '%s' with type %s is not supported, only Hive tables can "
		                              "be written",
		                              name, format.Describe(table_info));
	}
	GlueTableInfo definition;
	auto &glue_table = GetHiveTableForDML(context, *this, "INSERT", definition);
	// an INSERT writes by path, without the credentials the table's scans read with
	// TODO: support INSERT into a table with credentials, by writing its files with them
	if (!GlueFileCredentialsCallback::Get(context, glue_table).empty()) {
		throw NotImplementedException("INSERT into Glue table '%s', which is read with file credentials, is not "
		                              "supported",
		                              op.table.name.GetIdentifierName());
	}
	return GlueHiveInsert::PlanInsert(context, planner, op, glue_table, definition, plan);
}

virtual_column_map_t GlueTable::GetVirtualColumns() const {
	virtual_column_map_t result;
	if (table_info.GetFormat().IsHive()) {
		result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR));
		HiveMultiFileReader::AddMetadataColumns(result);
	}
	return result;
}

vector<column_t> GlueTable::GetRowIdColumns() const {
	return {};
}

TableFunction GlueTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto format = table_info.GetFormat();
	if (!format.IsHive()) {
		throw NotImplementedException("Glue table '%s.%s' has table format %s, which can not be read yet; use "
		                              "glue_get_table_response() to inspect its definition",
		                              table_info.schema_name, table_info.name, format.Describe(table_info));
	}
	return GetHiveScanFunction(context, bind_data);
}

TableFunction GlueTable::GetHiveScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	if (table_info.IsPartitionProjected()) {
		throw NotImplementedException("Glue table '%s.%s' uses Athena partition projection (projection.enabled), "
		                              "which is not supported yet: Athena derives its partitions from the "
		                              "projection.* parameters and ignores the ones registered in Glue, so a scan "
		                              "of the registered ones would read no rows or the wrong ones. Set "
		                              "projection.enabled to false and register the partitions to read it",
		                              table_info.schema_name, table_info.name);
	}
	// The scan produces the columns this entry was planned with: data columns first, partition keys last. The SerDe
	// decides the file format (throws for unsupported SerDes).
	auto scan_info = make_shared_ptr<HiveScanInfo>();
	scan_info->scan_id = HiveQueryCache::Get(context)->NextScanId();
	scan_info->catalog_name = ParentCatalog().GetName().GetIdentifierName();
	scan_info->database_name = table_info.schema_name;
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
			                   table_info.schema_name, table_info.name, property);
		}
	}
	scan_info->file_secret = GlueFileCredentialsCallback::Get(context, *this);
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
