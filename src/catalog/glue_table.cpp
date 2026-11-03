#include "catalog/glue_table.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "functions/glue_functions.hpp"
#include "core/glue_types.hpp"
#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_transaction.hpp"
#include "planning/hive_multi_file_reader.hpp"
#include "planning/hive_query_cache.hpp"

namespace duckdb {

namespace {

//! Build list_value('a', 'b', 'c') — used for BucketColumns.
unique_ptr<ParsedExpression> StringListExpr(const vector<string> &items) {
	vector<FunctionArgument> args;
	for (auto &item : items) {
		args.emplace_back(ConstantExpression::FromValue(Value(item)));
	}
	return make_uniq<FunctionExpression>(Identifier("list_value"), std::move(args));
}

//! Build list_value(struct_pack(Column := 'name', SortOrder := 1|0), ...) — used for SortColumns.
unique_ptr<ParsedExpression> SortColumnsExpr(const vector<GlueColumn> &sort_cols) {
	vector<FunctionArgument> structs;
	for (auto &sc : sort_cols) {
		vector<FunctionArgument> fields;
		fields.emplace_back(Identifier("Column"), ConstantExpression::FromValue(Value(sc.name)));
		int sort_order_int = (sc.sort_order == GlueSortOrder::DESCENDING) ? 0 : 1;
		fields.emplace_back(Identifier("SortOrder"), ConstantExpression::FromValue(Value::INTEGER(sort_order_int)));
		structs.emplace_back(make_uniq<FunctionExpression>(Identifier("struct_pack"), std::move(fields)));
	}
	return make_uniq<FunctionExpression>(Identifier("list_value"), std::move(structs));
}

//! Used by GlueTable::GetInfo() to propagate error in the sql field.
struct UnsupportedTableInfo : CreateTableInfo {
	using CreateTableInfo::CreateTableInfo;
	string error;
	string ToString() const override {
		return error;
	}
};

//! Iceberg stub — fill in when the extension supports Iceberg CREATE.
void AddIcebergTableProperties(CreateTableInfo &info, const GlueTableInfo &table_info) {
	throw NotImplementedException("Not supported: table format 'ICEBERG'");
}

//! HIVE path: adds format, SerDe parameters, and table properties. Throws "Not supported"
//! for anything the extension's CREATE TABLE path can not reproduce.
void AddHiveTableProperties(CreateTableInfo &info, const GlueTableInfo &table_info) {
	// File format check: GetFileFormat throws for unknown SerDes; rewrap with a consistent message.
	HiveFileFormat file_format;
	try {
		file_format = table_info.GetFileFormat();
	} catch (const NotImplementedException &) {
		throw NotImplementedException("Not supported: SerDe '%s' for Hive tables", table_info.serde_library);
	}

	// SerDe parameters check: CREATE path writes these 5 SerDe parameters.
	// Throw error for unknown SerDe parameter.
	static const char *SAFE_SERDE_KEYS[] = {"field.delim", "separatorChar", "quoteChar", "escapeChar",
	                                        "serialization.format"};
	for (auto &param : table_info.serde_parameters) {
		bool is_safe = false;
		for (auto &safe_key : SAFE_SERDE_KEYS) {
			if (StringUtil::CIEquals(param.first, safe_key)) {
				is_safe = true;
				break;
			}
		}
		if (!is_safe) {
			throw NotImplementedException("Not supported: SerDe parameter '%s' for Hive tables", param.first);
		}
	}

	// Partition Keys
	for (auto &pk : table_info.partition_keys) {
		info.partition_keys.emplace_back(make_uniq<ColumnRefExpression>(Identifier(pk.name)));
	}

	info.options["type"] = ConstantExpression::FromValue(Value("HIVE"));
	info.options["format"] = ConstantExpression::FromValue(Value(HiveFileFormatToString(file_format)));
	if (!table_info.location.empty()) {
		info.options["location"] = ConstantExpression::FromValue(Value(table_info.location));
	}

	// CSV properties — pulled from SerDe parameters.
	if (file_format == HiveFileFormat::CSV) {
		auto delimiter = table_info.GetFieldDelimiter();
		if (!delimiter.empty() && delimiter != ",") {
			info.options["delimiter"] = ConstantExpression::FromValue(Value(delimiter));
		}
		// quote/escape only exist on OpenCSVSerde.
		if (StringUtil::Contains(StringUtil::Lower(table_info.serde_library), "opencsvserde")) {
			info.options["quote"] = ConstantExpression::FromValue(Value(table_info.GetQuoteCharacter()));
			info.options["escape"] = ConstantExpression::FromValue(Value(table_info.GetEscapeCharacter()));
		}
		if (table_info.GetParameter("skip.header.line.count") == "1") {
			info.options["header"] = ConstantExpression::Boolean(true);
		}
	}

	// Bucketing.
	if (!table_info.bucket_columns.empty()) {
		info.options["BucketColumns"] = StringListExpr(table_info.bucket_columns);
		if (table_info.number_of_buckets > 0) {
			info.options["NumberOfBuckets"] =
			    ConstantExpression::FromValue(Value::INTEGER(table_info.number_of_buckets));
		}
		if (!table_info.sort_columns.empty()) {
			info.options["SortColumns"] = SortColumnsExpr(table_info.sort_columns);
		}
	}

	// Table.Parameters pass-through: emit each remaining key as a WITH option.
	// Skip keys we already set above.
	for (auto &param : table_info.parameters) {
		if (info.options.find(param.first) != info.options.end()) {
			continue;
		}
		info.options[param.first] = ConstantExpression::FromValue(Value(param.second));
	}
}

//! Add CREATE TABLE properties from Glue (serde, bucketing, Table.Parameters, ...) to the
//! CreateTableInfo. Picks the right handler by table format and throws "Not supported: ..."
//! for anything the extension can not round-trip.
void AddTableProperties(CreateTableInfo &info, const GlueTableInfo &table_info) {
	auto format = table_info.GetFormat();
	switch (format.format) {
	case GlueFormat::HIVE:
		AddHiveTableProperties(info, table_info);
		return;
	case GlueFormat::ICEBERG:
		AddIcebergTableProperties(info, table_info);
		return;
	default:
		throw NotImplementedException("Not supported: table format '%s'", format.ToString());
	}
}

} // namespace

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

//! Called by duckdb_tables().sql. On success, returns a CreateTableInfo so ToString() renders
//! a full CREATE TABLE statement. On failure, returns an UnsupportedTableInfo whose ToString()
//! renders the "Not supported: ..." reason, so a bulk catalog scan does not break on one bad table.
unique_ptr<CreateInfo> GlueTable::GetInfo() const {
	auto base = TableCatalogEntry::GetInfo();
	auto &info = base->Cast<CreateTableInfo>();
	try {
		AddTableProperties(info, table_info);
		return base;
	} catch (const NotImplementedException &e) {
		auto result = make_uniq<UnsupportedTableInfo>();
		result->error = ErrorData(e).RawMessage();
		return result;
	}
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
			                              table_info.schema_name, table_info.name);
		}
		return GetHiveScanFunction(context, bind_data);
	default:
		throw NotImplementedException("Glue table '%s.%s' has table format %s, which can not be read yet; use "
		                              "glue_get_table_response() to inspect its definition",
		                              table_info.schema_name, table_info.name, format.Describe(table_info));
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
