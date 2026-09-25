#include "catalog/glue_schema_entry.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/common/enum_util.hpp"

#include <algorithm>
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

#include "core/glue_types.hpp"
#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"

namespace duckdb {

GlueSchemaEntry::GlueSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, GlueDatabaseInfo database_info_p)
    : SchemaCatalogEntry(catalog, info), database_info(std::move(database_info_p)), tables(*this) {
}

GlueSchemaEntry::~GlueSchemaEntry() {
}

bool GlueSchemaEntry::CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return true;
	default:
		return false;
	}
}

//===--------------------------------------------------------------------===//
// Create / Drop / Alter
//===--------------------------------------------------------------------===//
GlueCreateTableOptions GlueSchemaEntry::ParseCreateTableOptions(ClientContext &context,
                                                                const CreateTableInfo &create_info) {
	GlueCreateTableOptions result;
	if (create_info.options.empty()) {
		return result;
	}
	auto binder = Binder::CreateBinder(context);
	TableFunctionBinder option_binder(*binder, context, "CREATE TABLE options");
	for (auto &option : create_info.options) {
		auto &key = option.first;
		auto expr_copy = option.second->Copy();
		auto bound_expr = option_binder.Bind(expr_copy);
		if (bound_expr->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		auto value = ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
		if (value.IsNull()) {
			throw BinderException("NULL is not a valid value for CREATE TABLE option '%s'", key);
		}
		auto string_value = value.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();

		if (StringUtil::CIEquals(key, "type")) {
			// only Hive (Glue native) tables can be created
			if (StringUtil::Upper(string_value) != "HIVE") {
				throw BinderException("Unknown Glue table type '%s' for option 'type', only 'HIVE' is supported",
				                      string_value);
			}
		} else if (StringUtil::CIEquals(key, "format")) {
			result.format = HiveFileFormatFromString(string_value);
		} else if (StringUtil::CIEquals(key, "location")) {
			result.location = string_value;
			StringUtil::RTrim(result.location, "/");
		} else if (StringUtil::CIEquals(key, "delimiter") || StringUtil::CIEquals(key, "quote") ||
		           StringUtil::CIEquals(key, "escape")) {
			// the csv dialect: Hive SerDes take single characters
			if (string_value.size() != 1) {
				throw BinderException("CREATE TABLE option '%s' must be a single character, got '%s'", key,
				                      string_value);
			}
			if (StringUtil::CIEquals(key, "delimiter")) {
				result.csv_delimiter = string_value;
			} else if (StringUtil::CIEquals(key, "quote")) {
				result.csv_quote = string_value;
			} else {
				result.csv_escape = string_value;
			}
		} else if (StringUtil::CIEquals(key, "header")) {
			// csv files with a header line: Hive's "TBLPROPERTIES ('skip.header.line.count' = '1')"
			if (value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>()) {
				result.parameters["skip.header.line.count"] = "1";
			}
		} else {
			// everything else is a table property, stored in Glue's table parameters (like Hive / Trino do)
			result.parameters[key] = string_value;
		}
	}
	return result;
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &context = transaction.GetContext();
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	auto &base = info.Base();
	auto table_name = base.GetTableName().GetIdentifierName();

	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table_name)));
	auto existing = tables.GetEntry(context, lookup);
	if (existing) {
		switch (base.on_conflict) {
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return nullptr;
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw CatalogException("Table with name \"%s\" already exists in Glue database \"%s\"", table_name,
			                       database_info.name);
		default:
			throw NotImplementedException(
			    "CREATE OR REPLACE TABLE is not supported for Glue catalogs, use separate DROP and CREATE statements");
		}
	}
	if (!base.constraints.empty()) {
		throw NotImplementedException("Constraints are not supported when creating tables in a Glue catalog");
	}
	auto options = ParseCreateTableOptions(context, base);
	// Hive partitions are columns: PARTITIONED BY must name columns of the table, which become the PartitionKeys
	// (in the given order) and are stored in the directory names rather than in the data files
	vector<string> partition_columns;
	for (auto &key : base.partition_keys) {
		if (key->GetExpressionType() != ExpressionType::COLUMN_REF) {
			throw BinderException("PARTITIONED BY for Hive tables only supports column names, got '%s'",
			                      key->ToString());
		}
		auto &column_ref = key->Cast<ColumnRefExpression>();
		if (column_ref.IsQualified()) {
			throw BinderException("PARTITIONED BY for Hive tables only supports plain column names, got '%s'",
			                      key->ToString());
		}
		auto &column_name = column_ref.GetColumnName().GetIdentifierName();
		if (!base.columns.ColumnExists(column_ref.GetColumnName())) {
			throw BinderException("PARTITIONED BY column '%s' is not a column of table '%s'", column_name, table_name);
		}
		for (auto &existing : partition_columns) {
			if (StringUtil::CIEquals(existing, column_name)) {
				throw BinderException("PARTITIONED BY column '%s' is listed twice", column_name);
			}
		}
		partition_columns.push_back(column_name);
	}
	auto is_partition_column = [&](const string &name) {
		for (auto &partition_column : partition_columns) {
			if (StringUtil::CIEquals(partition_column, name)) {
				return true;
			}
		}
		return false;
	};

	GlueTableInfo table;
	table.name = table_name;
	table.database_name = database_info.name;
	table.location =
	    options.location.empty() ? glue_catalog.GetTableLocation(database_info, table_name) : options.location;
	table.parameters = options.parameters;
	table.file_format = options.format;
	table.csv_delimiter = options.csv_delimiter;
	table.csv_quote = options.csv_quote;
	table.csv_escape = options.csv_escape;
	for (auto &column : base.columns.Physical()) {
		if (is_partition_column(column.Name().GetIdentifierName())) {
			continue;
		}
		GlueColumn glue_column;
		glue_column.name = column.Name().GetIdentifierName();
		glue_column.type = GlueTypes::FromLogicalType(column.Type());
		table.columns.push_back(std::move(glue_column));
	}
	for (auto &partition_column : partition_columns) {
		auto &column = base.columns.GetColumn(Identifier(partition_column));
		GlueColumn glue_column;
		glue_column.name = column.Name().GetIdentifierName();
		glue_column.type = GlueTypes::FromLogicalType(column.Type());
		table.partition_keys.push_back(std::move(glue_column));
	}
	if (table.columns.empty()) {
		throw BinderException("Table '%s' needs at least one column that is not a partition column", table_name);
	}
	if (table.file_format == HiveFileFormat::PARQUET && table.partition_keys.empty() && options.location.empty()) {
		// an explicit LOCATION may already hold files; partitioned tables keep statistics per partition
		table.parameters.emplace("numRows", "0");
		table.parameters.emplace("numFiles", "0");
		table.parameters.emplace("totalSize", "0");
	}
	GlueAPI::CreateHiveTable(context, glue_catalog, table);

	// re-fetch so the entry reflects what Glue stored
	GlueTableInfo created;
	if (!GlueAPI::GetTable(context, glue_catalog, database_info.name, table_name, created)) {
		throw CatalogException("Glue table \"%s.%s\" was created but could not be fetched afterwards",
		                       database_info.name, table_name);
	}
	return tables.CreateEntry(tables.CreateTableEntry(created));
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("Glue databases do not support creating functions");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                        TableCatalogEntry &table) {
	throw BinderException("Glue databases do not support creating indexes");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("Glue databases do not support creating views (yet)");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("Glue databases do not support creating sequences");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                CreateTableFunctionInfo &info) {
	throw BinderException("Glue databases do not support creating table functions");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                               CreateCopyFunctionInfo &info) {
	throw BinderException("Glue databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                 CreatePragmaFunctionInfo &info) {
	throw BinderException("Glue databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw BinderException("Glue databases do not support creating collations");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("Glue databases do not support creating types");
}

namespace {

//! Type changes Hive can read back from the existing parquet files: widening only
bool IsAllowedHiveTypeChange(const LogicalType &from, const LogicalType &to) {
	if (from == to) {
		return true;
	}
	auto rank = [](LogicalTypeId id) -> int {
		switch (id) {
		case LogicalTypeId::TINYINT:
			return 1;
		case LogicalTypeId::SMALLINT:
			return 2;
		case LogicalTypeId::INTEGER:
			return 3;
		case LogicalTypeId::BIGINT:
			return 4;
		default:
			return 0;
		}
	};
	if (rank(from.id()) > 0 && rank(to.id()) > 0) {
		return rank(to.id()) > rank(from.id());
	}
	if (from.id() == LogicalTypeId::FLOAT && to.id() == LogicalTypeId::DOUBLE) {
		return true;
	}
	if (to.id() == LogicalTypeId::VARCHAR) {
		// everything can be read as a string
		return true;
	}
	return false;
}

} // namespace

void GlueSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	auto &context = transaction.GetContext();
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	auto table_name = info.GetQualifiedName().Name().GetIdentifierName();

	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table_name)));
	auto entry = tables.GetEntry(context, lookup);
	if (!entry) {
		throw CatalogException("Table with name \"%s\" does not exist in Glue database \"%s\"", table_name,
		                       database_info.name);
	}
	auto &glue_table = entry->Cast<GlueTable>();
	if (glue_table.table_info.GetFormat() != GlueTableFormat::HIVE) {
		throw NotImplementedException("ALTER TABLE is only supported for Hive tables in a Glue catalog, '%s' is a %s "
		                              "table",
		                              table_name, glue_table.table_info.GetFormatName());
	}
	if (info.type != AlterType::ALTER_TABLE) {
		throw NotImplementedException("Only ALTER TABLE is supported for Glue tables");
	}
	auto &alter_table = info.Cast<AlterTableInfo>();

	// Work on the current Glue definition, not the cached one
	GlueTableInfo current;
	if (!GlueAPI::GetTable(context, glue_catalog, database_info.name, table_name, current)) {
		throw CatalogException("Table with name \"%s\" does not exist in Glue database \"%s\"", table_name,
		                       database_info.name);
	}
	auto find_column = [](vector<GlueColumn> &columns, const string &name) -> optional_ptr<GlueColumn> {
		for (auto &column : columns) {
			if (StringUtil::CIEquals(column.name, name)) {
				return &column;
			}
		}
		return nullptr;
	};
	auto is_partition_key = [&](const string &name) {
		return find_column(current.partition_keys, name) != nullptr;
	};

	auto columns = current.columns;
	switch (alter_table.alter_table_type) {
	case AlterTableType::ADD_COLUMN: {
		auto &add = alter_table.Cast<AddColumnInfo>();
		auto &name = add.new_column.Name().GetIdentifierName();
		if (find_column(columns, name) || is_partition_key(name)) {
			if (add.if_column_not_exists) {
				return;
			}
			throw CatalogException("Column with name \"%s\" already exists in table \"%s\"", name, table_name);
		}
		if (add.new_column.HasDefaultValue()) {
			throw NotImplementedException("Glue tables do not support column default values");
		}
		GlueColumn column;
		column.name = name;
		column.type = GlueTypes::FromLogicalType(add.new_column.Type());
		columns.push_back(std::move(column));
		break;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove = alter_table.Cast<RemoveColumnInfo>();
		auto &name = remove.removed_column.GetIdentifierName();
		if (is_partition_key(name)) {
			throw CatalogException("Column \"%s\" is a partition key of table \"%s\" and can not be dropped", name,
			                       table_name);
		}
		if (!find_column(columns, name)) {
			if (remove.if_column_exists) {
				return;
			}
			throw CatalogException("Table \"%s\" does not have a column with name \"%s\"", table_name, name);
		}
		if (columns.size() == 1) {
			throw CatalogException("Can not drop column \"%s\": table \"%s\" needs at least one column", name,
			                       table_name);
		}
		columns.erase(std::remove_if(columns.begin(), columns.end(),
		                             [&](const GlueColumn &column) { return StringUtil::CIEquals(column.name, name); }),
		              columns.end());
		break;
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto &change = alter_table.Cast<ChangeColumnTypeInfo>();
		auto &name = change.column_name.GetIdentifierName();
		if (is_partition_key(name)) {
			throw CatalogException("Column \"%s\" is a partition key of table \"%s\" and its type can not be "
			                       "changed",
			                       name, table_name);
		}
		auto column = find_column(columns, name);
		if (!column) {
			throw CatalogException("Table \"%s\" does not have a column with name \"%s\"", table_name, name);
		}
		auto from = GlueTypes::ToLogicalType(column->type);
		if (!IsAllowedHiveTypeChange(from, change.target_type)) {
			throw CatalogException("Can not change column \"%s\" of table \"%s\" from %s to %s: existing parquet "
			                       "files keep their types, only widening changes (e.g. INTEGER to BIGINT, FLOAT to "
			                       "DOUBLE, anything to VARCHAR) are supported for Hive tables",
			                       name, table_name, from.ToString(), change.target_type.ToString());
		}
		column->type = GlueTypes::FromLogicalType(change.target_type);
		break;
	}
	default:
		throw NotImplementedException("ALTER TABLE %s is not supported for Glue tables",
		                              EnumUtil::ToString(alter_table.alter_table_type));
	}

	GlueAPI::UpdateTableColumns(context, glue_catalog, database_info.name, table_name, columns);

	// refresh the cached entry from what Glue stored
	GlueTableInfo updated;
	if (!GlueAPI::GetTable(context, glue_catalog, database_info.name, table_name, updated)) {
		throw CatalogException("Table \"%s.%s\" was altered but could not be fetched afterwards", database_info.name,
		                       table_name);
	}
	tables.CreateEntry(tables.CreateTableEntry(updated));
}

void GlueSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (!CatalogTypeIsSupported(info.type)) {
		throw NotImplementedException("Glue databases only support dropping tables");
	}
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	auto table_name = info.GetQualifiedName().Name().GetIdentifierName();

	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table_name)));
	auto existing = tables.GetEntry(context, lookup);
	if (!existing) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("Table with name \"%s\" does not exist in Glue database \"%s\"", table_name,
		                       database_info.name);
	}
	// A VIEW_ENTRY lookup resolves to the GlueTable, so without this DROP VIEW on a table would delete it.
	// Exception wording matches the one in duck_schema_entry.cpp.
	if (existing->type != info.type) {
		throw CatalogException("Existing object %s is of type %s, trying to drop type %s", table_name,
		                       CatalogTypeToString(existing->type), CatalogTypeToString(info.type));
	}
	GlueAPI::DeleteTable(context, glue_catalog, database_info.name, table_name);
	tables.RemoveEntry(table_name);
}

//===--------------------------------------------------------------------===//
// Scan / Lookup
//===--------------------------------------------------------------------===//
void GlueSchemaEntry::Scan(ClientContext &context, CatalogType type,
                           const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}
	tables.Scan(context, callback);
}

void GlueSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported for Glue databases");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                        const EntryLookupInfo &lookup_info) {
	if (!CatalogTypeIsSupported(lookup_info.GetCatalogType())) {
		return nullptr;
	}
	auto &context = transaction.GetContext();
	return tables.GetEntry(context, lookup_info);
}

} // namespace duckdb
