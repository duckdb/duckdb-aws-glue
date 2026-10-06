#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_view.hpp"

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
#include "duckdb/common/error_data.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "catalog/glue_view.hpp"
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
vector<pair<string, Value>>
GlueSchemaEntry::EvaluateOptions(ClientContext &context,
                                 const case_insensitive_map_t<unique_ptr<ParsedExpression>> &options,
                                 const string &statement) {
	vector<pair<string, Value>> result;
	if (options.empty()) {
		return result;
	}
	auto binder = Binder::CreateBinder(context);
	TableFunctionBinder option_binder(*binder, context, statement + " options");
	for (auto &option : options) {
		auto expr_copy = option.second->Copy();
		auto bound_expr = option_binder.Bind(expr_copy);
		if (bound_expr->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		auto value = ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
		if (value.IsNull()) {
			throw BinderException("NULL is not a valid value for %s option '%s'", statement, option.first);
		}
		result.emplace_back(option.first, std::move(value));
	}
	return result;
}

bool GlueSchemaEntry::IsBucketingOption(const string &key) {
	return StringUtil::CIEquals(key, "BucketColumns") || StringUtil::CIEquals(key, "NumberOfBuckets") ||
	       StringUtil::CIEquals(key, "SortColumns");
}

//! The Glue option another spelling of a bucketing option stands for, empty if the key is none of them
static string BucketingOptionFor(const string &key) {
	static const pair<const char *, const char *> SPELLINGS[] = {
	    {"bucket_columns", "BucketColumns"}, {"bucketed_by", "BucketColumns"},
	    {"clustered_by", "BucketColumns"},   {"number_of_buckets", "NumberOfBuckets"},
	    {"bucket_count", "NumberOfBuckets"}, {"sort_columns", "SortColumns"},
	    {"sorted_by", "SortColumns"}};
	for (auto &spelling : SPELLINGS) {
		if (StringUtil::CIEquals(key, spelling.first)) {
			return spelling.second;
		}
	}
	return string();
}

static void CheckBucketedTablesEnabled(ClientContext &context, const string &key) {
	Value enabled;
	if (context.TryGetCurrentSetting("glue_create_bucketed_tables", enabled) && !enabled.IsNull() &&
	    enabled.GetValue<bool>()) {
		return;
	}
	throw BinderException("CREATE TABLE option '%s' creates a bucketed Glue table, which DuckDB can read but not "
	                      "write (INSERT into it is refused). SET glue_create_bucketed_tables = true to create one",
	                      key);
}

static optional<int32_t> ToInteger(const Value &value) {
	if (!value.type().IsIntegral()) {
		return nullopt;
	}
	auto integer = value.DefaultTryCastAs(LogicalType::INTEGER);
	if (!integer) {
		return nullopt;
	}
	return integer->GetValue<int32_t>();
}

static vector<string> ParseBucketColumns(const string &key, const Value &value) {
	if (value.type().id() != LogicalTypeId::LIST) {
		throw BinderException("CREATE TABLE option '%s' must be a list of column names, e.g. ['id'], got '%s'", key,
		                      value.ToString());
	}
	vector<string> result;
	for (auto &entry : ListValue::GetChildren(value)) {
		if (entry.IsNull()) {
			throw BinderException("CREATE TABLE option '%s' can not contain NULL", key);
		}
		result.push_back(entry.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>());
	}
	if (result.empty()) {
		throw BinderException("CREATE TABLE option '%s' needs at least one column", key);
	}
	return result;
}

static int32_t ParseNumberOfBuckets(const string &key, const Value &value) {
	auto count = ToInteger(value);
	if (!count || *count <= 0) {
		throw BinderException("CREATE TABLE option '%s' must be a positive integer, got '%s'", key, value.ToString());
	}
	return *count;
}

static GlueSortOrder ParseSortOrder(const string &key, const Value &value) {
	// Glue's SortOrder is 1 for ascending, 0 for descending
	auto sort_order = ToInteger(value);
	if (sort_order && *sort_order == 1) {
		return GlueSortOrder::ASCENDING;
	}
	if (sort_order && *sort_order == 0) {
		return GlueSortOrder::DESCENDING;
	}
	throw BinderException("SortOrder in CREATE TABLE option '%s' must be 1 (ascending) or 0 (descending), got '%s'",
	                      key, value.ToString());
}

static vector<GlueColumn> ParseSortColumns(const string &key, const Value &value) {
	auto &type = value.type();
	if (type.id() != LogicalTypeId::LIST || ListType::GetChildType(type).id() != LogicalTypeId::STRUCT) {
		throw BinderException(
		    "CREATE TABLE option '%s' must be a list of {'Column': name, 'SortOrder': 1 or 0}, got '%s'", key,
		    value.ToString());
	}
	vector<GlueColumn> result;
	for (auto &entry : ListValue::GetChildren(value)) {
		if (entry.IsNull()) {
			throw BinderException("CREATE TABLE option '%s' can not contain NULL", key);
		}
		auto &fields = StructType::GetChildTypes(entry.type());
		auto &values = StructValue::GetChildren(entry);
		GlueColumn sort_column;
		for (idx_t i = 0; i < fields.size(); i++) {
			auto &field = fields[i].first.GetIdentifierName();
			auto is_column = StringUtil::CIEquals(field, "Column");
			if (!is_column && !StringUtil::CIEquals(field, "SortOrder")) {
				throw BinderException("CREATE TABLE option '%s' takes structs of 'Column' and 'SortOrder', got '%s'",
				                      key, field);
			}
			if (values[i].IsNull()) {
				continue;
			}
			if (is_column) {
				sort_column.name = values[i].DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
			} else {
				sort_column.sort_order = ParseSortOrder(key, values[i]);
			}
		}
		if (sort_column.name.empty() || sort_column.sort_order == GlueSortOrder::UNSORTED) {
			throw BinderException(
			    "Every entry of CREATE TABLE option '%s' needs a 'Column' and a 'SortOrder', got '%s'", key,
			    entry.ToString());
		}
		result.push_back(std::move(sort_column));
	}
	if (result.empty()) {
		throw BinderException("CREATE TABLE option '%s' needs at least one column", key);
	}
	return result;
}

static void CheckNoDuplicates(const char *option, const vector<string> &names) {
	for (idx_t i = 0; i < names.size(); i++) {
		for (idx_t j = 0; j < i; j++) {
			if (StringUtil::CIEquals(names[i], names[j])) {
				throw BinderException("%s lists column '%s' twice", option, names[i]);
			}
		}
	}
}

//! Hive's CLUSTERED BY (...) [SORTED BY (...)] INTO n BUCKETS, over columns that are not partition columns
static void ResolveBucketing(GlueCreateTableOptions &options, const ColumnList &columns,
                             const vector<string> &partition_columns, const string &table_name) {
	if (options.bucket_columns.empty()) {
		if (options.number_of_buckets != -1 || !options.sort_columns.empty()) {
			throw BinderException("CREATE TABLE options NumberOfBuckets and SortColumns need BucketColumns");
		}
		return;
	}
	if (options.number_of_buckets == -1) {
		throw BinderException("CREATE TABLE option BucketColumns needs NumberOfBuckets");
	}
	auto resolve = [&](const char *option, const string &name) -> string {
		for (auto &partition_column : partition_columns) {
			if (StringUtil::CIEquals(partition_column, name)) {
				throw BinderException("%s column '%s' is a partition column of table '%s'", option, name, table_name);
			}
		}
		for (auto &column : columns.Physical()) {
			if (StringUtil::CIEquals(column.Name().GetIdentifierName(), name)) {
				return column.Name().GetIdentifierName();
			}
		}
		throw BinderException("%s column '%s' is not a column of table '%s'", option, name, table_name);
	};
	for (auto &name : options.bucket_columns) {
		name = resolve("BucketColumns", name);
	}
	vector<string> sort_names;
	for (auto &sort_column : options.sort_columns) {
		sort_column.name = resolve("SortColumns", sort_column.name);
		sort_names.push_back(sort_column.name);
	}
	CheckNoDuplicates("BucketColumns", options.bucket_columns);
	CheckNoDuplicates("SortColumns", sort_names);
}

GlueCreateTableOptions GlueSchemaEntry::ParseCreateTableOptions(ClientContext &context,
                                                                const CreateTableInfo &create_info) {
	GlueCreateTableOptions result;
	for (auto &option : EvaluateOptions(context, create_info.options, "CREATE TABLE")) {
		auto &key = option.first;
		auto &value = option.second;
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
		} else if (IsBucketingOption(key)) {
			CheckBucketedTablesEnabled(context, key);
			if (StringUtil::CIEquals(key, "BucketColumns")) {
				result.bucket_columns = ParseBucketColumns(key, value);
			} else if (StringUtil::CIEquals(key, "NumberOfBuckets")) {
				result.number_of_buckets = ParseNumberOfBuckets(key, value);
			} else {
				result.sort_columns = ParseSortColumns(key, value);
			}
		} else if (!BucketingOptionFor(key).empty()) {
			// stored as a table parameter this would silently leave the table unbucketed
			throw BinderException("Unknown CREATE TABLE option '%s', a bucketed Glue table is created with option '%s'",
			                      key, BucketingOptionFor(key));
		} else {
			// everything else is a table property, stored in Glue's table parameters (like Hive / Trino do)
			result.parameters[key] = string_value;
		}
	}
	return result;
}

//! Tables and views share one catalog set (as in DuckDB's own schema), so a lookup by name returns either; the
//! statements that mean one of the two check it here
static void CheckEntryType(optional_ptr<CatalogEntry> existing, CatalogType expected, const string &name,
                           const char *action) {
	if (existing && existing->type != expected) {
		throw CatalogException("Existing object %s is of type %s, trying to %s type %s", name,
		                       CatalogTypeToString(existing->type), action, CatalogTypeToString(expected));
	}
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &context = transaction.GetContext();
	GlueCatalog::ThrowIfInExplicitTransaction(context);
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	auto &base = info.Base();
	auto table_name = base.GetTableName().GetIdentifierName();

	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table_name)));
	auto existing = tables.GetEntry(context, lookup);
	CheckEntryType(existing, CatalogType::TABLE_ENTRY, table_name, "create");
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
	ResolveBucketing(options, base.columns, partition_columns, table_name);

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
	table.bucket_columns = options.bucket_columns;
	table.number_of_buckets = options.number_of_buckets;
	table.sort_columns = options.sort_columns;
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
	return tables.CreateEntry(tables.CreateEntry(created));
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("Glue databases do not support creating functions");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                        TableCatalogEntry &table) {
	throw BinderException("Glue databases do not support creating indexes");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	auto &context = transaction.GetContext();
	GlueCatalog::ThrowIfInExplicitTransaction(context);
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	auto view_name = info.GetQualifiedName().Name().GetIdentifierName();

	EntryLookupInfo lookup(CatalogType::VIEW_ENTRY, QualifiedName(Identifier(view_name)));
	auto existing = tables.GetEntry(context, lookup);
	CheckEntryType(existing, CatalogType::VIEW_ENTRY, view_name, "create");
	if (existing) {
		switch (info.on_conflict) {
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return existing;
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw CatalogException("View with name \"%s\" already exists in Glue database \"%s\"", view_name,
			                       database_info.name);
		default:
			if (!existing->Cast<GlueView>().IsDuckDBView()) {
				throw CatalogException(
				    "Glue view \"%s\" was not written by DuckDB; replace it from the engine that wrote it", view_name);
			}
			break;
		}
	}

	GlueViewInfo view;
	view.database_name = database_info.name;
	view.name = view_name;
	view.sql = GlueView::RenderViewSql(info);
	try {
		// what is stored must read back: never write a view DuckDB itself could not parse
		CreateViewInfo::ParseSelect(view.sql);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw InternalException("The SQL rendered for Glue view \"%s\" does not parse: %s\n%s", view_name,
		                        error.RawMessage(), view.sql);
	}
	view.secure = info.security_type == ViewSecurityType::SECURE_VIEW;
	// the stored columns carry the column alias list of CREATE VIEW: an alias replaces the query's own name
	for (idx_t i = 0; i < info.types.size(); i++) {
		GlueColumn column;
		auto aliased = i < info.aliases.size() && !info.aliases[i].GetIdentifierName().empty();
		column.name = (aliased ? info.aliases[i] : info.names[i]).GetIdentifierName();
		column.type = GlueTypes::FromLogicalType(info.types[i]);
		view.columns.push_back(std::move(column));
	}
	if (existing) {
		GlueAPI::UpdateView(context, glue_catalog, view);
	} else {
		GlueAPI::CreateView(context, glue_catalog, view);
	}

	// re-fetch so the entry reflects what Glue stored
	tables.RemoveEntry(view_name);
	GlueTableInfo created;
	if (!GlueAPI::GetTable(context, glue_catalog, database_info.name, view_name, created)) {
		throw CatalogException("Glue view \"%s.%s\" was created but could not be fetched afterwards",
		                       database_info.name, view_name);
	}
	return tables.CreateEntry(tables.CreateEntry(created));
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

//! The parameters GlueTableInfo::GetFormat() derives the table format from can not be set or reset: changing
//! table_type on a Hive table would relabel it as Iceberg or Delta without a metadata file behind it.
static void CheckTablePropertyChangeable(const string &key) {
	if (key.empty()) {
		throw InvalidInputException("A table property needs a name");
	}
	if (GlueTableInfo::IsFormatParameter(key)) {
		throw InvalidInputException("Table property '%s' decides how the table is read and can not be changed "
		                            "with ALTER TABLE",
		                            key);
	}
}

//! ALTER TABLE t SET (key = value, ...) / RESET (key, ...): Hive's SET / UNSET TBLPROPERTIES
void GlueSchemaEntry::AlterTableProperties(ClientContext &context, AlterTableInfo &alter_table) {
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	auto table_name = alter_table.GetQualifiedName().Name().GetIdentifierName();
	vector<pair<string, string>> set;
	vector<string> unset;
	if (alter_table.alter_table_type == AlterTableType::SET_TABLE_OPTIONS) {
		auto &options = alter_table.Cast<SetTableOptionsInfo>();
		for (auto &option : EvaluateOptions(context, options.table_options, "ALTER TABLE SET")) {
			CheckTablePropertyChangeable(option.first);
			// Glue stores every parameter as a string
			set.emplace_back(option.first, option.second.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>());
		}
	} else {
		for (auto &option : alter_table.Cast<ResetTableOptionsInfo>().table_options) {
			auto key = option.GetIdentifierName();
			CheckTablePropertyChangeable(key);
			unset.push_back(std::move(key));
		}
	}
	GlueAPI::UpdateTableParameters(context, glue_catalog, database_info.name, table_name, set, unset);

	GlueTableInfo updated;
	if (!GlueAPI::GetTable(context, glue_catalog, database_info.name, table_name, updated)) {
		throw CatalogException("Table \"%s.%s\" was altered but could not be fetched afterwards", database_info.name,
		                       table_name);
	}
	tables.CreateEntry(tables.CreateEntry(updated));
}

void GlueSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	auto &context = transaction.GetContext();
	GlueCatalog::ThrowIfInExplicitTransaction(context);
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	auto table_name = info.GetQualifiedName().Name().GetIdentifierName();

	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table_name)));
	auto entry = tables.GetEntry(context, lookup);
	if (!entry) {
		throw CatalogException("Table with name \"%s\" does not exist in Glue database \"%s\"", table_name,
		                       database_info.name);
	}
	if (entry->type == CatalogType::VIEW_ENTRY) {
		if (info.type == AlterType::ALTER_VIEW) {
			throw NotImplementedException(
			    "Glue cannot rename a view; use CREATE OR REPLACE VIEW under the new name and DROP "
			    "VIEW the old one");
		}
		if (info.type == AlterType::SET_COMMENT) {
			throw NotImplementedException("Comments on Glue views are not supported yet");
		}
		throw BinderException("\"%s\" is a view", table_name);
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
	if (alter_table.alter_table_type == AlterTableType::SET_TABLE_OPTIONS ||
	    alter_table.alter_table_type == AlterTableType::RESET_TABLE_OPTIONS) {
		AlterTableProperties(context, alter_table);
		return;
	}

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
		// Glue keeps BucketColumns and SortColumns as they are, naming a column the table no longer has
		for (auto &bucket_column : current.bucket_columns) {
			if (StringUtil::CIEquals(bucket_column, name)) {
				throw CatalogException("Column \"%s\" is a bucket column of table \"%s\" and can not be dropped", name,
				                       table_name);
			}
		}
		for (auto &sort_column : current.sort_columns) {
			if (StringUtil::CIEquals(sort_column.name, name)) {
				throw CatalogException("Column \"%s\" is a sort column of table \"%s\" and can not be dropped", name,
				                       table_name);
			}
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
	tables.CreateEntry(tables.CreateEntry(updated));
}

void GlueSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (!CatalogTypeIsSupported(info.type)) {
		throw NotImplementedException("Glue databases only support dropping tables");
	}
	GlueCatalog::ThrowIfInExplicitTransaction(context);
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	auto table_name = info.GetQualifiedName().Name().GetIdentifierName();

	EntryLookupInfo lookup(info.type, QualifiedName(Identifier(table_name)));
	auto existing = tables.GetEntry(context, lookup);
	if (!existing) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("%s with name \"%s\" does not exist in Glue database \"%s\"",
		                       info.type == CatalogType::VIEW_ENTRY ? "View" : "Table", table_name, database_info.name);
	}
	CheckEntryType(existing, info.type, table_name, "drop");
	if (existing->type == CatalogType::VIEW_ENTRY && !existing->Cast<GlueView>().IsDuckDBView()) {
		throw CatalogException("Glue view \"%s\" was not written by DuckDB; drop it from the engine that wrote it",
		                       table_name);
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
