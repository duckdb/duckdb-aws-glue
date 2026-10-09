#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/parser/parsed_expression.hpp"

#include "core/glue_info.hpp"
#include "catalog/glue_table_set.hpp"

namespace duckdb {
struct CreateTableInfo;

//! Options accepted in CREATE TABLE ... WITH (...) for Glue tables
struct GlueCreateTableOptions {
	//! Optional explicit S3 location of the table
	string location;
	//! The file format of the table (format = 'parquet' | 'csv' | 'json' | 'avro'), parquet by default
	HiveFileFormat format = HiveFileFormat::PARQUET;
	//! csv only: delimiter, quote and escape (single characters; quote and escape empty unless given)
	string csv_delimiter = ",";
	string csv_quote;
	string csv_escape;
	//! BucketColumns / NumberOfBuckets / SortColumns, named as in Glue's StorageDescriptor
	vector<string> bucket_columns;
	int32_t number_of_buckets = -1;
	vector<GlueColumn> sort_columns;
	//! Every other option is stored as a table parameter in Glue
	unordered_map<string, string> parameters;
};

//! Where a changed column goes: where it is, to the front, or after another column
enum class GlueColumnPosition : uint8_t { UNCHANGED, FIRST, AFTER };

//! A change to one data column of a Hive table; what is not set stays as it is
struct GlueColumnChange {
	//! The column to change
	string name;
	optional<string> new_name;
	optional<LogicalType> new_type;
	//! An empty comment removes the comment
	optional<string> comment;
	GlueColumnPosition position = GlueColumnPosition::UNCHANGED;
	//! The column to move it after, for GlueColumnPosition::AFTER
	string after;
};

//! A Glue database, exposed as a DuckDB schema
class GlueSchemaEntry : public SchemaCatalogEntry {
public:
	GlueSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, GlueDatabaseInfo database_info);
	~GlueSchemaEntry() override;

public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	//! Create a table from a definition whose options were already resolved while planning CTAS. This avoids evaluating
	//! location, format or dialect expressions a second time when execution begins.
	optional_ptr<CatalogEntry> CreateTableFromInfo(CatalogTransaction transaction, BoundCreateTableInfo &info,
	                                               const GlueTableInfo &table_info);
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

	//! Build and validate a Glue table definition without touching Glue. CTAS resolves it once while planning, then
	//! carries the same value into execution for both catalog creation and file-writer behavior.
	GlueTableInfo BuildTableInfo(ClientContext &context, const CreateTableInfo &create_info);
	//! Bind and evaluate the constant expressions of a WITH (<key> = <value>, ...) option list
	static vector<pair<string, Value>>
	EvaluateOptions(ClientContext &context, const case_insensitive_map_t<unique_ptr<ParsedExpression>> &options,
	                const string &statement);
	static GlueCreateTableOptions ParseCreateTableOptions(ClientContext &context, const CreateTableInfo &create_info);
	//! BucketColumns, NumberOfBuckets or SortColumns (case-insensitive)
	static bool IsBucketingOption(const string &key);
	//! Type changes Hive can read back from the existing files: widening only
	static bool IsAllowedHiveTypeChange(const LogicalType &from, const LogicalType &to);
	//! Replace the cached entry of an altered table with what Glue stored
	GlueTable &RefreshTable(ClientContext &context, const string &table_name);
	//! Change one data column of a Hive table in Glue and refresh the entry; the caller has checked that the table is
	//! a Hive table. A change the existing data files would read differently is refused: renaming a column of files
	//! matched to the columns by name, and moving a column of files read by position.
	GlueTable &ChangeColumn(ClientContext &context, const string &table_name, const GlueColumnChange &change);

private:
	optional_ptr<CatalogEntry> CreateTableInternal(CatalogTransaction transaction, BoundCreateTableInfo &info,
	                                               const GlueTableInfo *resolved_table_info);
	static bool CatalogTypeIsSupported(CatalogType type);
	void AlterTableProperties(ClientContext &context, AlterTableInfo &alter_table);

public:
	//! The database definition as returned by Glue
	GlueDatabaseInfo database_info;
	GlueTableSet tables;
};

} // namespace duckdb
