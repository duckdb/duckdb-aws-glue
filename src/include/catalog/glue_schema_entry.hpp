#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/parser/parsed_expression.hpp"

#include "core/glue_info.hpp"
#include "catalog/glue_table_set.hpp"

namespace duckdb {
struct BoundCreateTableInfo;
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

//! A Glue database, exposed as a DuckDB schema
class GlueSchemaEntry : public SchemaCatalogEntry {
public:
	GlueSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, GlueDatabaseInfo database_info);
	~GlueSchemaEntry() override;

public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
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

	//! Bind and evaluate the constant expressions of a WITH (<key> = <value>, ...) option list
	static vector<pair<string, Value>>
	EvaluateOptions(ClientContext &context, const case_insensitive_map_t<unique_ptr<ParsedExpression>> &options,
	                const string &statement);
	static GlueCreateTableOptions ParseCreateTableOptions(ClientContext &context, const CreateTableInfo &create_info);
	//! BucketColumns, NumberOfBuckets or SortColumns (case-insensitive)
	static bool IsBucketingOption(const string &key);
	//! The 'table_type' option of CREATE TABLE: HIVE (the default) or ICEBERG
	static GlueTableFormat GetCreateTableFormat(ClientContext &context, const CreateTableInfo &create_info);
	//! The existing entry when CREATE TABLE IF NOT EXISTS names one, nullptr when there is none; throws otherwise
	optional_ptr<CatalogEntry> CheckCreateTableConflict(ClientContext &context, const CreateTableInfo &create_info);
	//! The CREATE TABLE of an Iceberg table, rebound to the schema of the Iceberg catalog
	unique_ptr<BoundCreateTableInfo> BindIcebergCreateTable(ClientContext &context, BoundCreateTableInfo &info,
	                                                        SchemaCatalogEntry &iceberg_schema);
	//! Type changes Hive can read back from the existing parquet files: widening only
	static bool IsAllowedHiveTypeChange(const LogicalType &from, const LogicalType &to);
	//! Replace the cached entry of an altered table with what Glue stored
	GlueTable &RefreshTable(ClientContext &context, const string &table_name);

private:
	static bool CatalogTypeIsSupported(CatalogType type);
	void AlterTableProperties(ClientContext &context, AlterTableInfo &alter_table);
	optional_ptr<CatalogEntry> CreateIcebergTable(ClientContext &context, BoundCreateTableInfo &info);
	//! The Glue entry of an Iceberg table, or nullptr for any other entry
	static optional_ptr<GlueTable> AsIcebergTable(optional_ptr<CatalogEntry> entry);

public:
	//! The database definition as returned by Glue
	GlueDatabaseInfo database_info;
	GlueTableSet tables;
};

} // namespace duckdb
