#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "catalog/glue_options.hpp"
#include "catalog/glue_schema_set.hpp"

#include <memory>

namespace Aws {
namespace Glue {
class GlueClient;
} // namespace Glue
} // namespace Aws

namespace duckdb {
class GlueTable;

//! The outcome of looking a table up before its schema (GlueCatalog::TryLookupTableBeforeSchema)
enum class TableLookup : uint8_t {
	//! the schema is already held, or was added meanwhile: the regular lookup runs
	SKIPPED,
	//! the table exists, so its schema does too
	TABLE_FOUND,
	//! the schema exists, the table does not
	TABLE_NOT_FOUND,
	//! the schema does not exist
	SCHEMA_NOT_FOUND
};

class GlueCatalog : public Catalog {
public:
	explicit GlueCatalog(AttachedDatabase &db_p, AccessMode access_mode, GlueAttachOptions options);
	~GlueCatalog() override;

public:
	//! Look up the (s3 or aws) secret that holds the AWS credentials for this catalog
	static unique_ptr<SecretEntry> GetStorageSecret(ClientContext &context, const string &secret_name);
	//! Glue has no transactions: throw on DDL or INSERT inside an explicit (BEGIN) transaction
	static void ThrowIfInExplicitTransaction(ClientContext &context);
	//! The same for a glue_* function that changes Glue, naming it in the error
	static void ThrowIfInExplicitTransaction(ClientContext &context, const char *function);

	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "glue";
	}
	//! Functions are never looked up in a Glue catalog
	CatalogLookupBehavior CatalogTypeLookupRule(CatalogType type) const override {
		switch (type) {
		case CatalogType::TABLE_FUNCTION_ENTRY:
		case CatalogType::SCALAR_FUNCTION_ENTRY:
		case CatalogType::AGGREGATE_FUNCTION_ENTRY:
			return CatalogLookupBehavior::NEVER_LOOKUP;
		default:
			return CatalogLookupBehavior::STANDARD;
		}
	}
	bool CheckAmbiguousCatalogOrSchema(ClientContext &context, const Identifier &schema) override {
		return false;
	}
	optional<Identifier> GetDefaultSchema() const override;
	//! Allow CREATE TABLE ... PARTITIONED BY (...) WITH (location = '...', <property> = '...'); the options are
	//! validated in GlueSchemaEntry::CreateTable
	ErrorData SupportsCreateTable(BoundCreateTableInfo &info) override;
	//! Allow CREATE SCHEMA ... WITH (comment = '...', location = '...', <parameter> = '...'), applied in CreateSchema
	ErrorData SupportsCreateSchema(CreateSchemaInfo &info) override;
	void AlterSchema(CatalogTransaction transaction, SchemaCatalogEntry &schema, AlterSchemaInfo &info) override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void DropSchema(ClientContext &context, DropInfo &info) override;
	void OnDetach(ClientContext &context) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	//! Serves a single-name equality on schema_name with one database lookup instead of listing every database
	vector<reference<SchemaCatalogEntry>> GetSchemasForScan(ClientContext &context,
	                                                        const SchemaScanFilters &filters) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	GlueSchemaSet &GetSchemas();
	//! The S3 location a new database gets: <default_location>/<database>, or none when there is no DEFAULT_LOCATION
	string GetDatabaseLocation(const string &database_name) const;
	//! The S3 location a new table gets when none is given: <default_location>/<database>/<table> when the catalog
	//! was attached with DEFAULT_LOCATION, else <database LocationUri>/<table>; throws when neither is available
	string GetTableLocation(const GlueDatabaseInfo &database, const string &table_name) const;

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
	                                PhysicalOperator &plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

public:
	AccessMode access_mode;
	GlueAttachOptions options;

	//! the CatalogId Glue reports in the response of GetDatabases call (during attach probe or first usage)
	string FetchCatalogId(ClientContext &context);
	//! Whether 'catalog' is the one ICEBERG_CATALOG names; throws if it is not an Iceberg catalog over this one
	bool IsIcebergCatalog(Catalog &catalog);
	//! Whether Glue registers db.table of this catalog as an Iceberg table, looked up without expanding views
	bool IsIcebergTable(ClientContext &context, const Identifier &schema_name, const Identifier &table_name);

	//! The cached Glue client (managed by GlueAPI): rebuilt whenever the credentials it was built with change
	mutex client_lock;
	string client_cache_key;
	std::shared_ptr<Aws::Glue::GlueClient> glue_client;

private:
	//! Throw unless Glue's current definition of 'table' (returned in 'table_info') is a Hive table DuckDB writes
	static GlueTable &GetHiveTableForDML(ClientContext &context, TableCatalogEntry &table, const char *statement,
	                                     GlueTableInfo &table_info);
	//! Apply one CREATE / ALTER SCHEMA option: 'comment' is the Description, 'location' the LocationUri and any other
	//! key a database parameter
	static void SetDatabaseOption(GlueDatabaseInfo &database, const string &key, const Value &value);

	//! Looks a table up before its database: finding the table proves the database exists, saving GetDatabase
	CatalogEntryLookup TryLookupEntryInternal(CatalogTransaction transaction,
	                                          const EntryLookupInfo &lookup_info) override;
	//! Looks the table up before its schema; with SKIPPED the regular lookup runs instead
	TableLookup TryLookupTableBeforeSchema(CatalogTransaction transaction, const EntryLookupInfo &lookup_info,
	                                       const string &database_name, CatalogEntryLookup &result);
	//! A Glue view found by a table lookup is read with its Iceberg tables mapped (GlueView::ForReading)
	CatalogEntryLookup ViewForReading(CatalogTransaction transaction, CatalogEntryLookup result);

private:
	//! for reading - the fetched catalog id or account id from ATTACH
	string KnownCatalogId();
	friend struct GlueViewNames;

private:
	GlueSchemaSet schemas;
	mutex reported_id_lock;
	//! empty until Glue reports one: not cached as none, the catalog may get a database of its own later
	string reported_catalog_id;
};

} // namespace duckdb
