#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "glue_options.hpp"
#include "storage/glue_schema_set.hpp"

#include <memory>

namespace Aws {
namespace Glue {
class GlueClient;
} // namespace Glue
} // namespace Aws

namespace duckdb {
class GlueTable;

class GlueCatalog : public Catalog {
public:
	explicit GlueCatalog(AttachedDatabase &db_p, AccessMode access_mode, GlueAttachOptions options);
	~GlueCatalog() override;

public:
	//! Look up the (s3 or aws) secret that holds the AWS credentials for this catalog
	static unique_ptr<SecretEntry> GetStorageSecret(ClientContext &context, const string &secret_name);

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

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void DropSchema(ClientContext &context, DropInfo &info) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	GlueSchemaSet &GetSchemas();
	//! The S3 location a new database gets: <default_location>/<database>, or none when there is no DEFAULT_LOCATION
	string GetDatabaseLocation(const string &database_name) const;
	//! Build the Glue definition of a new database from an option list, on top of the catalog's defaults. 'comment'
	//! becomes the Glue Description, 'location' overrides GetDatabaseLocation, and every other key becomes a Glue
	//! database parameter (Athena's DBPROPERTIES) -- the same rule CREATE TABLE's options follow. Shared by
	//! CREATE SCHEMA (which has no option list of its own) and glue_create_database().
	GlueDatabaseInfo BuildDatabaseInfo(const string &database_name, const case_insensitive_map_t<string> &options,
	                                   const string &context_name) const;
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

	//! The cached Glue client (managed by GlueAPI): rebuilt whenever the credentials it was built with change
	mutex client_lock;
	string client_cache_key;
	std::shared_ptr<Aws::Glue::GlueClient> glue_client;

private:
	//! Throw unless 'table' is a Hive table, the only kind that can be written
	static GlueTable &GetHiveTableForDML(TableCatalogEntry &table, const char *statement);

private:
	GlueSchemaSet schemas;
};

} // namespace duckdb
