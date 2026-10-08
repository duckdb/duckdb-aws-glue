#include "catalog/glue_catalog.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/parser/parsed_data/alter_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"

#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_table.hpp"
#include "catalog/glue_transaction.hpp"
#include "planning/glue_hive_insert.hpp"

namespace duckdb {

static atomic<idx_t> next_iceberg_database_id {0};

GlueCatalog::GlueCatalog(AttachedDatabase &db_p, AccessMode access_mode, GlueAttachOptions options_p)
    : Catalog(db_p), access_mode(access_mode), options(std::move(options_p)), schemas(*this),
      // unique, so that ATTACH OR REPLACE can attach the new catalog's before the old one's is detached
      iceberg_database_name(StringUtil::Format("__glue_iceberg_%s_%llu", options.name, ++next_iceberg_database_id)) {
}

GlueCatalog::~GlueCatalog() {
}

unique_ptr<SecretEntry> GlueCatalog::GetStorageSecret(ClientContext &context, const string &secret_name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto &secret_manager = context.db->GetSecretManager();

	case_insensitive_set_t accepted_secret_types {"s3", "aws"};

	if (!secret_name.empty()) {
		auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name);
		if (secret_entry) {
			auto secret_type = secret_entry->secret->GetType();
			if (accepted_secret_types.count(secret_type.GetIdentifierName())) {
				return secret_entry;
			}
			throw InvalidConfigurationException(
			    "Found a secret by the name of '%s', but it is not of an accepted type for a 'secret', "
			    "accepted types are: 's3' or 'aws', found '%s'",
			    secret_name, secret_type.GetIdentifierName());
		}
		throw InvalidConfigurationException(
		    "No secret by the name of '%s' could be found, consider changing the 'secret'", secret_name);
	}

	for (auto &type : accepted_secret_types) {
		//! Lookup the default secret for this type
		auto secret_entry = secret_manager.GetSecretByName(transaction, StringUtil::Format("__default_%s", type));
		if (secret_entry) {
			return secret_entry;
		}
		auto secret_match = secret_manager.LookupSecret(transaction, type + "://", type);
		if (secret_match.HasMatch()) {
			return std::move(secret_match.secret_entry);
		}
	}
	throw InvalidConfigurationException("Could not find a valid storage secret (s3 or aws) to connect to Glue with");
}

void GlueCatalog::Initialize(bool load_builtin) {
}

ErrorData GlueCatalog::SupportsCreateTable(BoundCreateTableInfo &info) {
	auto &base = info.Base().Cast<CreateTableInfo>();
	// PARTITIONED BY is accepted here and validated in GlueSchemaEntry::CreateTable (Hive tables only, plain
	// column references)
	if (!base.sort_keys.empty()) {
		return ErrorData(ExceptionType::CATALOG, "SORTED BY is not supported for tables in a Glue catalog, the sort "
		                                         "order of a bucketed table is the SortColumns option");
	}
	return ErrorData();
}

ErrorData GlueCatalog::SupportsCreateSchema(CreateSchemaInfo &info) {
	return ErrorData();
}

optional<Identifier> GlueCatalog::GetDefaultSchema() const {
	if (options.default_schema.empty()) {
		return nullopt;
	}
	return options.default_schema;
}

//===--------------------------------------------------------------------===//
// Schemas
//===--------------------------------------------------------------------===//
GlueSchemaSet &GlueCatalog::GetSchemas() {
	return schemas;
}

optional_ptr<CatalogEntry> GlueCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &context = transaction.GetContext();
	ThrowIfInExplicitTransaction(context);
	auto schema_name = info.SchemaName().GetIdentifierName();

	auto existing = schemas.GetEntry(context, schema_name);
	if (existing) {
		switch (info.on_conflict) {
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return nullptr;
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw CatalogException("Schema with name \"%s\" already exists in Glue catalog \"%s\"", schema_name,
			                       GetName().GetIdentifierName());
		default:
			throw NotImplementedException("CREATE OR REPLACE SCHEMA is not supported for Glue catalogs");
		}
	}

	GlueDatabaseInfo database;
	database.name = schema_name;
	database.location_uri = GetDatabaseLocation(schema_name);
	for (auto &option : GlueSchemaEntry::EvaluateOptions(context, info.options, "CREATE SCHEMA")) {
		SetDatabaseOption(database, option.first, option.second);
	}
	GlueAPI::CreateDatabase(context, *this, database);

	// re-fetch so the cached entry reflects what Glue stored
	GlueDatabaseInfo created;
	if (!GlueAPI::GetDatabase(context, *this, schema_name, created)) {
		throw CatalogException("Glue database \"%s\" was created but could not be fetched afterwards", schema_name);
	}
	return schemas.CreateEntry(schemas.CreateSchemaEntry(created));
}

void GlueCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	ThrowIfInExplicitTransaction(context);
	auto schema_name = info.GetQualifiedName().Name().GetIdentifierName();
	auto existing = schemas.GetEntry(context, schema_name);
	if (!existing) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("Schema with name \"%s\" does not exist in Glue catalog \"%s\"", schema_name,
		                       GetName().GetIdentifierName());
	}
	// Glue deletes every table of the database along with it, so without CASCADE only an empty database is dropped
	string table_name;
	if (!info.cascade && GlueAPI::GetAnyTableName(context, *this, schema_name, table_name)) {
		throw DependencyException("Cannot drop Glue database \"%s\" because it still contains tables (e.g. \"%s\"). "
		                          "Use DROP SCHEMA ... CASCADE to drop the database together with all of its tables",
		                          schema_name, table_name);
	}
	GlueAPI::DeleteDatabase(context, *this, schema_name);
	schemas.RemoveEntry(schema_name);
}

static void EraseDatabaseParameter(GlueDatabaseInfo &database, const string &key) {
	for (auto it = database.parameters.begin(); it != database.parameters.end();) {
		if (StringUtil::CIEquals(it->first, key)) {
			it = database.parameters.erase(it);
		} else {
			++it;
		}
	}
}

void GlueCatalog::SetDatabaseOption(GlueDatabaseInfo &database, const string &key, const Value &value) {
	auto string_value = value.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
	if (StringUtil::CIEquals(key, "comment")) {
		database.description = string_value;
	} else if (StringUtil::CIEquals(key, "location")) {
		StringUtil::RTrim(string_value, "/");
		if (string_value.empty()) {
			throw BinderException("The location of Glue database \"%s\" must not be empty", database.name);
		}
		database.location_uri = string_value;
	} else {
		// everything else is a database property, stored in Glue's database parameters (like Hive's DBPROPERTIES)
		EraseDatabaseParameter(database, key);
		database.parameters[key] = string_value;
	}
}

void GlueCatalog::AlterSchema(CatalogTransaction transaction, SchemaCatalogEntry &schema, AlterSchemaInfo &info) {
	auto &context = transaction.GetContext();
	ThrowIfInExplicitTransaction(context);
	auto schema_name = schema.Cast<GlueSchemaEntry>().database_info.name;
	// work on the current Glue definition, not the cached one
	GlueDatabaseInfo database;
	if (!GlueAPI::GetDatabase(context, *this, schema_name, database)) {
		throw CatalogException("Schema with name \"%s\" does not exist in Glue catalog \"%s\"", schema_name,
		                       GetName().GetIdentifierName());
	}
	switch (info.alter_schema_type) {
	case AlterSchemaType::SET_SCHEMA_OPTIONS: {
		auto &set_info = info.Cast<SetSchemaOptionsInfo>();
		for (auto &option : GlueSchemaEntry::EvaluateOptions(context, set_info.options, "ALTER SCHEMA")) {
			SetDatabaseOption(database, option.first, option.second);
		}
		break;
	}
	case AlterSchemaType::RESET_SCHEMA_OPTIONS: {
		auto &reset_info = info.Cast<ResetSchemaOptionsInfo>();
		for (auto &option : reset_info.options) {
			auto key = option.GetIdentifierName();
			if (StringUtil::CIEquals(key, "comment")) {
				database.description.clear();
			} else if (StringUtil::CIEquals(key, "location")) {
				database.location_uri.clear();
			} else {
				EraseDatabaseParameter(database, key);
			}
		}
		break;
	}
	default:
		throw InternalException("Unrecognized alter schema type!");
	}
	GlueAPI::UpdateDatabase(context, *this, database);

	GlueDatabaseInfo updated;
	if (!GlueAPI::GetDatabase(context, *this, schema_name, updated)) {
		throw CatalogException("Glue database \"%s\" was updated but could not be fetched afterwards", schema_name);
	}
	schemas.CreateEntry(schemas.CreateSchemaEntry(updated));
}

string GlueCatalog::GetDatabaseLocation(const string &database_name) const {
	if (options.default_location.empty()) {
		return string();
	}
	return options.default_location + "/" + database_name;
}

string GlueCatalog::GetTableLocation(const GlueDatabaseInfo &database, const string &table_name) const {
	// DEFAULT_LOCATION on ATTACH wins over the LocationUri of the Glue database, so that everything a session creates
	// without an explicit location lands under one prefix
	if (!options.default_location.empty()) {
		return GetDatabaseLocation(database.name) + "/" + table_name;
	}
	if (!database.location_uri.empty()) {
		auto location = database.location_uri;
		StringUtil::RTrim(location, "/");
		return location + "/" + table_name;
	}
	throw InvalidConfigurationException(
	    "No location for table \"%s\": Glue database \"%s\" has no LocationUri and the catalog was attached without "
	    "DEFAULT_LOCATION. Provide one with CREATE TABLE ... WITH (location = 's3://...') or attach with "
	    "DEFAULT_LOCATION",
	    table_name, database.name);
}

void GlueCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<GlueSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> GlueCatalog::LookupSchema(CatalogTransaction transaction,
                                                           const EntryLookupInfo &schema_lookup,
                                                           OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	auto &context = transaction.GetContext();
	auto entry = schemas.GetEntry(context, schema_name);
	if (!entry) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw CatalogException(schema_lookup.GetErrorContext(), "Glue database with name \"%s\" does not exist",
		                       schema_name);
	}
	return &entry->Cast<SchemaCatalogEntry>();
}

void GlueCatalog::ThrowIfInExplicitTransaction(ClientContext &context) {
	if (!context.transaction.IsAutoCommit()) {
		throw TransactionException("This connection is currently in a transaction. Transaction support is not "
		                           "provided for Glue catalogs. Please call COMMIT, ABORT or ROLLBACK before trying "
		                           "the query again");
	}
}

GlueTable &GlueCatalog::GetHiveTableForDML(TableCatalogEntry &table, const char *statement) {
	auto &glue_table = table.Cast<GlueTable>();
	if (glue_table.table_info.GetFormat() != GlueTableFormat::HIVE) {
		throw NotImplementedException("%s on Glue table '%s' with type %s is not supported, only Hive tables can be "
		                              "written",
		                              statement, table.name.GetIdentifierName(), glue_table.table_info.GetFormatName());
	}
	// Hive and Spark bucket with different hash functions and file names; neither is implemented
	if (glue_table.table_info.IsBucketed()) {
		throw NotImplementedException("%s into Glue table '%s' is not supported: the table is %s, and DuckDB does not "
		                              "write a bucketed layout.",
		                              statement, table.name.GetIdentifierName(),
		                              glue_table.table_info.DescribeBucketing());
	}
	return glue_table;
}

//===--------------------------------------------------------------------===//
// Planning
//===--------------------------------------------------------------------===//
PhysicalOperator &GlueCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                          optional_ptr<PhysicalOperator> plan) {
	auto &glue_table = GetHiveTableForDML(op.table, "INSERT");
	ThrowIfInExplicitTransaction(context);
	return GlueHiveInsert::PlanInsert(context, planner, op, glue_table, plan);
}

PhysicalOperator &GlueCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalCreateTable &op, PhysicalOperator &plan) {
	if (GlueSchemaEntry::GetCreateTableFormat(context, op.info->Base()) == GlueTableFormat::ICEBERG) {
		return PlanIcebergCreateTableAs(context, planner, op, plan);
	}
	return GlueHiveInsert::PlanCreateTableAs(context, planner, op, plan);
}

PhysicalOperator &GlueCatalog::PlanIcebergCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                        LogicalCreateTable &op, PhysicalOperator &plan) {
	auto &glue_schema = op.schema.Cast<GlueSchemaEntry>();
	ThrowIfInExplicitTransaction(context);
	// CREATE TABLE ... AS on an existing table is planned as a plain CREATE TABLE: this only sees OR REPLACE
	if (glue_schema.CheckCreateTableConflict(context, op.info->Base())) {
		throw CatalogException("Table with name \"%s\" already exists in Glue database \"%s\"",
		                       op.info->Base().GetTableName().GetIdentifierName(), glue_schema.database_info.name);
	}
	auto &iceberg_schema = GetIcebergSchema(context, glue_schema.database_info.name);
	auto iceberg_info = glue_schema.BindIcebergCreateTable(context, *op.info, iceberg_schema);
	LogicalCreateTable iceberg_op(iceberg_schema, std::move(iceberg_info));
	iceberg_op.types = op.types;
	GlueTransaction::Get(context, *this)
	    .AddIcebergTable(glue_schema.database_info.name, op.info->Base().GetTableName().GetIdentifierName());
	return iceberg_schema.catalog.PlanCreateTableAs(context, planner, iceberg_op, plan);
}

PhysicalOperator &GlueCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                          PhysicalOperator &plan) {
	throw NotImplementedException("DELETE is not supported for Glue tables");
}

PhysicalOperator &GlueCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                          PhysicalOperator &plan) {
	throw NotImplementedException("UPDATE is not supported for Glue tables");
}

PhysicalOperator &GlueCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalMergeInto &op, PhysicalOperator &plan) {
	throw NotImplementedException("MERGE INTO is not supported for Glue tables");
}

unique_ptr<LogicalOperator> GlueCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                         TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("Indexes are not supported for Glue catalogs");
}

//===--------------------------------------------------------------------===//
// Iceberg
//===--------------------------------------------------------------------===//
Catalog &GlueCatalog::GetIcebergCatalog(ClientContext &context) {
	lock_guard<mutex> guard(iceberg_lock);
	auto &db_manager = DatabaseManager::Get(context);
	Identifier name(iceberg_database_name);
	if (iceberg_database) {
		// rolling back the transaction that attached it detaches it again
		if (db_manager.GetDatabase(name) == iceberg_database) {
			return iceberg_database->GetCatalog();
		}
		iceberg_database = nullptr;
	}
	AttachInfo info;
	info.name = name;
	info.path = options.path;
	auto uri =
	    options.endpoint.empty() ? StringUtil::Format("glue.%s.amazonaws.com", options.region) : options.endpoint;
	info.options = {{"type", Value("iceberg")},
	                {"endpoint_type", Value("glue")},
	                {"uri", Value(uri + "/iceberg")},
	                {"sigv4_region", Value(options.region)},
	                // Glue rejects DropTable with purgeRequested=true for Iceberg tables
	                {"purge_requested", Value::BOOLEAN(false)}};
	if (!options.secret_name.empty()) {
		info.options["secret"] = Value(options.secret_name);
	}
	AttachOptions attach_options(DBConfig::GetConfig(context).options);
	attach_options.access_mode = access_mode;
	attach_options.db_type = "iceberg";
	attach_options.visibility = AttachVisibility::HIDDEN;
	iceberg_database = db_manager.AttachDatabase(context, info, attach_options);
	if (!iceberg_database) {
		throw InternalException("Attaching the Iceberg catalog of Glue catalog \"%s\" failed",
		                        GetName().GetIdentifierName());
	}
	return iceberg_database->GetCatalog();
}

SchemaCatalogEntry &GlueCatalog::GetIcebergSchema(ClientContext &context, const string &database_name) {
	auto &iceberg_catalog = GetIcebergCatalog(context);
	EntryLookupInfo lookup(CatalogType::SCHEMA_ENTRY, QualifiedName(Identifier(database_name)));
	auto schema = iceberg_catalog.LookupSchema(iceberg_catalog.GetCatalogTransaction(context), lookup,
	                                           OnEntryNotFound::RETURN_NULL);
	if (!schema) {
		throw CatalogException("Glue database \"%s\" is not served by Glue's Iceberg REST endpoint", database_name);
	}
	return *schema;
}

TableCatalogEntry &GlueCatalog::GetIcebergTable(ClientContext &context, const string &database_name,
                                                const string &table_name, optional_ptr<BoundAtClause> at_clause) {
	auto &iceberg_catalog = GetIcebergCatalog(context);
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
	                       QualifiedName(iceberg_catalog.GetName(), Identifier(database_name), Identifier(table_name)),
	                       at_clause, QueryErrorContext());
	auto entry = Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		throw CatalogException("Iceberg table \"%s.%s\" is registered in Glue but Glue's Iceberg REST endpoint does "
		                       "not return it",
		                       database_name, table_name);
	}
	return entry->Cast<TableCatalogEntry>();
}

void GlueCatalog::AttachIcebergCatalog(ClientContext &context) {
	try {
		GetIcebergCatalog(context);
	} catch (std::exception &ex) {
		// e.g. a Glue compatible server without the Iceberg REST endpoint: Hive tables still work, the first use of
		// an Iceberg table tries again and reports the error
		ErrorData error(ex);
		DUCKDB_LOG_WARNING(context, "Glue catalog '%s': attaching its Iceberg catalog failed: %s",
		                   GetName().GetIdentifierName(), error.RawMessage());
	}
}

void GlueCatalog::OnDetach(ClientContext &context) {
	lock_guard<mutex> guard(iceberg_lock);
	if (!iceberg_database) {
		return;
	}
	auto &db_manager = DatabaseManager::Get(context);
	Identifier name(iceberg_database_name);
	if (db_manager.GetDatabase(name) == iceberg_database) {
		db_manager.DetachDatabase(context, name, OnEntryNotFound::RETURN_NULL);
	}
	iceberg_database = nullptr;
}

//===--------------------------------------------------------------------===//
// Misc
//===--------------------------------------------------------------------===//
DatabaseSize GlueCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	return size;
}

bool GlueCatalog::InMemory() {
	return false;
}

string GlueCatalog::GetDBPath() {
	return options.path;
}

} // namespace duckdb
