#include "storage/glue_catalog.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"

#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include "glue_api.hpp"
#include "storage/glue_schema_entry.hpp"
#include "storage/glue_table.hpp"
#include "storage/glue_hive_insert.hpp"

namespace duckdb {

GlueCatalog::GlueCatalog(AttachedDatabase &db_p, AccessMode access_mode, GlueAttachOptions options_p)
    : Catalog(db_p), access_mode(access_mode), options(std::move(options_p)), schemas(*this) {
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
		return ErrorData(ExceptionType::CATALOG, "SORTED BY is not supported for tables in a Glue catalog");
	}
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

	// DuckDB's CREATE SCHEMA grammar carries no options (CreateSchemaStmt <- 'SCHEMA' IfNotExists? QualifiedName), so
	// there is nothing to apply here; glue_create_database() is the surface that passes them
	auto database = BuildDatabaseInfo(schema_name, {}, "CREATE SCHEMA");
	GlueAPI::CreateDatabase(context, *this, database);

	// re-fetch so the cached entry reflects what Glue stored
	GlueDatabaseInfo created;
	if (!GlueAPI::GetDatabase(context, *this, schema_name, created)) {
		throw CatalogException("Glue database \"%s\" was created but could not be fetched afterwards", schema_name);
	}
	return schemas.CreateEntry(schemas.CreateSchemaEntry(created));
}

void GlueCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	auto schema_name = info.GetQualifiedName().Name().GetIdentifierName();
	auto existing = schemas.GetEntry(context, schema_name);
	if (!existing) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("Schema with name \"%s\" does not exist in Glue catalog \"%s\"", schema_name,
		                       GetName().GetIdentifierName());
	}
	if (!info.cascade) {
		// Glue's DeleteDatabase deletes every table of the database along with it and has no RESTRICT mode, so the
		// check has to happen here, before the call. Ask Glue rather than the cached table set: nothing invalidates
		// that cache during the life of the attach, so a stale "empty" answer would delete tables never seen here.
		string first_table_name;
		if (GlueAPI::DatabaseHasTables(context, *this, schema_name, &first_table_name)) {
			throw CatalogException("Cannot drop Glue database \"%s\" because it still contains tables (e.g. \"%s\"). "
			                       "Use DROP SCHEMA ... CASCADE to drop the database together with all of its tables",
			                       schema_name, first_table_name);
		}
	}
	// Glue deletes every table in the database along with it, which is the CASCADE behaviour
	GlueAPI::DeleteDatabase(context, *this, schema_name);
	schemas.RemoveEntry(schema_name);
}

string GlueCatalog::GetDatabaseLocation(const string &database_name) const {
	if (options.default_location.empty()) {
		return string();
	}
	return options.default_location + "/" + database_name;
}

GlueDatabaseInfo GlueCatalog::BuildDatabaseInfo(const string &database_name,
                                                const case_insensitive_map_t<string> &options_list,
                                                const string &context_name) const {
	GlueDatabaseInfo database;
	database.name = database_name;
	database.location_uri = GetDatabaseLocation(database_name);
	for (auto &option : options_list) {
		auto &key = option.first;
		if (StringUtil::CIEquals(key, "comment")) {
			database.description = option.second;
		} else if (StringUtil::CIEquals(key, "location")) {
			database.location_uri = option.second;
			StringUtil::RTrim(database.location_uri, "/");
		} else if (StringUtil::CIEquals(key, "type")) {
			// symmetry with CREATE TABLE's 'type' option, which only accepts Hive
			if (StringUtil::Upper(option.second) != "HIVE") {
				throw BinderException("%s: unknown Glue database type '%s' for option 'type', only 'HIVE' is supported",
				                      context_name, option.second);
			}
		} else {
			// everything else is a database property, stored in Glue's database parameters, the way CREATE TABLE's
			// unknown options become table parameters (Athena's DBPROPERTIES)
			database.parameters[key] = option.second;
		}
	}
	return database;
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

GlueTable &GlueCatalog::GetHiveTableForDML(TableCatalogEntry &table, const char *statement) {
	auto &glue_table = table.Cast<GlueTable>();
	if (glue_table.table_info.GetFormat() != GlueTableFormat::HIVE) {
		throw NotImplementedException("%s on Glue table '%s' with type %s is not supported, only Hive tables can be "
		                              "written",
		                              statement, table.name.GetIdentifierName(), glue_table.table_info.GetFormatName());
	}
	return glue_table;
}

//===--------------------------------------------------------------------===//
// Planning
//===--------------------------------------------------------------------===//
PhysicalOperator &GlueCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                          optional_ptr<PhysicalOperator> plan) {
	auto &glue_table = GetHiveTableForDML(op.table, "INSERT");
	return GlueHiveInsert::PlanInsert(context, planner, op, glue_table, plan);
}

PhysicalOperator &GlueCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalCreateTable &op, PhysicalOperator &plan) {
	return GlueHiveInsert::PlanCreateTableAs(context, planner, op, plan);
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
