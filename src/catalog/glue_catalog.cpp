#include "catalog/glue_catalog.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
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
	// PARTITIONED BY is accepted here and validated in GlueSchemaEntry::BuildTableInfo (Hive tables only, plain
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

void GlueCatalog::OnDetach(ClientContext &context) {
	GlueTransactionCache::OnDetach(context, *this);
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
			throw InvalidInputException("The location of Glue database \"%s\" must not be empty", database.name);
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

//! Glue databases are not nested: "x".db.t with an unknown catalog x arrives as [x, db]
static vector<string> DatabasePath(const vector<Identifier> &components, const string &catalog_name) {
	vector<string> path;
	for (auto &component : components) {
		if (!component.empty()) {
			path.push_back(component.GetIdentifierName());
		}
	}
	if (path.size() > 1 && StringUtil::CIEquals(path[0], catalog_name)) {
		path.erase(path.begin());
	}
	return path;
}

optional_ptr<SchemaCatalogEntry> GlueCatalog::LookupSchema(CatalogTransaction transaction,
                                                           const EntryLookupInfo &schema_lookup,
                                                           OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	auto path = DatabasePath(schema_lookup.GetQualifiedName().Path(), GetName().GetIdentifierName());
	if (path.size() > 1) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw CatalogException(schema_lookup.GetErrorContext(),
		                       "Glue database \"%s\" does not exist: Glue databases are not nested",
		                       schema_lookup.GetQualifiedName().ToString());
	}
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

CatalogEntryLookup GlueCatalog::TryLookupEntryInternal(CatalogTransaction transaction,
                                                       const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetAtClause() && !SupportsTimeTravel()) {
		return {nullptr, nullptr, ErrorData(BinderException("Catalog type does not support time travel"))};
	}
	CatalogEntryLookup result {nullptr, nullptr, ErrorData()};
	if (lookup_info.GetCatalogType() == CatalogType::TABLE_ENTRY &&
	    TryLookupTableBeforeSchema(transaction, lookup_info, result)) {
		return result;
	}
	// as Catalog::TryLookupEntryInternal, which is private
	auto &full_path = lookup_info.GetQualifiedName().Path();
	vector<Identifier> schema_path(full_path.begin(), full_path.end() - 1);
	auto schema_lookup = EntryLookupInfo::SchemaLookup(lookup_info, std::move(schema_path));
	result.schema = LookupSchema(transaction, schema_lookup, OnEntryNotFound::RETURN_NULL);
	if (result.schema) {
		result.entry = result.schema->LookupEntry(transaction, lookup_info);
	}
	return result;
}

bool GlueCatalog::TryLookupTableBeforeSchema(CatalogTransaction transaction, const EntryLookupInfo &lookup_info,
                                             CatalogEntryLookup &result) {
	auto &full_path = lookup_info.GetQualifiedName().Path();
	auto path = DatabasePath(vector<Identifier>(full_path.begin(), full_path.end() - 1), GetName().GetIdentifierName());
	if (path.size() != 1) {
		return false;
	}
	// Glue keeps database names in lower case, as GetDatabase would name the schema
	GlueDatabaseInfo database;
	database.name = StringUtil::Lower(path[0]);
	if (schemas.GetLoadedEntry(database.name)) {
		return false;
	}
	auto schema = schemas.CreateSchemaEntry(database, false);
	auto entry = schema->LookupEntry(transaction, lookup_info);
	if (!entry) {
		// the table or its database is missing: GetDatabase tells which, without asking for the table again
		auto loaded = schemas.GetEntry(transaction.GetContext(), database.name);
		if (loaded) {
			result.schema = &loaded->Cast<SchemaCatalogEntry>();
		}
		return true;
	}
	auto added = schemas.AddEntryIfAbsent(std::move(schema));
	if (!added) {
		// loaded meanwhile: the regular lookup finds the table in that entry, from the cache
		return false;
	}
	result.schema = &added->Cast<SchemaCatalogEntry>();
	result.entry = entry;
	return true;
}

//! "<12-digit account>" or "<12-digit account>:..."
static bool StartsWithAccountId(const string &path) {
	constexpr auto num_digits_in_aws_account_id = 12;
	if (path.size() < num_digits_in_aws_account_id) {
		return false;
	}
	for (idx_t i = 0; i < num_digits_in_aws_account_id; i++) {
		if (!StringUtil::CharacterIsDigit(path[i])) {
			return false;
		}
	}
	return path.size() == num_digits_in_aws_account_id || path[num_digits_in_aws_account_id] == ':';
}

string GlueCatalog::KnownCatalogId() {
	{
		lock_guard<mutex> guard(reported_id_lock);
		if (!reported_catalog_id.empty()) {
			return reported_catalog_id;
		}
	}
	return StartsWithAccountId(options.catalog_id) ? options.catalog_id : string();
}

string GlueCatalog::FetchCatalogId(ClientContext &context) {
	{
		lock_guard<mutex> guard(reported_id_lock);
		if (!reported_catalog_id.empty()) {
			return reported_catalog_id;
		}
	}
	// outside the lock, so that a read (KnownCatalogId) never waits on a Glue call
	auto catalog_id = GlueAPI::GetReportedCatalogId(context, *this);
	lock_guard<mutex> guard(reported_id_lock);
	if (reported_catalog_id.empty()) {
		reported_catalog_id = catalog_id;
	}
	return reported_catalog_id;
}

void GlueCatalog::ThrowIfInExplicitTransaction(ClientContext &context) {
	if (!context.transaction.IsAutoCommit()) {
		throw TransactionException("This connection is currently in a transaction. Transaction support is not "
		                           "provided for Glue catalogs. Please call COMMIT, ABORT or ROLLBACK before trying "
		                           "the query again");
	}
}

GlueTable &GlueCatalog::GetHiveTableForDML(ClientContext &context, TableCatalogEntry &table, const char *statement,
                                           GlueTableInfo &table_info) {
	auto &glue_table = table.Cast<GlueTable>();
	// another engine may have converted the table since the entry was cached
	table_info = glue_table.RefreshTableInfo(context);
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
	if (glue_table.table_info.GetSerDe() == HiveSerDe::OPENX_JSON) {
		throw NotImplementedException("%s into Glue table '%s' is not supported: the table uses the OpenX JSON SerDe "
		                              "'%s', DuckDB writes JSON tables only with the Hive JsonSerDe "
		                              "(org.apache.hive.hcatalog.data.JsonSerDe)",
		                              statement, table.name.GetIdentifierName(), glue_table.table_info.serde_library);
	}
	return glue_table;
}

//===--------------------------------------------------------------------===//
// Planning
//===--------------------------------------------------------------------===//
PhysicalOperator &GlueCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                          optional_ptr<PhysicalOperator> plan) {
	ThrowIfInExplicitTransaction(context);
	GlueTableInfo table_info;
	auto &glue_table = GetHiveTableForDML(context, op.table, "INSERT", table_info);
	return GlueHiveInsert::PlanInsert(context, planner, op, glue_table, table_info, plan);
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
