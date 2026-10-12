#include "catalog/glue_table_set.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include "core/glue_types.hpp"
#include "api/glue_api.hpp"
#include "api/glue_api_util.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_table_loader.hpp"
#include "catalog/glue_transaction.hpp"
#include "catalog/glue_transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <aws/core/utils/json/JsonSerializer.h>

namespace duckdb {

GlueTableSet::GlueTableSet(GlueSchemaEntry &schema) : schema(schema), catalog(schema.catalog.Cast<GlueCatalog>()) {
}

unique_ptr<CatalogEntry> GlueTableSet::CreateEntry(const GlueTableInfo &table) {
	if (table.IsView()) {
		return GlueView::FromTableInfo(catalog, schema, table);
	}
	CreateTableInfo info(schema, Identifier(table.name));
	for (auto &column : table.columns) {
		info.columns.AddColumn(ColumnDefinition(Identifier(column.name), GlueTypes::ToLogicalType(column.type)));
	}
	// Hive tables store their partition columns separately, they are regular (trailing) columns for a scan
	for (auto &column : table.partition_keys) {
		info.columns.AddColumn(ColumnDefinition(Identifier(column.name), GlueTypes::ToLogicalType(column.type)));
	}
	auto entry = make_uniq<GlueTable>(catalog, schema, info, table);
	SetTableTypeTag(*entry);
	return entry;
}

void GlueTableSet::SetTableTypeTag(GlueTable &entry) {
	// exposed through duckdb_tables().tags['table_type'] (ICEBERG / DELTA / HIVE / UNKNOWN)
	entry.tags["table_type"] = GlueTableFormatToString(entry.table_info.GetFormat());
}

void GlueTableSet::LoadEntries(ClientContext &context) {
	if (is_loaded) {
		return;
	}
	auto tables = GlueAPI::GetTables(context, catalog, schema.database_info.name);
	for (auto &table : tables) {
		if (entries.find(table.name) != entries.end()) {
			// already loaded through a direct lookup
			continue;
		}
		unique_ptr<CatalogEntry> entry;
		try {
			entry = CreateEntry(table);
		} catch (std::exception &ex) {
			// A table whose Glue definition we can not turn into a DuckDB table (e.g. an unsupported column type)
			// must not break listing the other tables: leave it out and log why. Looking the table up by name
			// still reports the error to the user.
			ErrorData error(ex);
			DUCKDB_LOG_ERROR(context, "Glue table '%s.%s' is not listed: %s", schema.database_info.name, table.name,
			                 error.RawMessage());
			continue;
		}
		entries.emplace(table.name, std::move(entry));
	}
	is_loaded = true;
}

optional_ptr<CatalogEntry> GlueTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	if (catalog.options.governance) {
		auto loaded = GetGovernedEntry(context, lookup.GetEntryName());
		return loaded ? &loaded->governed_entry.get() : nullptr;
	}
	auto &name = lookup.GetEntryName();
	// the reader's Glue transaction keeps the entry alive while it is used (GlueTransactionManager::RetireEntry); it
	// is started before entry_lock is taken, the order RetireEntry takes the locks in
	Transaction::Get(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	auto entry = entries.find(name);
	if (entry != entries.end()) {
		return entry->second.get();
	}
	// not cached, ask Glue for this table directly
	GlueTableInfo table;
	if (!GlueAPI::GetTable(context, catalog, schema.database_info.name, name, table)) {
		return nullptr;
	}
	auto result = entries.emplace(table.name, CreateEntry(table));
	return result.first->second.get();
}

static GlueLoadGovernedTableRequest GetLoadGovernedTableRequest(ClientContext &context, GlueCatalog &catalog,
                                                                const string &database_name, const string &table_name) {
	auto credentials = GetClientCredentials(context, catalog);
	GlueLoadGovernedTableRequest request;
	request.catalog_name = catalog.options.name;
	request.catalog_id = catalog.options.catalog_id;
	request.database_name = database_name;
	request.table_name = table_name;
	request.region = catalog.options.region;
	request.key_id = ToStdString(credentials.GetAWSAccessKeyId());
	request.secret = ToStdString(credentials.GetAWSSecretKey());
	request.session_token = ToStdString(credentials.GetSessionToken());
	return request;
}

optional_ptr<GovernedCatalogEntry> GlueTableSet::GetGovernedEntry(ClientContext &context, const string &name) {
	auto &transaction = GlueTransaction::Get(context, catalog);
	auto &database_name = schema.database_info.name;
	auto &governed_entries = transaction.governed_entries[database_name];
	auto loaded = governed_entries.find(name);
	if (loaded != governed_entries.end()) {
		return &loaded->second;
	}
	// Never fall back to GetTable, which bypasses the loader and the permissions it enforces
	auto loader = GovernedGlueTableLoader::Find(context);
	if (!loader) {
		throw InvalidConfigurationException(
		    "Catalog \"%s\" is attached with GOVERNANCE true, but no extension registered a GovernedGlueTableLoader",
		    catalog.GetName().GetIdentifierName());
	}
	auto request = GetLoadGovernedTableRequest(context, catalog, database_name, name);
	unique_ptr<CatalogEntry> raw;
	auto build_entry = [&](const string &table_json) -> CatalogEntry & {
		Aws::Utils::Json::JsonValue json(Aws::String(table_json.c_str(), table_json.size()));
		auto table = ToTableInfo(Aws::Glue::Model::Table(json));
		raw = CreateEntry(table);
		return *raw;
	};
	auto governed_entry = loader->Load(context, request, build_entry);
	if (!governed_entry) {
		return nullptr;
	}
	return &governed_entries.emplace(name, GovernedCatalogEntry {std::move(raw), *governed_entry}).first->second;
}

void GlueTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	// tables and views share the set and a scan of either type yields both, as in DuckDB's own schema: the catalog
	// functions filter by entry type themselves, and duckdb_columns() collects the columns of views through a
	// TABLE_ENTRY scan
	Transaction::Get(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	LoadEntries(context);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> GlueTableSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> guard(entry_lock);
	auto name = entry->name.GetIdentifierName();
	RetireEntry(name);
	auto result = entries.emplace(name, std::move(entry));
	return result.first->second.get();
}

void GlueTableSet::RemoveEntry(const string &name) {
	lock_guard<mutex> guard(entry_lock);
	RetireEntry(name);
}

void GlueTableSet::RetireEntry(const string &name) {
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return;
	}
	catalog.GetAttached().GetTransactionManager().Cast<GlueTransactionManager>().RetireEntry(std::move(entry->second));
	entries.erase(entry);
}

} // namespace duckdb
