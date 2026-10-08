#include "catalog/glue_schema_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"

namespace duckdb {

GlueSchemaSet::GlueSchemaSet(GlueCatalog &catalog) : catalog(catalog) {
}

unique_ptr<GlueSchemaEntry> GlueSchemaSet::CreateSchemaEntry(const GlueDatabaseInfo &database) {
	CreateSchemaInfo info;
	info.SetQualifiedName(
	    QualifiedName(info.GetQualifiedName().Catalog(), Identifier(database.name), info.GetQualifiedName().Name()));
	info.internal = false;
	return make_uniq<GlueSchemaEntry>(catalog, info, database);
}

void GlueSchemaSet::LoadEntries(ClientContext &context) {
	if (is_loaded) {
		return;
	}
	auto databases = GlueAPI::GetDatabases(context, catalog);
	for (auto &database : databases) {
		if (entries.find(database.name) != entries.end()) {
			// already loaded through a direct lookup
			continue;
		}
		entries.emplace(database.name, CreateSchemaEntry(database));
	}
	is_loaded = true;
}

optional_ptr<CatalogEntry> GlueSchemaSet::GetEntry(ClientContext &context, const string &name) {
	if (name.empty()) {
		return nullptr;
	}
	lock_guard<mutex> guard(entry_lock);
	auto entry = entries.find(name);
	if (entry != entries.end()) {
		return entry->second.get();
	}
	// not cached, ask Glue for this database directly
	GlueDatabaseInfo database;
	if (!GlueAPI::GetDatabase(context, catalog, name, database)) {
		return nullptr;
	}
	auto schema_entry = CreateSchemaEntry(database);
	auto result = entries.emplace(database.name, std::move(schema_entry));
	return result.first->second.get();
}

void GlueSchemaSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> guard(entry_lock);
	LoadEntries(context);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> GlueSchemaSet::CreateEntry(unique_ptr<GlueSchemaEntry> entry) {
	lock_guard<mutex> guard(entry_lock);
	auto name = entry->name.GetIdentifierName();
	entries.erase(name);
	auto result = entries.emplace(name, std::move(entry));
	return result.first->second.get();
}

void GlueSchemaSet::RemoveEntry(const string &name) {
	lock_guard<mutex> guard(entry_lock);
	entries.erase(name);
}

void GlueSchemaSet::InvalidateTableEntry(const string &schema_name, const string &table_name) {
	lock_guard<mutex> guard(entry_lock);
	auto entry = entries.find(schema_name);
	if (entry != entries.end()) {
		entry->second->tables.InvalidateEntry(table_name);
	}
}

void GlueSchemaSet::ClearEntries() {
	lock_guard<mutex> guard(entry_lock);
	entries.clear();
	is_loaded = false;
}

} // namespace duckdb
