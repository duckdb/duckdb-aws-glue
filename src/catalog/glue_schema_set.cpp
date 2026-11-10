#include "catalog/glue_schema_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_entry_listing.hpp"
#include "catalog/glue_transaction.hpp"
#include "catalog/glue_transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

GlueSchemaSet::GlueSchemaSet(GlueCatalog &catalog) : catalog(catalog), id(GlueTransactionCache::NextId()) {
}

unique_ptr<GlueSchemaEntry> GlueSchemaSet::CreateSchemaEntry(const GlueDatabaseInfo &database, bool loaded) {
	CreateSchemaInfo info;
	info.SetQualifiedName(
	    QualifiedName(info.GetQualifiedName().Catalog(), Identifier(database.name), info.GetQualifiedName().Name()));
	info.internal = false;
	return make_uniq<GlueSchemaEntry>(catalog, info, database, loaded);
}

void GlueSchemaSet::LoadEntries(ClientContext &context, GlueTransactionCache &cache) {
	GlueEntryListing::Snapshot snapshot;
	{
		lock_guard<mutex> guard(entry_lock);
		if (cache.HasListed(id)) {
			return;
		}
		snapshot = GlueEntryListing::Take(entries);
	}
	auto databases = GlueAPI::GetDatabases(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	case_insensitive_set_t listed;
	for (auto &database : databases) {
		listed.insert(database.name);
		if (!GlueEntryListing::Unchanged(entries, snapshot, database.name)) {
			// another client changed the entry while Glue was asked: it knows more than the listing
			continue;
		}
		auto entry = entries.find(database.name);
		if (entry != entries.end()) {
			// already loaded through a direct lookup, possibly from a table lookup that only knew the name
			entry->second->SetSchemaInfo(database);
			continue;
		}
		entries.emplace(database.name, CreateSchemaEntry(database));
	}
	for (auto &name : GlueEntryListing::NotListed(entries, snapshot, listed)) {
		RetireEntry(name);
	}
	cache.MarkListed(id);
}

optional_ptr<CatalogEntry> GlueSchemaSet::GetEntry(ClientContext &context, const string &name) {
	if (name.empty()) {
		return nullptr;
	}
	// as in GlueTableSet::GetEntry, the reader's Glue transaction keeps a retired entry alive while it is used
	Transaction::Get(context, catalog);
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

optional_ptr<CatalogEntry> GlueSchemaSet::GetCachedEntry(const string &name) {
	lock_guard<mutex> guard(entry_lock);
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	return entry->second.get();
}

optional_ptr<CatalogEntry> GlueSchemaSet::AddEntryIfAbsent(unique_ptr<GlueSchemaEntry> entry) {
	lock_guard<mutex> guard(entry_lock);
	auto name = entry->name.GetIdentifierName();
	if (entries.find(name) != entries.end()) {
		return nullptr;
	}
	auto result = entries.emplace(name, std::move(entry));
	return result.first->second.get();
}

void GlueSchemaSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	// the reader's Glue transaction keeps the entries alive while they are used (GlueTransactionManager::RetireEntry)
	auto &transaction = GlueTransaction::Get(context, catalog);
	LoadEntries(context, *transaction.cache);
	lock_guard<mutex> guard(entry_lock);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> GlueSchemaSet::CreateEntry(unique_ptr<GlueSchemaEntry> entry) {
	lock_guard<mutex> guard(entry_lock);
	auto name = entry->name.GetIdentifierName();
	RetireEntry(name);
	auto result = entries.emplace(name, std::move(entry));
	return result.first->second.get();
}

void GlueSchemaSet::RemoveEntry(const string &name) {
	lock_guard<mutex> guard(entry_lock);
	RetireEntry(name);
}

void GlueSchemaSet::RetireEntry(const string &name) {
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return;
	}
	catalog.GetAttached().GetTransactionManager().Cast<GlueTransactionManager>().RetireEntry(std::move(entry->second));
	entries.erase(entry);
}

} // namespace duckdb
