#include "catalog/glue_schema_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

GlueSchemaSet::GlueSchemaSet(GlueCatalog &catalog) : catalog(catalog) {
}

static CreateSchemaInfo SchemaInfoForName(const string &name) {
	CreateSchemaInfo info;
	info.SetQualifiedName(
	    QualifiedName(info.GetQualifiedName().Catalog(), Identifier(name), info.GetQualifiedName().Name()));
	info.internal = false;
	return info;
}

string GlueSchemaSet::PathKey(const vector<string> &path) {
	// no name holds the unit separator
	return StringUtil::Join(path, "\x1f");
}

unique_ptr<GlueSchemaEntry> GlueSchemaSet::CreateSchemaEntry(const GlueDatabaseInfo &database, bool loaded) {
	auto info = SchemaInfoForName(database.name);
	return make_uniq<GlueSchemaEntry>(catalog, info, database, loaded);
}

unique_ptr<GlueSchemaEntry> GlueSchemaSet::CreateSchemaEntry(ClientContext &context, const vector<string> &path,
                                                             const GlueDatabaseInfo &database, bool loaded) {
	if (path.size() < 2) {
		return CreateSchemaEntry(database, loaded);
	}
	lock_guard<mutex> guard(entry_lock);
	return CreateSchemaEntryInternal(context, path, database, loaded);
}

unique_ptr<GlueSchemaEntry> GlueSchemaSet::CreateSchemaEntryInternal(ClientContext &context, const vector<string> &path,
                                                                     const GlueDatabaseInfo &database, bool loaded) {
	auto parent = ChildCatalogEntry(context, vector<string>(path.begin(), path.end() - 1));
	if (!parent) {
		return nullptr;
	}
	auto info = SchemaInfoForName(database.name);
	return make_uniq<GlueSchemaEntry>(catalog, info, database, loaded, parent->GetCatalogId(), parent.get());
}

optional_ptr<GlueSchemaEntry> GlueSchemaSet::ChildCatalogEntry(ClientContext &context, const vector<string> &path) {
	auto key = PathKey(path);
	auto existing = child_catalogs.find(key);
	if (existing != child_catalogs.end()) {
		return existing->second.entry.get();
	}
	string catalog_id;
	if (!catalog.TryGetChildCatalogId(context, path, catalog_id)) {
		return nullptr;
	}
	optional_ptr<GlueSchemaEntry> parent;
	if (path.size() > 1) {
		parent = ChildCatalogEntry(context, vector<string>(path.begin(), path.end() - 1));
		if (!parent) {
			return nullptr;
		}
	}
	// Glue keeps catalog names in lower case
	auto info = SchemaInfoForName(StringUtil::Lower(path.back()));
	ChildCatalog child;
	child.entry = make_uniq<GlueSchemaEntry>(catalog, info, std::move(catalog_id), parent.get());
	return child_catalogs.emplace(key, std::move(child)).first->second.entry.get();
}

void GlueSchemaSet::MarkKnown(const vector<string> &path) {
	vector<string> prefix;
	for (auto &level : path) {
		prefix.push_back(level);
		auto child = child_catalogs.find(PathKey(prefix));
		if (child != child_catalogs.end()) {
			child->second.known = true;
		}
	}
}

void GlueSchemaSet::LoadEntries(ClientContext &context) {
	if (is_loaded) {
		return;
	}
	auto databases = GlueAPI::GetDatabases(context, catalog, GlueAPI::OwnCatalog());
	for (auto &database : databases) {
		auto entry = entries.find(database.name);
		if (entry != entries.end()) {
			// already loaded through a direct lookup, possibly from a table lookup that only knew the name
			entry->second->SetSchemaInfo(database);
			continue;
		}
		entries.emplace(database.name, CreateSchemaEntry(database));
	}
	is_loaded = true;
}

void GlueSchemaSet::LoadChildCatalogs(ClientContext &context) {
	if (child_catalogs_loaded || !catalog.options.list_child_catalogs) {
		return;
	}
	for (auto &info : GlueAPI::GetCatalogs(context, catalog, GlueAPI::OwnCatalog())) {
		vector<string> path;
		if (!catalog.TryGetChildCatalogPath(context, info.catalog_id, path)) {
			continue;
		}
		auto child = ChildCatalogEntry(context, path);
		if (!child) {
			continue;
		}
		child->SetCatalogInfo(info);
		MarkKnown(path);
		vector<GlueDatabaseInfo> databases;
		try {
			databases = GlueAPI::GetDatabases(context, catalog, child->GetCatalogId());
		} catch (const GlueException &ex) {
			// a child catalog whose databases we may not list must not fail listing the others
			if (!ex.access_denied) {
				throw;
			}
			ErrorData error(ex);
			DUCKDB_LOG_WARNING(context, "Not listing the databases of Glue catalog '%s': %s", child->GetCatalogId(),
			                   error.RawMessage());
			continue;
		}
		for (auto &database : databases) {
			auto database_path = path;
			database_path.push_back(database.name);
			auto key = PathKey(database_path);
			auto entry = entries.find(key);
			if (entry != entries.end()) {
				entry->second->SetSchemaInfo(database);
				continue;
			}
			entries.emplace(key, CreateSchemaEntryInternal(context, database_path, database, true));
		}
	}
	child_catalogs_loaded = true;
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
	if (!GlueAPI::GetDatabase(context, catalog, GlueAPI::OwnCatalog(), name, database)) {
		return nullptr;
	}
	auto schema_entry = CreateSchemaEntry(database);
	auto result = entries.emplace(database.name, std::move(schema_entry));
	return result.first->second.get();
}

optional_ptr<CatalogEntry> GlueSchemaSet::GetEntry(ClientContext &context, const vector<string> &path) {
	if (path.empty()) {
		return nullptr;
	}
	if (path.size() == 1) {
		return GetEntry(context, path[0]);
	}
	Transaction::Get(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	auto entry = entries.find(PathKey(path));
	if (entry != entries.end()) {
		return entry->second.get();
	}
	vector<string> parent_path(path.begin(), path.end() - 1);
	auto parent = ChildCatalogEntry(context, parent_path);
	if (!parent) {
		return nullptr;
	}
	GlueDatabaseInfo database;
	if (!GlueAPI::GetDatabase(context, catalog, parent->GetCatalogId(), path.back(), database)) {
		return nullptr;
	}
	// a database Glue returned proves the catalogs above it
	MarkKnown(parent_path);
	auto schema_entry = CreateSchemaEntryInternal(context, path, database, true);
	parent_path.push_back(database.name);
	auto result = entries.emplace(PathKey(parent_path), std::move(schema_entry));
	return result.first->second.get();
}

optional_ptr<CatalogEntry> GlueSchemaSet::GetChildCatalog(ClientContext &context, const vector<string> &path) {
	if (path.empty()) {
		return nullptr;
	}
	Transaction::Get(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	auto existing = child_catalogs.find(PathKey(path));
	if (existing != child_catalogs.end() && existing->second.known) {
		return existing->second.entry.get();
	}
	string catalog_id;
	if (!catalog.TryGetChildCatalogId(context, path, catalog_id)) {
		return nullptr;
	}
	GlueCatalogInfo info;
	try {
		if (!GlueAPI::GetCatalog(context, catalog, catalog_id, info)) {
			return nullptr;
		}
	} catch (const GlueException &ex) {
		// Reached after a database of this name was not found. A Glue endpoint without GetCatalog, or a role that
		// may not call it, has no child catalog to offer: the name stays not found.
		ErrorData error(ex);
		DUCKDB_LOG_DEBUG(context, "Not looking for a child catalog '%s': %s", catalog_id, error.RawMessage());
		return nullptr;
	}
	auto entry = ChildCatalogEntry(context, path);
	if (!entry) {
		return nullptr;
	}
	entry->SetCatalogInfo(info);
	MarkKnown(path);
	return entry.get();
}

optional_ptr<CatalogEntry> GlueSchemaSet::GetCachedEntry(const string &name) {
	lock_guard<mutex> guard(entry_lock);
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	return entry->second.get();
}

optional_ptr<CatalogEntry> GlueSchemaSet::GetCachedEntry(const vector<string> &path) {
	return GetCachedEntry(PathKey(path));
}

optional_ptr<CatalogEntry> GlueSchemaSet::AddEntryIfAbsent(unique_ptr<GlueSchemaEntry> entry) {
	lock_guard<mutex> guard(entry_lock);
	auto path = entry->GetPath();
	auto key = PathKey(path);
	if (entries.find(key) != entries.end()) {
		return nullptr;
	}
	path.pop_back();
	MarkKnown(path);
	auto result = entries.emplace(key, std::move(entry));
	return result.first->second.get();
}

void GlueSchemaSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	Transaction::Get(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	LoadEntries(context);
	LoadChildCatalogs(context);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
	for (auto &child : child_catalogs) {
		// a database and a child catalog of one name are one schema, listed as the database
		if (child.second.known && entries.find(child.first) == entries.end()) {
			callback(*child.second.entry);
		}
	}
}

void GlueSchemaSet::ScanChildren(ClientContext &context, const vector<string> &path,
                                 const std::function<void(CatalogEntry &)> &callback) {
	Transaction::Get(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	LoadChildCatalogs(context);
	auto prefix = StringUtil::Lower(PathKey(path)) + "\x1f";
	auto is_child = [&](const string &key) {
		return key.size() > prefix.size() && StringUtil::StartsWith(StringUtil::Lower(key), prefix) &&
		       key.find('\x1f', prefix.size()) == string::npos;
	};
	for (auto &entry : entries) {
		if (is_child(entry.first)) {
			callback(*entry.second);
		}
	}
	for (auto &child : child_catalogs) {
		if (child.second.known && is_child(child.first) && entries.find(child.first) == entries.end()) {
			callback(*child.second.entry);
		}
	}
}

void GlueSchemaSet::ScanNested(ClientContext &context, const string &name,
                               const std::function<void(CatalogEntry &)> &callback) {
	Transaction::Get(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	LoadChildCatalogs(context);
	auto suffix = "\x1f" + StringUtil::Lower(name);
	for (auto &entry : entries) {
		if (StringUtil::EndsWith(StringUtil::Lower(entry.first), suffix)) {
			callback(*entry.second);
		}
	}
	for (auto &child : child_catalogs) {
		auto key = StringUtil::Lower(child.first);
		if (child.second.known && entries.find(child.first) == entries.end() &&
		    (key == StringUtil::Lower(name) || StringUtil::EndsWith(key, suffix))) {
			callback(*child.second.entry);
		}
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
