#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"

#include "core/glue_info.hpp"
#include "catalog/glue_schema_entry.hpp"

namespace duckdb {
class GlueCatalog;

//! The schemas of a Glue catalog, lazily loaded from Glue: the databases of the attached catalog, and below them the
//! child catalogs of the attached catalog with their databases. A schema below the top is named by its path, the
//! names of its levels from the attached catalog down to its own.
class GlueSchemaSet {
public:
	explicit GlueSchemaSet(GlueCatalog &catalog);

public:
	//! Look up a database of the attached catalog by name. Loads the schema from Glue if it is not cached yet, returns
	//! nullptr if it does not exist
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);
	//! The same for the database 'path' names: its last level, in the child catalog the levels before it name
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const vector<string> &path);
	//! The child catalog 'path' names, asked from Glue unless it is known already; nullptr if there is none
	optional_ptr<CatalogEntry> GetChildCatalog(ClientContext &context, const vector<string> &path);
	//! The schema this set already holds, nullptr if it holds none of that name; never asks Glue. A schema found
	//! through a table lookup is held before its definition is loaded (GlueSchemaEntry::GetSchemaInfo)
	optional_ptr<CatalogEntry> GetCachedEntry(const string &name);
	optional_ptr<CatalogEntry> GetCachedEntry(const vector<string> &path);
	//! Insert 'entry' unless a schema of that path was loaded meanwhile; returns nullptr if one was
	optional_ptr<CatalogEntry> AddEntryIfAbsent(unique_ptr<GlueSchemaEntry> entry);
	//! Every schema: the databases of the attached catalog, and the schemas below them that are known
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	//! The known schemas directly below the schema 'path' names
	void ScanChildren(ClientContext &context, const vector<string> &path,
	                  const std::function<void(CatalogEntry &)> &callback);
	//! The known schemas below the top whose own name is 'name'
	void ScanNested(ClientContext &context, const string &name, const std::function<void(CatalogEntry &)> &callback);
	//! Insert a (new) schema entry into the set, replacing any existing entry with the same name
	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<GlueSchemaEntry> entry);
	void RemoveEntry(const string &name);

	//! Build a schema catalog entry from a Glue database definition, or from its name alone ('loaded' false)
	unique_ptr<GlueSchemaEntry> CreateSchemaEntry(const GlueDatabaseInfo &database, bool loaded = true);
	//! The same for the database 'path' names; nullptr if the id of its child catalog can not be told
	unique_ptr<GlueSchemaEntry> CreateSchemaEntry(ClientContext &context, const vector<string> &path,
	                                              const GlueDatabaseInfo &database, bool loaded = true);

private:
	//! A child catalog is held from the first lookup that names it, and listed once Glue confirmed it
	struct ChildCatalog {
		unique_ptr<GlueSchemaEntry> entry;
		bool known = false;
	};

	void LoadEntries(ClientContext &context);
	//! With LIST_CHILD_CATALOGS: every child catalog and its databases, asked from Glue once. Called with entry_lock
	//! held.
	void LoadChildCatalogs(ClientContext &context);
	//! Move an entry out of the set; the transaction manager keeps it, and the tables it caches, alive while a
	//! running statement may use them. Called with entry_lock held.
	void RetireEntry(const string &name);
	static string PathKey(const vector<string> &path);
	//! The entry of the child catalog 'path' names, nullptr if its id can not be told. Its parents are created with
	//! it. Never retired: the schemas below it point to it. Called with entry_lock held.
	optional_ptr<GlueSchemaEntry> ChildCatalogEntry(ClientContext &context, const vector<string> &path);
	//! Glue confirmed the child catalog 'path' names, and with it the ones above. Called with entry_lock held.
	void MarkKnown(const vector<string> &path);
	unique_ptr<GlueSchemaEntry> CreateSchemaEntryInternal(ClientContext &context, const vector<string> &path,
	                                                      const GlueDatabaseInfo &database, bool loaded);

private:
	GlueCatalog &catalog;
	mutex entry_lock;
	//! Databases by path: those of the attached catalog by their name
	case_insensitive_map_t<unique_ptr<GlueSchemaEntry>> entries;
	case_insensitive_map_t<ChildCatalog> child_catalogs;
	bool is_loaded = false;
	bool child_catalogs_loaded = false;
};

} // namespace duckdb
