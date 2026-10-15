#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

#include "core/glue_info.hpp"
#include "catalog/glue_table.hpp"
#include "catalog/glue_view.hpp"

namespace duckdb {
class GlueCatalog;
class GlueSchemaEntry;

//! The set of tables of a single Glue database, lazily loaded from Glue
class GlueTableSet {
public:
	explicit GlueTableSet(GlueSchemaEntry &schema);

public:
	//! The entry with this name whatever its type: callers check entry->type against what they need
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	//! As above; 'fetched_now' tells whether this call asked Glue for the definition (an entry already in the set,
	//! or a definition the query or the transaction cache already had, does not)
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup, bool &fetched_now);
	//! The entries of the given type (TABLE_ENTRY or VIEW_ENTRY)
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	//! Insert a (new) entry into the set, replacing any existing entry with the same name
	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry);
	void RemoveEntry(const string &name);

	//! Build the catalog entry for a Glue table definition: a GlueView for a VIRTUAL_VIEW, else a GlueTable
	unique_ptr<CatalogEntry> CreateEntry(const GlueTableInfo &table);

private:
	void LoadEntries(ClientContext &context);
	static void SetTableTypeTag(GlueTable &entry);
	//! Move an entry out of the set; the transaction manager keeps it alive while a running statement may use it.
	//! Called with entry_lock held.
	void RetireEntry(const string &name);

private:
	GlueSchemaEntry &schema;
	GlueCatalog &catalog;
	mutex entry_lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;
	bool is_loaded = false;
};

} // namespace duckdb
