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
class GlueTransactionCache;

//! The set of tables of a single Glue database, lazily loaded from Glue
class GlueTableSet {
public:
	explicit GlueTableSet(GlueSchemaEntry &schema);

public:
	//! The entry with this name whatever its type (callers check entry->type against what they need), replaced when
	//! Glue holds another version of it and gone when Glue no longer has it
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	//! The entries of the given type (TABLE_ENTRY or VIEW_ENTRY), listed from Glue once per metadata cache id
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	//! The entries already in the set, without listing Glue. Runs without a catalog transaction, so the callback
	//! must not keep a reference to an entry past the call.
	void ScanLoaded(const std::function<void(CatalogEntry &)> &callback);
	//! Insert a (new) entry into the set, replacing any existing entry with the same name
	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry);
	void RemoveEntry(const string &name);

	//! Build the catalog entry for a Glue table definition: a GlueView for a VIRTUAL_VIEW, else a GlueTable
	unique_ptr<CatalogEntry> CreateEntry(const GlueTableInfo &table);
	//! The definition a table or view entry was built from
	static const GlueTableInfo &GetTableInfo(const CatalogEntry &entry);

private:
	void LoadEntries(ClientContext &context);
	//! One first-page GetTables warming the metadata cache, instead of a GetTable per table. Only warms the
	//! cache: entries stay per referenced table and Scan still lists, so neither outlives the page's truth.
	void BulkLoadTableInfo(ClientContext &context, GlueTransactionCache &cache);
	static bool BatchTableLookups(ClientContext &context);
	static void SetTableTypeTag(GlueTable &entry);
	//! Move an entry out of the set; the transaction manager keeps it alive while a running statement may use it.
	//! Called with entry_lock held.
	void RetireEntry(const string &name);

private:
	GlueSchemaEntry &schema;
	GlueCatalog &catalog;
	mutex entry_lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;
	//! The GlueTransactionCache::Id() the entries were last listed under (0 = never listed)
	idx_t listed_cache_id = 0;
	//! Direct-lookup round trips since the last bulk fetch: bulk pays off from the second one on
	idx_t direct_lookups = 0;
	//! The GlueTransactionCache::Id() the last successful bulk fetch warmed (0 = none yet)
	idx_t bulk_loaded_cache_id = 0;
	//! GetTables was refused for lack of permission: no more bulk fetches or listings
	bool get_tables_forbidden = false;
};

} // namespace duckdb
