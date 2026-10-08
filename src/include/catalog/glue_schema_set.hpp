#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"

#include "core/glue_info.hpp"
#include "catalog/glue_schema_entry.hpp"

namespace duckdb {
class GlueCatalog;

//! The set of databases of a Glue catalog, lazily loaded from Glue
class GlueSchemaSet {
public:
	explicit GlueSchemaSet(GlueCatalog &catalog);

public:
	//! Look up a schema by name. Loads the schema from Glue if it is not cached yet, returns nullptr if it does not
	//! exist
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	//! Insert a (new) schema entry into the set, replacing any existing entry with the same name
	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<GlueSchemaEntry> entry);
	void RemoveEntry(const string &name);
	void ClearEntries();
	//! Invalidate the cached entry of a table in a cached schema (see GlueTableSet::InvalidateEntry)
	void InvalidateTableEntry(const string &schema_name, const string &table_name);

	//! Build a schema catalog entry from a Glue database definition
	unique_ptr<GlueSchemaEntry> CreateSchemaEntry(const GlueDatabaseInfo &database);

private:
	void LoadEntries(ClientContext &context);

private:
	GlueCatalog &catalog;
	mutex entry_lock;
	case_insensitive_map_t<unique_ptr<GlueSchemaEntry>> entries;
	bool is_loaded = false;
};

} // namespace duckdb
