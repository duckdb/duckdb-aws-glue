#include "catalog/glue_table_set.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include "core/glue_types.hpp"
#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_transaction.hpp"
#include "catalog/glue_transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

GlueTableSet::GlueTableSet(GlueSchemaEntry &schema) : schema(schema), catalog(schema.catalog.Cast<GlueCatalog>()) {
}

unique_ptr<CatalogEntry> GlueTableSet::CreateEntry(const GlueTableInfo &table) {
	if (table.IsView()) {
		return GlueView::FromTableInfo(catalog, schema, table);
	}
	CreateTableInfo info(schema, Identifier(table.name));
	if (!table.description.empty()) {
		// So duckdb_tables().comment shows what Glue stores as Table.Description
		info.comment = Value(table.description);
	}
	auto add_column = [&](const GlueColumn &column) {
		ColumnDefinition definition(Identifier(column.name), GlueTypes::ToLogicalType(column.type));
		if (!column.comment.empty()) {
			definition.SetComment(Value(column.comment));
		}
		info.columns.AddColumn(std::move(definition));
	};
	for (auto &column : table.columns) {
		add_column(column);
	}
	// Hive tables store their partition columns separately, they are regular (trailing) columns for a scan
	for (auto &column : table.partition_keys) {
		add_column(column);
	}
	auto entry = make_uniq<GlueTable>(catalog, schema, info, table);
	SetTableTypeTag(*entry);
	return entry;
}

void GlueTableSet::SetTableTypeTag(GlueTable &entry) {
	// exposed through duckdb_tables().tags['table_type'] (ICEBERG / DELTA / HUDI / SYMLINK / HIVE / UNKNOWN)
	entry.tags["table_type"] = entry.table_info.GetFormat().ToString();
}

bool GlueTableSet::BatchTableLookups(ClientContext &context) {
	Value setting;
	if (context.TryGetCurrentSetting("glue_batch_table_lookup", setting) && !setting.IsNull()) {
		return BooleanValue::Get(setting);
	}
	return true;
}

void GlueTableSet::LoadEntries(ClientContext &context) {
	if (is_loaded) {
		return;
	}
	auto tables = GlueAPI::GetTables(context, catalog, schema.database_info.name);
	auto cache = GlueTransactionCache::Of(context, catalog);
	for (auto &table : tables) {
		if (cache) {
			// Without this the per reference GetEntry re-fetches every table this one request
			// already returned, which makes the bulk round trip pure added cost
			shared_ptr<const GlueTableInfo> info = make_shared_ptr<const GlueTableInfo>(table);
			cache->GetTable(schema.database_info.name, table.name, [&info]() { return info; });
		}
		auto existing = entries.find(table.name);
		if (existing != entries.end() && GetTableInfo(*existing->second).IsSameVersion(table)) {
			// already loaded through a direct lookup, and still what Glue holds
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
		RetireEntry(table.name);
		entries.emplace(table.name, std::move(entry));
	}
	is_loaded = true;
}

void GlueTableSet::BulkLoadTableInfo(ClientContext &context, GlueTransactionCache &cache) {
	// Both outcomes reset the round-trip count: on success the page is warm and later misses need no second
	// page; on failure the next bulk attempt waits for another direct round trip first, so a throttled or
	// failing GetTables costs at most one extra request per direct lookup while the per-table path keeps working.
	direct_lookups = 0;
	bool has_more = false;
	vector<GlueTableInfo> tables;
	try {
		tables = GlueAPI::GetTables(context, catalog, schema.database_info.name, true, &has_more);
	} catch (const GlueException &ex) {
		// A failed bulk fetch must not fail a lookup that never needed GetTables: log it and let the caller
		// fall through to the direct GetTable
		if (ex.access_denied) {
			batch_forbidden = true;
		}
		ErrorData error(ex);
		DUCKDB_LOG_WARNING(context, "Not batching the table lookups of Glue database '%s', GetTables failed: %s",
		                   schema.database_info.name, error.RawMessage());
		return;
	}
	// One page per cache instance: misses past the page (a database larger than one page) stay direct instead
	// of re-reading the same first page
	bulk_loaded_cache_id = cache.Id();
	for (auto &table : tables) {
		// Warm the metadata cache only - already-cached values win; the entry/Scan story is on the declaration
		shared_ptr<const GlueTableInfo> info = make_shared_ptr<const GlueTableInfo>(table);
		cache.GetTable(schema.database_info.name, table.name, [&info]() { return info; });
	}
	if (has_more) {
		DUCKDB_LOG_INFO(context,
		                "Glue database '%s' holds more tables than one GetTables page: read %llu, the "
		                "remaining lookups stay per table",
		                schema.database_info.name, NumericCast<uint64_t>(tables.size()));
	}
}

optional_ptr<CatalogEntry> GlueTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	auto &name = lookup.GetEntryName();
	// the reader's Glue transaction keeps the entry alive while it is used (GlueTransactionManager::RetireEntry); it
	// is started before entry_lock is taken, the order RetireEntry takes the locks in
	Transaction::Get(context, catalog);
	auto cache = GlueTransactionCache::Of(context, catalog);
	bool cached;
	string glue_name;
	{
		lock_guard<mutex> guard(entry_lock);
		auto existing = entries.find(name);
		cached = existing != entries.end();
		// the lookup name may differ in case from the one Glue knows
		glue_name = cached ? existing->second->name.GetIdentifierName() : name;
		// The attach's first round-trip miss stays direct: a lone lookup must not enumerate a large database.
		// From the second round-trip-causing miss on (round trips count across cache instances), one GetTables
		// page warms the cache, at most once per instance - under the default scope that is once per transaction.
		if (!cached && cache && !batch_forbidden && cache->Id() != bulk_loaded_cache_id && direct_lookups > 0 &&
		    BatchTableLookups(context)) {
			BulkLoadTableInfo(context, *cache);
		}
	}
	bool asked_glue = false;
	auto load = [&]() -> shared_ptr<const GlueTableInfo> {
		asked_glue = true;
		GlueTableInfo table;
		if (!GlueAPI::GetTable(context, catalog, schema.database_info.name, glue_name, table)) {
			return nullptr;
		}
		return make_shared_ptr<const GlueTableInfo>(std::move(table));
	};
	// the definition Glue holds, fetched once per transaction (GlueTransactionCache) and outside entry_lock, so that a
	// Glue round trip does not hold up the lookups of other tables
	auto table = cache ? cache->GetTable(schema.database_info.name, glue_name, load) : load();
	lock_guard<mutex> guard(entry_lock);
	if (!cached && asked_glue) {
		// counts the round trips of misses, not lookups: a cache hit (including a bulk-warmed one) costs nothing and
		// must not count towards switching to bulk
		direct_lookups++;
	}
	if (!table) {
		RetireEntry(glue_name);
		return nullptr;
	}
	// another lookup may have replaced the entry while Glue was asked: compare with what is there now
	auto existing = entries.find(table->name);
	if (existing != entries.end() && GetTableInfo(*existing->second).IsSameVersion(*table)) {
		return existing->second.get();
	}
	// new, or another client changed it since the entry was built: a statement that bound the old entry keeps it
	RetireEntry(table->name);
	auto result = entries.emplace(table->name, CreateEntry(*table));
	return result.first->second.get();
}

const GlueTableInfo &GlueTableSet::GetTableInfo(const CatalogEntry &entry) {
	if (entry.type == CatalogType::VIEW_ENTRY) {
		return entry.Cast<GlueView>().table_info;
	}
	return entry.Cast<GlueTable>().table_info;
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
