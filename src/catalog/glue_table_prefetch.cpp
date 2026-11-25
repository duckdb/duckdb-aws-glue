#include "catalog/glue_table_prefetch.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_transaction.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>

namespace duckdb {

static constexpr idx_t GLUE_MAX_LIST_TABLES_PARALLELISM = 32;

void GlueTablePrefetchState::SetScannedSchemas(vector<string> names) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	scanned_schemas = std::move(names);
	scanned_table_scans = 0;
	in_window.clear();
}

vector<string> GlueTablePrefetchState::TakeWindow(const string &current, idx_t window) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	vector<string> names;
	if (++scanned_table_scans < 2 || in_window.count(current)) {
		return names;
	}
	auto position = std::find(scanned_schemas.begin(), scanned_schemas.end(), current);
	for (; position != scanned_schemas.end() && names.size() < window; ++position) {
		in_window.insert(*position);
		names.push_back(*position);
	}
	return names;
}

void GlueTablePrefetchState::Reset() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	scanned_schemas.clear();
	scanned_table_scans = 0;
	in_window.clear();
}

GlueTablePrefetch::GlueTablePrefetch(GlueCatalog &catalog) : catalog(catalog) {
}

void GlueTablePrefetch::RememberScannedSchemas(ClientContext &context, vector<string> names) {
	auto cache = GlueTransactionCache::Of(context, catalog);
	if (!cache) {
		return;
	}
	// the order Catalog::GetAllSchemas visits them in
	std::sort(names.begin(), names.end(),
	          [](const string &left, const string &right) { return Identifier(left) < Identifier(right); });
	cache->prefetch_state.SetScannedSchemas(std::move(names));
}

void GlueTablePrefetch::PrefetchTables(ClientContext &context, const string &schema_name) {
	Value setting;
	idx_t parallelism = 1;
	if (context.TryGetCurrentSetting("glue_list_tables_parallelism", setting) && !setting.IsNull()) {
		parallelism = setting.GetValue<idx_t>();
	}
	auto cache = GlueTransactionCache::Of(context, catalog);
	if (parallelism <= 1 || !cache) {
		return;
	}
	parallelism = MinValue<idx_t>(parallelism, GLUE_MAX_LIST_TABLES_PARALLELISM);
	// The transaction's own state, held only while the window is chosen: the Glue calls below run without a lock, so
	// another connection's scan does not wait for them, and this scan can be interrupted
	auto window = cache->prefetch_state.TakeWindow(schema_name, parallelism);
	auto &schemas = catalog.GetSchemas();
	vector<string> to_list;
	vector<glue_entry_set_id_t> set_ids;
	for (auto &name : window) {
		auto entry = schemas.GetCachedEntry(name);
		if (!entry) {
			continue;
		}
		auto set_id = entry->Cast<GlueSchemaEntry>().tables.Id();
		// Claimed, so that a scan reaching it meanwhile waits for this listing and does not repeat it
		if (cache->TryBeginPrefetch(set_id)) {
			to_list.push_back(name);
			set_ids.push_back(set_id);
		}
	}
	if (to_list.empty()) {
		return;
	}
	vector<GlueDatabaseTables> listed;
	try {
		listed = GlueAPI::GetTablesOfDatabases(context, catalog, to_list, parallelism);
	} catch (...) {
		for (auto set_id : set_ids) {
			cache->EndPrefetch(set_id, nullptr);
		}
		throw;
	}
	for (idx_t i = 0; i < to_list.size(); i++) {
		cache->EndPrefetch(set_ids[i], listed[i].fetched ? &listed[i].tables : nullptr);
	}
}

} // namespace duckdb
