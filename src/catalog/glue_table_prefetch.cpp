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
}

bool GlueTablePrefetchState::TakeSchemasToPrefetch(const string &current, vector<string> &names) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (++scanned_table_scans < 2) {
		return false;
	}
	auto position = std::find(scanned_schemas.begin(), scanned_schemas.end(), current);
	if (position == scanned_schemas.end()) {
		return false;
	}
	names.assign(position, scanned_schemas.end());
	return true;
}

void GlueTablePrefetchState::AddPrefetched(const vector<string> &names, idx_t generation) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	for (auto &name : names) {
		prefetched.push_back(GluePrefetchedListing {name, generation});
	}
}

vector<GluePrefetchedListing> GlueTablePrefetchState::Reset() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	scanned_schemas.clear();
	scanned_table_scans = 0;
	vector<GluePrefetchedListing> result;
	std::swap(result, prefetched);
	return result;
}

GlueTablePrefetch::GlueTablePrefetch(GlueCatalog &catalog) : catalog(catalog) {
}

void GlueTablePrefetch::RememberScannedSchemas(ClientContext &context, vector<string> names) {
	auto cache = GlueTransactionCache::Of(context, catalog);
	if (!cache) {
		return;
	}
	// in the order the catalog scans visit them (Catalog::GetAllSchemas)
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
	vector<string> names;
	// the first database is listed on its own, so that a statement on one database does not list the others
	if (!cache->prefetch_state.TakeSchemasToPrefetch(schema_name, names)) {
		return;
	}
	lock_guard<mutex> guard(lock);
	// taken before listing: a write meanwhile makes the listing stale
	auto listing_generation = Generation();
	auto &schemas = catalog.GetSchemas();
	vector<string> to_list;
	for (auto &name : names) {
		if (to_list.size() >= parallelism) {
			break;
		}
		auto entry = schemas.GetCachedEntry(name);
		bool needs_listing = entry && entry->Cast<GlueSchemaEntry>().tables.NeedsListing(listing_generation);
		if (to_list.empty() && !needs_listing) {
			return;
		}
		if (needs_listing) {
			to_list.push_back(name);
		}
	}
	if (to_list.size() < 2) {
		return;
	}
	auto listed = GlueAPI::GetTablesOfDatabases(context, catalog, to_list, parallelism);
	cache->prefetch_state.AddPrefetched(to_list, listing_generation);
	for (idx_t i = 0; i < to_list.size(); i++) {
		auto entry = schemas.GetCachedEntry(to_list[i]);
		if (listed[i].fetched && entry) {
			entry->Cast<GlueSchemaEntry>().tables.SetListing(listing_generation, std::move(listed[i].tables));
		}
	}
}

void GlueTablePrefetch::EndTransaction(GlueTablePrefetchState &state) {
	auto &schemas = catalog.GetSchemas();
	for (auto &listing : state.Reset()) {
		auto entry = schemas.GetCachedEntry(listing.database_name);
		if (entry) {
			entry->Cast<GlueSchemaEntry>().tables.DropListing(listing.generation);
		}
	}
}

} // namespace duckdb
