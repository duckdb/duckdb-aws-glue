#include "planning/hive_query_cache.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"
#include "core/glue_log_types.hpp"
#include "planning/hive_multi_file_reader.hpp"

#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"

#include <chrono>

namespace duckdb {

//! glue_partition_load_parallelism when unset
static constexpr idx_t DEFAULT_PARTITION_LOAD_PARALLELISM = 8;
//! glue_partition_load_cooldown when unset, in seconds
static constexpr idx_t DEFAULT_PARTITION_LOAD_COOLDOWN = 300;

static idx_t PartitionLoadParallelism(ClientContext &context) {
	Value setting;
	if (context.TryGetCurrentSetting("glue_partition_load_parallelism", setting) && !setting.IsNull()) {
		return setting.GetValue<idx_t>();
	}
	return DEFAULT_PARTITION_LOAD_PARALLELISM;
}

static idx_t PartitionLoadCooldownSeconds(ClientContext &context) {
	Value setting;
	if (context.TryGetCurrentSetting("glue_partition_load_cooldown", setting) && !setting.IsNull()) {
		return setting.GetValue<idx_t>();
	}
	return DEFAULT_PARTITION_LOAD_COOLDOWN;
}

namespace {

//! When Glue throttled a parallel load, by CatalogIdentity: process-wide, as every connection shares Glue's rate
class ThrottledCatalogs {
public:
	static ThrottledCatalogs &Get() {
		static ThrottledCatalogs instance;
		return instance;
	}
	void Throttled(const string &catalog) {
		lock_guard<mutex> guard(lock);
		since[catalog] = std::chrono::steady_clock::now();
	}
	//! Whether 'catalog' was throttled less than 'cooldown_seconds' ago
	bool CoolingDown(const string &catalog, idx_t cooldown_seconds) {
		lock_guard<mutex> guard(lock);
		auto entry = since.find(catalog);
		if (entry == since.end()) {
			return false;
		}
		if (std::chrono::steady_clock::now() - entry->second <
		    std::chrono::seconds(static_cast<int64_t>(cooldown_seconds))) {
			return true;
		}
		since.erase(entry);
		return false;
	}

private:
	mutex lock;
	unordered_map<string, std::chrono::steady_clock::time_point> since;
};

//! A table of the query to load, and the first of its scans to ask
struct TableToLoad {
	shared_ptr<HiveScanInfo> scan;
	string key;
};

} // namespace

//! The Glue catalog behind an attach: two attaches of one account and region share Glue's rate
static string CatalogIdentity(const GlueCatalog &catalog) {
	return catalog.options.catalog_id + "\x1f" + catalog.options.region + "\x1f" + catalog.options.endpoint;
}

//! Loaded by an earlier scan of the query, or by an earlier statement under glue_metadata_cache_scope = 'connection'
static bool PartitionsCached(ClientContext &context, HiveQueryCache &cache, GlueCatalog &catalog,
                             const HiveScanInfo &scan, const string &key) {
	{
		auto entry = cache.GetGlueTable(key);
		annotated_lock_guard<annotated_mutex> guard(entry->lock);
		if (entry->partitions) {
			return true;
		}
	}
	auto transaction_cache = GlueTransactionCache::Of(context, catalog);
	return transaction_cache && transaction_cache->HasPartitions(scan.database_name, scan.table_name);
}

//! Keep a table's partitions where its scan's own load would have put them (GetTablePartitions)
static void StorePartitions(ClientContext &context, HiveQueryCache &cache, GlueCatalog &catalog,
                            const TableToLoad &table, vector<GluePartitionInfo> partitions) {
	auto value = make_shared_ptr<const vector<GluePartitionInfo>>(std::move(partitions));
	auto entry = cache.GetGlueTable(table.key);
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	if (entry->partitions) {
		return;
	}
	auto transaction_cache = GlueTransactionCache::Of(context, catalog);
	entry->partitions = transaction_cache
	                        ? transaction_cache->GetPartitions(table.scan->database_name, table.scan->table_name,
	                                                           [&]() { return value; })
	                        : value;
}

void LoadPartitionsTogether(ClientContext &context) {
	auto cache = HiveQueryCache::Get(context);
	auto scans = cache->TakeRegisteredScans();
	if (scans.size() < 2) {
		return;
	}
	auto parallelism = PartitionLoadParallelism(context);
	if (parallelism <= 1 || cache->ParallelLoadsStopped()) {
		return;
	}
	map<string, vector<TableToLoad>> by_catalog;
	unordered_set<string> seen;
	for (auto &scan : scans) {
		if (scan->partition_keys.empty() || scan->catalog_name.empty() || scan->PartitionsLoaded()) {
			continue;
		}
		auto key = HiveQueryCache::TableKey(scan->catalog_name, scan->database_name, scan->table_name);
		if (!seen.insert(key).second) {
			continue;
		}
		auto &catalog = Catalog::GetCatalog(context, Identifier(scan->catalog_name)).Cast<GlueCatalog>();
		if (PartitionsCached(context, *cache, catalog, *scan, key)) {
			continue;
		}
		by_catalog[scan->catalog_name].push_back(TableToLoad {scan, std::move(key)});
	}
	auto cooldown = PartitionLoadCooldownSeconds(context);
	for (auto &entry : by_catalog) {
		auto &tables = entry.second;
		if (tables.size() < 2) {
			continue;
		}
		auto &catalog = Catalog::GetCatalog(context, Identifier(entry.first)).Cast<GlueCatalog>();
		auto identity = CatalogIdentity(catalog);
		if (ThrottledCatalogs::Get().CoolingDown(identity, cooldown)) {
			GlueScanEvent event(entry.first, 0, "parallel_load");
			event.Finish();
			event.detail =
			    StringUtil::Format("paused for %llu s after Glue throttled a parallel load: %llu tables load "
			                       "one at a time",
			                       cooldown, tables.size());
			DUCKDB_LOG(context, GlueScanLogType, event);
			continue;
		}
		for (idx_t start = 0; start < tables.size(); start += parallelism) {
			auto end = MinValue<idx_t>(start + parallelism, tables.size());
			if (end - start < 2) {
				// one table is its scan's own load
				break;
			}
			vector<GluePartitionLoad> loads;
			vector<string> names;
			for (idx_t i = start; i < end; i++) {
				GluePartitionLoad load;
				load.database_name = tables[i].scan->database_name;
				load.table_name = tables[i].scan->table_name;
				loads.push_back(std::move(load));
				names.push_back(tables[i].scan->Describe());
			}
			GlueScanEvent event(StringUtil::Join(names, ", "), 0, "parallel_load");
			GlueAPI::GetPartitionsOfTables(context, catalog, loads);
			idx_t loaded = 0;
			idx_t partitions = 0;
			string failure;
			bool throttled = false;
			for (idx_t i = 0; i < loads.size(); i++) {
				auto &load = loads[i];
				throttled = throttled || load.throttled;
				if (load.loaded) {
					loaded++;
					partitions += load.partitions.size();
					StorePartitions(context, *cache, catalog, tables[start + i], std::move(load.partitions));
				} else if (failure.empty() && !load.error.empty()) {
					failure = load.error;
				}
			}
			event.partitions = partitions;
			event.Finish();
			event.detail = StringUtil::Format("loaded %llu of %llu tables", loaded, loads.size());
			if (loaded < loads.size()) {
				event.detail += "; the others load one at a time, as each scan needs them";
				if (!failure.empty()) {
					event.detail += ": " + failure;
				}
				if (throttled) {
					event.detail += StringUtil::Format("; Glue throttled the requests, so loads against this catalog "
					                                   "run one at a time for %llu s",
					                                   cooldown);
				}
			}
			DUCKDB_LOG(context, GlueScanLogType, event);
			if (loaded < loads.size()) {
				cache->StopParallelLoads();
				if (throttled) {
					ThrottledCatalogs::Get().Throttled(identity);
				}
				return;
			}
		}
	}
}

} // namespace duckdb
