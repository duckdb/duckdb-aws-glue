#include "planning/hive_partition_cache.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"
#include "core/glue_log_types.hpp"
#include "planning/hive_multi_file_reader.hpp"
#include "planning/hive_query_cache.hpp"

#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

#include <chrono>

namespace duckdb {

static idx_t GetIdxSetting(ClientContext &context, const char *name) {
	Value setting;
	if (!context.TryGetCurrentSetting(name, setting) || setting.IsNull()) {
		// SET ... = NULL: the database's value, which is the registered default unless SET GLOBAL
		DBConfig::GetConfig(context).TryGetCurrentSetting(name, setting);
	}
	if (setting.IsNull()) {
		throw InvalidInputException("%s can not be NULL", name);
	}
	return setting.GetValue<idx_t>();
}

namespace {

//! When Glue last throttled a prefetch, by GlueCatalog::GlueCatalogIdentity. Process wide: every connection and
//! database shares Glue's request rate
class GlueThrottledCatalogs {
public:
	static GlueThrottledCatalogs &Get() {
		static GlueThrottledCatalogs instance;
		return instance;
	}
	void Throttled(const string &catalog) {
		lock_guard<mutex> guard(lock);
		since[catalog] = std::chrono::steady_clock::now();
	}
	//! Whether Glue throttled a prefetch against 'catalog' less than 'cooldown_seconds' ago
	bool CoolingDown(const string &catalog, idx_t cooldown_seconds) {
		lock_guard<mutex> guard(lock);
		auto entry = since.find(catalog);
		if (entry == since.end()) {
			return false;
		}
		// kept past this caller's cooldown: another connection may wait longer
		return std::chrono::steady_clock::now() - entry->second <
		       std::chrono::seconds(static_cast<int64_t>(cooldown_seconds));
	}

private:
	mutex lock;
	unordered_map<string, std::chrono::steady_clock::time_point> since;
};

} // namespace

static GlueCatalog &GetGlueCatalog(ClientContext &context, const HiveScanInfo &scan) {
	return Catalog::GetCatalog(context, Identifier(scan.catalog_name)).Cast<GlueCatalog>();
}

shared_ptr<const vector<GluePartitionInfo>> HivePartitionCache::Resolve(ClientContext &context, GlueCatalog &catalog,
                                                                        const HiveScanInfo &scan, const Loader &load) {
	auto entry =
	    query_cache.GetGlueTable(HiveQueryCache::TableKey(scan.catalog_name, scan.database_name, scan.table_name));
	// per table, so the Glue calls for different tables do not wait on each other
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	if (!entry->partitions) {
		auto transaction_cache = GlueTransactionCache::Of(context, catalog);
		entry->partitions =
		    transaction_cache ? transaction_cache->GetPartitions(scan.database_name, scan.table_name, load) : load();
	}
	return entry->partitions;
}

shared_ptr<const vector<GluePartitionInfo>> HivePartitionCache::Get(ClientContext &context, const HiveScanInfo &scan) {
	{
		annotated_lock_guard<annotated_mutex> guard(prefetch_lock);
		if (!registered_scans.empty()) {
			vector<shared_ptr<const HiveScanInfo>> scans;
			for (auto &registered : registered_scans) {
				auto alive = registered.lock();
				if (alive) {
					scans.push_back(std::move(alive));
				}
			}
			registered_scans.clear();
			Prefetch(context, scans);
		}
	}
	auto &catalog = GetGlueCatalog(context, scan);
	return Resolve(context, catalog, scan, [&]() {
		return make_shared_ptr<const vector<GluePartitionInfo>>(
		    GlueAPI::GetPartitions(context, catalog, scan.database_name, scan.table_name));
	});
}

shared_ptr<const vector<GluePartitionInfo>> HivePartitionCache::GetFiltered(ClientContext &context,
                                                                            const HiveScanInfo &scan,
                                                                            const string &expression, bool &filtered) {
	{
		annotated_lock_guard<annotated_mutex> guard(prefetch_lock);
		for (auto &registered : registered_scans) {
			if (registered.lock().get() == &scan) {
				registered.reset();
			}
		}
	}
	filtered = false;
	auto &catalog = GetGlueCatalog(context, scan);
	auto entry =
	    query_cache.GetGlueTable(HiveQueryCache::TableKey(scan.catalog_name, scan.database_name, scan.table_name));
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	if (!entry->partitions) {
		auto transaction_cache = GlueTransactionCache::Of(context, catalog);
		if (transaction_cache) {
			entry->partitions = transaction_cache->CachedPartitions(scan.database_name, scan.table_name);
		}
	}
	if (entry->partitions) {
		// every partition is at hand: it is the scan's pruning that selects them, not Glue
		return entry->partitions;
	}
	if (entry->refused_expressions.count(expression)) {
		throw InvalidInputException(GlueAPI::PartitionExpressionRejectedInfo(),
		                            StringUtil::Format("Glue already refused the expression '%s' in this query",
		                                               expression));
	}
	auto &partitions = entry->filtered_partitions[expression];
	if (!partitions) {
		try {
			partitions = make_shared_ptr<const vector<GluePartitionInfo>>(
			    GlueAPI::GetPartitions(context, catalog, scan.database_name, scan.table_name, expression));
		} catch (std::exception &ex) {
			entry->filtered_partitions.erase(expression);
			if (GlueAPI::IsPartitionExpressionRejected(ErrorData(ex))) {
				entry->refused_expressions.insert(expression);
			}
			throw;
		}
	}
	filtered = true;
	return partitions;
}

void HivePartitionCache::RegisterScan(const shared_ptr<const HiveScanInfo> &scan) {
	annotated_lock_guard<annotated_mutex> guard(prefetch_lock);
	registered_scans.push_back(scan);
}

void HivePartitionCache::Clear() {
	annotated_lock_guard<annotated_mutex> guard(prefetch_lock);
	registered_scans.clear();
}

bool HivePartitionCache::PrefetchBatch(ClientContext &context, GlueCatalog &catalog, const string &identity,
                                       const vector<reference<const HiveScanInfo>> &batch) {
	vector<GluePartitionLoad> loads;
	vector<string> names;
	for (auto &scan : batch) {
		GluePartitionLoad load;
		load.database_name = scan.get().database_name;
		load.table_name = scan.get().table_name;
		loads.push_back(std::move(load));
		names.push_back(scan.get().Describe());
	}
	GlueScanEvent event(StringUtil::Join(names, ", "), 0, "prefetch");
	ErrorData failure;
	try {
		GlueAPI::GetPartitionsOfTables(context, catalog, loads);
	} catch (std::exception &ex) {
		failure = ErrorData(ex);
		if (failure.Type() == ExceptionType::INTERRUPT) {
			throw;
		}
	}
	idx_t loaded = 0;
	idx_t partitions = 0;
	bool throttled = false;
	for (idx_t i = 0; i < loads.size(); i++) {
		auto &load = loads[i];
		throttled = throttled || load.throttled;
		if (!failure.HasError() && load.error.HasError()) {
			failure = load.error;
		}
		if (!load.partitions) {
			continue;
		}
		loaded++;
		partitions += load.partitions->size();
		Resolve(context, catalog, batch[i], [&]() { return load.partitions; });
	}
	event.partitions = partitions;
	event.Finish();
	event.detail = StringUtil::Format("loaded %llu of %llu tables", loaded, loads.size());
	if (loaded < loads.size()) {
		event.detail += "; the others load when their scans need them";
		if (failure.HasError()) {
			event.detail += ": " + failure.RawMessage();
		}
	}
	if (throttled) {
		auto cooldown = GetIdxSetting(context, "glue_partition_load_cooldown");
		event.detail +=
		    StringUtil::Format("; Glue throttled the requests: no prefetch against this catalog for %llu s", cooldown);
		GlueThrottledCatalogs::Get().Throttled(identity);
	}
	DUCKDB_LOG(context, GlueScanLogType, event);
	return loaded == loads.size();
}

void HivePartitionCache::Prefetch(ClientContext &context, const vector<shared_ptr<const HiveScanInfo>> &scans) {
	auto parallelism = GetIdxSetting(context, "glue_partition_load_parallelism");
	if (parallelism <= 1) {
		return;
	}
	// per catalog, one scan of each table no cache holds
	map<string, vector<reference<const HiveScanInfo>>> by_catalog;
	unordered_set<string> seen;
	for (auto &scan : scans) {
		auto &info = *scan;
		if (!seen.insert(HiveQueryCache::TableKey(info.catalog_name, info.database_name, info.table_name)).second) {
			continue;
		}
		auto &catalog = GetGlueCatalog(context, info);
		if (Resolve(context, catalog, info, []() { return nullptr; })) {
			continue;
		}
		by_catalog[info.catalog_name].push_back(info);
	}
	auto cooldown = GetIdxSetting(context, "glue_partition_load_cooldown");
	for (auto &entry : by_catalog) {
		auto &tables = entry.second;
		if (tables.size() < 2) {
			// nothing to gain: the table loads when its scan first needs it
			continue;
		}
		auto &catalog = GetGlueCatalog(context, tables[0].get());
		string identity;
		try {
			identity = catalog.GlueCatalogIdentity(context);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (error.Type() == ExceptionType::INTERRUPT) {
				throw;
			}
			GlueScanEvent event(entry.first, 0, "prefetch");
			event.Finish();
			event.detail = StringUtil::Format("skipped %llu tables: the Glue catalog id is unknown: %s", tables.size(),
			                                  error.RawMessage());
			DUCKDB_LOG(context, GlueScanLogType, event);
			continue;
		}
		if (GlueThrottledCatalogs::Get().CoolingDown(identity, cooldown)) {
			GlueScanEvent event(entry.first, 0, "prefetch");
			event.Finish();
			event.detail = StringUtil::Format("skipped %llu tables: Glue throttled a prefetch against this catalog "
			                                  "less than %llu s ago",
			                                  tables.size(), cooldown);
			DUCKDB_LOG(context, GlueScanLogType, event);
			continue;
		}
		for (idx_t start = 0; start < tables.size(); start += parallelism) {
			auto end = MinValue<idx_t>(start + parallelism, tables.size());
			vector<reference<const HiveScanInfo>> batch(tables.begin() + static_cast<int64_t>(start),
			                                            tables.begin() + static_cast<int64_t>(end));
			if (!PrefetchBatch(context, catalog, identity, batch)) {
				break;
			}
		}
	}
}

} // namespace duckdb
