#include "planning/hive_partition_cache.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"
#include "core/glue_log_types.hpp"
#include "planning/hive_multi_file_reader.hpp"
#include "planning/hive_query_cache.hpp"
#include "planning/stats/hive_stats.hpp"

#include "duckdb/common/map.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/planner_extension.hpp"

#include <chrono>

namespace duckdb {

//! glue_partition_load_parallelism when unset
static constexpr idx_t DEFAULT_PARTITION_LOAD_PARALLELISM = 8;
//! glue_partition_load_cooldown when unset, in seconds
static constexpr idx_t DEFAULT_PARTITION_LOAD_COOLDOWN = 300;

static idx_t GetIdxSetting(ClientContext &context, const char *name, idx_t default_value) {
	Value setting;
	if (context.TryGetCurrentSetting(name, setting) && !setting.IsNull()) {
		return setting.GetValue<idx_t>();
	}
	return default_value;
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

//! The scans of partitioned Glue tables in a bound plan whose partitions are not loaded yet, subqueries included
class HiveScanCollector : public LogicalOperatorVisitor {
public:
	void VisitOperator(LogicalOperator &op) override {
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			// BindHiveScan sets this cardinality, on every scan of a Glue Hive table and only on those
			if (get.function.cardinality == HiveScanCardinality && get.bind_data) {
				auto &bind_data = get.bind_data->Cast<MultiFileBindData>();
				auto &scan = bind_data.file_list->Cast<HiveMultiFileList>().ScanInfo();
				if (!scan.catalog_name.empty() && !scan.partition_keys.empty() && !scan.PartitionsLoaded()) {
					scans.push_back(scan);
				}
			}
		}
		VisitOperatorExpressions(op);
		VisitOperatorChildren(op);
	}

	vector<reference<const HiveScanInfo>> scans;

protected:
	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		auto &subquery = expr.SubqueryMutable();
		if (subquery.plan) {
			VisitOperator(*subquery.plan);
		}
		return nullptr;
	}
};

} // namespace

static GlueCatalog &GetGlueCatalog(ClientContext &context, const HiveScanInfo &scan) {
	return Catalog::GetCatalog(context, Identifier(scan.catalog_name)).Cast<GlueCatalog>();
}

shared_ptr<const vector<GluePartitionInfo>> HivePartitionCache::Resolve(ClientContext &context, GlueCatalog &catalog,
                                                                        const HiveScanInfo &scan, const Loader &load) {
	shared_ptr<Entry> entry;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &slot = tables[HiveQueryCache::TableKey(scan.catalog_name, scan.database_name, scan.table_name)];
		if (!slot) {
			slot = make_shared_ptr<Entry>();
		}
		entry = slot;
	}
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
	auto &catalog = GetGlueCatalog(context, scan);
	return Resolve(context, catalog, scan, [&]() {
		return make_shared_ptr<const vector<GluePartitionInfo>>(
		    GlueAPI::GetPartitions(context, catalog, scan.database_name, scan.table_name));
	});
}

void HivePartitionCache::Clear() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	tables.clear();
}

bool HivePartitionCache::PrefetchBatch(ClientContext &context, GlueCatalog &catalog,
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
	GluePartitionLoadResult result;
	try {
		result = GlueAPI::GetPartitionsOfTables(context, catalog, loads);
	} catch (std::exception &ex) {
		result.error = ErrorData(ex);
	}
	if (result.error.HasError() && result.error.Type() == ExceptionType::INTERRUPT) {
		result.error.Throw();
	}
	idx_t loaded = 0;
	idx_t partitions = 0;
	for (idx_t i = 0; i < loads.size(); i++) {
		auto &load = loads[i];
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
	if (result.error.HasError()) {
		event.detail += "; the others load when their scans need them: " + result.error.RawMessage();
	}
	if (result.throttled) {
		auto cooldown = GetIdxSetting(context, "glue_partition_load_cooldown", DEFAULT_PARTITION_LOAD_COOLDOWN);
		event.detail +=
		    StringUtil::Format("; Glue throttled the requests: no prefetch against this catalog for %llu s", cooldown);
		GlueThrottledCatalogs::Get().Throttled(catalog.GlueCatalogIdentity());
	}
	DUCKDB_LOG(context, GlueScanLogType, event);
	return !result.error.HasError();
}

void HivePartitionCache::Prefetch(ClientContext &context, const vector<reference<const HiveScanInfo>> &scans) {
	auto parallelism = GetIdxSetting(context, "glue_partition_load_parallelism", DEFAULT_PARTITION_LOAD_PARALLELISM);
	if (parallelism <= 1) {
		return;
	}
	// per catalog, one scan of each table no cache holds
	map<string, vector<reference<const HiveScanInfo>>> by_catalog;
	unordered_set<string> seen;
	for (auto &scan : scans) {
		auto &info = scan.get();
		if (!seen.insert(HiveQueryCache::TableKey(info.catalog_name, info.database_name, info.table_name)).second) {
			continue;
		}
		auto &catalog = GetGlueCatalog(context, info);
		if (Resolve(context, catalog, info, []() { return nullptr; })) {
			continue;
		}
		by_catalog[info.catalog_name].push_back(info);
	}
	auto cooldown = GetIdxSetting(context, "glue_partition_load_cooldown", DEFAULT_PARTITION_LOAD_COOLDOWN);
	for (auto &entry : by_catalog) {
		auto &tables = entry.second;
		if (tables.size() < 2) {
			// nothing to gain: the table loads when its scan first needs it
			continue;
		}
		auto &catalog = GetGlueCatalog(context, tables[0].get());
		if (GlueThrottledCatalogs::Get().CoolingDown(catalog.GlueCatalogIdentity(), cooldown)) {
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
			if (!PrefetchBatch(context, catalog, batch)) {
				return;
			}
		}
	}
}

void PrefetchHivePartitions(PlannerExtensionInput &input, BoundStatement &statement) {
	if (!statement.plan) {
		return;
	}
	HiveScanCollector collector;
	collector.VisitOperator(*statement.plan);
	if (collector.scans.size() < 2) {
		return;
	}
	HiveQueryCache::Get(input.context)->Partitions().Prefetch(input.context, collector.scans);
}

} // namespace duckdb
