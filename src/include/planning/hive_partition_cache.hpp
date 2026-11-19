#pragma once

#include "duckdb/common/mutex.hpp"

#include "core/glue_info.hpp"

#include <functional>

namespace duckdb {
class ClientContext;
class GlueCatalog;
class HiveQueryCache;
struct HiveScanInfo;

//! Loads the partitions of the Glue tables the running query scans into its GlueTableEntries (HiveQueryCache) and the
//! transaction's metadata cache, once per table however often the query scans it
class HivePartitionCache {
public:
	explicit HivePartitionCache(HiveQueryCache &query_cache) : query_cache(query_cache) {
	}

	//! The partitions of the table 'scan' reads, loaded from Glue unless a cache holds them. The first call prefetches
	//! the tables of the registered scans first
	shared_ptr<const vector<GluePartitionInfo>> Get(ClientContext &context, const HiveScanInfo &scan);
	//! Prefetch the table of 'scan' when a scan first needs partitions. Held weakly: a scan the optimizer removes
	//! before then is destroyed with its bind data, and its table is not prefetched
	void RegisterScan(const shared_ptr<const HiveScanInfo> &scan);
	void Clear();

private:
	using Loader = std::function<shared_ptr<const vector<GluePartitionInfo>>()>;

	//! The table's partitions from the query's cache, then the transaction's, then 'load'; null when 'load' returns
	//! null
	shared_ptr<const vector<GluePartitionInfo>> Resolve(ClientContext &context, GlueCatalog &catalog,
	                                                    const HiveScanInfo &scan, const Loader &load);
	//! Load the partitions of the registered scans' tables that no cache holds, glue_partition_load_parallelism tables
	//! at a time. A table whose load fails is left to Get; only an interrupt is thrown
	void Prefetch(ClientContext &context, const vector<shared_ptr<const HiveScanInfo>> &scans);
	//! Loads one batch of tables of 'catalog'; false when it failed and the catalog's later batches are not to be sent
	bool PrefetchBatch(ClientContext &context, GlueCatalog &catalog,
	                   const vector<reference<const HiveScanInfo>> &tables);

private:
	HiveQueryCache &query_cache;
	//! Held from taking the registered scans until their prefetch is done, so that no scan loads a table meanwhile
	annotated_mutex prefetch_lock;
	vector<weak_ptr<const HiveScanInfo>> registered_scans DUCKDB_GUARDED_BY(prefetch_lock);
};

} // namespace duckdb
