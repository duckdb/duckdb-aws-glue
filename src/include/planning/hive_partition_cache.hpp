#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"

#include "core/glue_info.hpp"

#include <functional>

namespace duckdb {
class ClientContext;
class GlueCatalog;
struct BoundStatement;
struct HiveScanInfo;
struct PlannerExtensionInput;

//! The partitions Glue registers for the tables the running query scans, loaded once per table however often the
//! query scans it, and kept in the transaction's metadata cache as well
class HivePartitionCache {
public:
	//! The partitions of the table 'scan' reads, loaded from Glue unless a cache holds them. The first call prefetches
	//! the tables of the registered scans first
	shared_ptr<const vector<GluePartitionInfo>> Get(ClientContext &context, const HiveScanInfo &scan);
	//! Prefetch the tables of 'scans' when a scan first needs partitions. Held weakly: a scan the optimizer removes
	//! before then is destroyed with its bind data, and its table is not prefetched
	void RegisterScans(const vector<shared_ptr<const HiveScanInfo>> &scans);
	void Clear();

private:
	struct Entry {
		annotated_mutex lock;
		//! Null until the table is loaded
		shared_ptr<const vector<GluePartitionInfo>> partitions DUCKDB_GUARDED_BY(lock);
	};
	using Loader = std::function<shared_ptr<const vector<GluePartitionInfo>>()>;

	//! The table's partitions from this cache, then the transaction's, then 'load'; null when 'load' returns null
	shared_ptr<const vector<GluePartitionInfo>> Resolve(ClientContext &context, GlueCatalog &catalog,
	                                                    const HiveScanInfo &scan, const Loader &load);
	//! Load the partitions of the registered scans' tables that no cache holds, glue_partition_load_parallelism tables
	//! at a time. A table whose load fails is left to Get; only an interrupt is thrown
	void Prefetch(ClientContext &context, const vector<shared_ptr<const HiveScanInfo>> &scans);
	//! Loads one batch of tables of 'catalog'; false when the batch failed and no further batch is to be sent
	bool PrefetchBatch(ClientContext &context, GlueCatalog &catalog,
	                   const vector<reference<const HiveScanInfo>> &tables);

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<Entry>> tables DUCKDB_GUARDED_BY(lock);
	//! Held from taking the registered scans until their prefetch is done, so that no scan loads a table meanwhile
	annotated_mutex prefetch_lock;
	vector<weak_ptr<const HiveScanInfo>> registered_scans DUCKDB_GUARDED_BY(prefetch_lock);
};

//! PlannerExtension post_bind: register the scans of partitioned Glue tables in the bound statement, so that the
//! first of them to need partitions (at filter pushdown or its first estimate) prefetches the tables of all of them
void RegisterHiveScansForPrefetch(PlannerExtensionInput &input, BoundStatement &statement);

} // namespace duckdb
