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
	//! The partitions of the table 'scan' reads, loaded from Glue unless a cache holds them
	shared_ptr<const vector<GluePartitionInfo>> Get(ClientContext &context, const HiveScanInfo &scan);
	//! Load the partitions of the tables of 'scans' that no cache holds, glue_partition_load_parallelism tables at a
	//! time. A table whose load fails is left to Get; only an interrupt is thrown
	void Prefetch(ClientContext &context, const vector<reference<const HiveScanInfo>> &scans);
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
	//! Loads one batch of tables of 'catalog'; false when the batch failed and no further batch is to be sent
	bool PrefetchBatch(ClientContext &context, GlueCatalog &catalog,
	                   const vector<reference<const HiveScanInfo>> &tables);

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<Entry>> tables DUCKDB_GUARDED_BY(lock);
};

//! PlannerExtension post_bind: prefetch the partitions of the partitioned Glue tables the bound statement scans, so
//! that filter pushdown and join ordering, which load them one scan after another, find them loaded
void PrefetchHivePartitions(PlannerExtensionInput &input, BoundStatement &statement);

} // namespace duckdb
