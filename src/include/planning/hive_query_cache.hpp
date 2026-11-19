#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context_state.hpp"

#include "core/glue_info.hpp"
#include "planning/hive_partition_cache.hpp"

namespace duckdb {
class BaseUnionData;
struct HiveScanInfo;
struct HiveTableSample;

//! What Glue said about a table the running query scans
struct GlueTableEntry {
	annotated_mutex lock;
	//! Null until a scan of a partitioned table first needs them (HivePartitionCache)
	shared_ptr<const vector<GluePartitionInfo>> partitions DUCKDB_GUARDED_BY(lock);
};

//! The sample of a table, measured once per query for every scan of the table
struct HiveTableSampleEntry {
	annotated_mutex lock;
	//! Null until the first scan of the table measures it. Shared and immutable once set: every scan of the table in
	//! the query (self joins, subqueries, a CTE inlined twice) reads the one sample, after the lock is released
	shared_ptr<const HiveTableSample> sample DUCKDB_GUARDED_BY(lock);
};

//! The footers of a small unpartitioned parquet table's files, read once per query for every scan of the table
struct HiveFooterEntry {
	annotated_mutex lock;
	//! Set by the first scan to ask; the footers stay empty when the table has none to give
	bool read DUCKDB_GUARDED_BY(lock) = false;
	vector<shared_ptr<BaseUnionData>> footers DUCKDB_GUARDED_BY(lock);
};

//! A directory's recursive listing, fetched a page at a time as it is read
struct HiveDirectoryListingEntry {
	annotated_mutex lock;
	//! Null until some scan first asks for this directory
	shared_ptr<MultiFileList> listing DUCKDB_GUARDED_BY(lock);
};

//! Recursive directory listings by directory, each fetched once, page by page as it is read
class HiveDirectoryListings {
public:
	shared_ptr<MultiFileList> Get(ClientContext &context, const string &directory);
	void Clear();

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<HiveDirectoryListingEntry>> listings DUCKDB_GUARDED_BY(lock);
};

//! What the running query learned about the tables it scans, dropped when the query ends: the partitions of a table
//! (asked for once however often the query scans it), the recursive listings of directories (shared by the estimate and
//! every scan listing the same directory), the table samples and the footers read for statistics
class HiveQueryCache : public ClientContextState {
public:
	HiveQueryCache() : partitions(*this) {
	}

	static shared_ptr<HiveQueryCache> Get(ClientContext &context);
	//! The key of a Glue table in the query's caches
	static string TableKey(const string &catalog, const string &database, const string &table);

	void QueryEnd(ClientContext &context) override;

	HivePartitionCache &Partitions() {
		return partitions;
	}
	//! 'key' identifies the table (TableKey)
	shared_ptr<GlueTableEntry> GetGlueTable(const string &key);
	//! 'table' identifies the table: its catalog and name, or the location of a hive_scan
	shared_ptr<HiveTableSampleEntry> GetSample(const string &table);
	//! 'key' identifies the table and how its footers are read
	shared_ptr<HiveFooterEntry> GetFooters(const string &key);
	//! The recursive listing of 'directory', fetched page by page as it is read. With 'cache_in_connection' the
	//! listing is kept beyond the query (glue_cache_listings)
	shared_ptr<MultiFileList> GetDirectoryListing(ClientContext &context, const string &directory,
	                                              bool cache_in_connection = false);
	//! The id of the next scan bound in this query, starting at 1
	idx_t NextScanId();

private:
	HivePartitionCache partitions;
	HiveDirectoryListings directory_listings;
	annotated_mutex lock;
	unordered_map<string, shared_ptr<GlueTableEntry>> glue_tables DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<HiveTableSampleEntry>> samples DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<HiveFooterEntry>> footers DUCKDB_GUARDED_BY(lock);
	idx_t scan_count DUCKDB_GUARDED_BY(lock) = 0;
};

//! Drop the connection's cached directory listings
void ClearHiveListingCache(ClientContext &context);

} // namespace duckdb
