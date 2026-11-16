#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context_state.hpp"

#include "core/glue_info.hpp"

namespace duckdb {
struct HiveScanInfo;
struct HiveTableSample;

//! What Glue said about a table the running query scans
struct GlueTableEntry {
	annotated_mutex lock;
	//! Null until the first scan of a partitioned table asks Glue for them
	shared_ptr<const vector<GluePartitionInfo>> partitions DUCKDB_GUARDED_BY(lock);
};

//! The sample of a table, measured once per query for every scan of the table
struct HiveTableSampleEntry {
	annotated_mutex lock;
	//! Null until the first scan of the table measures it. Shared and immutable once set: every scan of the table in
	//! the query (self joins, subqueries, a CTE inlined twice) reads the one sample, after the lock is released
	shared_ptr<const HiveTableSample> sample DUCKDB_GUARDED_BY(lock);
};

//! The listing of a glob (a directory, recursively, or a range of partitions), fetched a page at a time as it is read
struct HiveDirectoryListingEntry {
	annotated_mutex lock;
	//! Null until some scan first asks for this glob
	shared_ptr<MultiFileList> listing DUCKDB_GUARDED_BY(lock);

	//! The listing, listed now if it is not yet; locked per entry, so no listing waits on another's first page
	shared_ptr<MultiFileList> GetOrList(ClientContext &context, const string &glob);
};

//! What the running query learned about the tables it scans, dropped when the query ends: the partitions of a table
//! (asked for once however often the query scans it), the recursive listings of directories (shared by the estimate and
//! every scan listing the same directory) and the table samples
class HiveQueryCache : public ClientContextState {
public:
	static shared_ptr<HiveQueryCache> Get(ClientContext &context);
	//! The key of a Glue table in GetGlueTable
	static string TableKey(const string &catalog, const string &database, const string &table);

	void QueryEnd(ClientContext &context) override;

	//! 'key' identifies the table (TableKey)
	shared_ptr<GlueTableEntry> GetGlueTable(const string &key);
	//! 'table' identifies the table: its catalog and name, or the location of a hive_scan
	shared_ptr<HiveTableSampleEntry> GetSample(const string &table);
	//! The files matching 'glob', fetched as they are read; kept beyond the query with 'cache_in_connection'
	shared_ptr<MultiFileList> GetListing(ClientContext &context, const string &glob, bool cache_in_connection = false);
	//! The id of the next scan bound in this query, starting at 1
	idx_t NextScanId();

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<GlueTableEntry>> glue_tables DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<HiveTableSampleEntry>> samples DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<HiveDirectoryListingEntry>> directory_listings DUCKDB_GUARDED_BY(lock);
	idx_t scan_count DUCKDB_GUARDED_BY(lock) = 0;
};

//! The partitions Glue registers for the table of 'info' in this query, fetched once for every scan of the table
shared_ptr<const vector<GluePartitionInfo>> GetTablePartitions(ClientContext &context, const HiveScanInfo &info);
//! Drop the connection's cached directory listings
void ClearHiveListingCache(ClientContext &context);

} // namespace duckdb
