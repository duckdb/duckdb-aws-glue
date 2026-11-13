#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
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
	//! Why asking Glue for the partitions failed: every scan of the table gets this error, none asks again
	ErrorData partitions_error DUCKDB_GUARDED_BY(lock);
};

//! The sample of a table, measured once per query for every scan of the table
struct HiveTableSampleEntry {
	annotated_mutex lock;
	//! Null until the first scan of the table measures it. Shared and immutable once set: every scan of the table in
	//! the query (self joins, subqueries, a CTE inlined twice) reads the one sample, after the lock is released
	shared_ptr<const HiveTableSample> sample DUCKDB_GUARDED_BY(lock);
};

//! A directory's recursive listing, fetched a page at a time as it is read
struct HiveDirectoryListingEntry {
	annotated_mutex lock;
	//! Null until some scan first asks for this directory
	shared_ptr<MultiFileList> listing DUCKDB_GUARDED_BY(lock);
	//! Why listing it failed in this query, which every later scan of it gets
	ErrorData error DUCKDB_GUARDED_BY(lock);
};

//! A file list of a Hive scan in the running query, as the prefetches see it: what they need to list ahead of the scan
//! on lists of their own, never on the scan's
struct HiveFileListEntry {
	HiveFileListEntry(shared_ptr<HiveScanInfo> scan_info_p, shared_ptr<const vector<idx_t>> partition_indexes_p)
	    : scan_info(std::move(scan_info_p)), partition_indexes(std::move(partition_indexes_p)) {
	}

	//! Why listing the sample directory ahead of the scan failed, which the scan's sample reports
	ErrorData SampleListingError();
	void SetSampleListingError(ErrorData error);

	const shared_ptr<HiveScanInfo> scan_info;
	//! Null for every partition of the table
	const shared_ptr<const vector<idx_t>> partition_indexes;
	//! Set once the optimizer pushed the scan's filters into the list: before that, its partitions are not the ones
	//! the scan reads
	atomic<bool> filters_pushed {false};
	//! Set once a prefetch took up listing the sample directory
	atomic<bool> sample_prefetched {false};

private:
	annotated_mutex lock;
	ErrorData sample_listing_error DUCKDB_GUARDED_BY(lock);
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
	//! The recursive listing of 'directory', fetched page by page as it is read. With 'cache_in_connection' the
	//! listing is kept beyond the query (glue_cache_listings)
	shared_ptr<MultiFileList> GetDirectoryListing(ClientContext &context, const string &directory,
	                                              bool cache_in_connection = false);
	//! The id of the next scan bound in this query, starting at 1
	idx_t NextScanId();

	//! The file lists of the Hive scans alive in the running query, which the prefetches below work for: the cache
	//! keeps the entry for as long as the list holds it
	shared_ptr<HiveFileListEntry> RegisterFileList(shared_ptr<HiveScanInfo> scan_info,
	                                               shared_ptr<const vector<idx_t>> partition_indexes);
	//! Fetch the partitions of the partitioned Glue tables the query scans at the same time, rather than one table
	//! after the other as planning reaches each scan, each table once per query
	void PrefetchPartitions(ClientContext &context);
	//! List the sample directory of the tables the query scans once at the same time, before their cardinality is
	//! measured: those of every scan whose filters were pushed down, each once. Only fills the query's caches, so every
	//! scan reads what it would anyway
	void PrefetchSampleListings(ClientContext &context);

private:
	//! The registered file lists still alive, dropping the others
	vector<shared_ptr<HiveFileListEntry>> LiveFileLists() DUCKDB_REQUIRES(lock);

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<GlueTableEntry>> glue_tables DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<HiveTableSampleEntry>> samples DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<HiveDirectoryListingEntry>> directory_listings DUCKDB_GUARDED_BY(lock);
	idx_t scan_count DUCKDB_GUARDED_BY(lock) = 0;
	vector<weak_ptr<HiveFileListEntry>> file_lists DUCKDB_GUARDED_BY(lock);
	//! The tables (TableKey) whose partitions a prefetch fetched
	unordered_set<string> prefetched_partitions DUCKDB_GUARDED_BY(lock);
};

//! The partitions Glue registers for the table of 'info' in this query, fetched once for every scan of the table. If
//! that failed, every scan of the table in the query gets the error
shared_ptr<const vector<GluePartitionInfo>> GetTablePartitions(ClientContext &context, const HiveScanInfo &info);
//! Drop the connection's cached directory listings
void ClearHiveListingCache(ClientContext &context);

} // namespace duckdb
