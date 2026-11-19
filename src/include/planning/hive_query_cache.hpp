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

//! A value the running query loads once: the first caller loads it while the others wait. A load that throws or
//! returns null leaves the slot empty, so that the next caller loads it again
template <class T>
class HiveCacheSlot {
public:
	template <class LOAD>
	shared_ptr<T> GetOrLoad(LOAD &&load) {
		// per slot, so that loads of different values do not wait on each other
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (!value) {
			value = load();
		}
		return value;
	}

private:
	annotated_mutex lock;
	shared_ptr<T> value DUCKDB_GUARDED_BY(lock);
};

//! Entries by key, created on first use. The map's lock is only held to find an entry, never while it loads
template <class V>
class HiveCacheMap {
public:
	shared_ptr<V> Get(const string &key) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &entry = entries[key];
		if (!entry) {
			entry = make_shared_ptr<V>();
		}
		return entry;
	}
	void Clear() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		entries.clear();
	}

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<V>> entries DUCKDB_GUARDED_BY(lock);
};

using HiveFooters = vector<shared_ptr<BaseUnionData>>;

//! What the running query learned about one table (HiveScanInfo::CacheKey), shared by every scan of it
struct HiveTableCacheEntry {
	//! Asked from Glue (HivePartitionCache)
	HiveCacheSlot<const vector<GluePartitionInfo>> partitions;
	//! Measured by the first scan of the table to ask, and read by the others (self joins, a CTE inlined twice)
	HiveCacheSlot<const HiveTableSample> sample;
	//! A small unpartitioned parquet table's footers, by how they are read (HiveScanInfo::FootersVariant); empty when
	//! the table has none to give
	HiveCacheMap<HiveCacheSlot<const HiveFooters>> footers;
};

//! Recursive directory listings by directory, each fetched once, page by page as it is read
class HiveDirectoryListings {
public:
	shared_ptr<MultiFileList> Get(ClientContext &context, const string &directory);
	void Clear() {
		listings.Clear();
	}

private:
	HiveCacheMap<HiveCacheSlot<MultiFileList>> listings;
};

//! The Hive scans bound in the running query, held weakly: a scan the optimizer removes is destroyed with its bind
//! data and drops out. Each prefetch keeps its own cursor, so that it sees the scans bound since it last ran
class HiveScanRegistry {
public:
	void Register(const shared_ptr<const HiveScanInfo> &scan);
	//! The scans registered since 'cursor' that are still bound; moves 'cursor' past them
	vector<shared_ptr<const HiveScanInfo>> TakeSince(idx_t &cursor);
	void Clear();

private:
	annotated_mutex lock;
	vector<weak_ptr<const HiveScanInfo>> scans DUCKDB_GUARDED_BY(lock);
};

//! What the running query learned about the tables it scans, dropped when the query ends: per table its partitions
//! (asked for once however often the query scans it), its sample and its footers, and the recursive listings of
//! directories (shared by the estimate and every scan listing the same directory)
class HiveQueryCache : public ClientContextState {
public:
	HiveQueryCache() : partitions(*this) {
	}

	static shared_ptr<HiveQueryCache> Get(ClientContext &context);
	//! The key of a Glue table in the query's caches
	static string TableKey(const string &catalog, const string &database, const string &table);

	void QueryEnd(ClientContext &context) override;

	HiveScanRegistry &Scans() {
		return scans;
	}
	HivePartitionCache &Partitions() {
		return partitions;
	}
	//! 'key' identifies the table: HiveScanInfo::CacheKey
	shared_ptr<HiveTableCacheEntry> GetTable(const string &key) {
		return tables.Get(key);
	}
	//! The recursive listing of 'directory', fetched page by page as it is read. With 'cache_in_connection' the
	//! listing is kept beyond the query (glue_cache_listings)
	shared_ptr<MultiFileList> GetDirectoryListing(ClientContext &context, const string &directory,
	                                              bool cache_in_connection = false);
	//! The id of the next scan bound in this query, starting at 1
	idx_t NextScanId();

private:
	HiveScanRegistry scans;
	HivePartitionCache partitions;
	HiveCacheMap<HiveTableCacheEntry> tables;
	HiveDirectoryListings directory_listings;
	annotated_mutex lock;
	idx_t scan_count DUCKDB_GUARDED_BY(lock) = 0;
};

//! Drop the connection's cached directory listings
void ClearHiveListingCache(ClientContext &context);

} // namespace duckdb
