#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context_state.hpp"

#include "core/glue_info.hpp"

namespace duckdb {
class GlueCatalog;
struct HiveScanInfo;
struct HiveTableSample;

//! What Glue said about a table the running query scans
struct GlueTableEntry {
	annotated_mutex lock;
	//! Whether table_info was loaded; a loaded null value means the table did not exist
	bool table_info_loaded DUCKDB_GUARDED_BY(lock) = false;
	shared_ptr<const GlueTableInfo> table_info DUCKDB_GUARDED_BY(lock);
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

//! What the running query learned about the tables it scans: the Glue definition and partitions of a table (asked for
//! once however often the query scans it), the recursive listings of directories (shared by the estimate and every
//! scan listing the same directory) and the table samples. Dropped when the query ends, and when its transaction does:
//! a lookup outside a query (Connection::TableInfo, binding a relation) runs in a transaction but sees no QueryEnd
class HiveQueryCache : public ClientContextState {
public:
	static shared_ptr<HiveQueryCache> Get(ClientContext &context);
	//! The key of a Glue table in GetGlueTable; Glue folds names to lower case, so the key does too
	static string TableKey(const string &catalog, const string &database, const string &table);

	void QueryBegin(ClientContext &context) override;
	void QueryEnd(ClientContext &context) override;
	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override;
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override;

	//! 'key' identifies the table (TableKey)
	shared_ptr<GlueTableEntry> GetGlueTable(const string &key);
	//! 'table' identifies the table: its catalog and name, or the location of a hive_scan
	shared_ptr<HiveTableSampleEntry> GetSample(const string &table);
	//! The recursive listing of 'directory', fetched page by page as it is read
	shared_ptr<MultiFileList> GetDirectoryListing(ClientContext &context, const string &directory);

private:
	void Clear();

	annotated_mutex lock;
	unordered_map<string, shared_ptr<GlueTableEntry>> glue_tables DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<HiveTableSampleEntry>> samples DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<MultiFileList>> directory_listings DUCKDB_GUARDED_BY(lock);
};

//! The table definition in this query, loaded once; null means Glue did not have the table
shared_ptr<const GlueTableInfo> GetQueryTableInfo(ClientContext &context, GlueCatalog &catalog, const string &database,
                                                  const string &table);

//! The partitions Glue registers for the table of 'info' in this query, fetched once for every scan of the table
shared_ptr<const vector<GluePartitionInfo>> GetTablePartitions(ClientContext &context, const HiveScanInfo &info);

} // namespace duckdb
