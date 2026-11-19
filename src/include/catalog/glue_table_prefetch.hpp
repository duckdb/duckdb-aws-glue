#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;
class GlueCatalog;

//! A listing GlueTablePrefetch made: the database and the generation it was made under
struct GluePrefetchedListing {
	string database_name;
	idx_t generation;
};

//! What a transaction's catalog scan visits and what GlueTablePrefetch listed for it (GlueTransactionCache)
class GlueTablePrefetchState {
public:
	void SetScannedSchemas(vector<string> names);
	//! Called as a scan of database 'current' starts: from the second scan of a catalog scan on, returns true with
	//! 'current' and the databases the scan visits after it
	bool TakeSchemasToPrefetch(const string &current, vector<string> &names);
	void AddPrefetched(const vector<string> &names, idx_t generation);
	//! Forget the catalog scan, and return the listings made for it
	vector<GluePrefetchedListing> Reset();

private:
	annotated_mutex lock;
	vector<string> scanned_schemas DUCKDB_GUARDED_BY(lock);
	idx_t scanned_table_scans DUCKDB_GUARDED_BY(lock) = 0;
	vector<GluePrefetchedListing> prefetched DUCKDB_GUARDED_BY(lock);
};

//! Lists the tables of the databases a catalog scan visits next together, glue_list_tables_parallelism at a time
class GlueTablePrefetch {
public:
	explicit GlueTablePrefetch(GlueCatalog &catalog);

	//! The databases the catalog scan now under way visits
	void RememberScannedSchemas(ClientContext &context, vector<string> names);
	//! Called as a scan of database 'schema_name' starts: from the second scan on, lists it together with the next
	//! databases the catalog scan visits
	void PrefetchTables(ClientContext &context, const string &schema_name);
	//! Drop the listings made for the transaction that no scan used
	void EndTransaction(GlueTablePrefetchState &state);
	//! Tags the listings; new after every write through the catalog, from any connection
	idx_t Generation() const {
		return generation;
	}
	void Invalidate() {
		generation++;
	}

private:
	GlueCatalog &catalog;
	atomic<idx_t> generation {0};
	//! Held while listing, so that scans of other connections and of the other children of a union wait for it
	//! and find the listings made
	mutex lock;
};

} // namespace duckdb
