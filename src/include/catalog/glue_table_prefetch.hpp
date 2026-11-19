#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;
class GlueCatalog;

struct GluePrefetchedListing {
	string database_name;
	idx_t generation;
};

//! The catalog scan of a transaction, for GlueTablePrefetch
class GlueTablePrefetchState {
public:
	void SetScannedSchemas(vector<string> names);
	//! From the second database scan on: 'current' and the databases after it
	bool TakeSchemasToPrefetch(const string &current, vector<string> &names);
	void AddPrefetched(const vector<string> &names, idx_t generation);
	//! Clears the state and returns the listings made
	vector<GluePrefetchedListing> Reset();

private:
	annotated_mutex lock;
	vector<string> scanned_schemas DUCKDB_GUARDED_BY(lock);
	idx_t scanned_table_scans DUCKDB_GUARDED_BY(lock) = 0;
	vector<GluePrefetchedListing> prefetched DUCKDB_GUARDED_BY(lock);
};

//! Lists the next databases a catalog scan visits in parallel
class GlueTablePrefetch {
public:
	explicit GlueTablePrefetch(GlueCatalog &catalog);

	void RememberScannedSchemas(ClientContext &context, vector<string> names);
	void PrefetchTables(ClientContext &context, const string &schema_name);
	void EndTransaction(GlueTablePrefetchState &state);
	//! Bumped by every write through the catalog
	idx_t Generation() const {
		return generation;
	}
	void Invalidate() {
		generation++;
	}

private:
	GlueCatalog &catalog;
	atomic<idx_t> generation {0};
	//! Concurrent scans wait for a listing in progress
	mutex lock;
};

} // namespace duckdb
