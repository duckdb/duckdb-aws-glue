#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;
class GlueCatalog;

//! The catalog scan of a transaction, for GlueTablePrefetch
class GlueTablePrefetchState {
public:
	//! A new scan begins: the databases it visits, in scan order
	void SetScannedSchemas(vector<string> names);
	//! From the second database scan on, the databases from 'current' on that no earlier window covers, at most
	//! 'window' of them. Empty when 'current' lies in a window already taken.
	vector<string> TakeWindow(const string &current, idx_t window);
	void Reset();

private:
	annotated_mutex lock;
	vector<string> scanned_schemas DUCKDB_GUARDED_BY(lock);
	idx_t scanned_table_scans DUCKDB_GUARDED_BY(lock) = 0;
	case_insensitive_set_t in_window DUCKDB_GUARDED_BY(lock);
};

//! Lists the next databases a catalog scan visits in parallel, ahead of the scan
class GlueTablePrefetch {
public:
	explicit GlueTablePrefetch(GlueCatalog &catalog);

	void RememberScannedSchemas(ClientContext &context, vector<string> names);
	void PrefetchTables(ClientContext &context, const string &schema_name);

private:
	GlueCatalog &catalog;
};

} // namespace duckdb
