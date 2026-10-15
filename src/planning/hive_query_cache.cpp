#include "planning/hive_query_cache.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"
#include "planning/hive_multi_file_reader.hpp"

namespace duckdb {

static constexpr const char *HIVE_QUERY_CACHE = "glue_hive_query_cache";

shared_ptr<HiveQueryCache> HiveQueryCache::Get(ClientContext &context) {
	return context.registered_state->GetOrCreate<HiveQueryCache>(HIVE_QUERY_CACHE);
}

string HiveQueryCache::TableKey(const string &catalog, const string &database, const string &table) {
	return catalog + "\x1f" + StringUtil::Lower(database) + "\x1f" + StringUtil::Lower(table);
}

void HiveQueryCache::QueryBegin(ClientContext &context) {
	// a lookup outside a query sees no QueryEnd, what it cached must not seed this query
	Clear();
}

void HiveQueryCache::QueryEnd(ClientContext &context) {
	Clear();
}

void HiveQueryCache::TransactionCommit(MetaTransaction &transaction, ClientContext &context) {
	Clear();
}

void HiveQueryCache::TransactionRollback(MetaTransaction &transaction, ClientContext &context) {
	Clear();
}

void HiveQueryCache::Clear() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	glue_tables.clear();
	samples.clear();
	directory_listings.clear();
}

shared_ptr<GlueTableEntry> HiveQueryCache::GetGlueTable(const string &key) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto &entry = glue_tables[key];
	if (!entry) {
		entry = make_shared_ptr<GlueTableEntry>();
	}
	return entry;
}

shared_ptr<HiveTableSampleEntry> HiveQueryCache::GetSample(const string &table) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto &entry = samples[table];
	if (!entry) {
		entry = make_shared_ptr<HiveTableSampleEntry>();
	}
	return entry;
}

shared_ptr<MultiFileList> HiveQueryCache::GetDirectoryListing(ClientContext &context, const string &directory) {
	auto key = directory;
	StringUtil::RTrim(key, "/");
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto &listing = directory_listings[key];
	if (!listing) {
		auto &fs = FileSystem::GetFileSystem(context);
		listing = shared_ptr<MultiFileList>(fs.GlobFileList(key + "/**", FileGlobOptions::ALLOW_EMPTY));
	}
	return listing;
}

shared_ptr<const GlueTableInfo> GetQueryTableInfo(ClientContext &context, GlueCatalog &catalog, const string &database,
                                                  const string &table) {
	bool fetched_now;
	return GetQueryTableInfo(context, catalog, database, table, fetched_now);
}

shared_ptr<const GlueTableInfo> GetQueryTableInfo(ClientContext &context, GlueCatalog &catalog, const string &database,
                                                  const string &table, bool &fetched_now) {
	auto entry = HiveQueryCache::Get(context)->GetGlueTable(
	    HiveQueryCache::TableKey(catalog.GetName().GetIdentifierName(), database, table));
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	// set by the load below, not by the statement cache miss: the transaction cache may answer without Glue
	fetched_now = false;
	if (!entry->table_info_loaded) {
		auto load = [&]() -> shared_ptr<const GlueTableInfo> {
			fetched_now = true;
			GlueTableInfo current;
			if (!GlueAPI::GetTable(context, catalog, database, table, current)) {
				return nullptr;
			}
			return make_shared_ptr<const GlueTableInfo>(std::move(current));
		};
		auto cache = GlueTransactionCache::Of(context, catalog);
		entry->table_info = cache ? cache->GetTable(database, table, load) : load();
		entry->table_info_loaded = true;
	}
	return entry->table_info;
}

shared_ptr<const vector<GluePartitionInfo>> GetTablePartitions(ClientContext &context, const HiveScanInfo &info) {
	auto entry = HiveQueryCache::Get(context)->GetGlueTable(
	    HiveQueryCache::TableKey(info.catalog_name, info.database_name, info.table_name));
	// per table, so the Glue calls for different tables do not wait on each other
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	if (!entry->partitions) {
		auto &catalog = Catalog::GetCatalog(context, Identifier(info.catalog_name)).Cast<GlueCatalog>();
		auto load = [&]() {
			return make_shared_ptr<const vector<GluePartitionInfo>>(
			    GlueAPI::GetPartitions(context, catalog, info.database_name, info.table_name));
		};
		auto cache = GlueTransactionCache::Of(context, catalog);
		entry->partitions = cache ? cache->GetPartitions(info.database_name, info.table_name, load) : load();
	}
	return entry->partitions;
}

} // namespace duckdb
