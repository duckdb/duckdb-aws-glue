#include "planning/hive_query_cache.hpp"

#include "core/hive_location.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"
#include "planning/hive_multi_file_reader.hpp"

namespace duckdb {

static constexpr const char *HIVE_QUERY_CACHE = "glue_hive_query_cache";
static constexpr const char *HIVE_LISTING_CACHE = "glue_listing_cache";

namespace {

//! The directory listings of Glue tables, kept across statements under glue_cache_listings
class HiveListingCache : public ClientContextState {
public:
	shared_ptr<MultiFileList> GetListing(ClientContext &context, const string &directory) {
		return listings.Get(context, directory);
	}

	void Clear() {
		listings.Clear();
	}

	void QueryEnd(ClientContext &context) override {
		Value enabled;
		if (!context.TryGetCurrentSetting("glue_cache_listings", enabled) || enabled.IsNull() ||
		    !BooleanValue::Get(enabled)) {
			Clear();
		}
	}

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		if (!GlueTransactionCache::ConnectionScoped(context)) {
			Clear();
		}
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		Clear();
	}

private:
	HiveDirectoryListings listings;
};

} // namespace

shared_ptr<MultiFileList> HiveDirectoryListings::Get(ClientContext &context, const string &directory) {
	// per directory: GlobFileList fetches the first page right away, and listings of other directories must not wait
	// behind it
	return listings.Get(directory)->GetOrLoad([&]() {
		auto &fs = FileSystem::GetFileSystem(context);
		return shared_ptr<MultiFileList>(ListLocation(fs, directory));
	});
}

void HiveScanRegistry::Register(const shared_ptr<const HiveScanInfo> &scan) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	scans.push_back(scan);
}

vector<shared_ptr<const HiveScanInfo>> HiveScanRegistry::TakeSince(idx_t &cursor) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	vector<shared_ptr<const HiveScanInfo>> result;
	for (; cursor < scans.size(); cursor++) {
		auto alive = scans[cursor].lock();
		if (alive) {
			result.push_back(std::move(alive));
		}
	}
	return result;
}

void HiveScanRegistry::Clear() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	scans.clear();
}

void ClearHiveListingCache(ClientContext &context) {
	auto cache = context.registered_state->Get<HiveListingCache>(HIVE_LISTING_CACHE);
	if (cache) {
		cache->Clear();
	}
}

shared_ptr<HiveQueryCache> HiveQueryCache::Get(ClientContext &context) {
	return context.registered_state->GetOrCreate<HiveQueryCache>(HIVE_QUERY_CACHE);
}

string HiveQueryCache::TableKey(const string &catalog, const string &database, const string &table) {
	return catalog + "\x1f" + database + "\x1f" + table;
}

void HiveQueryCache::QueryEnd(ClientContext &context) {
	scans.Clear();
	partitions.Clear();
	tables.Clear();
	directory_listings.Clear();
	annotated_lock_guard<annotated_mutex> guard(lock);
	scan_count = 0;
}

idx_t HiveQueryCache::NextScanId() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return ++scan_count;
}

shared_ptr<MultiFileList> HiveQueryCache::GetDirectoryListing(ClientContext &context, const string &directory,
                                                              bool cache_in_connection) {
	auto key = directory;
	StringUtil::RTrim(key, "/");
	if (cache_in_connection) {
		// the connection's listings (glue_cache_listings): kept for later statements, cleared with the metadata cache
		return context.registered_state->GetOrCreate<HiveListingCache>(HIVE_LISTING_CACHE)->GetListing(context, key);
	}
	return directory_listings.Get(context, key);
}

} // namespace duckdb
