#include "planning/hive_query_cache.hpp"

#include "core/hive_location.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"
#include "planning/hive_multi_file_reader.hpp"

namespace duckdb {

static constexpr const char *HIVE_QUERY_CACHE = "glue_hive_query_cache";
static constexpr const char *HIVE_LISTING_CACHE = "glue_listing_cache";

//! The listing of a directory, started under the entry's own lock: starting it fetches the first page, which must
//! not hold up the listings of other directories
static shared_ptr<MultiFileList> GetListing(ClientContext &context, HiveDirectoryListingEntry &entry,
                                            const string &directory) {
	annotated_lock_guard<annotated_mutex> guard(entry.lock);
	if (!entry.listing) {
		auto &fs = FileSystem::GetFileSystem(context);
		entry.listing = shared_ptr<MultiFileList>(ListLocation(fs, directory));
	}
	return entry.listing;
}

static shared_ptr<HiveDirectoryListingEntry> &
FindListing(unordered_map<string, shared_ptr<HiveDirectoryListingEntry>> &listings, const string &directory) {
	auto &entry = listings[directory];
	if (!entry) {
		entry = make_shared_ptr<HiveDirectoryListingEntry>();
	}
	return entry;
}

namespace {

//! The directory listings of Glue tables, kept across statements under glue_cache_listings
class HiveListingCache : public ClientContextState {
public:
	shared_ptr<MultiFileList> GetListing(ClientContext &context, const string &directory) {
		shared_ptr<HiveDirectoryListingEntry> entry;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			entry = FindListing(listings, directory);
		}
		return duckdb::GetListing(context, *entry, directory);
	}

	void Clear() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		listings.clear();
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
	annotated_mutex lock;
	unordered_map<string, shared_ptr<HiveDirectoryListingEntry>> listings DUCKDB_GUARDED_BY(lock);
};

} // namespace

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
	annotated_lock_guard<annotated_mutex> guard(lock);
	glue_tables.clear();
	samples.clear();
	directory_listings.clear();
	scan_count = 0;
	file_lists.clear();
	prefetched_partitions.clear();
}

idx_t HiveQueryCache::NextScanId() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return ++scan_count;
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

shared_ptr<MultiFileList> HiveQueryCache::GetDirectoryListing(ClientContext &context, const string &directory,
                                                              bool cache_in_connection) {
	auto key = directory;
	StringUtil::RTrim(key, "/");
	if (cache_in_connection) {
		// the connection's listings (glue_cache_listings): kept for later statements, cleared with the metadata cache
		return context.registered_state->GetOrCreate<HiveListingCache>(HIVE_LISTING_CACHE)->GetListing(context, key);
	}
	shared_ptr<HiveDirectoryListingEntry> entry;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		entry = FindListing(directory_listings, key);
	}
	return duckdb::GetListing(context, *entry, key);
}

ErrorData HiveFileListEntry::SampleListingError() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return sample_listing_error;
}

void HiveFileListEntry::SetSampleListingError(ErrorData error) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	sample_listing_error = std::move(error);
}

shared_ptr<HiveFileListEntry> HiveQueryCache::RegisterFileList(shared_ptr<HiveScanInfo> scan_info,
                                                               shared_ptr<const vector<idx_t>> partition_indexes) {
	auto entry = make_shared_ptr<HiveFileListEntry>(std::move(scan_info), std::move(partition_indexes));
	annotated_lock_guard<annotated_mutex> guard(lock);
	file_lists.push_back(entry);
	return entry;
}

vector<shared_ptr<HiveFileListEntry>> HiveQueryCache::LiveFileLists() {
	vector<shared_ptr<HiveFileListEntry>> result;
	idx_t live = 0;
	for (auto &weak_entry : file_lists) {
		auto entry = weak_entry.lock();
		if (!entry) {
			continue;
		}
		file_lists[live++] = weak_entry;
		result.push_back(std::move(entry));
	}
	file_lists.resize(live);
	return result;
}

namespace {

//! Metadata fetched ahead of the scan that needs it. A failure is remembered where the result would have been and
//! reported to that scan; an interrupt ends the query
class HivePrefetchTask : public BaseExecutorTask {
public:
	HivePrefetchTask(TaskExecutor &executor, ClientContext &context, std::function<void()> fetch_p)
	    : BaseExecutorTask(executor), context(context), fetch(std::move(fetch_p)) {
	}

	void ExecuteTask() override {
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
		try {
			fetch();
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (error.Type() == ExceptionType::INTERRUPT) {
				throw;
			}
		}
	}
	string TaskType() const override {
		return "HivePrefetchTask";
	}

private:
	ClientContext &context;
	std::function<void()> fetch;
};

void RunPrefetchTasks(ClientContext &context, vector<std::function<void()>> fetches) {
	TaskExecutor executor(context, TaskSchedulerType::ASYNC);
	for (auto &fetch : fetches) {
		executor.ScheduleTask(make_uniq<HivePrefetchTask>(executor, context, std::move(fetch)));
	}
	executor.WorkOnTasks();
}

} // namespace

void HiveQueryCache::PrefetchPartitions(ClientContext &context) {
	vector<shared_ptr<HiveFileListEntry>> lists;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		lists = LiveFileLists();
	}
	// not under the lock: a scan loading its partitions holds the scan info's lock and then takes this one
	unordered_map<string, shared_ptr<HiveScanInfo>> tables;
	for (auto &list : lists) {
		auto &info = *list->scan_info;
		if (!list->filters_pushed || info.partition_keys.empty() || info.catalog_name.empty() ||
		    info.PartitionsLoaded()) {
			continue;
		}
		tables.emplace(TableKey(info.catalog_name, info.database_name, info.table_name), list->scan_info);
	}
	vector<std::function<void()>> fetches;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		vector<string> keys;
		for (auto &table : tables) {
			if (prefetched_partitions.find(table.first) == prefetched_partitions.end()) {
				keys.push_back(table.first);
			}
		}
		if (keys.size() < 2) {
			return;
		}
		for (auto &key : keys) {
			prefetched_partitions.insert(key);
			auto info = tables[key];
			fetches.push_back([&context, info]() { GetTablePartitions(context, *info); });
		}
	}
	RunPrefetchTasks(context, std::move(fetches));
}

void HiveQueryCache::PrefetchSampleListings(ClientContext &context) {
	vector<shared_ptr<HiveFileListEntry>> candidates;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto lists = LiveFileLists();
		// a table scanned more than once is measured from the scan that asks first, which is not known here
		unordered_map<string, idx_t> scans_per_table;
		for (auto &list : lists) {
			auto &info = *list->scan_info;
			scans_per_table[info.catalog_name + "." + info.Describe()]++;
		}
		for (auto &list : lists) {
			auto &info = *list->scan_info;
			auto table = info.catalog_name + "." + info.Describe();
			if (list->filters_pushed && !list->sample_prefetched && scans_per_table[table] == 1 &&
			    samples.find(table) == samples.end() && info.file_format != HiveFileFormat::AVRO) {
				candidates.push_back(list);
			}
		}
		if (candidates.size() < 2) {
			return;
		}
		for (auto &list : candidates) {
			list->sample_prefetched = true;
		}
	}
	PrefetchPartitions(context);
	vector<std::function<void()>> fetches;
	for (auto &entry : candidates) {
		// a list of the scan's partitions: listing it fills the caches the scan's own listing reads
		auto list = make_shared_ptr<HiveMultiFileList>(context, entry->scan_info, entry->partition_indexes, false);
		if (!entry->scan_info->partition_keys.empty() && list->PartitionIndexes()->empty()) {
			continue;
		}
		fetches.push_back([entry, list]() {
			try {
				list->ListSampleDirectory();
			} catch (std::exception &ex) {
				ErrorData error(ex);
				if (error.Type() != ExceptionType::INTERRUPT) {
					entry->SetSampleListingError(std::move(error));
				}
				throw;
			}
		});
	}
	if (fetches.size() < 2) {
		return;
	}
	RunPrefetchTasks(context, std::move(fetches));
}

shared_ptr<const vector<GluePartitionInfo>> GetTablePartitions(ClientContext &context, const HiveScanInfo &info) {
	auto entry = HiveQueryCache::Get(context)->GetGlueTable(
	    HiveQueryCache::TableKey(info.catalog_name, info.database_name, info.table_name));
	// per table, so the Glue calls for different tables do not wait on each other
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	if (!entry->partitions) {
		if (entry->partitions_error.HasError()) {
			entry->partitions_error.Throw();
		}
		auto &catalog = Catalog::GetCatalog(context, Identifier(info.catalog_name)).Cast<GlueCatalog>();
		auto load = [&]() {
			return make_shared_ptr<const vector<GluePartitionInfo>>(
			    GlueAPI::GetPartitions(context, catalog, info.database_name, info.table_name));
		};
		auto cache = GlueTransactionCache::Of(context, catalog);
		try {
			entry->partitions = cache ? cache->GetPartitions(info.database_name, info.table_name, load) : load();
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (error.Type() != ExceptionType::INTERRUPT) {
				entry->partitions_error = std::move(error);
			}
			throw;
		}
	}
	return entry->partitions;
}

} // namespace duckdb
