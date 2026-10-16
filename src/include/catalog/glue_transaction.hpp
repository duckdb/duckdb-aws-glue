#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/transaction/transaction.hpp"

#include "api/glue_api.hpp"

#include <functional>

namespace duckdb {
class GlueCatalog;

enum class GlueTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

//! Glue metadata resolved in this transaction
class GlueTransactionCache {
public:
	//! The current transaction's cache, or null when no catalog transaction is active (ATTACH, bare mutations)
	static optional_ptr<GlueTransactionCache> Of(ClientContext &context, Catalog &catalog);
	static void Invalidate(ClientContext &context, Catalog &catalog);
	//! DETACH erases the catalog's entry from the detaching connection's cache state; the attach-identity
	//! keying remains the safety net for connection states a DETACH cannot reach
	static void OnDetach(ClientContext &context, Catalog &catalog);
	//! Clear the cache when the enclosing Glue mutation returns or throws
	struct InvalidateOnExit {
		InvalidateOnExit(ClientContext &context, Catalog &catalog) : context(context), catalog(catalog) {
		}
		//! Invalidates; an exception is logged, not rethrown (defined in glue_transaction.cpp for the logger)
		~InvalidateOnExit();
		ClientContext &context;
		Catalog &catalog;
	};

	shared_ptr<const GlueTableInfo> GetTable(const string &database_name, const string &table_name,
	                                         const std::function<shared_ptr<const GlueTableInfo>()> &load) {
		shared_ptr<CacheEntry<GlueTableInfo>> entry;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			entry = FindEntry(tables, database_name + "\n" + table_name);
		}
		return Resolve(*entry, load);
	}
	shared_ptr<const vector<GluePartitionInfo>>
	GetPartitions(const string &database_name, const string &table_name,
	              const std::function<shared_ptr<const vector<GluePartitionInfo>>()> &load) {
		shared_ptr<CacheEntry<vector<GluePartitionInfo>>> entry;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			entry = FindEntry(partitions, database_name + "\n" + table_name);
		}
		return Resolve(*entry, load);
	}
	void Clear() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		tables.clear();
		partitions.clear();
	}
	//! Tells cache instances apart (monotonic, never reused): a bulk table fetch warms each instance at most once
	//! (GlueTableSet), and under the default scope a new transaction brings a new instance
	idx_t Id() const {
		return id;
	}

private:
	static idx_t NextId();

	//! One cached value and the lock that guards loading it. The lock is per entry so the Glue calls for
	//! different tables do not wait on each other (resolving N tables runs their fetches in parallel).
	template <class T>
	struct CacheEntry {
		annotated_mutex load_lock;
		//! Null until a load succeeds; a failed load leaves it null so the next reference retries
		shared_ptr<const T> value DUCKDB_GUARDED_BY(load_lock);
	};

	//! Keys fold case (Glue database/table names are case-insensitive). The map lock is only held to find or
	//! create the entry (FindEntry); the entry's own lock is held across 'load' (Resolve) so concurrent references
	//! to the same key fetch once while different keys load in parallel; 'load' must not call back into this cache
	template <class T>
	shared_ptr<CacheEntry<T>> FindEntry(unordered_map<string, shared_ptr<CacheEntry<T>>> &map, const string &key)
	    DUCKDB_REQUIRES(lock) {
		auto &slot = map[StringUtil::Lower(key)];
		if (!slot) {
			slot = make_shared_ptr<CacheEntry<T>>();
		}
		return slot;
	}
	template <class T>
	static shared_ptr<const T> Resolve(CacheEntry<T> &entry, const std::function<shared_ptr<const T>()> &load) {
		annotated_lock_guard<annotated_mutex> guard(entry.load_lock);
		if (entry.value) {
			return entry.value;
		}
		entry.value = load();
		return entry.value;
	}

	annotated_mutex lock;
	unordered_map<string, shared_ptr<CacheEntry<GlueTableInfo>>> tables DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<CacheEntry<vector<GluePartitionInfo>>>> partitions DUCKDB_GUARDED_BY(lock);
	const idx_t id = NextId();
};

//! The connection's caches under glue_metadata_cache_scope='connection', one per attached Glue catalog. Lives in
//! the connection's registered state, so two connections never share a cache. A rollback clears the caches held
//! here: a failed statement must not leave metadata it resolved behind.
class GlueMetadataCacheState : public ClientContextState {
public:
	shared_ptr<GlueTransactionCache> GetCache(const string &catalog_name, idx_t attach_oid) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &entry = caches[catalog_name];
		// Keyed by the attach identity, not just the alias: after DETACH + re-ATTACH under the same alias the
		// new attached database must not be served the old catalog's metadata, even where the DETACH hook
		// (EraseCache) could not reach this state. Replacing the entry also keeps one entry per alias.
		if (!entry.cache || entry.attach_oid != attach_oid) {
			entry = {attach_oid, make_shared_ptr<GlueTransactionCache>()};
		}
		return entry.cache;
	}

	void EraseCache(const string &catalog_name, idx_t attach_oid) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto entry = caches.find(catalog_name);
		if (entry != caches.end() && entry->second.attach_oid == attach_oid) {
			caches.erase(entry);
		}
	}

	void ClearAll() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		for (auto &entry : caches) {
			entry.second.cache->Clear();
		}
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		ClearAll();
	}

private:
	struct CatalogCache {
		idx_t attach_oid;
		shared_ptr<GlueTransactionCache> cache;
	};

	annotated_mutex lock;
	unordered_map<string, CatalogCache> caches DUCKDB_GUARDED_BY(lock);
};

//! Glue has no transactional semantics in this extension: every DDL statement is executed against the Glue API
//! immediately. The transaction exists because every attached database needs one, and carries the metadata cache.
class GlueTransaction : public Transaction {
public:
	GlueTransaction(GlueCatalog &glue_catalog, TransactionManager &manager, ClientContext &context);
	~GlueTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	static GlueTransaction &Get(ClientContext &context, Catalog &catalog);

public:
	GlueCatalog &glue_catalog;
	//! Per transaction by default; shared through the connection state under glue_metadata_cache_scope='connection'
	//! (so a prepared statement's two binds share it).
	shared_ptr<GlueTransactionCache> cache;

private:
	GlueTransactionState transaction_state;
};

} // namespace duckdb
