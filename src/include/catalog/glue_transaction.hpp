#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/transaction/transaction.hpp"

#include "api/glue_api.hpp"
#include "execution/glue_hive_write.hpp"

#include <functional>

namespace duckdb {
class GlueCatalog;
class GlueSchemaEntry;

enum class GlueTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

//! Glue metadata resolved in this transaction
class GlueTransactionCache {
public:
	//! The current transaction's cache, or null when no catalog transaction is active (ATTACH, bare mutations)
	static optional_ptr<GlueTransactionCache> Of(ClientContext &context, Catalog &catalog);
	//! Clears the connection's directory listings too
	static void Invalidate(ClientContext &context, Catalog &catalog);
	//! DETACH erases the catalog's entry from the detaching connection's cache state; the attach-identity
	//! keying remains the safety net for connection states a DETACH cannot reach
	static void OnDetach(ClientContext &context, Catalog &catalog);
	//! Whether glue_metadata_cache_scope is 'connection'
	static bool ConnectionScoped(ClientContext &context);
	//! Whether a query runs on the connection: only a lookup within one keeps a table Glue reports missing
	static bool InQuery(ClientContext &context);
	//! Clear the cache when the enclosing Glue mutation returns or throws
	struct InvalidateOnExit {
		InvalidateOnExit(ClientContext &context, Catalog &catalog) : context(context), catalog(catalog) {
		}
		//! Invalidates; an exception is logged, not rethrown (defined in glue_transaction.cpp for the logger)
		~InvalidateOnExit();
		ClientContext &context;
		Catalog &catalog;
	};

	//! 'keep_miss' false: a table Glue reports missing is not kept (a lookup outside a query, which no hook ends)
	shared_ptr<const GlueTableInfo> GetTable(const string &database_name, const string &table_name,
	                                         const std::function<shared_ptr<const GlueTableInfo>()> &load,
	                                         bool keep_miss = true);
	shared_ptr<const vector<GluePartitionInfo>>
	GetPartitions(const string &database_name, const string &table_name,
	              const std::function<shared_ptr<const vector<GluePartitionInfo>>()> &load) {
		shared_ptr<CacheEntry<vector<GluePartitionInfo>>> entry;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			entry = FindEntry(partitions, Key(database_name, table_name));
		}
		return Resolve(*entry, load);
	}
	//! Store a table Glue just returned: it replaces a kept miss, a cached table stays
	void PutTable(const string &database_name, const string &table_name, shared_ptr<const GlueTableInfo> table);
	//! Whether GetTable answers from the cache, a known miss included, without calling Glue
	bool IsTableResolved(const string &database_name, const string &table_name) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto slot = tables.find(Key(database_name, table_name));
		return slot != tables.end() && slot->second->loaded;
	}
	void Clear() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		tables.clear();
		partitions.clear();
		missing_tables.clear();
	}
	//! Forget the tables Glue reported missing; the tables it returned stay
	void ClearMissingTables() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		for (auto &key : missing_tables) {
			tables.erase(key);
		}
		missing_tables.clear();
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
		//! Null until a load succeeds, and after one that found nothing
		shared_ptr<const T> value DUCKDB_GUARDED_BY(load_lock);
		//! Set when a load returns, a miss included; a load that throws leaves it unset
		atomic<bool> loaded {false};
	};

	//! Glue database and table names are case-insensitive, so keys fold case
	static string Key(const string &database_name, const string &table_name) {
		return StringUtil::Lower(database_name + "\n" + table_name);
	}
	//! The map lock is only held to find or create the entry (FindEntry); the entry's own lock is held across 'load'
	//! (Resolve) so concurrent references to the same key fetch once while different keys load in parallel; 'load'
	//! must not call back into this cache
	template <class T>
	shared_ptr<CacheEntry<T>> FindEntry(unordered_map<string, shared_ptr<CacheEntry<T>>> &map, const string &key)
	    DUCKDB_REQUIRES(lock) {
		auto &slot = map[key];
		if (!slot) {
			slot = make_shared_ptr<CacheEntry<T>>();
		}
		return slot;
	}
	template <class T>
	static shared_ptr<const T> Resolve(CacheEntry<T> &entry, const std::function<shared_ptr<const T>()> &load) {
		annotated_lock_guard<annotated_mutex> guard(entry.load_lock);
		if (entry.loaded) {
			return entry.value;
		}
		entry.value = load();
		entry.loaded = true;
		return entry.value;
	}

	annotated_mutex lock;
	unordered_map<string, shared_ptr<CacheEntry<GlueTableInfo>>> tables DUCKDB_GUARDED_BY(lock);
	unordered_map<string, shared_ptr<CacheEntry<vector<GluePartitionInfo>>>> partitions DUCKDB_GUARDED_BY(lock);
	//! The keys of 'tables' whose entry is a miss
	unordered_set<string> missing_tables DUCKDB_GUARDED_BY(lock);
	const idx_t id = NextId();
};

//! The connection's caches under glue_metadata_cache_scope='connection', one per attached Glue catalog, and the
//! default-scope caches of its transactions, whose misses it clears. Lives in the connection's registered state, so two
//! connections never share a cache. A rollback clears the connection's caches: a failed statement must not leave
//! metadata it resolved behind.
class GlueMetadataCacheState : public ClientContextState {
public:
	static constexpr const char *NAME = "glue_metadata_cache";
	static shared_ptr<GlueMetadataCacheState> Get(ClientContext &context) {
		return context.registered_state->GetOrCreate<GlueMetadataCacheState>(NAME);
	}

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

	//! Under the default scope a transaction's own cache, so that the misses in it end with the query too
	void TrackTransactionCache(const shared_ptr<GlueTransactionCache> &cache);

	//! A miss only serves the query that looked the table up
	void QueryBegin(ClientContext &context) override;
	void QueryEnd(ClientContext &context) override;

private:
	struct CatalogCache {
		idx_t attach_oid;
		shared_ptr<GlueTransactionCache> cache;
	};

	void ClearMisses();
	void PruneTransactionCaches() DUCKDB_REQUIRES(lock);

	annotated_mutex lock;
	unordered_map<string, CatalogCache> caches DUCKDB_GUARDED_BY(lock);
	//! Not owned: a transaction's cache ends with its transaction
	vector<weak_ptr<GlueTransactionCache>> transaction_caches DUCKDB_GUARDED_BY(lock);
};

//! Glue has no transactional semantics in this extension: every DDL statement is executed against the Glue API
//! immediately. The transaction exists because every attached database needs one, and carries the metadata cache. It
//! also keeps the Hive writes of the running statement: a write that is still there when the transaction ends belongs
//! to a statement that failed, and is undone (UndoHiveWrite).
class GlueTransaction : public Transaction {
public:
	GlueTransaction(GlueCatalog &glue_catalog, TransactionManager &manager, ClientContext &context);
	~GlueTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	static GlueTransaction &Get(ClientContext &context, Catalog &catalog);

	//! A Hive write starts: it is undone when the transaction ends before CompleteHiveWrite. Writes are refused in
	//! explicit transactions, so a transaction holds at most one
	void AddHiveWrite(GlueHiveWrite write);
	//! The write, or null when it was not added or has completed; the pointer stays valid until then
	optional_ptr<GlueHiveWrite> FindHiveWrite(const string &write_id);
	//! The write's files are written and its partitions registered, nothing to undo
	void CompleteHiveWrite(const string &write_id);

public:
	GlueCatalog &glue_catalog;
	//! Per transaction by default; shared through the connection state under glue_metadata_cache_scope='connection'
	//! (so a prepared statement's two binds share it).
	shared_ptr<GlueTransactionCache> cache;

private:
	//! Undo the write if it did not complete (GlueHiveWrite::Undo); nothing is done once the connection is gone
	void UndoHiveWrite();

	GlueTransactionState transaction_state;
	annotated_mutex hive_write_lock;
	unique_ptr<GlueHiveWrite> hive_write DUCKDB_GUARDED_BY(hive_write_lock);
};

} // namespace duckdb
