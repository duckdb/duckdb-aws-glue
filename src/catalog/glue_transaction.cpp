#include "catalog/glue_transaction.hpp"
#include "catalog/glue_catalog.hpp"
#include "planning/hive_query_cache.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include <algorithm>

namespace duckdb {

GlueTransaction::GlueTransaction(GlueCatalog &glue_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), glue_catalog(glue_catalog),
      transaction_state(GlueTransactionState::TRANSACTION_NOT_YET_STARTED) {
	if (GlueTransactionCache::ConnectionScoped(context)) {
		cache = GlueMetadataCacheState::Get(context)->GetCache(glue_catalog.GetName().GetIdentifierName(),
		                                                       glue_catalog.GetAttached().oid);
	} else {
		cache = make_shared_ptr<GlueTransactionCache>();
		GlueMetadataCacheState::Get(context)->TrackTransactionCache(cache);
	}
}

GlueTransaction::~GlueTransaction() {
}

void GlueTransaction::Start() {
	transaction_state = GlueTransactionState::TRANSACTION_STARTED;
}

void GlueTransaction::Commit() {
	// writes are refused in explicit transactions, so a write still here at a commit is a bug, not a failed
	// statement: its files may be in the table, so they stay
	unique_ptr<GlueHiveWrite> write;
	{
		annotated_lock_guard<annotated_mutex> guard(hive_write_lock);
		write = std::move(hive_write);
	}
	if (write) {
		auto client_context = context.lock();
		if (client_context) {
			DUCKDB_LOG_WARNING(*client_context,
			                   "A write into Hive table '%s.%s' had not completed when its transaction committed; its "
			                   "files are kept",
			                   write->database_name, write->table_name);
		}
	}
	if (transaction_state == GlueTransactionState::TRANSACTION_STARTED) {
		transaction_state = GlueTransactionState::TRANSACTION_FINISHED;
	}
}

void GlueTransaction::Rollback() {
	UndoHiveWrite();
	if (transaction_state == GlueTransactionState::TRANSACTION_STARTED) {
		transaction_state = GlueTransactionState::TRANSACTION_FINISHED;
	}
}

GlueTransaction &GlueTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<GlueTransaction>();
}

void GlueTransaction::AddHiveWrite(GlueHiveWrite write) {
	annotated_lock_guard<annotated_mutex> guard(hive_write_lock);
	if (hive_write) {
		throw InternalException("Hive write '%s' started while write '%s' of the same transaction had not completed",
		                        write.write_id, hive_write->write_id);
	}
	hive_write = make_uniq<GlueHiveWrite>(std::move(write));
}

optional_ptr<GlueHiveWrite> GlueTransaction::FindHiveWrite(const string &write_id) {
	annotated_lock_guard<annotated_mutex> guard(hive_write_lock);
	if (hive_write && hive_write->write_id == write_id) {
		return hive_write.get();
	}
	return nullptr;
}

void GlueTransaction::CompleteHiveWrite(const string &write_id) {
	annotated_lock_guard<annotated_mutex> guard(hive_write_lock);
	if (hive_write && hive_write->write_id == write_id) {
		hive_write.reset();
	}
}

void GlueTransaction::UndoHiveWrite() {
	unique_ptr<GlueHiveWrite> write;
	{
		annotated_lock_guard<annotated_mutex> guard(hive_write_lock);
		write = std::move(hive_write);
	}
	if (!write) {
		return;
	}
	auto client_context = context.lock();
	if (!client_context) {
		return;
	}
	write->Undo(*client_context, glue_catalog);
}

void GlueMetadataCacheState::TrackTransactionCache(const shared_ptr<GlueTransactionCache> &cache) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	PruneTransactionCaches();
	transaction_caches.push_back(cache);
}

void GlueMetadataCacheState::QueryBegin(ClientContext &context) {
	ClearMisses();
}

void GlueMetadataCacheState::QueryEnd(ClientContext &context) {
	ClearMisses();
}

void GlueMetadataCacheState::ClearMisses() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	for (auto &entry : caches) {
		entry.second.cache->ClearMissingTables();
	}
	PruneTransactionCaches();
	for (auto &weak : transaction_caches) {
		auto cache = weak.lock();
		if (cache) {
			cache->ClearMissingTables();
		}
	}
}

void GlueMetadataCacheState::PruneTransactionCaches() {
	transaction_caches.erase(
	    std::remove_if(transaction_caches.begin(), transaction_caches.end(),
	                   [](const weak_ptr<GlueTransactionCache> &cache) { return cache.expired(); }),
	    transaction_caches.end());
}

idx_t GlueTransactionCache::NextId() {
	static atomic<idx_t> next_id {0};
	return ++next_id;
}

shared_ptr<const GlueTableInfo>
GlueTransactionCache::GetTable(const string &database_name, const string &table_name,
                               const std::function<shared_ptr<const GlueTableInfo>()> &load, bool keep_miss) {
	auto key = Key(database_name, table_name);
	shared_ptr<CacheEntry<GlueTableInfo>> entry;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		entry = FindEntry(tables, key);
	}
	auto table = Resolve(*entry, load);
	if (!table) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		// only the slot still in the map: a clear may have replaced it while Glue was asked
		auto slot = tables.find(key);
		if (slot != tables.end() && slot->second == entry) {
			if (keep_miss) {
				missing_tables.insert(key);
			} else {
				tables.erase(slot);
			}
		}
	}
	return table;
}

void GlueTransactionCache::PutTable(const string &database_name, const string &table_name,
                                    shared_ptr<const GlueTableInfo> table) {
	if (!table) {
		return;
	}
	auto key = Key(database_name, table_name);
	shared_ptr<CacheEntry<GlueTableInfo>> entry;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		entry = FindEntry(tables, key);
	}
	bool replaced_miss;
	{
		annotated_lock_guard<annotated_mutex> guard(entry->load_lock);
		if (entry->loaded && entry->value) {
			return;
		}
		replaced_miss = entry->loaded;
		entry->value = std::move(table);
		entry->loaded = true;
	}
	if (replaced_miss) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto slot = tables.find(key);
		if (slot != tables.end() && slot->second == entry) {
			missing_tables.erase(key);
		}
	}
}

optional_ptr<GlueTransactionCache> GlueTransactionCache::Of(ClientContext &context, Catalog &catalog) {
	auto transaction = Transaction::TryGet(context, catalog.GetAttached());
	if (!transaction) {
		return nullptr;
	}
	return transaction->Cast<GlueTransaction>().cache.get();
}

void GlueTransactionCache::Invalidate(ClientContext &context, Catalog &catalog) {
	// Get() not Of(): a connection-scoped cache outlives transactions and must be cleared even from a bare mutation
	GlueTransaction::Get(context, catalog).cache->Clear();
	// The scope setting can change between statements, so the active transaction's cache may not be the
	// connection-level one this write would otherwise leave stale: clear those too (all of them, matching
	// the rollback hook — a write through any alias of the same Glue catalog must not be hidden by another's cache)
	auto state = context.registered_state->Get<GlueMetadataCacheState>(GlueMetadataCacheState::NAME);
	if (state) {
		state->ClearAll();
	}
	ClearHiveListingCache(context);
}

bool GlueTransactionCache::InQuery(ClientContext &context) {
	// the transaction's active query, set for the connection's first query too
	return context.transaction.HasActiveTransaction() && context.transaction.GetActiveQuery() != MAXIMUM_QUERY_ID;
}

bool GlueTransactionCache::ConnectionScoped(ClientContext &context) {
	Value scope;
	if (context.TryGetCurrentSetting("glue_metadata_cache_scope", scope) && !scope.IsNull()) {
		return StringUtil::Lower(StringValue::Get(scope)) == "connection";
	}
	// matches the registered default of glue_metadata_cache_scope: 'transaction'
	return false;
}

void GlueTransactionCache::OnDetach(ClientContext &context, Catalog &catalog) {
	auto state = context.registered_state->Get<GlueMetadataCacheState>(GlueMetadataCacheState::NAME);
	if (state) {
		state->EraseCache(catalog.GetName().GetIdentifierName(), catalog.GetAttached().oid);
	}
	ClearHiveListingCache(context);
}

GlueTransactionCache::InvalidateOnExit::~InvalidateOnExit() {
	// A destructor must not throw: log what went wrong instead, so the swallowed exception is not silent
	try {
		Invalidate(context, catalog);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		DUCKDB_LOG_WARNING(context, "Could not invalidate the Glue metadata cache of catalog '%s': %s",
		                   catalog.GetName().GetIdentifierName(), error.RawMessage());
	} catch (...) {
		DUCKDB_LOG_WARNING(context, "Could not invalidate the Glue metadata cache of catalog '%s': unknown exception",
		                   catalog.GetName().GetIdentifierName());
	}
}

} // namespace duckdb
