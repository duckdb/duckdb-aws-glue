#include "catalog/glue_transaction.hpp"
#include "catalog/glue_catalog.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "planning/hive_query_cache.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

GlueTransaction::GlueTransaction(GlueCatalog &glue_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), glue_catalog(glue_catalog),
      transaction_state(GlueTransactionState::TRANSACTION_NOT_YET_STARTED) {
	if (GlueTransactionCache::ConnectionScoped(context)) {
		cache = GlueMetadataCacheState::Get(context)->GetCache(glue_catalog.GetName().GetIdentifierName(),
		                                                       glue_catalog.GetAttached().oid);
	} else {
		cache = make_shared_ptr<GlueTransactionCache>();
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

shared_ptr<GlueMetadataCacheState> GlueMetadataCacheState::Get(ClientContext &context) {
	return context.registered_state->GetOrCreate<GlueMetadataCacheState>(NAME);
}

idx_t GlueTransactionCache::NextId() {
	static atomic<idx_t> next_id {0};
	return ++next_id;
}

shared_ptr<const GlueTableInfo>
GlueTransactionCache::GetTable(const string &database_name, const string &table_name,
                               const std::function<shared_ptr<const GlueTableInfo>()> &load, transaction_t query) {
	shared_ptr<CacheEntry<GlueTableInfo>> entry;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		entry = FindEntry(tables, Key(database_name, table_name));
	}
	return Resolve(*entry, load, query);
}

void GlueTransactionCache::PutTable(const string &database_name, const string &table_name,
                                    shared_ptr<const GlueTableInfo> table) {
	if (!table) {
		return;
	}
	shared_ptr<CacheEntry<GlueTableInfo>> entry;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		entry = FindEntry(tables, Key(database_name, table_name));
	}
	annotated_lock_guard<annotated_mutex> guard(entry->load_lock);
	if (entry->found) {
		return;
	}
	entry->value = std::move(table);
	entry->found = true;
}

bool GlueTransactionCache::IsTableResolved(const string &database_name, const string &table_name, transaction_t query) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto slot = tables.find(Key(database_name, table_name));
	return slot != tables.end() && slot->second->IsResolved(query);
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

transaction_t GlueTransactionCache::ActiveQuery(ClientContext &context) {
	return context.transaction.HasActiveTransaction() ? context.transaction.GetActiveQuery() : MAXIMUM_QUERY_ID;
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
