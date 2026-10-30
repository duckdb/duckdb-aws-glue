#include "catalog/glue_transaction.hpp"
#include "catalog/glue_catalog.hpp"
#include "planning/hive_query_cache.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

GlueTransaction::GlueTransaction(GlueCatalog &glue_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), glue_catalog(glue_catalog),
      transaction_state(GlueTransactionState::TRANSACTION_NOT_YET_STARTED) {
	if (GlueTransactionCache::ConnectionScoped(context)) {
		auto state = context.registered_state->GetOrCreate<GlueMetadataCacheState>("glue_metadata_cache");
		cache = state->GetCache(glue_catalog.GetName().GetIdentifierName(), glue_catalog.GetAttached().oid);
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
	if (transaction_state == GlueTransactionState::TRANSACTION_STARTED) {
		transaction_state = GlueTransactionState::TRANSACTION_FINISHED;
	}
	InvalidateIcebergTables();
}

void GlueTransaction::Rollback() {
	if (transaction_state == GlueTransactionState::TRANSACTION_STARTED) {
		transaction_state = GlueTransactionState::TRANSACTION_FINISHED;
	}
	InvalidateIcebergTables();
}

void GlueTransaction::AddIcebergTable(const string &database_name, const string &table_name) {
	iceberg_tables.emplace_back(database_name, table_name);
}

void GlueTransaction::InvalidateIcebergTables() {
	for (auto &table : iceberg_tables) {
		glue_catalog.GetSchemas().InvalidateTableEntry(table.first, table.second);
	}
	iceberg_tables.clear();
}

GlueTransaction &GlueTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<GlueTransaction>();
}

idx_t GlueTransactionCache::NextId() {
	static atomic<idx_t> next_id {0};
	return ++next_id;
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
	auto state = context.registered_state->Get<GlueMetadataCacheState>("glue_metadata_cache");
	if (state) {
		state->ClearAll();
	}
	ClearHiveListingCache(context);
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
	auto state = context.registered_state->Get<GlueMetadataCacheState>("glue_metadata_cache");
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
