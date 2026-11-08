#include "catalog/glue_transaction_manager.hpp"
#include "catalog/glue_catalog.hpp"

#include "duckdb/main/attached_database.hpp"

#include <algorithm>

namespace duckdb {

GlueTransactionManager::GlueTransactionManager(AttachedDatabase &db_p, GlueCatalog &glue_catalog)
    : TransactionManager(db_p), glue_catalog(glue_catalog) {
}

Transaction &GlueTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<GlueTransaction>(glue_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData GlueTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &glue_transaction = transaction.Cast<GlueTransaction>();
	glue_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	pins.erase(transaction);
	transactions.erase(transaction);
	FreeRetiredEntries();
	return ErrorData();
}

void GlueTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &glue_transaction = transaction.Cast<GlueTransaction>();
	glue_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	pins.erase(transaction);
	transactions.erase(transaction);
	FreeRetiredEntries();
}

void GlueTransactionManager::RetireEntry(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(transaction_lock);
	retired_entries.push_back(RetiredEntry {next_sequence, std::move(entry)});
}

void GlueTransactionManager::PinEntries(Transaction &transaction) {
	lock_guard<mutex> l(transaction_lock);
	auto entry = pins.find(transaction);
	if (entry != pins.end() && !IsReleased(transaction, entry->second)) {
		return;
	}
	pins[transaction] = Pin {next_sequence++, transaction.active_query.load()};
}

idx_t GlueTransactionManager::RetiredEntryCount() {
	lock_guard<mutex> l(transaction_lock);
	return retired_entries.size();
}

bool GlueTransactionManager::IsReleased(Transaction &transaction, const Pin &pin) {
	// the core sets active_query while a statement of the transaction runs, and resets it between statements; an API
	// call that binds outside a statement does not, so its pin is kept until the transaction ends
	return pin.query != MAXIMUM_QUERY_ID && transaction.active_query.load() != pin.query;
}

void GlueTransactionManager::FreeRetiredEntries() {
	auto oldest_pin = NumericLimits<idx_t>::Maximum();
	for (auto &entry : pins) {
		if (!IsReleased(entry.first, entry.second)) {
			oldest_pin = MinValue(oldest_pin, entry.second.sequence);
		}
	}
	retired_entries.erase(std::remove_if(retired_entries.begin(), retired_entries.end(),
	                                     [&](const RetiredEntry &retired) { return retired.retired_at <= oldest_pin; }),
	                      retired_entries.end());
}

void GlueTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// nothing to checkpoint, all changes are already in Glue
}

} // namespace duckdb
