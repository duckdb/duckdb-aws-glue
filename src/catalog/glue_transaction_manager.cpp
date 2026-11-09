#include "catalog/glue_transaction_manager.hpp"
#include "catalog/glue_catalog.hpp"

#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"

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
	EndPin(transaction);
	transactions.erase(transaction);
	FreeRetiredEntries();
	return ErrorData();
}

void GlueTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &glue_transaction = transaction.Cast<GlueTransaction>();
	glue_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	EndPin(transaction);
	transactions.erase(transaction);
	FreeRetiredEntries();
}

void GlueTransactionManager::RetireEntry(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(transaction_lock);
	retired_entries.push_back(RetiredEntry {next_sequence, std::move(entry)});
}

void GlueTransactionManager::PinEntries(ClientContext &context, Transaction &transaction) {
	lock_guard<mutex> l(transaction_lock);
	auto entry = pins.find(transaction);
	if (entry != pins.end() && !IsReleased(entry->second)) {
		return;
	}
	// an API call that binds outside a query (ExtractPlan) keeps its pin until the transaction ends
	shared_ptr<GlueQueryEnds> query_ends;
	if (transaction.active_query.load() != MAXIMUM_QUERY_ID) {
		query_ends = context.registered_state->GetOrCreate<GlueQueryEnds>("glue_query_ends");
	}
	auto ended = query_ends ? query_ends->ended.load() : 0;
	pins[transaction] = Pin {next_sequence++, query_ends != nullptr, query_ends, ended};
}

void GlueTransactionManager::EndPin(Transaction &transaction) {
	auto entry = pins.find(transaction);
	if (entry == pins.end()) {
		return;
	}
	// an explicit transaction of a connection being destroyed is rolled back before its query's tasks are cancelled
	if (transaction.context.expired() && entry->second.in_query && !IsReleased(entry->second)) {
		closing_pins.push_back(std::move(entry->second));
	}
	pins.erase(entry);
}

idx_t GlueTransactionManager::RetiredEntryCount() {
	lock_guard<mutex> l(transaction_lock);
	return retired_entries.size();
}

bool GlueTransactionManager::IsReleased(const Pin &pin) {
	if (!pin.in_query) {
		return false;
	}
	auto query_ends = pin.query_ends.lock();
	return !query_ends || query_ends->ended.load() != pin.ended;
}

void GlueTransactionManager::FreeRetiredEntries() {
	closing_pins.erase(std::remove_if(closing_pins.begin(), closing_pins.end(), IsReleased), closing_pins.end());
	auto oldest_pin = NumericLimits<idx_t>::Maximum();
	for (auto &pin : closing_pins) {
		oldest_pin = MinValue(oldest_pin, pin.sequence);
	}
	for (auto &entry : pins) {
		if (!IsReleased(entry.second)) {
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
