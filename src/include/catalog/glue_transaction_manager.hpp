#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "catalog/glue_transaction.hpp"

namespace duckdb {
class GlueCatalog;

class GlueTransactionManager : public TransactionManager {
public:
	GlueTransactionManager(AttachedDatabase &db_p, GlueCatalog &glue_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

	//! Keep a catalog entry that was replaced or removed alive until every statement that may still use it has ended:
	//! a running statement refers to the entries it bound. A later statement of the same transaction looks the entry up
	//! again, and finds its replacement
	void RetireEntry(unique_ptr<CatalogEntry> entry);
	//! The transaction is about to hand out catalog entries: keep alive those retired from now on until the statement
	//! that asks has ended, or, when no statement is running (a client API call that binds), until the transaction ends
	void PinEntries(Transaction &transaction);
	//! The entries retired and not freed yet, for the tests
	idx_t RetiredEntryCount();

private:
	struct Pin {
		//! The entries retired after the pin was taken have a higher sequence number
		idx_t sequence;
		//! The statement that took it, MAXIMUM_QUERY_ID when none was running
		transaction_t query;
	};
	struct RetiredEntry {
		idx_t retired_at;
		unique_ptr<CatalogEntry> entry;
	};

	//! Whether the statement that took the pin has ended, so the transaction no longer uses what it was handed
	static bool IsReleased(Transaction &transaction, const Pin &pin);
	//! Free the retired entries no pin still holds. Called with transaction_lock held.
	void FreeRetiredEntries();

private:
	GlueCatalog &glue_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<GlueTransaction>> transactions;
	//! The latest pin of each transaction that looked up an entry
	reference_map_t<Transaction, Pin> pins;
	idx_t next_sequence = 0;
	vector<RetiredEntry> retired_entries;
};

} // namespace duckdb
