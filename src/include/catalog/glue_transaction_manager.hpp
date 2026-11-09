#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "catalog/glue_transaction.hpp"

namespace duckdb {
class GlueCatalog;

//! Counts the connection's ended queries: QueryEnd runs once a query's tasks are cancelled
class GlueQueryEnds : public ClientContextState {
public:
	void QueryEnd(ClientContext &context) override {
		ended++;
	}

	atomic<idx_t> ended {0};
};

class GlueTransactionManager : public TransactionManager {
public:
	GlueTransactionManager(AttachedDatabase &db_p, GlueCatalog &glue_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

	//! Keep a replaced or removed entry alive until no query that may use it is running
	void RetireEntry(unique_ptr<CatalogEntry> entry);
	//! Keep the entries retired from now on alive until the connection's query ends (or the transaction, outside one)
	void PinEntries(ClientContext &context, Transaction &transaction);
	//! For the tests
	idx_t RetiredEntryCount();

private:
	struct Pin {
		//! Entries retired after the pin have a higher sequence number
		idx_t sequence;
		//! Taken inside a query: released when it ends, or when the connection is gone
		bool in_query;
		weak_ptr<GlueQueryEnds> query_ends;
		//! Their count when the pin was taken
		idx_t ended;
	};
	struct RetiredEntry {
		idx_t retired_at;
		unique_ptr<CatalogEntry> entry;
	};

	static bool IsReleased(const Pin &pin);
	//! The transaction ended: drop its pin, unless its query still runs. Called with transaction_lock held.
	void EndPin(Transaction &transaction);
	//! Free the retired entries no pin still holds. Called with transaction_lock held.
	void FreeRetiredEntries();

private:
	GlueCatalog &glue_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<GlueTransaction>> transactions;
	//! The latest pin of each transaction
	reference_map_t<Transaction, Pin> pins;
	//! Pins of transactions that ended while their connection was destroyed, until its query ends
	vector<Pin> closing_pins;
	idx_t next_sequence = 0;
	vector<RetiredEntry> retired_entries;
};

} // namespace duckdb
