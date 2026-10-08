#include "catalog/glue_transaction.hpp"
#include "catalog/glue_catalog.hpp"

#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

GlueTransaction::GlueTransaction(GlueCatalog &glue_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), glue_catalog(glue_catalog),
      transaction_state(GlueTransactionState::TRANSACTION_NOT_YET_STARTED) {
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

} // namespace duckdb
