#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class GlueCatalog;

enum class GlueTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

//! Glue has no transactional semantics in this extension: every DDL statement is executed against the Glue API
//! immediately. The transaction only exists because every attached database needs one.
class GlueTransaction : public Transaction {
public:
	GlueTransaction(GlueCatalog &glue_catalog, TransactionManager &manager, ClientContext &context);
	~GlueTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	static GlueTransaction &Get(ClientContext &context, Catalog &catalog);
	//! An Iceberg table this transaction looked up: the Iceberg catalog binds its DDL, so the cached Glue entry of
	//! the table is refreshed once the transaction ends
	void AddIcebergTable(const string &database_name, const string &table_name);

public:
	GlueCatalog &glue_catalog;

private:
	void InvalidateIcebergTables();

private:
	GlueTransactionState transaction_state;
	vector<pair<string, string>> iceberg_tables;
};

} // namespace duckdb
