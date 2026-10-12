#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class GlueCatalog;

//! A table loaded under GOVERNANCE: the entry built from its Glue Table JSON, owned until the transaction ends, and
//! the entry the catalog returns for the table to DDL and queries alike: that entry or a view over it
struct GovernedCatalogEntry {
	unique_ptr<CatalogEntry> table;
	reference<CatalogEntry> governed_entry;
};

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

public:
	GlueCatalog &glue_catalog;
	//! Under GOVERNANCE, the tables loaded in this transaction: database name -> table name -> table
	case_insensitive_map_t<case_insensitive_map_t<GovernedCatalogEntry>> governed_entries;

private:
	GlueTransactionState transaction_state;
};

} // namespace duckdb
