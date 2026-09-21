#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/transaction/transaction.hpp"

#include "glue_api.hpp"

#include <functional>

namespace duckdb {
class GlueCatalog;

enum class GlueTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

//! Glue metadata resolved in this transaction
class GlueTransactionCache {
public:
	//! The current transaction's cache, or null when there is none (during ATTACH)
	static optional_ptr<GlueTransactionCache> Of(ClientContext &context, Catalog &catalog);
	static void Invalidate(ClientContext &context, Catalog &catalog);
	//! Clear the cache when the enclosing Glue mutation returns or throws
	struct InvalidateOnExit {
		InvalidateOnExit(ClientContext &context, Catalog &catalog) : context(context), catalog(catalog) {
		}
		~InvalidateOnExit() {
			try {
				Invalidate(context, catalog);
			} catch (...) {
			}
		}
		ClientContext &context;
		Catalog &catalog;
	};

	shared_ptr<const GlueTableInfo> GetTable(const string &database_name, const string &table_name,
	                                         const std::function<shared_ptr<const GlueTableInfo>()> &load) {
		return GetOrLoad(tables, database_name + "\n" + table_name, load);
	}
	shared_ptr<const vector<GluePartitionInfo>>
	GetPartitions(const string &database_name, const string &table_name,
	              const std::function<shared_ptr<const vector<GluePartitionInfo>>()> &load) {
		return GetOrLoad(partitions, database_name + "\n" + table_name, load);
	}
	void Clear() {
		lock_guard<mutex> guard(lock);
		tables.clear();
		partitions.clear();
	}

private:
	//! Holds the lock across 'load' so concurrent references fetch once; 'load' must not call back into this cache
	template <class T>
	shared_ptr<const T> GetOrLoad(unordered_map<string, shared_ptr<const T>> &map, const string &key,
	                              const std::function<shared_ptr<const T>()> &load) {
		lock_guard<mutex> guard(lock);
		auto entry = map.find(StringUtil::Lower(key));
		if (entry != map.end()) {
			return entry->second;
		}
		auto value = load();
		if (value) {
			map[StringUtil::Lower(key)] = value;
		}
		return value;
	}

	mutex lock;
	unordered_map<string, shared_ptr<const GlueTableInfo>> tables;
	unordered_map<string, shared_ptr<const vector<GluePartitionInfo>>> partitions;
};

//! Glue has no transactional semantics in this extension: every DDL statement is executed against the Glue API
//! immediately. The transaction exists because every attached database needs one, and carries the metadata cache.
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
	GlueTransactionCache cache;

private:
	GlueTransactionState transaction_state;
};

} // namespace duckdb
