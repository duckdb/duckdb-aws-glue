#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context_state.hpp"

namespace duckdb {

//! The data files the connection's last INSERT or CREATE TABLE AS into a Hive table wrote. When such a statement ends,
//! its files replace the list, which is emptied instead if the statement failed while running.
class GlueWrittenFilesState : public ClientContextState {
public:
	static shared_ptr<GlueWrittenFilesState> Get(ClientContext &context);

	void BeginWrite();
	//! Records the files of a write whose partitions are registered
	void AddFiles(const vector<string> &files);
	vector<string> GetFiles();

	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override;

private:
	annotated_mutex lock;
	bool write_started DUCKDB_GUARDED_BY(lock) = false;
	vector<string> pending_files DUCKDB_GUARDED_BY(lock);
	vector<string> files DUCKDB_GUARDED_BY(lock);
};

TableFunction GetGlueWrittenFilesFunction();

} // namespace duckdb
