#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string.hpp"

#include "api/glue_api.hpp"

namespace duckdb {
class ClientContext;
class GlueCatalog;

//! What an INSERT or CREATE TABLE AS into a Hive table changed, so that a failed statement can be taken back
struct GlueHiveWrite {
	string write_id;
	string database_name;
	string table_name;
	//! The files of the write are named <write_id>_..., below this location
	string location;
	//! CREATE TABLE AS created the table, and the client to drop it with (the rollback runs when the transaction has
	//! ended, too late to look up the secret)
	bool created_table = false;
	std::shared_ptr<Aws::Glue::GlueClient> client;
	//! The CreateTime of the table it created, if it could be fetched: a table with another CreateTime was created
	//! again by someone else, and is not dropped. Glue keeps whole seconds, so a table dropped and created again within
	//! the same second is not told apart
	optional_idx created_table_time;
	//! The statement deleted its files itself (a failed partition registration does), so the undo need not look
	bool files_deleted = false;
};

//! Undo a write whose statement failed: delete its files and, for CREATE TABLE AS, drop the table it created. Best
//! effort: a step that fails is logged
void UndoHiveWrite(ClientContext &context, GlueCatalog &catalog, GlueHiveWrite &write);

} // namespace duckdb
