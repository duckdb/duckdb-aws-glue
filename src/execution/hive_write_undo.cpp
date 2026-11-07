#include "execution/hive_write_undo.hpp"

#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_schema_set.hpp"
#include "core/hive_location.hpp"
#include "planning/hive_multi_file_reader.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

void UndoHiveWrite(ClientContext &context, GlueCatalog &catalog, GlueHiveWrite &write) {
	auto table = write.database_name + "." + write.table_name;
	// CREATE TABLE AS: whether the location holds data files of another writer, which a dropped table would leave
	// behind unread. Counted as ClearLocation does, so files a scan skips (_SUCCESS, markers) do not keep the table
	bool others_files = false;
	bool listed = false;
	if (LocationHasGlob(write.location)) {
		// the listing below would read the location as a pattern
		DUCKDB_LOG_WARNING(context,
		                   "Could not list the location of Hive table '%s' to undo a failed write: '%s' contains a "
		                   "glob character",
		                   table, write.location);
	} else if (!write.files_deleted || write.created_table) {
		try {
			// the database's file system, not one that stops at the interrupt the failed query set to end its pipelines
			auto &fs = DatabaseInstance::GetDatabase(context).GetFileSystem();
			auto prefix = write.write_id + "_";
			auto data_prefix = write.location + "/";
			vector<string> written_files;
			// S3 lists by key prefix, not by file name, so the files of the write are found by listing the location
			auto listing = ListLocation(fs, write.location);
			for (auto &file : listing->GetAllFiles()) {
				auto name = file.path.substr(file.path.find_last_of('/') + 1);
				if (StringUtil::StartsWith(name, prefix)) {
					written_files.push_back(file.path);
				} else if (write.created_table && IsDataFile(file, data_prefix)) {
					others_files = true;
				}
			}
			listed = true;
			if (!write.files_deleted && !written_files.empty()) {
				fs.RemoveFiles(written_files);
			}
		} catch (std::exception &ex) {
			DUCKDB_LOG_WARNING(context, "Could not delete the files a failed write left in Hive table '%s': %s", table,
			                   ErrorData(ex).RawMessage());
		}
	}
	if (!write.created_table) {
		return;
	}
	if (!listed || others_files) {
		// another writer's files would be left in a table nobody reads: keep the table
		DUCKDB_LOG_WARNING(context, "Kept Hive table '%s' after CREATE TABLE AS failed: %s", table,
		                   listed ? "it holds files another writer added" : "its location could not be listed");
		return;
	}
	try {
		if (write.created_table_time.IsValid()) {
			auto create_time =
			    GlueAPI::GetTableCreateTime(write.client, catalog, write.database_name, write.table_name);
			if (create_time.IsValid() && create_time.GetIndex() != write.created_table_time.GetIndex()) {
				// most likely dropped and created again by someone else while the statement ran: keep it
				DUCKDB_LOG_WARNING(context,
				                   "Kept Hive table '%s' after CREATE TABLE AS failed: it was created again by another "
				                   "client while the statement ran",
				                   table);
				return;
			}
		}
		GlueAPI::DeleteTable(context, write.client, catalog, write.database_name, write.table_name);
		// looked up again by name: the schema entry the statement used may have been replaced since
		auto schema = catalog.GetSchemas().GetCachedEntry(write.database_name);
		if (schema) {
			schema->Cast<GlueSchemaEntry>().tables.RemoveEntry(write.table_name);
		}
	} catch (std::exception &ex) {
		DUCKDB_LOG_WARNING(context, "Could not drop Hive table '%s' after CREATE TABLE AS failed: %s", table,
		                   ErrorData(ex).RawMessage());
	}
}

} // namespace duckdb
