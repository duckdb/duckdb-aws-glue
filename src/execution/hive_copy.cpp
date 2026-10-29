#include "execution/hive_copy.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "planning/hive_multi_file_reader.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//! A CREATE TABLE AS writes only to a location without data files: the files of a dropped table there would read as
//! rows of the new one. Files a scan skips (_SUCCESS, directory markers, empty files) do not count; with 'overwrite'
//! the data files are deleted instead.
//! 'location' without a trailing slash, with s3a:// and s3n:// read as s3://, so that locations can be compared
static string ComparableLocation(string location) {
	StringUtil::RTrim(location, "/");
	auto scheme_end = location.find("://");
	if (scheme_end == string::npos) {
		return location;
	}
	auto scheme = StringUtil::Lower(location.substr(0, scheme_end));
	if (scheme == "s3a" || scheme == "s3n") {
		scheme = "s3";
	}
	return scheme + location.substr(scheme_end);
}

//! Whether one location holds the other, so that files of one are files of the other
static bool LocationsOverlap(const string &a, const string &b) {
	return a == b || StringUtil::StartsWith(a, b + "/") || StringUtil::StartsWith(b, a + "/");
}

//! 'overwrite' deletes files that must not belong to a table: refuse when a table of the same Glue database reads
//! them. Tables in other databases, catalogs or accounts are not checked.
static void ThrowIfLocationInUse(ClientContext &context, GlueSchemaEntry &schema, const GlueTableInfo &table_info,
                                 const string &location) {
	auto &catalog = schema.catalog.Cast<GlueCatalog>();
	auto target = ComparableLocation(location);
	auto tables = GlueAPI::GetTables(context, catalog, schema.database_info.name, false, nullptr, true);
	for (auto &table : tables) {
		if (table.location.empty() || !LocationsOverlap(target, ComparableLocation(table.location))) {
			continue;
		}
		throw InvalidInputException("CREATE TABLE AS for Hive table '%s.%s' WITH (overwrite = true) is not "
		                            "supported: its location '%s' overlaps the location '%s' of table '%s.%s', whose "
		                            "files it would delete. Give the table another location",
		                            table_info.schema_name, table_info.name, location, table.location,
		                            table_info.schema_name, table.name);
	}
}

static void ClearLocation(ClientContext &context, GlueSchemaEntry &schema, const GlueTableInfo &table_info,
                          const string &location, bool overwrite) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto listing = fs.GlobFileList(location + "/**", FileGlobOptions::ALLOW_EMPTY);
	vector<string> data_files;
	for (auto &file : listing->Files()) {
		if (!IsDataFile(file, location + "/")) {
			continue;
		}
		if (!overwrite) {
			throw InvalidInputException("CREATE TABLE AS for Hive table '%s.%s' is not supported: its location '%s' "
			                            "already holds files (e.g. '%s'), such as those of a dropped table, which "
			                            "would read as rows of the new table. Delete them, give the table another "
			                            "location, or create it WITH (overwrite = true) to delete them",
			                            table_info.schema_name, table_info.name, location, file.path);
		}
		data_files.push_back(file.path);
	}
	if (data_files.empty()) {
		return;
	}
	ThrowIfLocationInUse(context, schema, table_info, location);
	try {
		fs.RemoveFiles(data_files);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw IOException("CREATE TABLE AS for Hive table '%s.%s' failed deleting the %d data files at its location "
		                  "'%s', so the table was not created; some of the files may already be deleted: %s",
		                  table_info.schema_name, table_info.name, data_files.size(), location, error.RawMessage());
	}
}

GlueHiveCopy::GlueHiveCopy(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
                           unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality,
                           optional_ptr<GlueSchemaEntry> schema_p, unique_ptr<BoundCreateTableInfo> info_p,
                           GlueTableInfo table_info_p)
    : PhysicalCopyToFile(physical_plan, std::move(types), std::move(function), std::move(bind_data),
                         estimated_cardinality),
      schema(schema_p), info(std::move(info_p)), table_info(std::move(table_info_p)) {
	D_ASSERT((schema != nullptr) == (info != nullptr));
}

unique_ptr<GlobalSinkState> GlueHiveCopy::GetGlobalSinkState(ClientContext &context) const {
	if (info) {
		// CREATE TABLE AS
		annotated_lock_guard<annotated_mutex> guard(create_lock);
		if (!table_created) {
			auto location = table_info.location;
			StringUtil::RTrim(location, "/");
			auto overwrite = GlueSchemaEntry::ParseCreateTableOptions(context, info->Base()).overwrite;
			auto &create_schema = *schema.get_mutable();
			ClearLocation(context, create_schema, table_info, location, overwrite);
			auto &catalog = create_schema.catalog;
			auto created = create_schema.CreateTableFromInfo(catalog.GetCatalogTransaction(context), *info, table_info);
			if (!created) {
				throw CatalogException("Table with name \"%s\" appeared between planning and execution; retry CREATE "
				                       "TABLE IF NOT EXISTS",
				                       table_info.name);
			}
			table_created = true;
		}
	}
	return PhysicalCopyToFile::GetGlobalSinkState(context);
}

string GlueHiveCopy::GetName() const {
	return info ? "GLUE_HIVE_CREATE_TABLE_AS_COPY" : PhysicalCopyToFile::GetName();
}

} // namespace duckdb
