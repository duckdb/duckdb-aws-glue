#pragma once

#include "core/glue_info.hpp"

#include <memory>

namespace Aws {
namespace Glue {
class GlueClient;
} // namespace Glue
} // namespace Aws

namespace duckdb {
class ClientContext;
class GlueCatalog;

//! Thin wrapper around the AWS SDK Glue client
class GlueAPI {
public:
	//! Verify that the catalog can be reached with the configured credentials
	static void VerifyConnection(ClientContext &context, GlueCatalog &catalog);

	//! List all databases of the catalog
	static vector<GlueDatabaseInfo> GetDatabases(ClientContext &context, GlueCatalog &catalog);
	//! Fetch a single database, returns false if it does not exist
	static bool GetDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                        GlueDatabaseInfo &result);
	//! List all tables of a database
	static vector<GlueTableInfo> GetTables(ClientContext &context, GlueCatalog &catalog, const string &database_name);
	//! Fetch a single table, returns false if it does not exist. 'raw_json' (optional) receives the Glue Table
	//! object of the response as JSON.
	static bool GetTable(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                     const string &table_name, GlueTableInfo &result, string *raw_json = nullptr);

	//! Create a database, throws a CatalogException if it already exists
	static void CreateDatabase(ClientContext &context, GlueCatalog &catalog, const GlueDatabaseInfo &database);
	//! Delete a database (and all of its tables), throws a CatalogException if it does not exist
	static void DeleteDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name);
	//! Create a standard (Hive style) Glue table storing parquet files at 'table.location', with the columns and
	//! partition keys in 'table'
	static void CreateHiveTable(ClientContext &context, GlueCatalog &catalog, const GlueTableInfo &table);
	//! Replace the (data) columns of a table, keeping everything else of its Glue definition as is. Used for
	//! ALTER TABLE on Hive tables; open table formats keep their schema in their own metadata.
	static void UpdateTableColumns(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                               const string &table_name, const vector<GlueColumn> &columns);
	//! Move the table (StorageDescriptor.Location); existing partitions keep their own locations
	static void SetTableLocation(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                             const string &table_name, const string &location);
	//! Add the statistics of newly written files to the numRows, numFiles and totalSize parameters of the table.
	//! Nothing changes when the table has no (valid) statistics.
	static void AddTableStatistics(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                               const string &table_name, const GlueBasicStatistics &statistics);
	//! Point a partition at another location, throws a CatalogException if the partition does not exist
	static void SetPartitionLocation(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                                 const string &table_name, const vector<string> &values, const string &location);
	//! Fetch a single partition by its values, returns false if it does not exist
	static bool GetPartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                         const string &table_name, const vector<string> &values, GluePartitionInfo &result);
	//! Register one partition of a Hive table. Returns false if it already exists and 'if_not_exists' is set, throws
	//! a CatalogException if it already exists otherwise.
	static bool CreatePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                            const string &table_name, const GluePartitionInput &partition, bool if_not_exists);
	//! Unregister a partition (the data files are left in place), returns false if it does not exist
	static bool DeletePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                            const string &table_name, const vector<string> &values);
	//! Change the values of a partition, keeping its location and everything else (UpdatePartition). Throws a
	//! CatalogException if the partition does not exist or a partition with the new values already exists.
	static void RenamePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                            const string &table_name, const vector<string> &values,
	                            const vector<string> &new_values);
	//! List the partitions of a Hive table (GetPartitions, all pages)
	static vector<GluePartitionInfo> GetPartitions(ClientContext &context, GlueCatalog &catalog,
	                                               const string &database_name, const string &table_name);
	//! Register partitions of a Hive table (BatchCreatePartition). Partitions that already exist are skipped, but get
	//! the statistics of a partition input added to theirs (when they have valid statistics and the same location).
	static void BatchCreatePartitions(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                                  const string &table_name, const vector<GluePartitionInput> &partitions);
	//! Delete a table (the data files are left in place), throws a CatalogException if it does not exist
	static void DeleteTable(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                        const string &table_name);

private:
	//! Get (or create) the Glue client for the catalog, using the credentials of the configured DuckDB secret
	static std::shared_ptr<Aws::Glue::GlueClient> GetClient(ClientContext &context, GlueCatalog &catalog);
};

} // namespace duckdb
