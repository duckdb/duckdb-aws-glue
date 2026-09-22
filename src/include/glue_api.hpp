#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

#include <memory>

namespace Aws {
namespace Glue {
class GlueClient;
} // namespace Glue
} // namespace Aws

namespace duckdb {
class ClientContext;
class GlueCatalog;

//! The (open) table format a Glue table is stored in, derived from the table parameters
enum class GlueTableFormat : uint8_t { ICEBERG, DELTA, HUDI, HIVE, UNKNOWN };

string GlueTableFormatToString(GlueTableFormat format);

struct GlueColumn {
	string name;
	//! The Glue (Hive style) type string, e.g. 'int', 'decimal(10,2)', 'array<string>'
	string type;
	string comment;
};

//! A Glue "Database", exposed as a DuckDB schema
struct GlueDatabaseInfo {
	string name;
	string description;
	//! Optional S3 location tables of this database default to
	string location_uri;
	unordered_map<string, string> parameters;
};

//! The file format of a Hive table's data files, decided by its SerDe
enum class HiveFileFormat : uint8_t { PARQUET, CSV, JSON, AVRO };
string HiveFileFormatToString(HiveFileFormat format);
//! Parse 'parquet' | 'csv' | 'json' | 'avro' (case-insensitive), throws for anything else
HiveFileFormat HiveFileFormatFromString(const string &format);

//! A Glue "Table"
struct GlueTableInfo {
	string name;
	string database_name;
	//! The Glue TableType (EXTERNAL_TABLE, VIRTUAL_VIEW, ...), not the open table format
	string glue_table_type;
	//! StorageDescriptor.Location
	string location;
	string input_format;
	string output_format;
	//! StorageDescriptor.SerdeInfo: decides how the data files are read
	string serde_library;
	unordered_map<string, string> serde_parameters;
	vector<GlueColumn> columns;
	vector<GlueColumn> partition_keys;
	unordered_map<string, string> parameters;
	//! The file format to create the table with (CreateHiveTable); for a fetched table use GetFileFormat()
	HiveFileFormat file_format = HiveFileFormat::PARQUET;
	//! The CSV dialect to create a csv table with (CreateHiveTable); for a fetched table use GetFieldDelimiter(),
	//! GetQuoteCharacter() and GetEscapeCharacter(). With a quote or escape character the table gets OpenCSVSerde
	//! (which quotes), without both LazySimpleSerDe (which does not)
	string csv_delimiter = ",";
	string csv_quote;
	string csv_escape;

public:
	//! Derive the open table format from the table parameters
	GlueTableFormat GetFormat() const;
	//! Human readable description of the table type, used in error messages
	string GetFormatName() const;
	//! The 'metadata_location' parameter of an Iceberg table (empty if not present)
	string GetMetadataLocation() const;
	//! Look up a table parameter (case-insensitive key), returns empty string if missing
	string GetParameter(const string &key) const;
	//! Look up a SerDe parameter (case-insensitive key), returns empty string if missing
	string GetSerdeParameter(const string &key) const;
	//! The file format of the data files, derived from the SerDe; throws NotImplementedException for other SerDes
	HiveFileFormat GetFileFormat() const;
	//! The field delimiter of a CSV table (field.delim / separatorChar), ',' when the SerDe does not say
	string GetFieldDelimiter() const;
	//! Whether the data files of a CSV table start with a header line (skip.header.line.count)
	bool HasHeader() const;
	//! The quote character of a CSV table (quoteChar of OpenCSVSerde), '"' when the SerDe does not say
	string GetQuoteCharacter() const;
	//! The escape character of a CSV table (escapeChar of OpenCSVSerde), else the quote character
	string GetEscapeCharacter() const;
};

//! A partition of a Hive table to register: the partition values (in partition key order) and its location
struct GluePartitionInput {
	vector<string> values;
	string location;
};

//! A partition of a Hive table as registered in Glue: the partition values (in partition key order, as strings)
//! and the location of its data files, which need not follow the <key>=<value> layout
struct GluePartitionInfo {
	vector<string> values;
	string location;
};

//! Thin wrapper around the AWS SDK Glue client
class GlueAPI {
public:
	//! Verify that the catalog can be reached with the configured credentials
	static void VerifyConnection(ClientContext &context, GlueCatalog &catalog);

	//! List all databases of the catalog
	static vector<GlueDatabaseInfo> GetDatabases(ClientContext &context, GlueCatalog &catalog);
	//! Fetch a single database, returns false if it does not exist. 'raw_json' (optional) receives the Glue Database
	//! object of the response as JSON.
	static bool GetDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                        GlueDatabaseInfo &result, string *raw_json = nullptr);
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
	//! Merge 'parameters' into the Glue parameters of a database, keeping the rest of its definition as is. Follows
	//! Hive's ALTER DATABASE SET DBPROPERTIES: the listed keys are added or overwritten, unlisted ones are kept.
	//! Throws a CatalogException if the database does not exist.
	static void SetDatabaseProperties(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                                  const unordered_map<string, string> &parameters);
	//! Set the description (Athena's COMMENT) of a database, keeping the rest of its definition as is. Throws a
	//! CatalogException if the database does not exist.
	static void SetDatabaseDescription(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                                   const string &description);
	//! Whether the database has at least one table (or view). Cheaper than GetTables, which pages to the end and
	//! parses every table: this stops at the first entry, which is all a RESTRICT drop needs to know.
	//! 'first_table_name' (optional) receives the name of that entry, to name it in an error message.
	static bool DatabaseHasTables(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                              string *first_table_name = nullptr);
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
	//! Register partitions of a Hive table (BatchCreatePartition). Partitions that already exist are skipped.
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
