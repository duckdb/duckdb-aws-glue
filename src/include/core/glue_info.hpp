#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

//! The (open) table format a Glue table is stored in, derived from the table parameters
enum class GlueTableFormat : uint8_t { ICEBERG, DELTA, HUDI, HIVE, UNKNOWN };

string GlueTableFormatToString(GlueTableFormat format);

enum class GlueSortOrder : uint8_t { UNSORTED, ASCENDING, DESCENDING };

struct GlueColumn {
	string name;
	//! The Glue (Hive style) type string, e.g. 'int', 'decimal(10,2)', 'array<string>'
	string type;
	string comment;
	//! Only set for entries of GlueTableInfo::sort_columns
	GlueSortOrder sort_order = GlueSortOrder::UNSORTED;

public:
	//! 'ASC', 'DESC', or empty when unsorted
	string DescribeSortOrder() const;
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
	//! StorageDescriptor.BucketColumns / NumberOfBuckets / SortColumns
	vector<string> bucket_columns;
	//! -1 if unbucketed or Glue did not record it, 0 for Hive's unbucketed. Neither implies the table
	//! is unbucketed on its own - see IsBucketed.
	int32_t number_of_buckets = -1;
	//! StorageDescriptor.SortColumns, in Glue's order; only name and sort_order are set
	vector<GlueColumn> sort_columns;
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
	bool IsBucketed() const;
	//! Hive-style description of the bucketing, used in error messages
	string DescribeBucketing() const;
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

//! The key/value parameters of a Glue table or partition
using GlueParameters = unordered_map<string, string>;

//! Hive's basic statistics of a table or partition, stored as its numRows, numFiles and totalSize parameters
struct GlueBasicStatistics {
	idx_t num_rows = 0;
	idx_t num_files = 0;
	idx_t total_size = 0;
};

//! A partition of a Hive table to register: the partition values (in partition key order) and its location
struct GluePartitionInput {
	vector<string> values;
	string location;
	//! The files written to the partition: its statistics when it is new, added to them when it exists
	optional<GlueBasicStatistics> statistics;
};

//! A partition of a Hive table as registered in Glue: the partition values (in partition key order, as strings)
//! and the location of its data files, which need not follow the <key>=<value> layout
struct GluePartitionInfo {
	vector<string> values;
	string location;
	GlueParameters parameters;
};

} // namespace duckdb
