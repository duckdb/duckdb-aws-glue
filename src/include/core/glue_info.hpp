#pragma once

#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

#include "core/glue_format.hpp"
#include "core/readers/hive_csv_options.hpp"
#include "core/readers/hive_json_options.hpp"

namespace duckdb {

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
	//! the catalog the database resides in
	string catalog_id;
	//! a Glue resource link (TargetDatabase set): a database of another catalog
	bool is_resource_link = false;
	string name;
	string description;
	//! Optional S3 location tables of this database default to
	string location_uri;
	unordered_map<string, string> parameters;
};

//! The file format of a Hive table's data files, decided by its SerDe
enum class HiveFileFormat : uint8_t { PARQUET, CSV, JSON, AVRO };
string HiveFileFormatToString(HiveFileFormat format);
//! Whether the data files are text files (csv, json), whose codec the table records rather than the files themselves
bool IsTextFileFormat(HiveFileFormat format);
//! Parse 'parquet' | 'csv' | 'json' | 'avro' (case-insensitive), throws for anything else
HiveFileFormat HiveFileFormatFromString(const string &format);

//! The SerDe of a Hive table, decided by its SerializationLibrary; SerDes of one file format can differ in how they
//! read the data files and which engines write them
enum class HiveSerDe : uint8_t { PARQUET, LAZY_SIMPLE, OPEN_CSV, HIVE_JSON, OPENX_JSON, AVRO };
HiveFileFormat HiveSerDeFileFormat(HiveSerDe serde);

//! Glue's TableType, as far as this extension decides anything on it. The field is a free string (EXTERNAL_TABLE,
//! VIRTUAL_VIEW, GOVERNED, whatever a writer sets), so anything else is OTHER and the raw value is kept alongside.
enum class GlueTableType : uint8_t { EXTERNAL_TABLE, VIRTUAL_VIEW, OTHER };
GlueTableType GlueTableTypeFromString(const string &type);

//! A Glue "Table"
struct GlueTableInfo {
	string name;
	string database_name;
	//! The Glue TableType as written (EXTERNAL_TABLE, VIRTUAL_VIEW, ...), not the open table format
	string glue_table_type;
	GlueTableType table_type = GlueTableType::OTHER;
	//! ViewOriginalText / ViewExpandedText of a VIRTUAL_VIEW
	string view_original_text;
	string view_expanded_text;
	//! Table Description
	string description;
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
	//! Glue's VersionId and CreateTime of a fetched table; a DROP + CREATE starts at version 1 again, CreateTime
	//! tells the two apart. Empty for a definition not fetched from Glue.
	string version_id;
	int64_t create_time_ms = 0;
	//! The file format to create the table with (CreateHiveTable); for a fetched table use GetFileFormat()
	HiveFileFormat file_format = HiveFileFormat::PARQUET;

public:
	bool IsView() const {
		return table_type == GlueTableType::VIRTUAL_VIEW;
	}
	//! The table format and what it was derived from (GlueTableFormat::Detect)
	GlueTableFormat GetFormat() const;
	//! Whether 'other' is the same Glue version of the table; false when either version is unknown
	bool IsSameVersion(const GlueTableInfo &other) const;
	//! The 'metadata_location' parameter of an Iceberg table (empty if not present)
	string GetMetadataLocation() const;
	//! Look up a table parameter (case-insensitive key), returns empty string if missing
	string GetParameter(const string &key) const;
	bool IsBucketed() const;
	//! Whether this is a partitioned table with Athena partition projection on (projection.enabled): Athena then
	//! derives the partitions from the projection.* parameters and ignores the ones registered in Glue
	bool IsPartitionProjected() const;
	//! Hive-style description of the bucketing, used in error messages
	string DescribeBucketing() const;
	//! The SerDe, derived from serde_library; throws NotImplementedException for other SerDes
	HiveSerDe GetSerDe() const;
	//! The file format of the data files, derived from the SerDe; throws NotImplementedException for other SerDes
	HiveFileFormat GetFileFormat() const;
	//! A property of the table as Hive hands it to the SerDe: a table parameter, else a SerDe parameter
	bool TryGetProperty(const string &key, string &result) const;
	//! separatorChar for OpenCSVSerde, else field.delim, serialization.format or '\001'
	string GetFieldDelimiter() const;
	//! The codec to write a csv / json table's files with (write.compression, else write_compression, else
	//! compressionType), uncompressed when the table names none; throws for a codec DuckDB can not write. Files are
	//! read with the codec their extension says, whatever the table records.
	FileCompressionType GetTextCompression() const;
	//! The codec the table records for its files, as DuckDB's writer for 'format' names it ('null' for an uncompressed
	//! avro file), empty when it records none; throws for a codec DuckDB can not write
	string GetCodec(HiveFileFormat format) const;
	//! The format's codec parameter 'key', else write_compression, empty when the table records neither
	string GetCodecParameter(const string &key) const;
	//! compression_level, empty when the table does not say
	string GetCompressionLevel() const;
	//! serialization.null.format ('\N' by default); empty for OpenCSVSerde
	string GetNullFormat() const;
	//! skip.header.line.count, 0 without
	idx_t GetHeaderLineCount() const;
	//! Throws for text tables DuckDB can not read or write: footer lines, JSON header lines, nested csv columns, ...
	void CheckTextSerdeSupported(HiveFileFormat format) const;
	//! OpenCSVSerde's quoteChar ('"' by default); empty for LazySimpleSerDe, which does not quote
	string GetQuoteCharacter() const;
	//! OpenCSVSerde's escapeChar, else its quote character; empty for LazySimpleSerDe
	string GetEscapeCharacter() const;
	//! How the files of a csv table are read
	HiveCSVOptions GetCSVOptions() const;
	//! How the files of a json table are read
	HiveJSONOptions GetJSONOptions() const;

private:
	//! A non-negative integer property, 0 when not set
	idx_t GetCountProperty(const string &key) const;
	//! An OpenCSVSerde character property (its first character), 'fallback' when not set
	string GetOpenCSVCharacter(const string &key, const string &fallback) const;
};

//! What CreateView / UpdateView write: a Hive style view (TableType VIRTUAL_VIEW) marked as written by DuckDB
struct GlueViewInfo {
	//! The Glue database (a DuckDB schema) the view lives in
	string database_name;
	string name;
	//! ViewOriginalText: the SELECT as written
	string sql;
	//! ViewExpandedText: tables as <db>.<table>, of other Glue catalogs by catalog id (executed)
	string expanded_sql;
	//! The bound output columns; empty for a view created with DEFER_BINDING
	vector<GlueColumn> columns;
	bool secure = false;
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

//! A partition as GetPartition returns it: also its storage descriptor and parameters
struct GluePartitionDetails : public GluePartitionInfo {
	string serde_library;
	unordered_map<string, string> serde_parameters;
	vector<GlueColumn> columns;
	unordered_map<string, string> parameters;
};

} // namespace duckdb
