#pragma once

#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class Serializer;
class Deserializer;

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
//! Whether the data files are text files (csv, json), whose codec the table records rather than the files themselves
bool IsTextFileFormat(HiveFileFormat format);
//! Parse 'parquet' | 'csv' | 'json' | 'avro' (case-insensitive), throws for anything else
HiveFileFormat HiveFileFormatFromString(const string &format);

//! How the files of a csv table are read
struct HiveCSVOptions {
	string delimiter = ",";
	string quote = "\"";
	string escape = "\"";
	//! The header lines every file starts with
	idx_t skip_lines = 0;
	string null_string;
	//! Read fields as text and TRY_CAST them; short rows padded with NULL, extra fields ignored
	bool serde_fields = false;

public:
	void Serialize(Serializer &serializer) const;
	static HiveCSVOptions Deserialize(Deserializer &deserializer);
};

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
	//! The file format to create the table with (CreateHiveTable); for a fetched table use GetFileFormat()
	HiveFileFormat file_format = HiveFileFormat::PARQUET;

public:
	bool IsView() const {
		return table_type == GlueTableType::VIRTUAL_VIEW;
	}
	//! Derive the open table format from the table parameters
	GlueTableFormat GetFormat() const;
	//! Whether 'key' is one of the table parameters GetFormat() derives the format from
	static bool IsFormatParameter(const string &key);
	//! Human readable description of the table type, used in error messages
	string GetFormatName() const;
	//! The 'metadata_location' parameter of an Iceberg table (empty if not present)
	string GetMetadataLocation() const;
	//! Look up a table parameter (case-insensitive key), returns empty string if missing
	string GetParameter(const string &key) const;
	bool IsBucketed() const;
	//! Hive-style description of the bucketing, used in error messages
	string DescribeBucketing() const;
	//! The file format of the data files, derived from the SerDe; throws NotImplementedException for other SerDes
	HiveFileFormat GetFileFormat() const;
	bool IsOpenCSVSerde() const;
	//! A property of the table as Hive hands it to the SerDe: a table parameter, else a SerDe parameter
	bool TryGetProperty(const string &key, string &result) const;
	//! separatorChar for OpenCSVSerde, else field.delim, serialization.format or '\001'
	string GetFieldDelimiter() const;
	//! The codec to write a csv / json table's files with (write.compression, else compressionType), uncompressed when
	//! the table names none; throws for a codec DuckDB can not write. Files are read with the codec their extension
	//! says, whatever the table records.
	FileCompressionType GetTextCompression() const;
	//! The codec the table records for its files, as DuckDB's writer for 'format' names it ('null' for an uncompressed
	//! avro file), empty when it records none; throws for a codec DuckDB can not write
	string GetCodec(HiveFileFormat format) const;
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
	//! The SELECT as DuckDB prints it; unqualified names in it belong to database_name
	string sql;
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

} // namespace duckdb
