#include "core/glue_info.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

string GlueTableFormatToString(GlueTableFormat format) {
	switch (format) {
	case GlueTableFormat::ICEBERG:
		return "ICEBERG";
	case GlueTableFormat::DELTA:
		return "DELTA";
	case GlueTableFormat::HUDI:
		return "HUDI";
	case GlueTableFormat::HIVE:
		return "HIVE";
	default:
		return "UNKNOWN";
	}
}

string GlueTableInfo::GetParameter(const string &key) const {
	for (auto &entry : parameters) {
		if (StringUtil::CIEquals(entry.first, key)) {
			return entry.second;
		}
	}
	return string();
}

string HiveFileFormatToString(HiveFileFormat format) {
	switch (format) {
	case HiveFileFormat::PARQUET:
		return "parquet";
	case HiveFileFormat::CSV:
		return "csv";
	case HiveFileFormat::JSON:
		return "json";
	case HiveFileFormat::AVRO:
		return "avro";
	}
	throw InternalException("Unknown HiveFileFormat");
}

bool IsTextFileFormat(HiveFileFormat format) {
	return format == HiveFileFormat::CSV || format == HiveFileFormat::JSON;
}

HiveFileFormat HiveFileFormatFromString(const string &format) {
	auto lower = StringUtil::Lower(format);
	if (lower == "parquet") {
		return HiveFileFormat::PARQUET;
	}
	if (lower == "csv") {
		return HiveFileFormat::CSV;
	}
	if (lower == "json") {
		return HiveFileFormat::JSON;
	}
	if (lower == "avro") {
		return HiveFileFormat::AVRO;
	}
	throw BinderException("Unknown Hive file format '%s', expected 'parquet', 'csv', 'json' or 'avro'", format);
}

string GlueTableInfo::GetSerdeParameter(const string &key) const {
	for (auto &entry : serde_parameters) {
		if (StringUtil::CIEquals(entry.first, key)) {
			return entry.second;
		}
	}
	return string();
}

bool GlueTableInfo::IsBucketed() const {
	// NumberOfBuckets is -1 or 0 for unbucketed tables but can also be unset on bucketed ones
	return !bucket_columns.empty();
}

string GlueColumn::DescribeSortOrder() const {
	switch (sort_order) {
	case GlueSortOrder::ASCENDING:
		return "ASC";
	case GlueSortOrder::DESCENDING:
		return "DESC";
	default:
		return string();
	}
}

string GlueTableInfo::DescribeBucketing() const {
	auto result = StringUtil::Format("clustered by (%s)", StringUtil::Join(bucket_columns, ", "));
	if (number_of_buckets > 0) {
		result += StringUtil::Format(" into %d buckets", number_of_buckets);
	}
	if (!sort_columns.empty()) {
		vector<string> sorted;
		for (auto &sort_column : sort_columns) {
			sorted.push_back(sort_column.name + " " + sort_column.DescribeSortOrder());
		}
		result += StringUtil::Format(", sorted by (%s)", StringUtil::Join(sorted, ", "));
	}
	return result;
}

HiveFileFormat GlueTableInfo::GetFileFormat() const {
	auto serde = StringUtil::Lower(serde_library);
	if (StringUtil::Contains(serde, "parquet")) {
		return HiveFileFormat::PARQUET;
	}
	if (StringUtil::Contains(serde, "lazysimpleserde") || StringUtil::Contains(serde, "opencsvserde")) {
		return HiveFileFormat::CSV;
	}
	if (StringUtil::Contains(serde, "json")) {
		return HiveFileFormat::JSON;
	}
	if (StringUtil::Contains(serde, "avro")) {
		return HiveFileFormat::AVRO;
	}
	throw NotImplementedException("Hive table '%s.%s' uses SerDe '%s', only parquet (ParquetHiveSerDe), csv "
	                              "(LazySimpleSerDe, OpenCSVSerde), json (JsonSerDe) and avro (AvroSerDe) tables are "
	                              "supported",
	                              database_name, name, serde_library);
}

string GlueTableInfo::GetFieldDelimiter() const {
	// LazySimpleSerDe: field.delim, OpenCSVSerde: separatorChar
	auto delimiter = GetSerdeParameter("field.delim");
	if (delimiter.empty()) {
		delimiter = GetSerdeParameter("separatorChar");
	}
	if (delimiter.empty()) {
		return ",";
	}
	return delimiter;
}

bool GlueTableInfo::HasHeader() const {
	return GetParameter("skip.header.line.count") == "1";
}

FileCompressionType GlueTableInfo::GetTextCompression() const {
	auto codec = GetParameter("write.compression");
	if (codec.empty()) {
		codec = GetParameter("compressionType");
	}
	if (codec.empty()) {
		return FileCompressionType::AUTO_DETECT;
	}
	FileCompressionType compression(codec);
	if (compression.IsCompressed() && compression != FileCompressionType::GZIP &&
	    compression != FileCompressionType::ZSTD) {
		throw NotImplementedException("Hive table '%s.%s' is %s compressed, DuckDB reads and writes only gzip and zstd "
		                              "compressed csv and json files",
		                              database_name, name, compression.ToString());
	}
	return compression;
}

string GlueTableInfo::GetParquetCompression() const {
	auto codec = StringUtil::Lower(GetParameter("parquet.compression"));
	return codec == "none" ? "uncompressed" : codec;
}

string GlueTableInfo::GetCompressionLevel() const {
	return GetParameter("compression_level");
}

string GlueTableInfo::GetQuoteCharacter() const {
	// OpenCSVSerde: quoteChar. LazySimpleSerDe does not quote at all, but DuckDB writes (and reads) quoted fields
	// with the '"' it defaults to, which is also OpenCSVSerde's default
	auto quote = GetSerdeParameter("quoteChar");
	if (quote.empty()) {
		return "\"";
	}
	return quote;
}

string GlueTableInfo::GetEscapeCharacter() const {
	auto escape = GetSerdeParameter("escapeChar");
	if (escape.empty()) {
		return GetQuoteCharacter();
	}
	return escape;
}

GlueTableFormat GlueTableInfo::GetFormat() const {
	// Open table formats register themselves through the 'table_type' parameter
	auto table_type = StringUtil::Upper(GetParameter("table_type"));
	if (table_type == "ICEBERG") {
		return GlueTableFormat::ICEBERG;
	}
	if (table_type == "DELTA") {
		return GlueTableFormat::DELTA;
	}
	if (table_type == "HUDI") {
		return GlueTableFormat::HUDI;
	}
	// Spark registers Delta tables through the data source provider
	auto provider = StringUtil::Lower(GetParameter("spark.sql.sources.provider"));
	if (provider == "delta") {
		return GlueTableFormat::DELTA;
	}
	if (provider == "iceberg") {
		return GlueTableFormat::ICEBERG;
	}
	if (provider == "hudi") {
		return GlueTableFormat::HUDI;
	}
	if (!GetMetadataLocation().empty()) {
		return GlueTableFormat::ICEBERG;
	}
	if (!input_format.empty() || !location.empty()) {
		// Regular (Hive style) table with a storage descriptor
		return GlueTableFormat::HIVE;
	}
	return GlueTableFormat::UNKNOWN;
}

GlueTableType GlueTableTypeFromString(const string &type) {
	if (StringUtil::CIEquals(type, "EXTERNAL_TABLE")) {
		return GlueTableType::EXTERNAL_TABLE;
	}
	if (StringUtil::CIEquals(type, "VIRTUAL_VIEW")) {
		return GlueTableType::VIRTUAL_VIEW;
	}
	return GlueTableType::OTHER;
}

string GlueTableInfo::GetFormatName() const {
	auto format = GetFormat();
	if (format == GlueTableFormat::HIVE) {
		return StringUtil::Format("HIVE (input format '%s')", input_format);
	}
	if (format == GlueTableFormat::UNKNOWN && !glue_table_type.empty()) {
		return StringUtil::Format("UNKNOWN (glue table type '%s')", glue_table_type);
	}
	return GlueTableFormatToString(format);
}

string GlueTableInfo::GetMetadataLocation() const {
	return GetParameter("metadata_location");
}

bool GlueTableInfo::IsFormatParameter(const string &key) {
	return StringUtil::CIEquals(key, "table_type") || StringUtil::CIEquals(key, "spark.sql.sources.provider") ||
	       StringUtil::CIEquals(key, "metadata_location");
}

} // namespace duckdb
