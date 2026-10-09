#include "core/glue_info.hpp"
#include "core/helpers.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

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

bool GlueTableInfo::IsOpenCSVSerde() const {
	return StringUtil::Contains(StringUtil::Lower(serde_library), "opencsvserde");
}

bool GlueTableInfo::TryGetProperty(const string &key, string &result) const {
	// as Hive: the table's parameters override the SerDe's, and keys are case-sensitive
	for (auto properties : {&parameters, &serde_parameters}) {
		auto entry = properties->find(key);
		if (entry != properties->end()) {
			result = entry->second;
			return true;
		}
	}
	return false;
}

string GlueTableInfo::GetOpenCSVCharacter(const string &key, const string &fallback) const {
	string value;
	if (!TryGetProperty(key, value) || value.empty()) {
		return fallback;
	}
	string character;
	if (!TryFirstCharacter(value, character)) {
		throw InvalidInputException("Hive table '%s.%s' has an invalid '%s' of '%s', expected text", database_name,
		                            name, key, value);
	}
	return character;
}

//! Hive's LazyUtils.getByte: a byte code from -128 to 127 ('1' is '\001'), else the first character
static string LazySimpleSeparator(const GlueTableInfo &table, const string &value) {
	if (value.empty()) {
		return "\x01";
	}
	int64_t number;
	auto separator = TryParseInteger(value, -128, 127, number) ? static_cast<uint8_t>(number & 0xFF)
	                                                           : static_cast<uint8_t>(value[0]);
	if (separator == 0 || separator == '\n' || separator == '\r' || separator >= 0x80) {
		throw NotImplementedException("Hive table '%s.%s' has the field delimiter '%s', a byte DuckDB can not split "
		                              "fields on; only ASCII delimiters other than NUL and line breaks are supported",
		                              table.database_name, table.name, value);
	}
	return string(1, static_cast<char>(separator));
}

string GlueTableInfo::GetFieldDelimiter() const {
	if (IsOpenCSVSerde()) {
		return GetOpenCSVCharacter("separatorChar", ",");
	}
	string delimiter;
	if (!TryGetProperty("field.delim", delimiter)) {
		TryGetProperty("serialization.format", delimiter);
	}
	return LazySimpleSeparator(*this, delimiter);
}

string GlueTableInfo::GetNullFormat() const {
	if (IsOpenCSVSerde()) {
		return string();
	}
	string null_format;
	return TryGetProperty("serialization.null.format", null_format) ? null_format : "\\N";
}

idx_t GlueTableInfo::GetCountProperty(const string &key) const {
	string value;
	if (!TryGetProperty(key, value) || value.empty()) {
		return 0;
	}
	int64_t count;
	if (!TryParseInteger(value, 0, NumericLimits<int32_t>::Maximum(), count)) {
		throw InvalidInputException("Hive table '%s.%s' has an invalid '%s' of '%s', expected a non-negative number",
		                            database_name, name, key, value);
	}
	return NumericCast<idx_t>(count);
}

idx_t GlueTableInfo::GetHeaderLineCount() const {
	return GetCountProperty("skip.header.line.count");
}

void GlueTableInfo::CheckTextSerdeSupported(HiveFileFormat format) const {
	D_ASSERT(IsTextFileFormat(format));
	auto refuse = [&](const string &what) {
		throw NotImplementedException("Hive table '%s.%s' %s, which DuckDB can not read or write", database_name, name,
		                              what);
	};
	if (GetCountProperty("skip.footer.line.count") > 0) {
		refuse("has 'skip.footer.line.count' set");
	}
	if (format == HiveFileFormat::JSON) {
		if (GetHeaderLineCount() > 0) {
			refuse("has 'skip.header.line.count' set on JSON files");
		}
		return;
	}
	for (auto &column : columns) {
		if (column.type.find('<') != string::npos) {
			refuse(StringUtil::Format("has the nested column '%s' (%s), stored with collection delimiters", column.name,
			                          column.type));
		}
	}
	if (IsOpenCSVSerde()) {
		if (GetQuoteCharacter().size() > 1 || GetEscapeCharacter().size() > 1) {
			refuse("has a quoteChar or escapeChar of more than one byte");
		}
		return;
	}
	string value;
	if (TryGetProperty("escape.delim", value)) {
		refuse("has 'escape.delim' set");
	}
	if (TryGetProperty("serialization.encoding", value) && !StringUtil::CIEquals(value, "UTF-8") &&
	    !StringUtil::CIEquals(value, "UTF8")) {
		refuse(StringUtil::Format("has the 'serialization.encoding' '%s' (DuckDB reads UTF-8)", value));
	}
	if (TryGetProperty("serialization.last.column.takes.rest", value) && StringUtil::CIEquals(value, "true")) {
		refuse("has 'serialization.last.column.takes.rest' set");
	}
	if (StringUtil::Contains(GetNullFormat(), GetFieldDelimiter())) {
		refuse(StringUtil::Format("has the field delimiter '%s' in its NULL string '%s'", GetFieldDelimiter(),
		                          GetNullFormat()));
	}
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
		throw NotImplementedException("Can not write to Hive table '%s.%s': it records %s compression, DuckDB writes "
		                              "only gzip and zstd compressed csv and json files",
		                              database_name, name, compression.ToString());
	}
	return compression;
}

string GlueTableInfo::GetCodec(HiveFileFormat format) const {
	switch (format) {
	case HiveFileFormat::PARQUET: {
		auto codec = StringUtil::Lower(GetParameter("parquet.compression"));
		return codec == "none" ? "uncompressed" : codec;
	}
	case HiveFileFormat::AVRO: {
		auto codec = StringUtil::Lower(GetParameter("avro.output.codec"));
		if (codec == "none" || codec == "uncompressed") {
			return "null";
		}
		if (!codec.empty() && codec != "snappy" && codec != "deflate" && codec != "null") {
			throw NotImplementedException("Can not write to Hive table '%s.%s': it records %s compression, DuckDB "
			                              "writes only snappy and deflate compressed avro files",
			                              database_name, name, codec);
		}
		return codec;
	}
	case HiveFileFormat::CSV:
	case HiveFileFormat::JSON: {
		auto compression = GetTextCompression();
		return compression.IsCompressed() ? compression.ToString() : string();
	}
	}
	throw InternalException("Unknown Hive file format");
}

string GlueTableInfo::GetCompressionLevel() const {
	return GetParameter("compression_level");
}

string GlueTableInfo::GetQuoteCharacter() const {
	// LazySimpleSerDe does not quote
	return IsOpenCSVSerde() ? GetOpenCSVCharacter("quoteChar", "\"") : string();
}

string GlueTableInfo::GetEscapeCharacter() const {
	return IsOpenCSVSerde() ? GetOpenCSVCharacter("escapeChar", GetQuoteCharacter()) : string();
}

HiveCSVOptions GlueTableInfo::GetCSVOptions() const {
	HiveCSVOptions options;
	options.delimiter = GetFieldDelimiter();
	options.quote = GetQuoteCharacter();
	options.escape = GetEscapeCharacter();
	options.skip_lines = GetHeaderLineCount();
	// OpenCSVSerde has no NULL: "\n" matches no unquoted field
	options.null_string = IsOpenCSVSerde() ? "\n" : GetNullFormat();
	options.serde_fields = true;
	return options;
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

void HiveCSVOptions::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "delimiter", delimiter);
	serializer.WriteProperty(101, "quote", quote);
	serializer.WriteProperty(102, "escape", escape);
	serializer.WriteProperty(103, "skip_lines", skip_lines);
	serializer.WriteProperty(104, "null_string", null_string);
	serializer.WriteProperty(105, "serde_fields", serde_fields);
}

HiveCSVOptions HiveCSVOptions::Deserialize(Deserializer &deserializer) {
	HiveCSVOptions result;
	result.delimiter = deserializer.ReadProperty<string>(100, "delimiter");
	result.quote = deserializer.ReadProperty<string>(101, "quote");
	result.escape = deserializer.ReadProperty<string>(102, "escape");
	result.skip_lines = deserializer.ReadProperty<idx_t>(103, "skip_lines");
	result.null_string = deserializer.ReadProperty<string>(104, "null_string");
	result.serde_fields = deserializer.ReadProperty<bool>(105, "serde_fields");
	return result;
}

} // namespace duckdb
