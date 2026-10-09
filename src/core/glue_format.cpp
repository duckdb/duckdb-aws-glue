#include "core/glue_format.hpp"

#include "duckdb/common/string_util.hpp"

#include "core/glue_info.hpp"

namespace duckdb {

//! The format the table parameters announce, UNKNOWN when they announce none
static GlueFormat FromParameters(const GlueTableInfo &info) {
	// Open table formats register themselves through the 'table_type' parameter
	auto table_type = StringUtil::Upper(info.GetParameter("table_type"));
	if (table_type == "ICEBERG") {
		return GlueFormat::ICEBERG;
	}
	if (table_type == "DELTA") {
		return GlueFormat::DELTA;
	}
	if (table_type == "HUDI") {
		return GlueFormat::HUDI;
	}
	// Spark registers Delta tables through the data source provider
	auto provider = StringUtil::Lower(info.GetParameter("spark.sql.sources.provider"));
	if (provider == "delta") {
		return GlueFormat::DELTA;
	}
	if (provider == "iceberg") {
		return GlueFormat::ICEBERG;
	}
	if (provider == "hudi" || provider == "org.apache.hudi") {
		return GlueFormat::HUDI;
	}
	if (!info.GetMetadataLocation().empty()) {
		return GlueFormat::ICEBERG;
	}
	return GlueFormat::UNKNOWN;
}

//! The format another engine's InputFormat class reveals, UNKNOWN for the InputFormats of plain Hive tables
static GlueFormat FromInputFormat(const string &input_format) {
	auto lower = StringUtil::Lower(input_format);
	auto class_name = lower.substr(lower.find_last_of('.') + 1);
	// org.apache.hudi.hadoop.HoodieParquetInputFormat, HoodieParquetRealtimeInputFormat, legacy com.uber.hoodie
	if (StringUtil::StartsWith(lower, "org.apache.hudi.") || StringUtil::StartsWith(lower, "com.uber.hoodie.") ||
	    StringUtil::StartsWith(class_name, "hoodie")) {
		return GlueFormat::HUDI;
	}
	// org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat: the location holds manifests listing the data files
	if (class_name == "symlinktextinputformat") {
		return GlueFormat::SYMLINK;
	}
	// the Hive connectors: org.apache.iceberg.mr.hive.HiveIcebergInputFormat, io.delta.hive.DeltaInputFormat
	if (StringUtil::StartsWith(lower, "org.apache.iceberg.")) {
		return GlueFormat::ICEBERG;
	}
	if (StringUtil::StartsWith(lower, "io.delta.")) {
		return GlueFormat::DELTA;
	}
	return GlueFormat::UNKNOWN;
}

GlueTableFormat GlueTableFormat::Of(const GlueTableInfo &info) {
	GlueTableFormat result;
	result.format = FromParameters(info);
	if (result.format != GlueFormat::UNKNOWN) {
		result.source = GlueFormatSource::PARAMETERS;
		return result;
	}
	result.format = FromInputFormat(info.input_format);
	if (result.format != GlueFormat::UNKNOWN) {
		result.source = GlueFormatSource::INPUT_FORMAT;
		return result;
	}
	if (!info.input_format.empty() || !info.location.empty()) {
		result.format = GlueFormat::HIVE;
		result.source = GlueFormatSource::STORAGE_DESCRIPTOR;
	}
	return result;
}

bool GlueTableFormat::IsParameter(const string &key) {
	return StringUtil::CIEquals(key, "table_type") || StringUtil::CIEquals(key, "spark.sql.sources.provider") ||
	       StringUtil::CIEquals(key, "metadata_location");
}

bool GlueTableFormat::IsWritable(const GlueTableInfo &info, HiveFileFormat file_format) const {
	if (!IsHive()) {
		return false;
	}
	if (info.input_format.empty()) {
		return true;
	}
	auto lower = StringUtil::Lower(info.input_format);
	switch (file_format) {
	case HiveFileFormat::PARQUET:
		return lower == "org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat" ||
		       lower == "org.apache.parquet.hadoop.parquetinputformat" ||
		       lower == "parquet.hadoop.parquetinputformat" || lower == "parquet.hive.deprecatedparquetinputformat";
	case HiveFileFormat::CSV:
	case HiveFileFormat::JSON:
		return lower == "org.apache.hadoop.mapred.textinputformat" ||
		       lower == "org.apache.hadoop.mapred.lib.combinetextinputformat";
	case HiveFileFormat::AVRO:
		return lower == "org.apache.hadoop.hive.ql.io.avro.avrocontainerinputformat";
	}
	return false;
}

string GlueTableFormat::ToString() const {
	switch (format) {
	case GlueFormat::ICEBERG:
		return "ICEBERG";
	case GlueFormat::DELTA:
		return "DELTA";
	case GlueFormat::HUDI:
		return "HUDI";
	case GlueFormat::SYMLINK:
		return "SYMLINK";
	case GlueFormat::HIVE:
		return "HIVE";
	default:
		return "UNKNOWN";
	}
}

string GlueTableFormat::Describe(const GlueTableInfo &info) const {
	switch (source) {
	case GlueFormatSource::INPUT_FORMAT:
	case GlueFormatSource::STORAGE_DESCRIPTOR:
		return StringUtil::Format("%s (input format '%s')", ToString(), info.input_format);
	case GlueFormatSource::NONE:
		if (!info.glue_table_type.empty()) {
			return StringUtil::Format("UNKNOWN (glue table type '%s')", info.glue_table_type);
		}
		return ToString();
	case GlueFormatSource::PARAMETERS:
		return ToString();
	}
	return ToString();
}

} // namespace duckdb
