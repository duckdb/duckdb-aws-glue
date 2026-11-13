#include "core/readers/hive_csv_options.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "re2/re2.h"
#include "duckdb/common/types.hpp"

namespace duckdb {

HiveFieldConversion HiveCSVOptions::FieldConversion(const LogicalType &type) const {
	// the whole type, not the id: an alias of VARCHAR such as JSON keeps its own conversion
	if (type == LogicalType::VARCHAR) {
		return HiveFieldConversion::NONE;
	}
	auto open_csv = serde == HiveSerDe::OPEN_CSV;
	if (open_csv && type.id() == LogicalTypeId::BOOLEAN) {
		return HiveFieldConversion::FALSE_UNLESS_TRUE;
	}
	if (open_csv && (type == LogicalType::TIMESTAMP || type == LogicalType::DATE)) {
		return null_for_invalid_data ? HiveFieldConversion::TRY_EPOCH : HiveFieldConversion::EPOCH;
	}
	if (type == LogicalType::TIMESTAMP && (!timestamp_formats.empty() || timestamp_millis)) {
		return HiveFieldConversion::TIMESTAMP_FORMATS;
	}
	return null_for_invalid_data ? HiveFieldConversion::TRY_CAST : HiveFieldConversion::CAST;
}

string HiveCSVOptions::FieldEscapePattern() const {
	return duckdb_re2::RE2::QuoteMeta(field_escape) + "([" + duckdb_re2::RE2::QuoteMeta(field_escape) +
	       duckdb_re2::RE2::QuoteMeta(quote) + "])?";
}

bool HiveCSVOptions::operator==(const HiveCSVOptions &other) const {
	return delimiter == other.delimiter && quote == other.quote && escape == other.escape &&
	       skip_lines == other.skip_lines && null_string == other.null_string && serde_fields == other.serde_fields &&
	       null_for_invalid_data == other.null_for_invalid_data && serde == other.serde &&
	       field_escape == other.field_escape && timestamp_formats == other.timestamp_formats &&
	       timestamp_millis == other.timestamp_millis;
}

bool HiveCSVOptions::operator!=(const HiveCSVOptions &other) const {
	return !(*this == other);
}

void HiveCSVOptions::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "delimiter", delimiter);
	serializer.WriteProperty(101, "quote", quote);
	serializer.WriteProperty(102, "escape", escape);
	serializer.WriteProperty(103, "skip_lines", skip_lines);
	serializer.WriteProperty(104, "null_string", null_string);
	serializer.WriteProperty(105, "serde_fields", serde_fields);
	serializer.WritePropertyWithDefault<bool>(106, "null_for_invalid_data", null_for_invalid_data, true);
	serializer.WritePropertyWithDefault<string>(109, "field_escape", field_escape, string());
	serializer.WritePropertyWithDefault<vector<string>>(110, "timestamp_formats", timestamp_formats);
	serializer.WritePropertyWithDefault<bool>(111, "timestamp_millis", timestamp_millis, false);
	serializer.WritePropertyWithDefault<uint8_t>(112, "serde", static_cast<uint8_t>(serde),
	                                             static_cast<uint8_t>(HiveSerDe::LAZY_SIMPLE));
}

HiveCSVOptions HiveCSVOptions::Deserialize(Deserializer &deserializer) {
	HiveCSVOptions result;
	result.delimiter = deserializer.ReadProperty<string>(100, "delimiter");
	result.quote = deserializer.ReadProperty<string>(101, "quote");
	result.escape = deserializer.ReadProperty<string>(102, "escape");
	result.skip_lines = deserializer.ReadProperty<idx_t>(103, "skip_lines");
	result.null_string = deserializer.ReadProperty<string>(104, "null_string");
	result.serde_fields = deserializer.ReadProperty<bool>(105, "serde_fields");
	result.null_for_invalid_data =
	    deserializer.ReadPropertyWithExplicitDefault<bool>(106, "null_for_invalid_data", true);
	result.field_escape = deserializer.ReadPropertyWithExplicitDefault<string>(109, "field_escape", string());
	result.timestamp_formats = deserializer.ReadPropertyWithDefault<vector<string>>(110, "timestamp_formats");
	result.timestamp_millis = deserializer.ReadPropertyWithExplicitDefault<bool>(111, "timestamp_millis", false);
	result.serde = static_cast<HiveSerDe>(deserializer.ReadPropertyWithExplicitDefault<uint8_t>(
	    112, "serde", static_cast<uint8_t>(HiveSerDe::LAZY_SIMPLE)));
	return result;
}

} // namespace duckdb
