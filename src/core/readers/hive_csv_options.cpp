#include "core/readers/hive_csv_options.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/string_util.hpp"
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
	if (open_csv && (type == LogicalType::TINYINT || type == LogicalType::SMALLINT || type == LogicalType::INTEGER ||
	                 type == LogicalType::BIGINT)) {
		return null_for_invalid_data ? HiveFieldConversion::TRY_INTEGER : HiveFieldConversion::INTEGER;
	}
	if (type == LogicalType::TIMESTAMP && (!timestamp_formats.empty() || timestamp_millis)) {
		return HiveFieldConversion::TIMESTAMP_FORMATS;
	}
	return null_for_invalid_data ? HiveFieldConversion::TRY_CAST : HiveFieldConversion::CAST;
}

bool HiveCSVOptions::operator==(const HiveCSVOptions &other) const {
	return delimiter == other.delimiter && quote == other.quote && escape == other.escape &&
	       skip_lines == other.skip_lines && null_string == other.null_string && serde_fields == other.serde_fields &&
	       null_for_invalid_data == other.null_for_invalid_data && serde == other.serde &&
	       timestamp_formats == other.timestamp_formats && timestamp_millis == other.timestamp_millis;
}

bool HiveCSVOptions::operator!=(const HiveCSVOptions &other) const {
	return !(*this == other);
}

//! A Joda-Time pattern as a strptime format; false for a pattern strptime can not express, or one without a year,
//! whose default year (1970 in Hive, 1900 in strptime) differs
static bool JodaPatternToStrptime(const string &pattern, string &result) {
	result.clear();
	bool has_year = false;
	auto append_literal = [&](char c) {
		result += c == '%' ? string("%%") : string(1, c);
	};
	idx_t i = 0;
	while (i < pattern.size()) {
		auto c = pattern[i];
		if (c == '\'') {
			// a quoted literal, in which '' is a quote; '' outside one is a quote too
			idx_t end = i + 1;
			if (end < pattern.size() && pattern[end] == '\'') {
				append_literal('\'');
				i = end + 1;
				continue;
			}
			bool closed = false;
			for (; end < pattern.size(); end++) {
				if (pattern[end] != '\'') {
					append_literal(pattern[end]);
				} else if (end + 1 < pattern.size() && pattern[end + 1] == '\'') {
					append_literal('\'');
					end++;
				} else {
					closed = true;
					break;
				}
			}
			if (!closed) {
				return false;
			}
			i = end + 1;
			continue;
		}
		if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))) {
			append_literal(c);
			i++;
			continue;
		}
		idx_t count = 1;
		while (i + count < pattern.size() && pattern[i + count] == c) {
			count++;
		}
		i += count;
		switch (c) {
		case 'y':
			result += count == 2 ? "%y" : "%Y";
			has_year = true;
			break;
		case 'M':
			result += count >= 4 ? "%B" : count == 3 ? "%b" : "%m";
			break;
		case 'd':
			result += "%d";
			break;
		case 'H':
			result += "%H";
			break;
		case 'h':
			result += "%I";
			break;
		case 'm':
			result += "%M";
			break;
		case 's':
			result += "%S";
			break;
		case 'S':
			if (count != 3) {
				return false;
			}
			result += "%g";
			break;
		case 'a':
			result += "%p";
			break;
		case 'E':
			result += count >= 4 ? "%A" : "%a";
			break;
		default:
			// time zones (Z, z) would make the value a TIMESTAMP WITH TIME ZONE; other fields have no strptime code
			return false;
		}
	}
	return has_year;
}

void HiveCSVOptions::SetTimestampFormats(const string &property) {
	timestamp_formats.clear();
	timestamp_millis = false;
	// patterns are separated by ',', and '\' escapes the next character (a ',' in a pattern is '\,')
	vector<string> patterns(1);
	for (idx_t i = 0; i < property.size(); i++) {
		if (property[i] == '\\' && i + 1 < property.size()) {
			patterns.back() += property[++i];
		} else if (property[i] == ',') {
			patterns.emplace_back();
		} else {
			patterns.back() += property[i];
		}
	}
	for (auto &pattern : patterns) {
		if (StringUtil::CIEquals(pattern, "millis")) {
			timestamp_millis = true;
			continue;
		}
		string format;
		if (!pattern.empty() && JodaPatternToStrptime(pattern, format)) {
			timestamp_formats.push_back(format);
		}
	}
}

void HiveCSVOptions::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "delimiter", delimiter);
	serializer.WriteProperty(101, "quote", quote);
	serializer.WriteProperty(102, "escape", escape);
	serializer.WriteProperty(103, "skip_lines", skip_lines);
	serializer.WriteProperty(104, "null_string", null_string);
	serializer.WriteProperty(105, "serde_fields", serde_fields);
	serializer.WritePropertyWithDefault<bool>(106, "null_for_invalid_data", null_for_invalid_data, true);
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
	result.timestamp_formats = deserializer.ReadPropertyWithDefault<vector<string>>(110, "timestamp_formats");
	result.timestamp_millis = deserializer.ReadPropertyWithExplicitDefault<bool>(111, "timestamp_millis", false);
	result.serde = static_cast<HiveSerDe>(deserializer.ReadPropertyWithExplicitDefault<uint8_t>(
	    112, "serde", static_cast<uint8_t>(HiveSerDe::LAZY_SIMPLE)));
	return result;
}

} // namespace duckdb
