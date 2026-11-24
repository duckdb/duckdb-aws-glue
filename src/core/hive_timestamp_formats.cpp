#include "core/hive_timestamp_formats.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

bool JodaPatternToStrptime(const string &pattern, string &result) {
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

HiveTimestampFormats HiveTimestampFormats::Parse(const string &property) {
	HiveTimestampFormats result;
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
			result.millis = true;
			continue;
		}
		string format;
		if (!pattern.empty() && JodaPatternToStrptime(pattern, format)) {
			result.strptime_formats.push_back(format);
		}
	}
	return result;
}

} // namespace duckdb
