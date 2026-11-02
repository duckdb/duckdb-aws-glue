#include "core/java_date_format.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"

namespace duckdb {

static bool IsSupportedLetter(char c) {
	return c == 'y' || c == 'M' || c == 'd' || c == 'H' || c == 'm' || c == 's' || c == 'S';
}

static bool IsAsciiLetter(char c) {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

//! Java's limits: two of d H m s, nine S, four M (a narrow month name cannot be parsed back), any number of y
static idx_t MaxLetterCount(char letter) {
	switch (letter) {
	case 'S':
		return 9;
	case 'M':
		return 4;
	case 'y':
		return NumericLimits<idx_t>::Maximum();
	default:
		return 2;
	}
}

static const char *CountWord(idx_t count) {
	switch (count) {
	case 2:
		return "twice";
	case 4:
		return "four times";
	default:
		return "nine times";
	}
}

JavaDateFormat::JavaDateFormat(const string &pattern, const string &parameter, const string &table_description) {
	bool has_year_field = false;
	if (pattern.empty()) {
		throw InvalidInputException("Glue table '%s': %s is required for a date partition column", table_description,
		                            parameter);
	}
	auto add_literal = [&](char c) {
		if (elements.empty() || elements.back().letter != 0) {
			elements.push_back(Element());
		}
		elements.back().literal += c;
	};
	idx_t position = 0;
	while (position < pattern.size()) {
		auto c = pattern[position];
		if (c == '\'') {
			// 'text' is a literal, '' a single quote, inside quotes and outside
			if (position + 1 < pattern.size() && pattern[position + 1] == '\'') {
				add_literal('\'');
				position += 2;
				continue;
			}
			auto end = position + 1;
			while (true) {
				if (end >= pattern.size()) {
					throw InvalidInputException("Glue table '%s': %s '%s' has an unterminated quote", table_description,
					                            parameter, pattern);
				}
				if (pattern[end] != '\'') {
					add_literal(pattern[end]);
					end++;
					continue;
				}
				if (end + 1 < pattern.size() && pattern[end + 1] == '\'') {
					add_literal('\'');
					end += 2;
					continue;
				}
				break;
			}
			position = end + 1;
			continue;
		}
		if (IsAsciiLetter(c)) {
			if (!IsSupportedLetter(c)) {
				throw InvalidInputException("Glue table '%s': %s '%s' uses the pattern letter '%c', which is not "
				                            "supported: the letters y, M, d, H, m, s and S and quoted text are",
				                            table_description, parameter, pattern, c);
			}
			idx_t count = 0;
			while (position + count < pattern.size() && pattern[position + count] == c) {
				count++;
			}
			if (count > MaxLetterCount(c)) {
				throw InvalidInputException(
				    "Glue table '%s': %s '%s' repeats '%c' %d times, Java allows '%c' at most %s", table_description,
				    parameter, pattern, c, count, c, CountWord(MaxLetterCount(c)));
			}
			Element element;
			element.letter = c;
			element.count = count;
			elements.push_back(element);
			if (c == 'y') {
				has_year_field = true;
			} else if (c == 'd') {
				has_day_field = true;
			} else if (c == 'M') {
				has_month_field = true;
			} else {
				has_time_field = true;
			}
			position += count;
			continue;
		}
		if (c == '[' || c == ']' || c == '{' || c == '}' || c == '#') {
			throw InvalidInputException("Glue table '%s': %s '%s' contains '%c', which is reserved in Java date "
			                            "patterns and not supported here; quote it to output it",
			                            table_description, parameter, pattern, c);
		}
		add_literal(c);
		position++;
	}
	if (!has_year_field) {
		// Athena reads a date key as a year, a year-month, a date or a date-time, all of which need the year
		throw InvalidInputException("Glue table '%s': %s '%s' has no year field (y), so its values do not name a date",
		                            table_description, parameter, pattern);
	}
}

//===--------------------------------------------------------------------===//
// Format
//===--------------------------------------------------------------------===//
//! Java's number rule: one letter is the minimum number of digits, more letters a zero-padded width
static string FormatNumber(int64_t value, idx_t width) {
	auto text = to_string(value < 0 ? -value : value);
	if (text.size() < width) {
		text.insert(0, width - text.size(), '0');
	}
	return value < 0 ? "-" + text : text;
}

string JavaDateFormat::Format(timestamp_t value) const {
	date_t date;
	dtime_t time;
	Timestamp::Convert(value, date, time);
	int32_t year, month, day;
	Date::Convert(date, year, month, day);
	int32_t hour, minute, second, micros;
	Time::Convert(time, hour, minute, second, micros);
	string result;
	for (auto &element : elements) {
		switch (element.letter) {
		case 0:
			result += element.literal;
			break;
		case 'y':
			if (element.count == 2) {
				// the reduced form: the rightmost two digits
				result += FormatNumber(((year % 100) + 100) % 100, 2);
			} else {
				result += FormatNumber(year, element.count);
			}
			break;
		case 'M':
			if (element.count == 3) {
				result += Date::MONTH_NAMES_ABBREVIATED[month - 1].GetString();
			} else if (element.count == 4) {
				result += Date::MONTH_NAMES[month - 1].GetString();
			} else {
				result += FormatNumber(month, element.count);
			}
			break;
		case 'd':
			result += FormatNumber(day, element.count);
			break;
		case 'H':
			result += FormatNumber(hour, element.count);
			break;
		case 'm':
			result += FormatNumber(minute, element.count);
			break;
		case 's':
			result += FormatNumber(second, element.count);
			break;
		case 'S': {
			// the fraction of a second as nine digits, truncated to the count of letters
			auto nanos = FormatNumber(micros, 6) + "000";
			result += nanos.substr(0, element.count);
			break;
		}
		default:
			throw InternalException("JavaDateFormat: unexpected pattern letter");
		}
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Parse
//===--------------------------------------------------------------------===//
static bool IsDigit(char c) {
	return c >= '0' && c <= '9';
}

//! 'min_width' to 'max_width' digits, leaving 'reserved' digits to the fixed-width fields that follow
static bool ParseDigits(const string &text, idx_t &position, idx_t min_width, idx_t max_width, idx_t reserved,
                        int64_t &value) {
	idx_t available = 0;
	while (position + available < text.size() && IsDigit(text[position + available])) {
		available++;
	}
	auto take = available > reserved ? available - reserved : 0;
	take = MinValue<idx_t>(take, max_width);
	if (take < min_width || take == 0 || take > available) {
		return false;
	}
	value = 0;
	for (idx_t i = 0; i < take; i++) {
		value = value * 10 + (text[position + i] - '0');
	}
	position += take;
	return true;
}

bool JavaDateFormat::TryParse(const string &text, timestamp_t &result) const {
	int64_t year = 1970, month = 1, day = 1, hour = 0, minute = 0, second = 0, fraction = 0;
	idx_t fraction_digits = 0;
	idx_t position = 0;
	for (idx_t index = 0; index < elements.size(); index++) {
		auto &element = elements[index];
		if (element.letter == 0) {
			if (text.compare(position, element.literal.size(), element.literal) != 0) {
				return false;
			}
			position += element.literal.size();
			continue;
		}
		if (element.letter == 'M' && element.count >= 3) {
			auto &names = element.count == 3 ? Date::MONTH_NAMES_ABBREVIATED : Date::MONTH_NAMES;
			bool found = false;
			for (idx_t i = 0; i < 12; i++) {
				auto name = names[i].GetString();
				if (text.compare(position, name.size(), name) == 0) {
					month = NumericCast<int64_t>(i + 1);
					position += name.size();
					found = true;
					break;
				}
			}
			if (!found) {
				return false;
			}
			continue;
		}
		// Java's widths: a variable-width number leaves the fixed-width numbers after it their digits
		auto fixed_width = [](const Element &e) {
			return e.count >= 2 && !(e.letter == 'y' && e.count != 2);
		};
		idx_t reserved = 0;
		for (idx_t next = index + 1; next < elements.size() && elements[next].letter != 0; next++) {
			if (!fixed_width(elements[next]) || (elements[next].letter == 'M' && elements[next].count >= 3)) {
				break;
			}
			reserved += elements[next].count;
		}
		idx_t min_width = element.count == 1 ? 1 : element.count;
		idx_t max_width = fixed_width(element) ? element.count : (element.letter == 'y' ? 9 : 2);
		int64_t value;
		if (!ParseDigits(text, position, min_width, max_width, reserved, value)) {
			return false;
		}
		switch (element.letter) {
		case 'y':
			year = element.count == 2 ? 2000 + value : value;
			break;
		case 'M':
			month = value;
			break;
		case 'd':
			day = value;
			break;
		case 'H':
			hour = value;
			break;
		case 'm':
			minute = value;
			break;
		case 's':
			second = value;
			break;
		case 'S':
			fraction = value;
			fraction_digits = element.count;
			break;
		default:
			throw InternalException("JavaDateFormat: unexpected pattern letter");
		}
	}
	if (position != text.size()) {
		return false;
	}
	if (month < 1 || month > 12 || day < 1 || hour > 23 || minute > 59 || second > 59 ||
	    year > NumericLimits<int32_t>::Maximum()) {
		return false;
	}
	date_t date;
	if (!Date::TryFromDate(NumericCast<int32_t>(year), NumericCast<int32_t>(month), NumericCast<int32_t>(day), date)) {
		return false;
	}
	// the fraction as microseconds: digits beyond the sixth are dropped, missing ones are zeros
	for (idx_t i = fraction_digits; i < 6; i++) {
		fraction *= 10;
	}
	for (idx_t i = 6; i < fraction_digits; i++) {
		fraction /= 10;
	}
	auto time = Time::FromTime(NumericCast<int32_t>(hour), NumericCast<int32_t>(minute), NumericCast<int32_t>(second),
	                           NumericCast<int32_t>(fraction));
	return Timestamp::TryFromDatetime(date, time, result);
}

} // namespace duckdb
