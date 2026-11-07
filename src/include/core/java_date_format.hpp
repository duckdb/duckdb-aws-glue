#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Java's DateTimeFormatter patterns as Athena partition projection uses them: y M d H m s S, quoted literals; UTC
class JavaDateFormat {
public:
	//! Throws InvalidInputException for another letter, [ ] { } #, an unterminated quote, too many letters or no year
	JavaDateFormat(const string &pattern, const string &parameter, const string &table_description);

	string Format(timestamp_t value) const;
	//! False when 'text' is not a value of this pattern; fields the pattern lacks read as January, the 1st, midnight
	bool TryParse(const string &text, timestamp_t &result) const;
	//! Whether the pattern has an hour, minute, second or fraction field
	bool HasTimeField() const {
		return has_time_field;
	}
	//! Whether the pattern has a day-of-month field
	bool HasDayField() const {
		return has_day_field;
	}
	//! Whether the pattern has a month field
	bool HasMonthField() const {
		return has_month_field;
	}

private:
	struct Element {
		//! A pattern letter, or 0 for a literal
		char letter = 0;
		idx_t count = 0;
		string literal;
	};

private:
	vector<Element> elements;
	bool has_time_field = false;
	bool has_day_field = false;
	bool has_month_field = false;
};

} // namespace duckdb
