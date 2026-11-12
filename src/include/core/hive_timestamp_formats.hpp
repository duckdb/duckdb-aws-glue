#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! A timestamp.formats property (LazySimpleSerDe, Hive JsonSerDe): Joda-Time patterns separated by ',' ('\,' is a
//! comma in a pattern), and 'millis' for milliseconds since the epoch
struct HiveTimestampFormats {
	//! The patterns strptime can express, as strptime formats, in the property's order
	vector<string> strptime_formats;
	//! The property lists 'millis'
	bool millis = false;

public:
	//! Patterns strptime can not express are skipped (JodaPatternToStrptime)
	static HiveTimestampFormats Parse(const string &property);
};

//! A Joda-Time pattern as a strptime format; false for a pattern strptime can not express (a time zone, a fraction
//! other than SSS, another field), or one without a year, whose default year (1970 in Hive, 1900 in strptime) differs
bool JodaPatternToStrptime(const string &pattern, string &result);

} // namespace duckdb
