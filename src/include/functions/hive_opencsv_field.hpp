#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

//! The separator, quote and escape character of an OpenCSVSerde file
struct OpenCSVDialect {
	char separator;
	char quote;
	char escape;
};

//! The first fields.size() fields of a line as Athena's OpenCSVSerde reader splits it; valid[i] is false when the line
//! has no field i, or when it is the line's last field and its quote is not closed
void SplitOpenCSVLine(const char *line, idx_t length, const OpenCSVDialect &dialect, vector<string> &fields,
                      vector<bool> &valid);

//! hive_opencsv_field(line, index, separator, quote, escape): field 'index' (from 0) of a line of an OpenCSVSerde file
//! as SplitOpenCSVLine splits it, NULL when the line has no such field
ScalarFunction GetHiveOpenCSVFieldFunction();

} // namespace duckdb
