#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! The separator (one UTF-8 character), quote and escape character of an OpenCSVSerde file
struct OpenCSVDialect {
	string separator;
	char quote;
	char escape;
};

//! The fields among the first fields.size() of a line that 'wanted' asks for, as OpenCSVSerde's reader splits it;
//! valid[i] is false when the line has no field i, or when it is the line's last field and its quote is not closed.
//! 'buffer' is scratch space kept between calls
void SplitOpenCSVLine(const char *line, idx_t length, const OpenCSVDialect &dialect, const vector<bool> &wanted,
                      vector<string> &fields, vector<bool> &valid, string &buffer);
//! Field 'index' (from 0) of a line as SplitOpenCSVLine splits it; false when the line has no such field
bool GetOpenCSVLineField(const char *line, idx_t length, const OpenCSVDialect &dialect, idx_t index, string &field);

} // namespace duckdb
