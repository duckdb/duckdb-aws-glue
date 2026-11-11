#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class BaseFileReader;
class DataChunk;
struct HiveCSVOptions;
struct MultiFileReaderData;

//! The separator (one UTF-8 character), quote and escape character of an OpenCSVSerde file
struct OpenCSVDialect {
	string separator;
	char quote;
	char escape;
};

//! The first fields.size() fields of a line as OpenCSVSerde's reader splits it; valid[i] is false when the line has no
//! field i, or when it is the line's last field and its quote is not closed
void SplitOpenCSVLine(const char *line, idx_t length, const OpenCSVDialect &dialect, vector<string> &fields,
                      vector<bool> &valid);
//! Field 'index' (from 0) of a line as SplitOpenCSVLine splits it; false when the line has no such field
bool GetOpenCSVLineField(const char *line, idx_t length, const OpenCSVDialect &dialect, idx_t index, string &field);

//! With line_fields read_csv returns each line split at its NUL bytes (the delimiter) into the file's columns: read
//! all of them
void ReadOpenCSVLines(BaseFileReader &reader);
//! Replace the columns of a line_fields file the reader's expressions read with the fields SplitOpenCSVLine splits from
//! each line, joined again at its NUL bytes, splitting each line once
void SplitOpenCSVLines(BaseFileReader &reader, const MultiFileReaderData &reader_data, DataChunk &chunk,
                       const HiveCSVOptions &options);

} // namespace duckdb
