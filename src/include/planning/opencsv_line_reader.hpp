#pragma once

namespace duckdb {

class BaseFileReader;
class DataChunk;
struct HiveCSVOptions;
struct MultiFileReaderData;

//! With line_fields read_csv returns each line as the file's first column: read it
void ReadOpenCSVLines(BaseFileReader &reader);
//! Replace the columns of a line_fields file the reader's expressions read with the fields SplitOpenCSVLine splits from
//! each line, splitting each line once
void SplitOpenCSVLines(BaseFileReader &reader, const MultiFileReaderData &reader_data, DataChunk &chunk,
                       const HiveCSVOptions &options);

} // namespace duckdb
