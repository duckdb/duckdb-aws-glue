#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class Serializer;
class Deserializer;
struct LogicalType;

//! The longest line a scan of a csv table reads by default (hive_csv_max_line_size): read_csv's buffers hold 32 MB
//! and each thread reads a quarter of one, so a line up to this size changes neither
static constexpr idx_t HIVE_CSV_DEFAULT_MAX_LINE_SIZE = 8000000;

//! How a field read as text becomes a value of its column's type
enum class HiveFieldConversion : uint8_t {
	//! the text itself, for a string column
	NONE,
	//! a field that does not parse fails the query
	CAST,
	//! a field that does not parse is NULL
	TRY_CAST,
	//! lower(text) = 'true': true for 'true' in any case and false otherwise, never an error
	FALSE_UNLESS_TRUE
};

//! How the files of a csv table are read
struct HiveCSVOptions {
	string delimiter = ",";
	string quote = "\"";
	string escape = "\"";
	//! The header lines every file starts with
	idx_t skip_lines = 0;
	string null_string;
	//! Read fields as text and TRY_CAST them; short rows padded with NULL, extra fields ignored
	bool serde_fields = false;
	//! With serde_fields: a field that does not parse as its column type is NULL (TRY_CAST), else the query fails
	bool null_for_invalid_data = true;
	//! With serde_fields: a boolean field is true when it is 'true' in any case and false otherwise, never an error
	bool false_unless_true = false;

public:
	//! With serde_fields: how a field becomes a value of a column of 'type', in the scan and in its reader call
	HiveFieldConversion FieldConversion(const LogicalType &type) const;
	void Serialize(Serializer &serializer) const;
	static HiveCSVOptions Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
