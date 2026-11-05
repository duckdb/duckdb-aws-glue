#pragma once

#include "core/hive_serde.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"

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
	FALSE_UNLESS_TRUE,
	//! the text as milliseconds (timestamp) or days (date) since the epoch; a field that is not an integer fails the
	//! query
	EPOCH,
	//! the text as milliseconds (timestamp) or days (date) since the epoch; NULL when not an integer or out of range
	TRY_EPOCH,
	//! an optional sign and digits (HIVE_INTEGER_FIELD_PATTERN) in the column type's range, else the query fails
	INTEGER,
	//! an optional sign and digits (HIVE_INTEGER_FIELD_PATTERN) in the column type's range, else NULL
	TRY_INTEGER,
	//! the text tried with try_strptime and the table's timestamp formats, as milliseconds when they list 'millis',
	//! else TRY_CAST; NULL when shorter than 8 bytes
	TIMESTAMP_FORMATS
};

//! The text of an integer field as Java's Integer.parseInt and Long.parseLong read it
static constexpr const char *HIVE_INTEGER_FIELD_PATTERN = "[+-]?[0-9]+";

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
	//! With serde_fields: the SerDe the fields are converted as. OpenCSVSerde reads a boolean as true for 'true' in
	//! any case and false otherwise, a timestamp or date as milliseconds or days since the epoch, and an integer as
	//! an optional sign and digits
	HiveSerDe serde = HiveSerDe::LAZY_SIMPLE;
	//! With serde_fields: the strptime formats a timestamp field is tried with first (LazySimpleSerDe
	//! timestamp.formats)
	vector<string> timestamp_formats;
	//! With serde_fields: a timestamp field may be milliseconds since the epoch (timestamp.formats 'millis')
	bool timestamp_millis = false;

public:
	//! With serde_fields: how a field becomes a value of a column of 'type', in the scan and in its reader call
	HiveFieldConversion FieldConversion(const LogicalType &type) const;
	bool operator==(const HiveCSVOptions &other) const;
	bool operator!=(const HiveCSVOptions &other) const;
	//! The patterns of a timestamp.formats property that strptime can express, and 'millis'; the others are skipped
	void SetTimestampFormats(const string &property);
	void Serialize(Serializer &serializer) const;
	static HiveCSVOptions Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
