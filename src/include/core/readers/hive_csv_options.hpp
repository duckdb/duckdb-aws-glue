#pragma once

#include "core/hive_serde.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
class Deserializer;
class Expression;
class Identifier;
class ParsedExpression;
class Serializer;
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
	//! the text tried with try_strptime and the table's timestamp formats, as milliseconds when they list 'millis',
	//! else TRY_CAST; NULL when shorter than 8 bytes
	TIMESTAMP_FORMATS
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
	//! With serde_fields: an escape character read_csv does not apply, dropped from a field's text; the escape or quote
	//! character after it is kept
	string field_escape;
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
	//! With serde_fields: make the reader's cast of a field's text to its column's type that conversion, in the scan
	//! (planning/hive_field_conversion.cpp)
	void ConvertField(ClientContext &context, unique_ptr<Expression> &expr) const;
	//! With serde_fields: the same conversion over the reader's column 'name', aliased as the column (the column itself
	//! for NONE), in the scan's reader call (planning/hive_field_conversion.cpp)
	unique_ptr<ParsedExpression> GetFieldReaderExpression(const Identifier &name, const LogicalType &type) const;
	//! The regexp_replace pattern that drops field_escape from a field's text, with '\1' the character it escapes
	string FieldEscapePattern() const;
	bool operator==(const HiveCSVOptions &other) const;
	bool operator!=(const HiveCSVOptions &other) const;
	void Serialize(Serializer &serializer) const;
	static HiveCSVOptions Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
