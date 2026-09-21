#include "glue_types.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/decimal.hpp"

namespace duckdb {

namespace {

//! Recursive descent parser for Hive style type strings
class GlueTypeParser {
public:
	explicit GlueTypeParser(const string &input) : input(input), pos(0) {
	}

	LogicalType Parse() {
		auto result = ParseType();
		SkipWhitespace();
		if (pos != input.size()) {
			throw InvalidInputException("Unexpected trailing characters in Glue type '%s' at position %d", input, pos);
		}
		return result;
	}

private:
	void SkipWhitespace() {
		while (pos < input.size() && StringUtil::CharacterIsSpace(input[pos])) {
			pos++;
		}
	}

	bool Peek(char c) {
		SkipWhitespace();
		return pos < input.size() && input[pos] == c;
	}

	void Expect(char c) {
		if (!Peek(c)) {
			throw InvalidInputException("Expected '%c' in Glue type '%s' at position %d", c, input, pos);
		}
		pos++;
	}

	//! Read an identifier, optionally quoted with backticks (struct field names)
	string ReadIdentifier() {
		SkipWhitespace();
		if (pos < input.size() && input[pos] == '`') {
			pos++;
			auto start = pos;
			while (pos < input.size() && input[pos] != '`') {
				pos++;
			}
			if (pos >= input.size()) {
				throw InvalidInputException("Unterminated quoted identifier in Glue type '%s'", input);
			}
			auto result = input.substr(start, pos - start);
			pos++;
			return result;
		}
		auto start = pos;
		while (pos < input.size() && input[pos] != '<' && input[pos] != '>' && input[pos] != ',' && input[pos] != ':' &&
		       input[pos] != '(' && input[pos] != ')' && !StringUtil::CharacterIsSpace(input[pos])) {
			pos++;
		}
		if (start == pos) {
			throw InvalidInputException("Expected a type name in Glue type '%s' at position %d", input, pos);
		}
		return input.substr(start, pos - start);
	}

	int64_t ReadInteger() {
		SkipWhitespace();
		auto start = pos;
		while (pos < input.size() && StringUtil::CharacterIsDigit(input[pos])) {
			pos++;
		}
		if (start == pos) {
			throw InvalidInputException("Expected a number in Glue type '%s' at position %d", input, pos);
		}
		return std::stoll(input.substr(start, pos - start));
	}

	LogicalType ParseType() {
		auto name = StringUtil::Lower(ReadIdentifier());
		if (name == "array") {
			Expect('<');
			auto child = ParseType();
			Expect('>');
			return LogicalType::LIST(child);
		}
		if (name == "map") {
			Expect('<');
			auto key = ParseType();
			Expect(',');
			auto value = ParseType();
			Expect('>');
			return LogicalType::MAP(key, value);
		}
		if (name == "struct") {
			Expect('<');
			child_list_t<LogicalType> children;
			while (true) {
				auto field_name = ReadIdentifier();
				Expect(':');
				auto field_type = ParseType();
				children.emplace_back(field_name, field_type);
				if (Peek(',')) {
					pos++;
					continue;
				}
				break;
			}
			Expect('>');
			return LogicalType::STRUCT(std::move(children));
		}
		if (name == "decimal" || name == "numeric") {
			// decimal(precision, scale), defaults to decimal(10,0) like Hive
			int64_t width = 10;
			int64_t scale = 0;
			if (Peek('(')) {
				pos++;
				width = ReadInteger();
				if (Peek(',')) {
					pos++;
					scale = ReadInteger();
				}
				Expect(')');
			}
			if (width < 1 || width > Decimal::MAX_WIDTH_DECIMAL || scale < 0 || scale > width) {
				throw InvalidInputException("Unsupported decimal type '%s' in Glue type", input);
			}
			return LogicalType::DECIMAL(NumericCast<uint8_t>(width), NumericCast<uint8_t>(scale));
		}
		if (name == "varchar" || name == "char") {
			// varchar(n) / char(n): the length is not enforced
			if (Peek('(')) {
				pos++;
				ReadInteger();
				Expect(')');
			}
			return LogicalType::VARCHAR;
		}
		if (name == "boolean" || name == "bool") {
			return LogicalType::BOOLEAN;
		}
		if (name == "tinyint") {
			return LogicalType::TINYINT;
		}
		if (name == "smallint") {
			return LogicalType::SMALLINT;
		}
		if (name == "int" || name == "integer") {
			return LogicalType::INTEGER;
		}
		if (name == "bigint" || name == "long") {
			return LogicalType::BIGINT;
		}
		if (name == "float" || name == "real") {
			return LogicalType::FLOAT;
		}
		if (name == "double") {
			return LogicalType::DOUBLE;
		}
		if (name == "string") {
			return LogicalType::VARCHAR;
		}
		if (name == "binary") {
			return LogicalType::BLOB;
		}
		if (name == "date") {
			return LogicalType::DATE;
		}
		if (name == "time") {
			return LogicalType::TIME;
		}
		if (name == "timestamp") {
			return LogicalType::TIMESTAMP;
		}
		if (name == "timestamptz" || name == "timestamp with time zone" || name == "timestamp_with_local_time_zone") {
			return LogicalType::TIMESTAMP_TZ;
		}
		if (name == "uuid") {
			return LogicalType::UUID;
		}
		if (name == "variant") {
			return LogicalType::VARIANT();
		}
		throw NotImplementedException("Glue type '%s' (in '%s') is not supported", name, input);
	}

private:
	const string &input;
	idx_t pos;
};

} // namespace

LogicalType GlueTypes::ToLogicalType(const string &glue_type) {
	GlueTypeParser parser(glue_type);
	return parser.Parse();
}

string GlueTypes::FromLogicalType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::TINYINT:
		return "tinyint";
	case LogicalTypeId::SMALLINT:
		return "smallint";
	case LogicalTypeId::INTEGER:
		return "int";
	case LogicalTypeId::BIGINT:
		return "bigint";
	case LogicalTypeId::UTINYINT:
		return "smallint";
	case LogicalTypeId::USMALLINT:
		return "int";
	case LogicalTypeId::UINTEGER:
		return "bigint";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::DECIMAL: {
		uint8_t width;
		uint8_t scale;
		type.GetDecimalProperties(width, scale);
		return StringUtil::Format("decimal(%d,%d)", width, scale);
	}
	case LogicalTypeId::VARCHAR:
		return "string";
	case LogicalTypeId::BLOB:
		return "binary";
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::TIME:
		return "time";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_SEC:
		return "timestamp";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "timestamptz";
	case LogicalTypeId::UUID:
		return "uuid";
	case LogicalTypeId::VARIANT:
		return "variant";
	case LogicalTypeId::LIST:
		return "array<" + FromLogicalType(ListType::GetChildType(type)) + ">";
	case LogicalTypeId::MAP:
		return "map<" + FromLogicalType(MapType::KeyType(type)) + "," + FromLogicalType(MapType::ValueType(type)) + ">";
	case LogicalTypeId::STRUCT: {
		vector<string> fields;
		auto &children = StructType::GetChildTypes(type);
		for (auto &child : children) {
			fields.push_back(child.first + ":" + FromLogicalType(child.second));
		}
		return "struct<" + StringUtil::Join(fields, ",") + ">";
	}
	default:
		throw NotImplementedException("DuckDB type '%s' can not be converted to a Glue type", type.ToString());
	}
}

Value GlueTypes::PartitionValue(ClientContext &context, const string &key, const string &str_value,
                                const LogicalType &type) {
	if (str_value == HivePartitioning::DEFAULT_PARTITION_NAME) {
		return Value(type);
	}
	if (type.id() == LogicalTypeId::VARCHAR) {
		// verbatim: what Glue holds IS the value
		return Value(str_value);
	}
	// for a non-string column these are the spellings a NULL arrives as
	if (StringUtil::CIEquals(str_value, "NULL") || str_value.empty()) {
		return Value(type);
	}
	Value value(str_value);
	auto cast = value.TryCastAs(context, type);
	if (!cast) {
		throw InvalidInputException("Unable to cast '%s' (from Glue partition column '%s') to: '%s'", str_value,
		                            StringUtil::Upper(key), type.ToString());
	}
	return std::move(*cast);
}

} // namespace duckdb
