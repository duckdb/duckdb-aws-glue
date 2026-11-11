#include "functions/hive_opencsv_field.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

struct OpenCSVFieldBindData : public FunctionData {
	OpenCSVFieldBindData(idx_t index, OpenCSVDialect dialect) : index(index), dialect(dialect) {
	}

	idx_t index;
	OpenCSVDialect dialect;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<OpenCSVFieldBindData>(index, dialect);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<OpenCSVFieldBindData>();
		return index == other.index && dialect.separator == other.dialect.separator &&
		       dialect.quote == other.dialect.quote && dialect.escape == other.dialect.escape;
	}
};

//! Java's Character.isWhitespace for ASCII
bool IsWhitespace(char c) {
	return c == ' ' || (c >= '\t' && c <= '\r') || (c >= 0x1C && c <= 0x1F);
}

bool IsAllWhitespace(const string &text) {
	for (auto c : text) {
		if (!IsWhitespace(c)) {
			return false;
		}
	}
	return true;
}

} // namespace

void SplitOpenCSVLine(const char *line, idx_t length, const OpenCSVDialect &dialect, vector<string> &fields,
                      vector<bool> &valid) {
	// as CsvDeserializer.parseLine
	auto field_count = fields.size();
	valid.assign(field_count, false);
	if (length == 0 || field_count == 0) {
		return;
	}
	string field;
	idx_t field_index = 0;
	bool in_quotes = false;
	bool in_field = false;
	// a position in UTF-16 code units, as Java counts the characters the embedded quote rule looks at
	idx_t char_position = 0;
	for (idx_t position = 0; position < length; position++) {
		auto start = position;
		auto c = line[position];
		auto has_next = position + 1 < length;
		if (c == dialect.escape) {
			if ((in_quotes || in_field) && has_next &&
			    (line[position + 1] == dialect.quote || line[position + 1] == dialect.escape)) {
				field += line[position + 1];
				position++;
			}
		} else if (c == dialect.quote) {
			if ((in_quotes || in_field) && has_next && line[position + 1] == dialect.quote) {
				field += line[position + 1];
				position++;
			} else {
				// a quote in the middle of a field, not next to a separator, is a literal character
				if (char_position > 2 && line[position - 1] != dialect.separator && has_next &&
				    line[position + 1] != dialect.separator) {
					if (!field.empty() && IsAllWhitespace(field)) {
						field.clear();
					} else {
						field += c;
					}
				}
				in_quotes = !in_quotes;
			}
			in_field = !in_field;
		} else if (c == dialect.separator && !in_quotes) {
			fields[field_index].assign(field);
			valid[field_index] = true;
			field_index++;
			if (field_index == field_count) {
				return;
			}
			field.clear();
			in_field = false;
		} else {
			field += c;
			in_field = true;
		}
		for (auto consumed = start; consumed <= position; consumed++) {
			auto byte = static_cast<uint8_t>(line[consumed]);
			if ((byte & 0xC0) != 0x80) {
				// a character above the BMP is two UTF-16 code units
				char_position += byte >= 0xF0 ? 2 : 1;
			}
		}
	}
	if (!in_quotes) {
		fields[field_index].assign(field);
		valid[field_index] = true;
	}
}

namespace {

char ConstantCharacter(BindScalarFunctionInput &input, idx_t argument, const char *name) {
	auto value = input.GetConstant(argument);
	auto text = value.IsNull() ? string() : StringValue::Get(value);
	if (text.size() != 1) {
		throw BinderException("hive_opencsv_field: the %s must be one byte, got '%s'", name, text);
	}
	return text[0];
}

unique_ptr<FunctionData> OpenCSVFieldBind(BindScalarFunctionInput &input) {
	auto index = input.GetConstant(1);
	if (index.IsNull() || index.GetValue<int64_t>() < 0) {
		throw BinderException("hive_opencsv_field: the field index must be a constant from 0");
	}
	OpenCSVDialect dialect {ConstantCharacter(input, 2, "separator"), ConstantCharacter(input, 3, "quote"),
	                        ConstantCharacter(input, 4, "escape")};
	return make_uniq<OpenCSVFieldBindData>(NumericCast<idx_t>(index.GetValue<int64_t>()), dialect);
}

void OpenCSVFieldFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &data = state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<OpenCSVFieldBindData>();
	vector<string> fields(data.index + 1);
	vector<bool> valid;
	UnaryExecutor::Execute<string_t, string_t>(
	    args.data[0], result, args.size(), [&](string_t line) -> optional<string_t> {
		    SplitOpenCSVLine(line.GetData(), line.GetSize(), data.dialect, fields, valid);
		    if (!valid[data.index]) {
			    return nullopt;
		    }
		    return StringVector::AddString(result, fields[data.index]);
	    });
}

} // namespace

ScalarFunction GetHiveOpenCSVFieldFunction() {
	ScalarFunction function(
	    "hive_opencsv_field",
	    {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    LogicalType::VARCHAR, OpenCSVFieldFunction, OpenCSVFieldBind);
	return function;
}

} // namespace duckdb
