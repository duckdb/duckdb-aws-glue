#include "functions/hive_opencsv_field.hpp"

#include "core/helpers.hpp"
#include "core/readers/opencsv_line.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

struct OpenCSVFieldBindData : public FunctionData {
	OpenCSVFieldBindData(idx_t index, OpenCSVDialect dialect_p) : index(index), dialect(std::move(dialect_p)) {
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

char ConstantCharacter(BindScalarFunctionInput &input, idx_t argument, const char *name) {
	auto value = input.GetConstant(argument);
	auto text = value.IsNull() ? string() : StringValue::Get(value);
	if (text.size() != 1) {
		throw BinderException("hive_opencsv_field: the %s must be one byte, got '%s'", name, text);
	}
	return text[0];
}

string ConstantSeparator(BindScalarFunctionInput &input) {
	auto value = input.GetConstant(2);
	auto text = value.IsNull() ? string() : StringValue::Get(value);
	string character;
	if (!TryFirstCharacter(text, character) || character != text) {
		throw BinderException("hive_opencsv_field: the separator must be one character, got '%s'", text);
	}
	return text;
}

unique_ptr<FunctionData> OpenCSVFieldBind(BindScalarFunctionInput &input) {
	auto index = input.GetConstant(1);
	if (index.IsNull() || index.GetValue<int64_t>() < 0) {
		throw BinderException("hive_opencsv_field: the field index must be a constant from 0");
	}
	OpenCSVDialect dialect {ConstantSeparator(input), ConstantCharacter(input, 3, "quote"),
	                        ConstantCharacter(input, 4, "escape")};
	return make_uniq<OpenCSVFieldBindData>(NumericCast<idx_t>(index.GetValue<int64_t>()), std::move(dialect));
}

void OpenCSVFieldFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &data = state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<OpenCSVFieldBindData>();
	string field;
	UnaryExecutor::Execute<string_t, string_t>(
	    args.data[0], result, args.size(), [&](string_t line) -> optional<string_t> {
		    if (!GetOpenCSVLineField(line.GetData(), line.GetSize(), data.dialect, data.index, field)) {
			    return nullopt;
		    }
		    return StringVector::AddString(result, field);
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
