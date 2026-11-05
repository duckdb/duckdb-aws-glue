#include "planning/hive_field_conversion.hpp"

#include "core/readers/hive_csv_options.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

//! The scalar function 'name' bound to 'children'
unique_ptr<Expression> BindFunction(ClientContext &context, const string &name,
                                    vector<unique_ptr<Expression>> children) {
	FunctionBinder function_binder(context);
	ErrorData error;
	auto result =
	    function_binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier(name), std::move(children), error);
	if (!result) {
		error.Throw();
	}
	return result;
}

//! Make the cast of a field's text to its column's type a TRY_CAST
static void MakeTryCast(ClientContext &context, unique_ptr<Expression> &expr) {
	if (!expr || !BoundCastExpression::IsCast(*expr)) {
		return;
	}
	auto &cast = expr->Cast<BoundFunctionExpression>();
	if (BoundCastExpression::IsTryCast(cast) ||
	    BoundCastExpression::Child(cast).GetReturnType().id() != LogicalTypeId::VARCHAR) {
		return;
	}
	auto target_type = cast.GetReturnType();
	auto child = std::move(BoundCastExpression::ChildMutable(cast));
	expr = BoundCastExpression::AddCastToType(context, std::move(child), target_type, true);
}

//! Make the cast of a field's text to BOOLEAN lower(text) = 'true', as OpenCSVSerde reads a boolean
static void MakeFalseUnlessTrue(ClientContext &context, unique_ptr<Expression> &expr) {
	if (!expr || !BoundCastExpression::IsCast(*expr) || expr->GetReturnType().id() != LogicalTypeId::BOOLEAN) {
		return;
	}
	auto &cast = expr->Cast<BoundFunctionExpression>();
	if (BoundCastExpression::Child(cast).GetReturnType().id() != LogicalTypeId::VARCHAR) {
		return;
	}
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(BoundCastExpression::ChildMutable(cast)));
	expr = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL,
	                                         BindFunction(context, "lower", std::move(children)),
	                                         make_uniq<BoundConstantExpression>(Value("true")));
}

//! Make the cast of a field's text to TIMESTAMP make_timestamp_ms(CAST(text AS BIGINT)), and to DATE
//! DATE '1970-01-01' + CAST(text AS INTEGER), as OpenCSVSerde reads a timestamp and a date
static void MakeEpoch(ClientContext &context, unique_ptr<Expression> &expr, bool try_cast) {
	if (!expr || !BoundCastExpression::IsCast(*expr)) {
		return;
	}
	auto is_date = expr->GetReturnType() == LogicalType::DATE;
	if (!is_date && expr->GetReturnType() != LogicalType::TIMESTAMP) {
		return;
	}
	auto &cast = expr->Cast<BoundFunctionExpression>();
	if (BoundCastExpression::Child(cast).GetReturnType().id() != LogicalTypeId::VARCHAR) {
		return;
	}
	vector<unique_ptr<Expression>> children;
	if (is_date) {
		children.push_back(make_uniq<BoundConstantExpression>(Value::DATE(date_t::epoch())));
	}
	children.push_back(BoundCastExpression::AddCastToType(context, std::move(BoundCastExpression::ChildMutable(cast)),
	                                                      is_date ? LogicalType::INTEGER : LogicalType::BIGINT,
	                                                      try_cast));
	auto value = BindFunction(context, is_date ? "+" : "make_timestamp_ms", std::move(children));
	if (try_cast) {
		// a number out of the type's range is NULL too
		auto try_value = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_TRY, value->GetReturnType());
		try_value->GetChildrenMutable().push_back(std::move(value));
		value = std::move(try_value);
	}
	expr = std::move(value);
}

//! Make the cast of a field's text to an integer type
//! CAST(CASE WHEN NOT regexp_full_match(text, HIVE_INTEGER_FIELD_PATTERN) THEN <invalid> ELSE text END AS type),
//! <invalid> an error or NULL (try_cast), as OpenCSVSerde reads an integer
static void MakeStrictInteger(ClientContext &context, unique_ptr<Expression> &expr, bool try_cast) {
	if (!expr || !BoundCastExpression::IsCast(*expr)) {
		return;
	}
	auto &cast = expr->Cast<BoundFunctionExpression>();
	if (BoundCastExpression::Child(cast).GetReturnType().id() != LogicalTypeId::VARCHAR) {
		return;
	}
	auto target_type = cast.GetReturnType();
	auto text = std::move(BoundCastExpression::ChildMutable(cast));
	vector<unique_ptr<Expression>> match_children;
	match_children.push_back(text->Copy());
	match_children.push_back(make_uniq<BoundConstantExpression>(Value(HIVE_INTEGER_FIELD_PATTERN)));
	auto not_integer = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
	not_integer->GetChildrenMutable().push_back(BindFunction(context, "regexp_full_match", std::move(match_children)));
	unique_ptr<Expression> invalid;
	if (try_cast) {
		invalid = make_uniq<BoundConstantExpression>(Value(LogicalType::VARCHAR));
	} else {
		vector<unique_ptr<Expression>> message_children;
		message_children.push_back(make_uniq<BoundConstantExpression>(Value("OpenCSVSerde field '")));
		message_children.push_back(text->Copy());
		message_children.push_back(make_uniq<BoundConstantExpression>(Value("' is not an integer")));
		vector<unique_ptr<Expression>> error_children;
		error_children.push_back(BindFunction(context, "concat", std::move(message_children)));
		invalid = BoundCastExpression::AddCastToType(context, BindFunction(context, "error", std::move(error_children)),
		                                             LogicalType::VARCHAR);
	}
	auto field = make_uniq<BoundCaseExpression>(std::move(not_integer), std::move(invalid), std::move(text));
	expr = BoundCastExpression::AddCastToType(context, std::move(field), target_type, try_cast);
}

//! The text's length below which Hive reads a timestamp field as NULL
static constexpr int64_t HIVE_TIMESTAMP_MIN_LENGTH = 8;

//! Make the cast of a field's text to TIMESTAMP, for a table with timestamp.formats,
//! CASE WHEN length(text) < 8 THEN NULL ELSE coalesce(try_strptime(trim(text), formats),
//! TRY(make_timestamp_ms(TRY_CAST(trim(text) AS BIGINT))), TRY_CAST(trim(text) AS TIMESTAMP)) END
static void MakeTimestampFormats(ClientContext &context, unique_ptr<Expression> &expr, const HiveCSVOptions &options) {
	if (!expr || !BoundCastExpression::IsCast(*expr) || expr->GetReturnType() != LogicalType::TIMESTAMP) {
		return;
	}
	auto &cast = expr->Cast<BoundFunctionExpression>();
	if (BoundCastExpression::Child(cast).GetReturnType().id() != LogicalTypeId::VARCHAR) {
		return;
	}
	auto text = std::move(BoundCastExpression::ChildMutable(cast));
	auto trimmed = [&]() {
		vector<unique_ptr<Expression>> children;
		children.push_back(text->Copy());
		return BindFunction(context, "trim", std::move(children));
	};
	auto value = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, LogicalType::TIMESTAMP);
	if (!options.timestamp_formats.empty()) {
		vector<Value> formats;
		for (auto &format : options.timestamp_formats) {
			formats.emplace_back(format);
		}
		vector<unique_ptr<Expression>> children;
		children.push_back(trimmed());
		children.push_back(make_uniq<BoundConstantExpression>(Value::LIST(LogicalType::VARCHAR, std::move(formats))));
		value->GetChildrenMutable().push_back(BindFunction(context, "try_strptime", std::move(children)));
	}
	if (options.timestamp_millis) {
		vector<unique_ptr<Expression>> children;
		children.push_back(BoundCastExpression::AddCastToType(context, trimmed(), LogicalType::BIGINT, true));
		auto millis = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_TRY, LogicalType::TIMESTAMP);
		millis->GetChildrenMutable().push_back(BindFunction(context, "make_timestamp_ms", std::move(children)));
		value->GetChildrenMutable().push_back(std::move(millis));
	}
	value->GetChildrenMutable().push_back(
	    BoundCastExpression::AddCastToType(context, trimmed(), LogicalType::TIMESTAMP, true));
	vector<unique_ptr<Expression>> length_children;
	length_children.push_back(std::move(text));
	auto too_short = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_LESSTHAN, BindFunction(context, "length", std::move(length_children)),
	    make_uniq<BoundConstantExpression>(Value::BIGINT(HIVE_TIMESTAMP_MIN_LENGTH)));
	expr = make_uniq<BoundCaseExpression>(
	    std::move(too_short), make_uniq<BoundConstantExpression>(Value(LogicalType::TIMESTAMP)), std::move(value));
}

void ConvertTextField(ClientContext &context, const HiveCSVOptions &options, unique_ptr<Expression> &expr) {
	if (!expr) {
		return;
	}
	switch (options.FieldConversion(expr->GetReturnType())) {
	case HiveFieldConversion::NONE:
	case HiveFieldConversion::CAST:
		break;
	case HiveFieldConversion::TRY_CAST:
		MakeTryCast(context, expr);
		break;
	case HiveFieldConversion::FALSE_UNLESS_TRUE:
		MakeFalseUnlessTrue(context, expr);
		break;
	case HiveFieldConversion::EPOCH:
		MakeEpoch(context, expr, false);
		break;
	case HiveFieldConversion::TRY_EPOCH:
		MakeEpoch(context, expr, true);
		break;
	case HiveFieldConversion::INTEGER:
		MakeStrictInteger(context, expr, false);
		break;
	case HiveFieldConversion::TRY_INTEGER:
		MakeStrictInteger(context, expr, true);
		break;
	case HiveFieldConversion::TIMESTAMP_FORMATS:
		MakeTimestampFormats(context, expr, options);
		break;
	}
}

//! A csv field read as text, barred (__internal_barrier) so that the caller's optimizer does not push its conversion
//! into read_csv (type_pushdown), which would convert the text itself, and strictly
static unique_ptr<ParsedExpression> BarredField(const Identifier &name) {
	vector<unique_ptr<ParsedExpression>> field;
	field.push_back(make_uniq<ColumnRefExpression>(name));
	return make_uniq<FunctionExpression>(Identifier("__internal_barrier"), std::move(field));
}

unique_ptr<ParsedExpression> TextFieldReaderExpression(const HiveCSVOptions &options, const Identifier &name,
                                                       const LogicalType &type) {
	unique_ptr<ParsedExpression> column;
	auto conversion = options.FieldConversion(type);
	switch (conversion) {
	case HiveFieldConversion::NONE:
		return make_uniq<ColumnRefExpression>(name);
	case HiveFieldConversion::CAST:
		column = make_uniq<CastExpression>(type, BarredField(name), false);
		break;
	case HiveFieldConversion::TRY_CAST:
		column = make_uniq<CastExpression>(type, BarredField(name), true);
		break;
	case HiveFieldConversion::FALSE_UNLESS_TRUE: {
		vector<unique_ptr<ParsedExpression>> lower;
		lower.push_back(BarredField(name));
		column = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
		                                         make_uniq<FunctionExpression>(Identifier("lower"), std::move(lower)),
		                                         ConstantExpression::FromValue(Value("true")));
		break;
	}
	case HiveFieldConversion::EPOCH:
	case HiveFieldConversion::TRY_EPOCH: {
		auto is_date = type == LogicalType::DATE;
		auto number = make_uniq<CastExpression>(is_date ? LogicalType::INTEGER : LogicalType::BIGINT, BarredField(name),
		                                        conversion == HiveFieldConversion::TRY_EPOCH);
		vector<unique_ptr<ParsedExpression>> children;
		if (is_date) {
			children.push_back(ConstantExpression::FromValue(Value::DATE(date_t::epoch())));
		}
		children.push_back(std::move(number));
		column = make_uniq<FunctionExpression>(Identifier(is_date ? "+" : "make_timestamp_ms"), std::move(children),
		                                       nullptr, nullptr, false, is_date);
		if (conversion == HiveFieldConversion::TRY_EPOCH) {
			column = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_TRY, std::move(column));
		}
		break;
	}
	case HiveFieldConversion::INTEGER:
	case HiveFieldConversion::TRY_INTEGER: {
		auto try_cast = conversion == HiveFieldConversion::TRY_INTEGER;
		vector<unique_ptr<ParsedExpression>> match;
		match.push_back(BarredField(name));
		match.push_back(ConstantExpression::FromValue(Value(HIVE_INTEGER_FIELD_PATTERN)));
		CaseCheck check;
		check.when_expr = make_uniq<OperatorExpression>(
		    ExpressionType::OPERATOR_NOT,
		    make_uniq<FunctionExpression>(Identifier("regexp_full_match"), std::move(match)));
		if (try_cast) {
			check.then_expr = ConstantExpression::FromValue(Value(LogicalType::VARCHAR));
		} else {
			vector<unique_ptr<ParsedExpression>> message;
			message.push_back(ConstantExpression::FromValue(Value("OpenCSVSerde field '")));
			message.push_back(BarredField(name));
			message.push_back(ConstantExpression::FromValue(Value("' is not an integer")));
			vector<unique_ptr<ParsedExpression>> raise;
			raise.push_back(make_uniq<FunctionExpression>(Identifier("concat"), std::move(message)));
			check.then_expr = make_uniq<CastExpression>(
			    LogicalType::VARCHAR, make_uniq<FunctionExpression>(Identifier("error"), std::move(raise)));
		}
		auto field = make_uniq<CaseExpression>();
		field->CaseChecksMutable().push_back(std::move(check));
		field->ElseMutable() = BarredField(name);
		column = make_uniq<CastExpression>(type, std::move(field), try_cast);
		break;
	}
	case HiveFieldConversion::TIMESTAMP_FORMATS: {
		auto trimmed = [&]() {
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(BarredField(name));
			return make_uniq<FunctionExpression>(Identifier("trim"), std::move(children));
		};
		vector<unique_ptr<ParsedExpression>> values;
		if (!options.timestamp_formats.empty()) {
			vector<Value> formats;
			for (auto &format : options.timestamp_formats) {
				formats.emplace_back(format);
			}
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(trimmed());
			children.push_back(ConstantExpression::FromValue(Value::LIST(LogicalType::VARCHAR, std::move(formats))));
			values.push_back(make_uniq<FunctionExpression>(Identifier("try_strptime"), std::move(children)));
		}
		if (options.timestamp_millis) {
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(make_uniq<CastExpression>(LogicalType::BIGINT, trimmed(), true));
			values.push_back(make_uniq<OperatorExpression>(
			    ExpressionType::OPERATOR_TRY,
			    make_uniq<FunctionExpression>(Identifier("make_timestamp_ms"), std::move(children))));
		}
		values.push_back(make_uniq<CastExpression>(LogicalType::TIMESTAMP, trimmed(), true));
		vector<unique_ptr<ParsedExpression>> length;
		length.push_back(BarredField(name));
		CaseCheck check;
		check.when_expr = make_uniq<ComparisonExpression>(
		    ExpressionType::COMPARE_LESSTHAN, make_uniq<FunctionExpression>(Identifier("length"), std::move(length)),
		    ConstantExpression::FromValue(Value::BIGINT(HIVE_TIMESTAMP_MIN_LENGTH)));
		check.then_expr = make_uniq<CastExpression>(LogicalType::TIMESTAMP, ConstantExpression::FromValue(Value()));
		auto field = make_uniq<CaseExpression>();
		field->CaseChecksMutable().push_back(std::move(check));
		field->ElseMutable() = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE, std::move(values));
		column = std::move(field);
		break;
	}
	}
	column->SetAlias(name);
	return column;
}

} // namespace duckdb
