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

//! Drop the escape character read_csv does not apply from a field's text, before the text is converted; only a field
//! that holds it goes through regexp_replace
static void UnescapeField(ClientContext &context, unique_ptr<Expression> &expr, const HiveCSVOptions &options) {
	auto &text = BoundCastExpression::IsCast(*expr)
	                 ? BoundCastExpression::ChildMutable(expr->Cast<BoundFunctionExpression>())
	                 : expr;
	if (text->GetReturnType() != LogicalType::VARCHAR || text->IsFoldable()) {
		return;
	}
	vector<unique_ptr<Expression>> contains;
	contains.push_back(text->Copy());
	contains.push_back(make_uniq<BoundConstantExpression>(Value(options.field_escape)));
	vector<unique_ptr<Expression>> replace;
	replace.push_back(text->Copy());
	replace.push_back(make_uniq<BoundConstantExpression>(Value(options.FieldEscapePattern())));
	replace.push_back(make_uniq<BoundConstantExpression>(Value("\\1")));
	replace.push_back(make_uniq<BoundConstantExpression>(Value("g")));
	text = make_uniq<BoundCaseExpression>(BindFunction(context, "contains", std::move(contains)),
	                                      BindFunction(context, "regexp_replace", std::move(replace)), std::move(text));
}

void HiveCSVOptions::ConvertField(ClientContext &context, unique_ptr<Expression> &expr) const {
	D_ASSERT(serde_fields);
	auto &options = *this;
	if (!expr) {
		return;
	}
	if (!options.field_escape.empty()) {
		UnescapeField(context, expr, options);
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

//! A field's text with the escape character read_csv does not apply dropped, as ConvertField reads it
static unique_ptr<ParsedExpression> UnescapedField(const HiveCSVOptions &options, unique_ptr<ParsedExpression> field) {
	if (options.field_escape.empty()) {
		return field;
	}
	vector<unique_ptr<ParsedExpression>> contains;
	contains.push_back(field->Copy());
	contains.push_back(ConstantExpression::FromValue(Value(options.field_escape)));
	vector<unique_ptr<ParsedExpression>> replace;
	replace.push_back(field->Copy());
	replace.push_back(ConstantExpression::FromValue(Value(options.FieldEscapePattern())));
	replace.push_back(ConstantExpression::FromValue(Value("\\1")));
	replace.push_back(ConstantExpression::FromValue(Value("g")));
	auto unescaped = make_uniq<CaseExpression>();
	CaseCheck check;
	check.when_expr = make_uniq<FunctionExpression>(Identifier("contains"), std::move(contains));
	check.then_expr = make_uniq<FunctionExpression>(Identifier("regexp_replace"), std::move(replace));
	unescaped->CaseChecksMutable().push_back(std::move(check));
	unescaped->ElseMutable() = std::move(field);
	return std::move(unescaped);
}

unique_ptr<ParsedExpression> HiveCSVOptions::GetFieldReaderExpression(const Identifier &name,
                                                                      const LogicalType &type) const {
	D_ASSERT(serde_fields);
	auto &options = *this;
	auto field = [&]() {
		return UnescapedField(options, BarredField(name));
	};
	unique_ptr<ParsedExpression> column;
	auto conversion = options.FieldConversion(type);
	switch (conversion) {
	case HiveFieldConversion::NONE:
		if (options.field_escape.empty()) {
			return make_uniq<ColumnRefExpression>(name);
		}
		column = UnescapedField(options, make_uniq<ColumnRefExpression>(name));
		break;
	case HiveFieldConversion::CAST:
		column = make_uniq<CastExpression>(type, field(), false);
		break;
	case HiveFieldConversion::TRY_CAST:
		column = make_uniq<CastExpression>(type, field(), true);
		break;
	case HiveFieldConversion::FALSE_UNLESS_TRUE: {
		vector<unique_ptr<ParsedExpression>> lower;
		lower.push_back(field());
		column = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
		                                         make_uniq<FunctionExpression>(Identifier("lower"), std::move(lower)),
		                                         ConstantExpression::FromValue(Value("true")));
		break;
	}
	case HiveFieldConversion::EPOCH:
	case HiveFieldConversion::TRY_EPOCH: {
		auto is_date = type == LogicalType::DATE;
		auto number = make_uniq<CastExpression>(is_date ? LogicalType::INTEGER : LogicalType::BIGINT, field(),
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
	case HiveFieldConversion::TIMESTAMP_FORMATS: {
		auto trimmed = [&]() {
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(field());
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
		length.push_back(field());
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
