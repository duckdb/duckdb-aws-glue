#include "catalog/scan_filter_equality.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

//! Whether the expression is the filtered column itself, or lower() of it
static bool IsNameOperand(const Expression &expr) {
	if (ExpressionFilter::IsSimpleFilterColumnRef(expr)) {
		return true;
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &function = expr.Cast<BoundFunctionExpression>();
	return function.Function().GetName() == "lower" && function.GetChildren().size() == 1 &&
	       ExpressionFilter::IsSimpleFilterColumnRef(*function.GetChildren()[0]);
}

//! The constant of a `<operand> = <constant>` filter whose other side satisfies 'is_operand'
static optional_ptr<const Value> TryGetEqualityConstant(optional_ptr<const TableFilter> filter, const char *caller,
                                                        bool (*is_operand)(const Expression &)) {
	if (!filter) {
		return nullptr;
	}
	auto &expr = *ExpressionFilter::GetExpressionFilter(*filter, caller).expr;
	if (!BoundComparisonExpression::IsComparison(expr) || expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
		return nullptr;
	}
	auto &comparison = expr.Cast<BoundFunctionExpression>();
	auto &left = BoundComparisonExpression::Left(comparison);
	auto &right = BoundComparisonExpression::Right(comparison);
	optional_ptr<const Expression> constant;
	if (is_operand(left) && right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		constant = right;
	} else if (is_operand(right) && left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		constant = left;
	} else {
		return nullptr;
	}
	auto &value = constant->Cast<BoundConstantExpression>().GetValue();
	if (value.IsNull()) {
		return nullptr;
	}
	return &value;
}

bool TryGetNameEquality(optional_ptr<const TableFilter> filter, const char *caller, string &name) {
	auto value = TryGetEqualityConstant(filter, caller, IsNameOperand);
	if (!value || value->type().id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	name = StringValue::Get(*value);
	return true;
}

static bool IsOidOperand(const Expression &expr) {
	return ExpressionFilter::IsSimpleFilterColumnRef(expr);
}

bool TryGetOidEquality(optional_ptr<const TableFilter> filter, const char *caller, idx_t &oid) {
	auto value = TryGetEqualityConstant(filter, caller, IsOidOperand);
	if (!value || !value->type().IsIntegral()) {
		return false;
	}
	auto converted = value->DefaultTryCastAs(LogicalType::UBIGINT);
	if (!converted) {
		return false;
	}
	oid = UBigIntValue::Get(*converted);
	return true;
}

} // namespace duckdb
