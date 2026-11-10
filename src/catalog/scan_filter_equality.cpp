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

bool TryGetNameEquality(optional_ptr<const TableFilter> filter, const char *caller, string &name) {
	if (!filter) {
		return false;
	}
	auto &expr = *ExpressionFilter::GetExpressionFilter(*filter, caller).expr;
	if (!BoundComparisonExpression::IsComparison(expr) || expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
		return false;
	}
	auto &comparison = expr.Cast<BoundFunctionExpression>();
	auto &left = BoundComparisonExpression::Left(comparison);
	auto &right = BoundComparisonExpression::Right(comparison);
	optional_ptr<const Expression> constant;
	if (IsNameOperand(left) && right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		constant = right;
	} else if (IsNameOperand(right) && left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		constant = left;
	} else {
		return false;
	}
	auto &value = constant->Cast<BoundConstantExpression>().GetValue();
	if (value.IsNull() || value.type().id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	name = StringValue::Get(value);
	return true;
}

} // namespace duckdb
