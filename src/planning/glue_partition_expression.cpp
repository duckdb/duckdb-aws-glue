#include "planning/glue_partition_expression.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "planning/hive_multi_file_reader.hpp"

namespace duckdb {

//! The longest expression GetPartitions accepts
static constexpr idx_t GLUE_MAX_PARTITION_EXPRESSION_LENGTH = 2048;

//! The partition key 'expr' refers to, if it is a column of this scan and the key's name needs no quoting
static optional_ptr<const string> PartitionKeyOf(const Expression &expr, TableIndex table_index,
                                                 const vector<PartitionKeyProjection> &projections,
                                                 const HiveScanInfo &info) {
	if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return nullptr;
	}
	auto &colref = expr.Cast<BoundColumnRefExpression>();
	if (colref.Binding().table_index != table_index) {
		return nullptr;
	}
	for (auto &projection : projections) {
		if (projection.projected_column_index != colref.Binding().column_index.GetIndex()) {
			continue;
		}
		auto &key = info.partition_keys[projection.partition_key_index];
		for (auto c : key) {
			if (!StringUtil::CharacterIsAlphaNumeric(c) && c != '_') {
				return nullptr;
			}
		}
		return &key;
	}
	return nullptr;
}

//! Whether 'str' is a date Glue reads as such: yyyy-MM-dd, so no BC dates and no year beyond four digits
static bool IsGlueDate(const string &str) {
	if (str.size() != 10 || str[4] != '-' || str[7] != '-') {
		return false;
	}
	for (idx_t i = 0; i < str.size(); i++) {
		if (i != 4 && i != 7 && !StringUtil::CharacterIsDigit(str[i])) {
			return false;
		}
	}
	return true;
}

//! Writes the literal if Glue compares it the way DuckDB does: an integer, a date, or a string that needs no
//! escaping. A range ('ordered') on a string also needs ASCII, the only case where Glue (UTF-16) and DuckDB (UTF-8)
//! sort alike.
static bool TryGlueLiteral(const Expression &expr, const LogicalType &key_type, bool ordered, string &result) {
	if (expr.GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		return false;
	}
	auto &value = expr.Cast<BoundConstantExpression>().GetValue();
	if (value.IsNull() || value.type() != key_type) {
		return false;
	}
	switch (key_type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		result = value.ToString();
		return true;
	case LogicalTypeId::DATE: {
		auto str = value.ToString();
		if (!IsGlueDate(str)) {
			return false;
		}
		result = "'" + str + "'";
		return true;
	}
	case LogicalTypeId::VARCHAR: {
		auto &str = StringValue::Get(value);
		for (auto c : str) {
			auto byte = static_cast<uint8_t>(c);
			if (c == '\'' || c == '\\' || byte < 0x20 || (ordered && byte >= 0x80)) {
				return false;
			}
		}
		result = "'" + str + "'";
		return true;
	}
	default:
		return false;
	}
}

//! The comparison in Glue's syntax, which spells not-equal '<>'
static string GlueComparisonOperator(ExpressionType type) {
	return type == ExpressionType::COMPARE_NOTEQUAL ? "<>" : ExpressionTypeToOperator(type);
}

//! Writes 'filter' as a Glue partition filter that keeps at least the partitions DuckDB keeps. False when there is no
//! way to write it.
static bool TryGlueFilter(const Expression &filter, TableIndex table_index,
                          const vector<PartitionKeyProjection> &projections, const HiveScanInfo &info, string &result) {
	auto type = filter.GetExpressionType();
	if (type == ExpressionType::CONJUNCTION_AND || type == ExpressionType::CONJUNCTION_OR) {
		// any child of an AND narrows the partitions down on its own; an OR needs every child
		vector<string> parts;
		for (auto &child : filter.Cast<BoundConjunctionExpression>().GetChildren()) {
			string part;
			if (TryGlueFilter(*child, table_index, projections, info, part)) {
				parts.push_back("(" + part + ")");
			} else if (type == ExpressionType::CONJUNCTION_OR) {
				return false;
			}
		}
		if (parts.empty()) {
			return false;
		}
		result = StringUtil::Join(parts, type == ExpressionType::CONJUNCTION_AND ? " AND " : " OR ");
		return true;
	}
	if (type == ExpressionType::COMPARE_IN) {
		auto &children = filter.Cast<BoundOperatorExpression>().GetChildren();
		auto key = PartitionKeyOf(*children[0], table_index, projections, info);
		if (!key) {
			return false;
		}
		vector<string> literals;
		for (idx_t i = 1; i < children.size(); i++) {
			string literal;
			if (!TryGlueLiteral(*children[i], children[0]->GetReturnType(), false, literal)) {
				return false;
			}
			literals.push_back(literal);
		}
		result = *key + " IN (" + StringUtil::Join(literals, ", ") + ")";
		return true;
	}
	if (type == ExpressionType::COMPARE_BETWEEN) {
		auto &between = filter.Cast<BoundFunctionExpression>();
		if (!BoundBetweenExpression::HasValidBindData(between)) {
			return false;
		}
		auto &input = BoundBetweenExpression::Input(between);
		auto key = PartitionKeyOf(input, table_index, projections, info);
		string lower, upper;
		if (!key || !TryGlueLiteral(BoundBetweenExpression::LowerBound(between), input.GetReturnType(), true, lower) ||
		    !TryGlueLiteral(BoundBetweenExpression::UpperBound(between), input.GetReturnType(), true, upper)) {
			return false;
		}
		result = *key + " " + GlueComparisonOperator(BoundBetweenExpression::LowerComparisonType(between)) + " " +
		         lower + " AND " + *key + " " +
		         GlueComparisonOperator(BoundBetweenExpression::UpperComparisonType(between)) + " " + upper;
		return true;
	}
	if (!BoundComparisonExpression::IsComparison(filter)) {
		return false;
	}
	auto op = GlueComparisonOperator(type);
	if (op.empty()) {
		return false;
	}
	auto &comparison = filter.Cast<BoundFunctionExpression>();
	auto &left = BoundComparisonExpression::Left(comparison);
	auto &right = BoundComparisonExpression::Right(comparison);
	bool ordered = type != ExpressionType::COMPARE_EQUAL && type != ExpressionType::COMPARE_NOTEQUAL;
	string literal;
	auto key = PartitionKeyOf(left, table_index, projections, info);
	if (key && TryGlueLiteral(right, left.GetReturnType(), ordered, literal)) {
		result = *key + " " + op + " " + literal;
		return true;
	}
	key = PartitionKeyOf(right, table_index, projections, info);
	if (key && TryGlueLiteral(left, right.GetReturnType(), ordered, literal)) {
		result = *key + " " + GlueComparisonOperator(FlipComparisonExpression(type)) + " " + literal;
		return true;
	}
	return false;
}

string GluePartitionExpression(const HiveScanInfo &info, TableIndex table_index,
                               const vector<PartitionKeyProjection> &projections,
                               const vector<unique_ptr<Expression>> &filters, unordered_set<idx_t> &used_filters) {
	vector<string> parts;
	for (idx_t filter_index = 0; filter_index < filters.size(); filter_index++) {
		string part;
		if (TryGlueFilter(*filters[filter_index], table_index, projections, info, part)) {
			parts.push_back("(" + part + ")");
			used_filters.insert(filter_index);
		}
	}
	auto expression = StringUtil::Join(parts, " AND ");
	if (expression.size() > GLUE_MAX_PARTITION_EXPRESSION_LENGTH) {
		used_filters.clear();
		return string();
	}
	return expression;
}

} // namespace duckdb
