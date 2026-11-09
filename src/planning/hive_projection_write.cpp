#include "planning/hive_projection_write.hpp"

#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

unique_ptr<Expression> ProjectedPartitionPath(shared_ptr<PartitionProjectionWriter> writer,
                                              const vector<LogicalType> &types,
                                              const vector<idx_t> &partition_columns) {
	vector<unique_ptr<Expression>> arguments;
	vector<LogicalType> argument_types;
	for (idx_t i = 0; i < partition_columns.size(); i++) {
		arguments.push_back(make_uniq<BoundReferenceExpression>(types[partition_columns[i]], i));
		argument_types.push_back(types[partition_columns[i]]);
	}
	// the copy evaluates the path once per partition, on one row of its values; a relative date bound's NOW is the
	// executing transaction's start, so a prepared INSERT resolves it on each execution, as the scan does
	auto path = [writer = std::move(writer)](DataChunk &input, ExpressionState &state, Vector &result) {
		vector<Value> values(input.ColumnCount());
		for (idx_t row = 0; row < input.size(); row++) {
			for (idx_t column = 0; column < input.ColumnCount(); column++) {
				values[column] = input.data[column].GetValue(row);
			}
			result.SetValue(row, Value(writer->RelativePath(state.GetContext(), values)));
		}
	};
	// NULL reaches the function, which refuses it: no key derives a NULL value
	ScalarFunction function(Identifier("glue_projected_partition_path"), std::move(argument_types),
	                        LogicalType::VARCHAR, std::move(path), nullptr, nullptr, nullptr,
	                        LogicalType(LogicalTypeId::INVALID), FunctionStability::CONSISTENT,
	                        FunctionNullHandling::SPECIAL_HANDLING);
	function.SetFallible();
	return make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(arguments), nullptr);
}

} // namespace duckdb
