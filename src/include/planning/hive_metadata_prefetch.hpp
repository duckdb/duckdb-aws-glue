#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

//! Fetches the partitions and samples of a statement's Glue tables at the same time, before it is optimized
class HiveMetadataPrefetch : public OptimizerExtension {
public:
	HiveMetadataPrefetch();

	//! Whether the statement that started the prefetch has ended
	static bool Stopping(ClientContext &context);

private:
	static void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
