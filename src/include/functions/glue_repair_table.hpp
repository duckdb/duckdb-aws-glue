#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! glue_repair_table(table): registers the <key>=<value> directories under the table location that Glue does not
//! have yet. Returns one row per added partition, with a typed column per partition key and its location.
TableFunction GetGlueRepairTableFunction();

} // namespace duckdb
