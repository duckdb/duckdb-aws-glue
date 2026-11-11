#pragma once

#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

//! hive_opencsv_field(line, index, separator, quote, escape): field 'index' (from 0) of a line of an OpenCSVSerde file
//! as SplitOpenCSVLine splits it, NULL when the line has no such field
ScalarFunction GetHiveOpenCSVFieldFunction();

} // namespace duckdb
