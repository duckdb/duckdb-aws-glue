#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! The SerDe of a Hive table, decided by its SerializationLibrary; SerDes of one file format can differ in how they
//! read the data files and which engines write them
enum class HiveSerDe : uint8_t { PARQUET, LAZY_SIMPLE, OPEN_CSV, HIVE_JSON, OPENX_JSON, AVRO };

} // namespace duckdb
