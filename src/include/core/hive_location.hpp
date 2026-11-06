#pragma once

#include "duckdb/common/path.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! A location to compare with other locations: s3a://, s3n:// and S3:// name the same bucket and key as s3://, and a
//! trailing slash is dropped. Only for comparing; files are listed and read at the location as registered.
string ComparableLocation(const string &location);
//! Whether one comparable location holds the other, so that files of one are files of the other
bool LocationsOverlap(const string &a, const string &b);
//! The hive directory of a partition relative to its table: <key>=<value>/..., escaped, with a Glue value of
//! __HIVE_DEFAULT_PARTITION__ (NULL) kept as is
Path HivePartitionDirectory(const vector<string> &keys, const vector<string> &values);

} // namespace duckdb
