#pragma once

#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class LogicalGet;

//! A Hive scan (of a Glue table or a hive_scan() call) as the hive_scan() call that reads the same files the same way,
//! for an engine that hands some of a scan's files to machines without access to Glue. Core DuckDB types only, so a
//! caller needs no other header of this extension.
struct HiveScanCall {
	//! hive_scan's first argument
	string root;
	//! schema, format and its reading options, and partition_keys. The caller adds the files to read: partitions :=
	//! [{<key>: <value>, ..., location: ..., files: [...]}, ...], or files := [...] for a table without partition keys
	named_parameter_map_t arguments;
	//! In order; empty for a table without partition keys
	vector<string> partition_keys;
};

//! The partition a data file of a Hive scan belongs to
struct HiveScanFilePartition {
	//! As Glue stores them, in partition key order
	vector<string> values;
	string location;
};

//! Nothing when 'get' is not a Hive scan bound by this build of the extension
optional<HiveScanCall> HiveScanGetCall(const LogicalGet &get);
//! Nothing when 'get' is not such a scan, has no partition keys or does not read 'path'
optional<HiveScanFilePartition> HiveScanGetFilePartition(const LogicalGet &get, const string &path);

} // namespace duckdb
