#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class ClientContext;

//! Conversion between Glue (Hive style) type strings and DuckDB logical types
struct GlueTypes {
	//! Parse a Glue column type string, e.g. 'int', 'decimal(10,2)', 'array<string>', 'struct<a:int,b:string>'
	static LogicalType ToLogicalType(const string &glue_type);
	//! Produce a Glue column type string from a DuckDB logical type
	static string FromLogicalType(const LogicalType &type);

	//! One partition key's value as Glue stores it, converted to the column's declared type: the hive sentinel (and,
	//! for a non-string column, "NULL" or an empty string) becomes NULL, anything else is cast, and a value the type
	//! cannot hold throws naming the key. Every surface that reads a partition value goes through here, so that none of
	//! them can disagree about a partition.
	//!
	//! Deliberately NOT HivePartitioning::GetValue, which additionally unescapes. That is correct for its own callers,
	//! which read a value out of a <key>=<value> directory name, and wrong here: Glue's Partition.Values holds the RAW
	//! value -- the escaping belongs to the path, which is the distinction G23 established -- so unescaping it again
	//! corrupts any value that legitimately contains an escape sequence. 'a%20b' is a five-character value, not 'a b'.
	static Value PartitionValue(ClientContext &context, const string &key, const string &str_value,
	                            const LogicalType &type);
};

} // namespace duckdb
