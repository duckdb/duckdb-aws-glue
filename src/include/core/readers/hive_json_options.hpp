#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class Serializer;
class Deserializer;
struct GlueTableInfo;

//! How the files of a json table are read
struct HiveJSONOptions {
	//! A line that fails to parse or a value that does not fit its column is NULL (OpenX JsonSerDe
	//! ignore.malformed.json / use.null.for.invalid.data)
	bool ignore_errors = false;
	//! Match object keys to the columns ignoring case, as the Hive and the OpenX JsonSerDe do; OpenX
	//! case.insensitive=false matches them exactly
	bool case_insensitive_keys = true;
	//! SerDe properties set on the table that change how its files are read but are not applied; logged at bind
	vector<string> unsupported_properties;

public:
	//! The options for the table's SerDe and SerDe properties
	static HiveJSONOptions FromTable(const GlueTableInfo &table);
	void Serialize(Serializer &serializer) const;
	static HiveJSONOptions Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
