#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class ClientContext;
class Serializer;
class Deserializer;

//! How the files of a csv table are read
struct HiveCSVOptions {
	string delimiter = ",";
	string quote = "\"";
	string escape = "\"";
	//! The header lines every file starts with
	idx_t skip_lines = 0;
	string null_string;
	//! Read fields as text and TRY_CAST them; short rows padded with NULL, extra fields ignored
	bool serde_fields = false;
	//! With serde_fields: a field that does not parse as its column type is NULL (TRY_CAST), else the query fails
	bool null_for_invalid_data = true;
	//! Read a file with several threads (read_csv's parallel)
	bool parallel = true;

public:
	//! With serde_fields and a quote, the hive_csv_parallel setting decides 'parallel'
	void UseParallelSetting(ClientContext &context);

	void Serialize(Serializer &serializer) const;
	static HiveCSVOptions Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
