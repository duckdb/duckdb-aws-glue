#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

#include "core/glue_info.hpp"
#include "core/java_date_format.hpp"

#include "duckdb/common/types/interval.hpp"

namespace duckdb {
class ClientContext;

enum class PartitionProjectionType : uint8_t { ENUM, INTEGER, DATE };

//! A bound of a date range: a fixed instant, or NOW (the start of the transaction) plus an offset
struct DateBound {
	bool relative = false;
	timestamp_t fixed = timestamp_t(0);
	interval_t offset = {0, 0, 0};
};

//! One partition key of a projected table: the values its projection.<key>.* parameters describe
struct PartitionProjectionKey {
	string name;
	PartitionProjectionType type = PartitionProjectionType::ENUM;
	//! The type of the partition column, which decides the form of a date key's partition value
	LogicalType column_type;
	//! enum: projection.<key>.values
	vector<string> enum_values;
	//! integer: projection.<key>.range, .interval and .digits (0 for no padding)
	int64_t range_start = 0;
	int64_t range_end = 0;
	int64_t interval = 1;
	idx_t digits = 0;
	//! date: projection.<key>.format, .range (bounds fixed or relative to NOW) and the step .interval .interval.unit
	unique_ptr<JavaDateFormat> format;
	DateBound date_start;
	DateBound date_end;
	interval_t step = {0, 0, 0};
};

//! A partition projection as Athena reads it: the cross product of the keys' projected values, each at its location
class PartitionProjection {
public:
	//! Parse the projection.* parameters of 'table' for its partition keys; 'key_types' are the partition columns'
	//! types in key order. Throws InvalidInputException for a missing or malformed parameter and
	//! NotImplementedException for an injected key
	static unique_ptr<PartitionProjection> Parse(const GlueTableInfo &table, const vector<LogicalType> &key_types);

	//! The derived partitions: the cross product of the keys' values, first key outermost, each at its location
	vector<GluePartitionInfo> Derive(ClientContext &context) const;

private:
	struct TemplatePart {
		//! Literal text, or empty when the part is a placeholder
		string literal;
		//! The key whose value replaces the placeholder, DConstants::INVALID_INDEX for a literal
		idx_t key_index = DConstants::INVALID_INDEX;
	};

	//! A projected value: what the partition column reads, and how it is spelled in the directory name
	struct ProjectedValue {
		string value;
		string directory;
	};

private:
	void ParseTemplate(const string &location_template);
	vector<ProjectedValue> ProjectedValues(ClientContext &context, const PartitionProjectionKey &key) const;
	string DateValue(const PartitionProjectionKey &key, timestamp_t value) const;
	timestamp_t ResolveBound(ClientContext &context, const PartitionProjectionKey &key, const DateBound &bound) const;
	timestamp_t ReadBack(const PartitionProjectionKey &key, const string &text) const;
	string Location(const vector<vector<ProjectedValue>> &key_values, const vector<idx_t> &indexes) const;

private:
	//! <database>.<table>, for messages
	string table_description;
	string root_location;
	vector<PartitionProjectionKey> keys;
	//! storage.location.template, split into literals and placeholders; empty for the <key>=<value> layout
	vector<TemplatePart> template_parts;
};

} // namespace duckdb
