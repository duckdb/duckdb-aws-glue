#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"

#include "core/glue_info.hpp"
#include "core/java_date_format.hpp"

#include "duckdb/common/types/interval.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
class ClientContext;

enum class PartitionProjectionType : uint8_t { ENUM, INTEGER, DATE, INJECTED };

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
	//! types in key order. Throws InvalidInputException for a missing or malformed parameter. An injected key has no
	//! parameters beyond its type: the query supplies its values
	static unique_ptr<PartitionProjection> Parse(const GlueTableInfo &table, const vector<LogicalType> &key_types);
	//! Whether a key is injected: the table location then holds whatever values were ever written, which no
	//! parameter bounds
	bool HasInjectedKey() const;

	//! A predicate of the query on one key, applied to the key's values while they are enumerated
	class KeyFilter {
	public:
		virtual ~KeyFilter() = default;
		//! Whether a value of the key (as the partition column reads it) passes the predicate
		virtual bool Keep(const string &value) const = 0;
		//! The values an equality or IN predicate names (NULL among them names none); false when the predicate is of
		//! another shape
		virtual bool EqualityConstants(vector<Value> &constants) const = 0;
		//! Whether the predicate constrains the key to a range rather than a set of values (a comparison other than
		//! equality, BETWEEN, a LIKE with a literal prefix, IS NOT NULL, NOT of the key or of a set): what Athena calls
		//! a predicate that "cannot be converted to a valid partition", as opposed to one that leaves the key
		//! unconstrained or names no value
		virtual bool ConstrainsRange() const = 0;
		//! Tightens the inclusive bounds of an integer key with the predicate's comparisons of the key itself with a
		//! constant (`hour > 5`, `hour <= 9`); a predicate of another shape leaves them
		virtual void NarrowIntegerRange(int64_t &lower, int64_t &upper) const = 0;
	};
	struct Derivation {
		vector<GluePartitionInfo> partitions;
		//! The product of the keys' value counts before any filter, saturating
		idx_t unfiltered = 0;
		//! Per key, whether its filter dropped a value
		vector<bool> narrowed;
	};

	//! The partitions whose values pass the keys' filters ('filters' has one entry per key, null for none), first key
	//! outermost; throws when more than 'max_partitions' would be derived
	Derivation Derive(ClientContext &context, const vector<optional_ptr<const KeyFilter>> &filters,
	                  idx_t max_partitions) const;

	//! The projection as Parse read it, so a serialized scan parses the same projection again where it is read
	void Serialize(Serializer &serializer) const;
	static unique_ptr<PartitionProjection> Deserialize(Deserializer &deserializer);

	friend class PartitionProjectionWriter;

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
	//! What Parse read and the parsed keys do not keep: the table's name and its projection parameters
	struct Source {
		string schema_name;
		string table_name;
		unordered_map<string, string> parameters;
	};
	Source source;

private:
	void ParseTemplate(const string &location_template);
	//! One key's values that pass its filter
	struct KeyValues {
		vector<ProjectedValue> values;
		//! The key's values before the filter: exact for an integer or enum key, as far as enumerated for a date key
		idx_t count = 0;
		//! Whether more than the cap passed, or an integer key had more candidates than the cap: the enumeration
		//! stopped, or never started
		bool exceeded = false;
	};
	//! The key's values that pass 'filter', stopping once more than 'max_kept' pass; an equality on an integer key
	//! checks its constants instead of enumerating the range, a comparison narrows the range before it is enumerated,
	//! and an integer key with more candidates than 'max_kept' is not enumerated at all
	KeyValues ProjectedValues(ClientContext &context, const PartitionProjectionKey &key,
	                          optional_ptr<const KeyFilter> filter, idx_t max_kept) const;
	//! The values of an injected key: the constants the query's equality or IN on the column names, spelled as the
	//! column type's text; without such a predicate the scan fails, as Athena's does
	KeyValues InjectedValues(const PartitionProjectionKey &key, optional_ptr<const KeyFilter> filter,
	                         idx_t max_kept) const;
	[[noreturn]] void ThrowInjectedNeedsEquality(const PartitionProjectionKey &key, bool predicate_given) const;
	[[noreturn]] void ThrowTooManyPartitions(idx_t max_partitions) const;
	string DateValue(const PartitionProjectionKey &key, timestamp_t value) const;
	timestamp_t ResolveBound(ClientContext &context, const PartitionProjectionKey &key, const DateBound &bound) const;
	timestamp_t ReadBack(const PartitionProjectionKey &key, const string &text) const;
	string Location(const vector<vector<ProjectedValue>> &key_values, const vector<idx_t> &indexes) const;
	//! The location of the partition whose keys spell 'directories' (one per key, in key order)
	string Location(const vector<string> &directories) const;

private:
	//! <database>.<table>, for messages
	string table_description;
	string root_location;
	vector<PartitionProjectionKey> keys;
	//! storage.location.template, split into literals and placeholders; empty for the <key>=<value> layout
	vector<TemplatePart> template_parts;
};

//! A write into a projected table: where a row's partition values go, spelled as the scan derives the directory, and
//! nothing registered
class PartitionProjectionWriter {
public:
	explicit PartitionProjectionWriter(unique_ptr<PartitionProjection> projection);

	//! The COPY target: the table location, or storage.location.template's literal prefix up to the last '/' before
	//! its first placeholder
	const string &CopyTarget() const {
		return copy_target;
	}
	//! The partition directory of 'values' (one per key, in key order), relative to CopyTarget(); a relative date
	//! bound's NOW is the start of 'context's transaction, as on read. Throws InvalidInputException for a value no
	//! partition the projection derives has
	string RelativePath(ClientContext &context, const vector<Value> &values) const;

private:
	string Directory(ClientContext &context, const PartitionProjectionKey &key, const Value &value) const;
	string DateDirectory(ClientContext &context, const PartitionProjectionKey &key, const Value &value) const;
	[[noreturn]] void Refuse(const PartitionProjectionKey &key, const Value &value, const char *parameter) const;

private:
	unique_ptr<PartitionProjection> projection;
	string copy_target;
};

} // namespace duckdb
