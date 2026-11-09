#include "core/partition_projection.hpp"
#include "core/helpers.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/multiply.hpp"

#include <algorithm>
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Parsing the parameters
//===--------------------------------------------------------------------===//
static string ProjectionParameter(const string &key, const string &suffix) {
	return "projection." + key + "." + suffix;
}

static bool TrySplitRange(const string &text, string &lower, string &upper) {
	auto comma = text.find(',');
	if (comma == string::npos || text.find(',', comma + 1) != string::npos) {
		return false;
	}
	lower = text.substr(0, comma);
	upper = text.substr(comma + 1);
	StringUtil::Trim(lower);
	StringUtil::Trim(upper);
	return !lower.empty() && !upper.empty();
}

static void ParseEnumKey(const GlueTableInfo &table, const string &description, PartitionProjectionKey &key) {
	auto parameter = ProjectionParameter(key.name, "values");
	auto values = table.GetParameter(parameter);
	if (values.empty()) {
		throw InvalidInputException("Glue table '%s': %s is required for an enum partition column", description,
		                            parameter);
	}
	// white space is part of a value (Athena); an empty element is skipped and a repeated one counted once (Trino)
	unordered_set<string> seen;
	for (auto &value : StringUtil::Split(values, ',')) {
		if (!value.empty() && seen.insert(value).second) {
			key.enum_values.push_back(value);
		}
	}
	if (key.enum_values.empty()) {
		throw InvalidInputException("Glue table '%s': %s holds no values", description, parameter);
	}
}

//! Digits with an optional sign only: the BIGINT cast would read '1.5' as 2 and '1e3' as 1000
static bool TryParseStrictInteger(const string &text_p, int64_t min, int64_t max, int64_t &result) {
	auto text = text_p;
	StringUtil::Trim(text);
	idx_t start = !text.empty() && (text[0] == '-' || text[0] == '+') ? 1 : 0;
	if (text.size() == start) {
		return false;
	}
	for (idx_t i = start; i < text.size(); i++) {
		if (text[i] < '0' || text[i] > '9') {
			return false;
		}
	}
	return TryParseInteger(text, min, max, result);
}

static void ParseIntegerKey(const GlueTableInfo &table, const string &description, PartitionProjectionKey &key) {
	auto range_parameter = ProjectionParameter(key.name, "range");
	auto range = table.GetParameter(range_parameter);
	string lower;
	string upper;
	if (!TrySplitRange(range, lower, upper) ||
	    !TryParseStrictInteger(lower, NumericLimits<int64_t>::Minimum(), NumericLimits<int64_t>::Maximum(),
	                           key.range_start) ||
	    !TryParseStrictInteger(upper, NumericLimits<int64_t>::Minimum(), NumericLimits<int64_t>::Maximum(),
	                           key.range_end)) {
		throw InvalidInputException("Glue table '%s': %s must be two integers separated by a comma (min,max), got '%s'",
		                            description, range_parameter, range);
	}
	if (key.range_start > key.range_end) {
		throw InvalidInputException("Glue table '%s': %s '%s' has its lower bound after its upper bound", description,
		                            range_parameter, range);
	}
	auto interval_parameter = ProjectionParameter(key.name, "interval");
	auto interval = table.GetParameter(interval_parameter);
	if (!interval.empty() && !TryParseStrictInteger(interval, 1, NumericLimits<int64_t>::Maximum(), key.interval)) {
		throw InvalidInputException("Glue table '%s': %s must be a positive integer, got '%s'", description,
		                            interval_parameter, interval);
	}
	auto digits_parameter = ProjectionParameter(key.name, "digits");
	auto digits = table.GetParameter(digits_parameter);
	int64_t digit_count;
	if (!digits.empty()) {
		if (!TryParseStrictInteger(digits, 1, 64, digit_count)) {
			throw InvalidInputException("Glue table '%s': %s must be a positive integer up to 64, got '%s'",
			                            description, digits_parameter, digits);
		}
		key.digits = NumericCast<idx_t>(digit_count);
	}
}

//! Athena's NOW[ +|- n unit ] bound, in any case; false when 'text' is not one
static bool TryParseRelativeBound(const string &text, DateBound &bound) {
	auto rest = StringUtil::Upper(text);
	StringUtil::Trim(rest);
	if (!StringUtil::StartsWith(rest, "NOW")) {
		return false;
	}
	rest = rest.substr(3);
	StringUtil::Trim(rest);
	bound.relative = true;
	bound.offset = interval_t {0, 0, 0};
	if (rest.empty()) {
		return true;
	}
	auto sign = rest[0];
	if (sign != '+' && sign != '-') {
		return false;
	}
	rest = rest.substr(1);
	StringUtil::Trim(rest);
	idx_t digits = 0;
	while (digits < rest.size() && rest[digits] >= '0' && rest[digits] <= '9') {
		digits++;
	}
	int64_t count;
	if (digits == 0 || !TryParseInteger(rest.substr(0, digits), 0, NumericLimits<int32_t>::Maximum(), count)) {
		return false;
	}
	auto unit = rest.substr(digits);
	StringUtil::Trim(unit);
	if (unit.size() > 1 && unit.back() == 'S') {
		unit.pop_back();
	}
	auto signed_count = sign == '-' ? -count : count;
	if (unit == "YEAR") {
		return TryCast::Operation<int64_t, int32_t>(signed_count * Interval::MONTHS_PER_YEAR, bound.offset.months);
	}
	if (unit == "MONTH") {
		return TryCast::Operation<int64_t, int32_t>(signed_count, bound.offset.months);
	}
	if (unit == "WEEK") {
		return TryCast::Operation<int64_t, int32_t>(signed_count * Interval::DAYS_PER_WEEK, bound.offset.days);
	}
	if (unit == "DAY") {
		return TryCast::Operation<int64_t, int32_t>(signed_count, bound.offset.days);
	}
	if (unit == "HOUR") {
		bound.offset.micros = signed_count * Interval::MICROS_PER_HOUR;
	} else if (unit == "MINUTE") {
		bound.offset.micros = signed_count * Interval::MICROS_PER_MINUTE;
	} else if (unit == "SECOND") {
		bound.offset.micros = signed_count * Interval::MICROS_PER_SEC;
	} else {
		return false;
	}
	return true;
}

static bool TryParseIntervalUnit(const string &unit_text, int64_t count, interval_t &step) {
	auto unit = StringUtil::Upper(unit_text);
	StringUtil::Trim(unit);
	step = interval_t {0, 0, 0};
	int64_t scaled;
	// every java.time ChronoUnit that LocalDateTime.plus accepts, as Athena reads the unit
	int64_t years = unit == "MILLENNIA"   ? 1000
	                : unit == "CENTURIES" ? 100
	                : unit == "DECADES"   ? 10
	                : unit == "YEARS"     ? 1
	                                      : 0;
	if (years > 0) {
		return TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(count, years * Interval::MONTHS_PER_YEAR,
		                                                                 scaled) &&
		       TryCast::Operation<int64_t, int32_t>(scaled, step.months);
	}
	if (unit == "MONTHS") {
		return TryCast::Operation<int64_t, int32_t>(count, step.months);
	}
	if (unit == "WEEKS") {
		return TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(count, Interval::DAYS_PER_WEEK, scaled) &&
		       TryCast::Operation<int64_t, int32_t>(scaled, step.days);
	}
	if (unit == "DAYS") {
		return TryCast::Operation<int64_t, int32_t>(count, step.days);
	}
	if (unit == "HALF_DAYS") {
		return TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(count, 12 * Interval::MICROS_PER_HOUR,
		                                                                 step.micros);
	}
	if (unit == "HOURS") {
		return TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(count, Interval::MICROS_PER_HOUR, step.micros);
	}
	if (unit == "MINUTES") {
		return TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(count, Interval::MICROS_PER_MINUTE,
		                                                                 step.micros);
	}
	if (unit == "SECONDS") {
		return TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(count, Interval::MICROS_PER_SEC, step.micros);
	}
	if (unit == "MILLIS") {
		return TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(count, Interval::MICROS_PER_MSEC, step.micros);
	}
	if (unit == "MICROS") {
		step.micros = count;
		return true;
	}
	if (unit == "NANOS") {
		// a timestamp holds microseconds
		step.micros = count / 1000;
		return count % 1000 == 0;
	}
	return false;
}

static void ParseDateBound(const string &text, const string &description, const string &parameter, const string &range,
                           const PartitionProjectionKey &key, DateBound &bound) {
	if (TryParseRelativeBound(text, bound)) {
		return;
	}
	// a date literal is taken as written: Athena documents its white space as part of the date
	bound.relative = false;
	if (!key.format->TryParse(text, bound.fixed)) {
		throw InvalidInputException("Glue table '%s': %s '%s' must be two dates in the format of projection.%s.format "
		                            "or NOW[+-n unit] separated by a comma; '%s' is neither",
		                            description, parameter, range, key.name, text);
	}
}

static void ParseDateKey(const GlueTableInfo &table, const string &description, PartitionProjectionKey &key) {
	auto format_parameter = ProjectionParameter(key.name, "format");
	key.format = make_uniq<JavaDateFormat>(table.GetParameter(format_parameter), format_parameter, description);
	auto range_parameter = ProjectionParameter(key.name, "range");
	auto range = table.GetParameter(range_parameter);
	auto comma = range.find(',');
	if (range.empty() || comma == string::npos || range.find(',', comma + 1) != string::npos) {
		throw InvalidInputException("Glue table '%s': %s must be two dates separated by a comma (min,max), got '%s'",
		                            description, range_parameter, range);
	}
	ParseDateBound(range.substr(0, comma), description, range_parameter, range, key, key.date_start);
	ParseDateBound(range.substr(comma + 1), description, range_parameter, range, key, key.date_end);
	auto interval_parameter = ProjectionParameter(key.name, "interval");
	auto interval = table.GetParameter(interval_parameter);
	int64_t count = 1;
	if (!interval.empty() && !TryParseStrictInteger(interval, 1, NumericLimits<int64_t>::Maximum(), count)) {
		throw InvalidInputException("Glue table '%s': %s must be a positive integer, got '%s'", description,
		                            interval_parameter, interval);
	}
	auto unit_parameter = ProjectionParameter(key.name, "interval.unit");
	auto unit = table.GetParameter(unit_parameter);
	// a unit without an interval is ignored but has to name a unit: 1000 of any unit is a valid step
	if (!unit.empty() && !TryParseIntervalUnit(unit, interval.empty() ? 1000 : count, key.step)) {
		throw InvalidInputException(
		    "Glue table '%s': %s is '%s', expected MILLENNIA, CENTURIES, DECADES, YEARS, MONTHS, "
		    "WEEKS, DAYS, HALF_DAYS, HOURS, MINUTES, SECONDS, MILLIS, MICROS or NANOS (a "
		    "multiple of 1000)%s",
		    description, unit_parameter, unit, interval.empty() ? "" : ", and an interval that fits the unit");
	}
	if (interval.empty() || unit.empty()) {
		// Athena reads the interval and its unit only together; otherwise one of the format's finest field
		if (key.format->HasTimeField()) {
			throw InvalidInputException("Glue table '%s': %s and %s are required together: projection.%s.format '%s' "
			                            "has time fields, so the step does not default to 1 day, 1 month or 1 year",
			                            description, interval_parameter, unit_parameter, key.name,
			                            table.GetParameter(format_parameter));
		}
		key.step = key.format->FinestUnit();
	}
}

unique_ptr<PartitionProjection> PartitionProjection::Parse(const GlueTableInfo &table,
                                                           const vector<LogicalType> &key_types) {
	D_ASSERT(key_types.size() == table.partition_keys.size());
	auto result = make_uniq<PartitionProjection>();
	result->table_description = table.schema_name + "." + table.name;
	result->root_location = table.location;
	result->source.schema_name = table.schema_name;
	result->source.table_name = table.name;
	for (auto &entry : table.parameters) {
		auto name = StringUtil::Lower(entry.first);
		if (StringUtil::StartsWith(name, "projection.") || name == "storage.location.template") {
			result->source.parameters.insert(entry);
		}
	}
	// Athena's check comes first: every partition column needs a projection type
	vector<string> missing;
	for (auto &column : table.partition_keys) {
		if (table.GetParameter(ProjectionParameter(column.name, "type")).empty()) {
			missing.push_back(column.name);
		}
	}
	if (!missing.empty()) {
		throw InvalidInputException("Glue table '%s' is configured for partition projection, but the following "
		                            "partition columns are missing projection configuration: [%s]",
		                            result->table_description, StringUtil::Join(missing, ", "));
	}
	for (idx_t key_index = 0; key_index < table.partition_keys.size(); key_index++) {
		PartitionProjectionKey key;
		key.name = table.partition_keys[key_index].name;
		key.column_type = key_types[key_index];
		auto type_parameter = ProjectionParameter(key.name, "type");
		auto type = StringUtil::Lower(table.GetParameter(type_parameter));
		StringUtil::Trim(type);
		if (type == "enum") {
			key.type = PartitionProjectionType::ENUM;
			ParseEnumKey(table, result->table_description, key);
		} else if (type == "integer") {
			key.type = PartitionProjectionType::INTEGER;
			ParseIntegerKey(table, result->table_description, key);
		} else if (type == "date") {
			key.type = PartitionProjectionType::DATE;
			ParseDateKey(table, result->table_description, key);
		} else if (type == "injected") {
			// the query's equality or IN on the column supplies the values; no other parameter applies
			key.type = PartitionProjectionType::INJECTED;
		} else {
			throw InvalidInputException("Glue table '%s': %s is '%s', expected enum, integer, date or injected",
			                            result->table_description, type_parameter, table.GetParameter(type_parameter));
		}
		result->keys.push_back(std::move(key));
	}
	auto location_template = table.GetParameter("storage.location.template");
	if (!location_template.empty()) {
		result->ParseTemplate(location_template);
	}
	return result;
}

void PartitionProjection::ParseTemplate(const string &location_template) {
	vector<bool> seen(keys.size(), false);
	idx_t position = 0;
	while (position < location_template.size()) {
		auto start = location_template.find("${", position);
		if (start == string::npos) {
			template_parts.push_back(TemplatePart {location_template.substr(position), DConstants::INVALID_INDEX});
			break;
		}
		auto end = location_template.find('}', start + 2);
		if (end == string::npos) {
			throw InvalidInputException("Glue table '%s': storage.location.template '%s' has an unterminated ${ "
			                            "placeholder",
			                            table_description, location_template);
		}
		if (start > position) {
			template_parts.push_back(
			    TemplatePart {location_template.substr(position, start - position), DConstants::INVALID_INDEX});
		}
		auto name = location_template.substr(start + 2, end - start - 2);
		idx_t key_index = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < keys.size(); i++) {
			if (StringUtil::CIEquals(keys[i].name, name)) {
				key_index = i;
				break;
			}
		}
		if (key_index == DConstants::INVALID_INDEX) {
			throw InvalidInputException("Glue table '%s': storage.location.template '%s' refers to ${%s}, which is "
			                            "not a partition column",
			                            table_description, location_template, name);
		}
		seen[key_index] = true;
		template_parts.push_back(TemplatePart {string(), key_index});
		position = end + 1;
	}
	for (idx_t i = 0; i < keys.size(); i++) {
		if (!seen[i]) {
			throw InvalidInputException("Glue table '%s': storage.location.template '%s' has no ${%s} placeholder for "
			                            "partition column %s",
			                            table_description, location_template, keys[i].name, keys[i].name);
		}
	}
}

//===--------------------------------------------------------------------===//
// Deriving the partitions
//===--------------------------------------------------------------------===//
//! Athena spells no timestamp into a partition path: an injected column of a timestamp type is never read or written
static bool IsTimestampColumn(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return true;
	default:
		return false;
	}
}

//! Java's %0<digits>d: zero padding after the sign, the sign counted in the width
static string FormatInteger(int64_t value, idx_t digits) {
	auto negative = value < 0;
	auto magnitude =
	    negative ? UnsafeNumericCast<uint64_t>(0) - static_cast<uint64_t>(value) : static_cast<uint64_t>(value);
	auto text = to_string(magnitude);
	auto width = digits > (negative ? 1 : 0) ? digits - (negative ? 1 : 0) : 0;
	if (text.size() < width) {
		text.insert(0, width - text.size(), '0');
	}
	return negative ? "-" + text : text;
}

void PartitionProjection::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "schema_name", source.schema_name);
	serializer.WriteProperty(101, "table_name", source.table_name);
	serializer.WriteProperty(102, "location", root_location);
	vector<string> partition_keys;
	vector<LogicalType> key_types;
	for (auto &key : keys) {
		partition_keys.push_back(key.name);
		key_types.push_back(key.column_type);
	}
	serializer.WriteProperty(103, "partition_keys", partition_keys);
	serializer.WriteProperty(104, "key_types", key_types);
	serializer.WriteProperty(105, "parameters", source.parameters);
}

unique_ptr<PartitionProjection> PartitionProjection::Deserialize(Deserializer &deserializer) {
	GlueTableInfo table;
	table.schema_name = deserializer.ReadProperty<string>(100, "schema_name");
	table.name = deserializer.ReadProperty<string>(101, "table_name");
	table.location = deserializer.ReadProperty<string>(102, "location");
	for (auto &name : deserializer.ReadProperty<vector<string>>(103, "partition_keys")) {
		GlueColumn column;
		column.name = name;
		table.partition_keys.push_back(std::move(column));
	}
	auto key_types = deserializer.ReadProperty<vector<LogicalType>>(104, "key_types");
	table.parameters = deserializer.ReadProperty<unordered_map<string, string>>(105, "parameters");
	return Parse(table, key_types);
}

//! Whether 'value' is one of an integer key's values (in the range and on the interval grid), and which; read
//! leniently, since a padded key spells 7 as '07' and the predicate checks the spelling afterwards
static bool TryIntegerValue(const PartitionProjectionKey &key, const Value &value, int64_t &result) {
	auto integer = value.DefaultTryCastAs(LogicalType::BIGINT, nullptr, false);
	if (!integer || integer->IsNull()) {
		return false;
	}
	result = integer->GetValue<int64_t>();
	if (result < key.range_start || result > key.range_end) {
		return false;
	}
	auto offset = static_cast<uint64_t>(result) - static_cast<uint64_t>(key.range_start);
	return offset % static_cast<uint64_t>(key.interval) == 0;
}

//! How many values an integer key has, saturating
static idx_t IntegerCount(const PartitionProjectionKey &key) {
	auto span = static_cast<uint64_t>(key.range_end) - static_cast<uint64_t>(key.range_start);
	auto steps = span / static_cast<uint64_t>(key.interval);
	return steps == NumericLimits<uint64_t>::Maximum() ? steps : steps + 1;
}

//! The span an interval covers in microseconds, at its shortest (28-day months) or longest (31-day months), saturating
static int64_t IntervalMicros(const interval_t &interval, bool shortest) {
	hugeint_t days = hugeint_t(interval.months) * hugeint_t(shortest ? 28 : 31) + hugeint_t(interval.days);
	hugeint_t micros = days * hugeint_t(Interval::MICROS_PER_DAY) + hugeint_t(interval.micros);
	hugeint_t max = hugeint_t(NumericLimits<int64_t>::Maximum());
	return micros > max ? NumericLimits<int64_t>::Maximum() : Hugeint::Cast<int64_t>(micros);
}

bool PartitionProjection::HasInjectedKey() const {
	for (auto &key : keys) {
		if (key.type == PartitionProjectionType::INJECTED) {
			return true;
		}
	}
	return false;
}

void PartitionProjection::ThrowInjectedNeedsEquality(const PartitionProjectionKey &key, bool predicate_given) const {
	throw InvalidInputException("Glue table '%s': the injected partition column '%s' (projection.%s.type) needs an "
	                            "equality or IN on it in the WHERE clause, as Athena requires%s",
	                            table_description, key.name, key.name,
	                            predicate_given ? "; the predicate given does not name its values" : "");
}

PartitionProjection::KeyValues PartitionProjection::InjectedValues(const PartitionProjectionKey &key,
                                                                   optional_ptr<const KeyFilter> filter,
                                                                   idx_t max_kept) const {
	// Athena's message for a missing value has a second sentence only when the column is constrained to a range: a
	// column left alone (no predicate, a function of it) or named nothing usable (IS NULL, '') gets the first alone
	vector<Value> constants;
	if (!filter) {
		ThrowInjectedNeedsEquality(key, false);
	}
	if (!filter->EqualityConstants(constants)) {
		ThrowInjectedNeedsEquality(key, filter->ConstrainsRange());
	}
	if (IsTimestampColumn(key.column_type)) {
		ThrowInjectedNeedsEquality(key, true);
	}
	// the constants as the column type's text, which is what the partition column reads back from the directory; a
	// NULL or a constant the type can not hold names no value
	vector<string> texts;
	for (auto &constant : constants) {
		if (constant.IsNull()) {
			continue;
		}
		auto typed = constant.DefaultTryCastAs(key.column_type, nullptr, true);
		if (!typed || typed->IsNull()) {
			continue;
		}
		auto text = typed->ToString();
		if (!text.empty()) {
			texts.push_back(std::move(text));
		}
	}
	if (texts.empty()) {
		// `k = ''`, `k IN (NULL)`: a predicate on the column that names no value Athena can spell
		ThrowInjectedNeedsEquality(key, false);
	}
	std::sort(texts.begin(), texts.end());
	texts.erase(std::unique(texts.begin(), texts.end()), texts.end());
	KeyValues result;
	result.count = texts.size();
	for (auto &text : texts) {
		if (!filter->Keep(text)) {
			continue;
		}
		if (result.values.size() >= max_kept) {
			result.exceeded = true;
			break;
		}
		result.values.push_back(ProjectedValue {text, text});
	}
	return result;
}

PartitionProjection::KeyValues PartitionProjection::ProjectedValues(ClientContext &context,
                                                                    const PartitionProjectionKey &key,
                                                                    optional_ptr<const KeyFilter> filter,
                                                                    idx_t max_kept) const {
	if (key.type == PartitionProjectionType::INJECTED) {
		return InjectedValues(key, filter, max_kept);
	}
	KeyValues result;
	vector<Value> constants;
	// the values the predicate names are checked against the range instead of enumerating it; only for a column type
	// that keeps distinct integers distinct (a FLOAT merges 16777216 and 16777217)
	auto exact = key.column_type.IsIntegral() || key.column_type.id() == LogicalTypeId::VARCHAR;
	if (key.type == PartitionProjectionType::INTEGER && exact && filter && filter->EqualityConstants(constants)) {
		vector<int64_t> matches;
		for (auto &constant : constants) {
			int64_t match;
			if (TryIntegerValue(key, constant, match)) {
				matches.push_back(match);
			}
		}
		std::sort(matches.begin(), matches.end());
		matches.erase(std::unique(matches.begin(), matches.end()), matches.end());
		for (auto match : matches) {
			auto text = FormatInteger(match, key.digits);
			if (!filter->Keep(text)) {
				continue;
			}
			if (result.values.size() >= max_kept) {
				result.exceeded = true;
				break;
			}
			result.values.push_back(ProjectedValue {text, text});
		}
		result.count = IntegerCount(key);
		return result;
	}
	// false once more than 'max_kept' values passed: the key's enumeration stops there
	auto add = [&](ProjectedValue value) {
		result.count++;
		if (filter && !filter->Keep(value.value)) {
			return true;
		}
		if (result.values.size() >= max_kept) {
			result.exceeded = true;
			return false;
		}
		result.values.push_back(std::move(value));
		return true;
	};
	switch (key.type) {
	case PartitionProjectionType::INJECTED:
		throw InternalException("an injected key's values are derived by InjectedValues");
	case PartitionProjectionType::ENUM:
		for (auto &value : key.enum_values) {
			if (!add(ProjectedValue {value, value})) {
				break;
			}
		}
		result.count = key.enum_values.size();
		break;
	case PartitionProjectionType::INTEGER: {
		// the candidates: the range, tightened by the predicate's comparisons of the key (an integer column only: a
		// text column compares text), on the interval grid
		auto lower = key.range_start;
		auto upper = key.range_end;
		if (filter && key.column_type.IsIntegral()) {
			filter->NarrowIntegerRange(lower, upper);
		}
		auto interval = hugeint_t(key.interval);
		auto first = (hugeint_t(lower) - hugeint_t(key.range_start) + interval - 1) / interval * interval;
		auto last = (hugeint_t(upper) - hugeint_t(key.range_start)) / interval * interval;
		if (lower > upper || first > last) {
			// no candidate
		} else if ((last - first) / interval + 1 > Hugeint::Convert<idx_t>(max_kept)) {
			// more candidates than the cap are never enumerated: Athena fails a scan that would generate such a column
			result.exceeded = true;
		} else {
			auto current = Hugeint::Cast<int64_t>(hugeint_t(key.range_start) + first);
			auto end = Hugeint::Cast<int64_t>(hugeint_t(key.range_start) + last);
			while (true) {
				auto text = FormatInteger(current, key.digits);
				if (!add(ProjectedValue {text, text}) || current == end) {
					break;
				}
				context.InterruptCheck();
				current += key.interval;
			}
		}
		result.count = IntegerCount(key);
		break;
	}
	case PartitionProjectionType::DATE: {
		auto start = ResolveBound(context, key, key.date_start);
		auto end = ResolveBound(context, key, key.date_end);
		if (start > end) {
			throw InvalidInputException("Glue table '%s': projection.%s.range has its lower bound (%s) after its upper "
			                            "bound (%s)",
			                            table_description, key.name, key.format->Format(start),
			                            key.format->Format(end));
		}
		auto unit = key.format->FinestUnit();
		if (key.step.months == 0 && IntervalMicros(key.step, false) < IntervalMicros(unit, true)) {
			// a step finer than the format's finest field lands in every directory but possibly the last: visit the
			// directories and ask where the first step at or after each one falls, instead of walking the steps
			auto step = hugeint_t(IntervalMicros(key.step, true));
			auto current = start;
			while (current <= end) {
				auto steps = (hugeint_t(current.value) - hugeint_t(start.value) + step - hugeint_t(1)) / step;
				if (hugeint_t(start.value) + steps * step > hugeint_t(end.value)) {
					break;
				}
				auto directory = key.format->Format(current);
				if (!add(ProjectedValue {DateValue(key, ReadBack(key, directory)), directory})) {
					break;
				}
				context.InterruptCheck();
				timestamp_t next;
				if (!Interval::TryAdd(current, unit, next) || next <= current) {
					break;
				}
				current = next;
			}
			break;
		}
		auto current = start;
		string last_directory;
		while (current <= end) {
			auto directory = key.format->Format(current);
			// a finer step than the format spells a partition several times: keep one, valued as the directory reads
			if (last_directory != directory) {
				last_directory = directory;
				if (!add(ProjectedValue {DateValue(key, ReadBack(key, directory)), directory})) {
					break;
				}
			}
			context.InterruptCheck();
			timestamp_t next;
			if (!Interval::TryAdd(current, key.step, next) || next <= current) {
				break;
			}
			current = next;
		}
		break;
	}
	}
	return result;
}

//! A DATE or TIMESTAMP column reads Hive's canonical text, as in Trino; any other column reads the formatted text
string PartitionProjection::DateValue(const PartitionProjectionKey &key, timestamp_t value) const {
	switch (key.column_type.id()) {
	case LogicalTypeId::DATE:
		return Date::ToString(Timestamp::GetDate(value));
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return Timestamp::ToString(value);
	case LogicalTypeId::TIMESTAMP_TZ:
		// with an offset, so the cast does not read the UTC text in the session time zone
		return Timestamp::ToString(value) + "+00";
	default:
		return key.format->Format(value);
	}
}

//! NOW is the transaction start in UTC; formatting then parsing truncates the bound to the format's precision
timestamp_t PartitionProjection::ResolveBound(ClientContext &context, const PartitionProjectionKey &key,
                                              const DateBound &bound) const {
	auto instant = bound.fixed;
	if (bound.relative) {
		auto now = MetaTransaction::Get(context).start_timestamp;
		if (!Interval::TryAdd(now, bound.offset, instant)) {
			throw InvalidInputException("Glue table '%s': projection.%s.range: NOW offset out of range",
			                            table_description, key.name);
		}
	}
	return ReadBack(key, key.format->Format(instant));
}

timestamp_t PartitionProjection::ReadBack(const PartitionProjectionKey &key, const string &text) const {
	timestamp_t value;
	if (!key.format->TryParse(text, value)) {
		throw InvalidInputException("Glue table '%s': projection.%s value '%s' can not be read back in the format of "
		                            "projection.%s.format",
		                            table_description, key.name, text, key.name);
	}
	return value;
}

string PartitionProjection::Location(const vector<vector<ProjectedValue>> &key_values,
                                     const vector<idx_t> &indexes) const {
	vector<string> directories;
	for (idx_t i = 0; i < keys.size(); i++) {
		directories.push_back(key_values[i][indexes[i]].directory);
	}
	return Location(directories);
}

string PartitionProjection::Location(const vector<string> &directories) const {
	if (template_parts.empty()) {
		auto location = root_location;
		StringUtil::RTrim(location, "/");
		for (idx_t i = 0; i < keys.size(); i++) {
			location += "/" + keys[i].name + "=" + directories[i];
		}
		return location;
	}
	string location;
	for (auto &part : template_parts) {
		location += part.key_index == DConstants::INVALID_INDEX ? part.literal : directories[part.key_index];
	}
	return location;
}

void PartitionProjection::ThrowTooManyPartitions(idx_t max_partitions) const {
	throw InvalidInputException(
	    "Scan of Glue table '%s' would derive more than %d partitions "
	    "(glue_projection_max_partitions); filter on the partition columns or raise the setting",
	    table_description, max_partitions);
}

//! a * b, saturating at the largest idx_t
static idx_t SaturatingMultiply(idx_t a, idx_t b) {
	idx_t result;
	return TryMultiplyOperator::Operation<idx_t, idx_t, idx_t>(a, b, result) ? result : NumericLimits<idx_t>::Maximum();
}

PartitionProjection::Derivation PartitionProjection::Derive(ClientContext &context,
                                                            const vector<optional_ptr<const KeyFilter>> &filters,
                                                            idx_t max_partitions) const {
	D_ASSERT(filters.size() == keys.size());
	Derivation result;
	result.unfiltered = 1;
	idx_t derived = 1;
	bool exceeded = false;
	bool empty = false;
	vector<vector<ProjectedValue>> key_values;
	for (idx_t i = 0; i < keys.size(); i++) {
		auto values = ProjectedValues(context, keys[i], filters[i], max_partitions);
		// an injected key's predicate is what produced its values: it decided the files, as a narrowing one did
		result.narrowed.push_back(keys[i].type == PartitionProjectionType::INJECTED ||
		                          values.values.size() < values.count);
		result.unfiltered = SaturatingMultiply(result.unfiltered, values.count);
		derived = SaturatingMultiply(derived, values.values.size());
		exceeded = exceeded || values.exceeded;
		empty = empty || (values.values.empty() && !values.exceeded);
		key_values.push_back(std::move(values.values));
	}
	// a key no value of which passed empties the product whatever the other keys hold; only then is a key past the
	// cap no error (a cap of 0 stops a key before its first value, which is not an empty key)
	if (empty) {
		return result;
	}
	if (exceeded || derived > max_partitions) {
		ThrowTooManyPartitions(max_partitions);
	}
	// an odometer over the keys' values: the first key is the outermost
	vector<idx_t> indexes(keys.size(), 0);
	while (true) {
		GluePartitionInfo partition;
		for (idx_t i = 0; i < keys.size(); i++) {
			partition.values.push_back(key_values[i][indexes[i]].value);
		}
		partition.location = Location(key_values, indexes);
		result.partitions.push_back(std::move(partition));
		context.InterruptCheck();
		idx_t position = keys.size();
		while (position > 0) {
			position--;
			if (++indexes[position] < key_values[position].size()) {
				break;
			}
			indexes[position] = 0;
			if (position == 0) {
				return result;
			}
		}
	}
}

//! A component the copy drops or refuses: an empty one after the first, '.', '..', or a trailing '/' when it counts
static bool HasUnwritableSegment(const string &path, bool trailing_slash_counts) {
	if (path.empty() || (trailing_slash_counts && path.back() == '/')) {
		return true;
	}
	auto components = StringUtil::Split(path, '/');
	for (idx_t i = 0; i < components.size(); i++) {
		if ((components[i].empty() && i > 0) || components[i] == "." || components[i] == "..") {
			return true;
		}
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Writing
//===--------------------------------------------------------------------===//
PartitionProjectionWriter::PartitionProjectionWriter(unique_ptr<PartitionProjection> projection_p)
    : projection(std::move(projection_p)) {
	auto &parts = projection->template_parts;
	if (parts.empty()) {
		copy_target = projection->root_location;
		StringUtil::RTrim(copy_target, "/");
		return;
	}
	// the literal up to its last '/': a directory below the bucket, which a placeholder bucket does not give
	auto &literal = parts[0].literal;
	auto slash = parts[0].key_index == DConstants::INVALID_INDEX ? literal.rfind('/') : string::npos;
	auto scheme = literal.find("://");
	if (slash == string::npos || slash == 0 || (scheme != string::npos && slash < scheme + 3)) {
		throw InvalidInputException("Glue table '%s': storage.location.template has no directory before its first "
		                            "placeholder to write under",
		                            projection->table_description);
	}
	// the copy trims every trailing '/' of its target, so a prefix ending in "//" would write above the location
	if (literal[slash - 1] == '/') {
		throw InvalidInputException("Glue table '%s': storage.location.template has an empty segment before its first "
		                            "placeholder, which can not be written",
		                            projection->table_description);
	}
	copy_target = literal.substr(0, slash + 1);
}

string PartitionProjectionWriter::RelativePath(ClientContext &context, const vector<Value> &values) const {
	auto &keys = projection->keys;
	D_ASSERT(values.size() == keys.size());
	vector<string> directories;
	for (idx_t i = 0; i < keys.size(); i++) {
		directories.push_back(Directory(context, keys[i], values[i]));
	}
	// the copy drops an empty or '.' component and refuses '..': the file would land in another value's directory
	for (idx_t i = 0; i < keys.size(); i++) {
		if (HasUnwritableSegment(directories[i], true)) {
			throw InvalidInputException("Glue table '%s': partition value %s of '%s' spells a directory with an empty, "
			                            "'.' or '..' segment, which can not be written",
			                            projection->table_description, values[i].ToSQLString(), keys[i].name);
		}
	}
	auto location = projection->Location(directories);
	D_ASSERT(StringUtil::StartsWith(location, copy_target));
	auto relative = location.substr(copy_target.size());
	// the Hive layout's Location adds the '/' before the first <key>=; in a template a leading '/' is a value's own
	if (projection->template_parts.empty()) {
		relative.erase(0, 1);
	}
	// a template literal's segments; a value's leading '/' after the literal's '/' is an absolute path the copy refuses
	if (StringUtil::StartsWith(relative, "/") || HasUnwritableSegment(relative, false)) {
		throw InvalidInputException("Glue table '%s': the partition directory '%s' has an empty, '.' or '..' segment, "
		                            "which can not be written",
		                            projection->table_description, location);
	}
	return relative;
}

void PartitionProjectionWriter::Refuse(const PartitionProjectionKey &key, const Value &value,
                                       const char *parameter) const {
	auto name = ProjectionParameter(key.name, parameter);
	auto setting = projection->source.parameters.find(name);
	throw InvalidInputException(
	    "Glue table '%s': partition value %s of '%s' is not one its projection derives (%s = %s)",
	    projection->table_description, value.IsNull() ? string("NULL") : value.ToSQLString(), key.name, name,
	    setting == projection->source.parameters.end() ? string() : setting->second);
}

string PartitionProjectionWriter::Directory(ClientContext &context, const PartitionProjectionKey &key,
                                            const Value &value) const {
	// no key derives a NULL: the type names what it does derive
	if (value.IsNull()) {
		Refuse(key, value, "type");
	}
	switch (key.type) {
	case PartitionProjectionType::ENUM: {
		// the listed text whose value, read as the column type, is the row's; a VARCHAR column compares the text
		for (auto &text : key.enum_values) {
			if (key.column_type.id() == LogicalTypeId::VARCHAR) {
				if (StringValue::Get(value) == text) {
					return text;
				}
				continue;
			}
			auto typed = Value(text).DefaultTryCastAs(key.column_type, nullptr, true);
			if (typed && !typed->IsNull() && Value::NotDistinctFrom(*typed, value)) {
				return text;
			}
		}
		Refuse(key, value, "values");
	}
	case PartitionProjectionType::INTEGER: {
		// read leniently as the scan reads a directory, then checked against the range and the interval grid
		auto integer = value.DefaultTryCastAs(LogicalType::BIGINT, nullptr, false);
		if (!integer || integer->IsNull()) {
			Refuse(key, value, "type");
		}
		auto number = integer->GetValue<int64_t>();
		if (number < key.range_start || number > key.range_end) {
			Refuse(key, value, "range");
		}
		auto offset = static_cast<uint64_t>(number) - static_cast<uint64_t>(key.range_start);
		if (offset % static_cast<uint64_t>(key.interval) != 0) {
			Refuse(key, value, "interval");
		}
		auto directory = FormatInteger(number, key.digits);
		// the scan reads the directory as the column's value: a text column must hold the spelling, a numeric one the
		// integer itself (a FLOAT may not)
		if (key.column_type.id() == LogicalTypeId::VARCHAR) {
			if (StringValue::Get(value) != directory) {
				Refuse(key, value, key.digits > 0 ? "digits" : "type");
			}
		} else if (!key.column_type.IsIntegral()) {
			auto back = Value::BIGINT(number).DefaultTryCastAs(key.column_type, nullptr, true);
			if (!back || !Value::NotDistinctFrom(*back, value)) {
				Refuse(key, value, "type");
			}
		}
		return directory;
	}
	case PartitionProjectionType::DATE:
		return DateDirectory(context, key, value);
	case PartitionProjectionType::INJECTED: {
		// what the scan reads from the query: an empty text names no partition there, and a TIMESTAMP column is never
		// read (InjectedValues refuses it, as Athena does)
		if (IsTimestampColumn(key.column_type) || value.ToString().empty()) {
			Refuse(key, value, "type");
		}
		return value.ToString();
	}
	}
	throw InternalException("Unknown partition projection type");
}

string PartitionProjectionWriter::DateDirectory(ClientContext &context, const PartitionProjectionKey &key,
                                                const Value &value) const {
	timestamp_t instant;
	switch (key.column_type.id()) {
	case LogicalTypeId::DATE:
		// an infinite date, or one beyond the timestamp range, is outside every range
		if (!Timestamp::TryFromDatetime(value.GetValue<date_t>(), dtime_t(0), instant)) {
			Refuse(key, value, "range");
		}
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		instant = value.GetValue<timestamp_t>();
		if (!Value::IsFinite(instant)) {
			Refuse(key, value, "range");
		}
		break;
	default:
		// a text (or other) column holds the directory's own spelling
		if (!key.format->TryParse(value.ToString(), instant)) {
			Refuse(key, value, "format");
		}
		break;
	}
	auto directory = key.format->Format(instant);
	// the scan values the partition as its directory reads: a value finer than the format, or a text spelled another
	// way, is not what it would read back
	if (projection->ReadBack(key, directory) != instant ||
	    (!key.column_type.IsTemporal() && value.ToString() != directory)) {
		Refuse(key, value, "format");
	}
	// NOW is the start of this transaction, as on read
	auto start = projection->ResolveBound(context, key, key.date_start);
	auto end = projection->ResolveBound(context, key, key.date_end);
	if (instant < start || instant > end) {
		Refuse(key, value, "range");
	}
	// the first step at or after the value must land in its directory (a step finer than the format fills every one)
	timestamp_t at = start;
	if (key.step.months != 0) {
		while (at < instant) {
			timestamp_t next;
			if (!Interval::TryAdd(at, key.step, next) || next <= at) {
				Refuse(key, value, "interval");
			}
			at = next;
		}
	} else {
		auto step = hugeint_t(IntervalMicros(key.step, true));
		auto steps = (hugeint_t(instant.value) - hugeint_t(start.value) + step - hugeint_t(1)) / step;
		auto landing = hugeint_t(start.value) + steps * step;
		if (landing > hugeint_t(NumericLimits<int64_t>::Maximum())) {
			Refuse(key, value, "interval");
		}
		at = timestamp_t(Hugeint::Cast<int64_t>(landing));
	}
	if (at > end || key.format->Format(at) != directory) {
		Refuse(key, value, "interval");
	}
	return directory;
}

} // namespace duckdb
