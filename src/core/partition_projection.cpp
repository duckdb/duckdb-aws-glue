#include "core/partition_projection.hpp"
#include "core/helpers.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/date.hpp"
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
	// any white space is part of a value, as Athena documents; an empty element is skipped, as Trino does
	for (auto &value : StringUtil::Split(values, ',')) {
		if (!value.empty()) {
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

//! Athena's NOW[ +|- n unit ] bound; false when 'text' is not one
static bool TryParseRelativeBound(const string &text, DateBound &bound) {
	auto rest = text;
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
	if (!unit.empty() && !TryParseIntervalUnit(unit, count, key.step)) {
		throw InvalidInputException(
		    "Glue table '%s': %s is '%s', expected MILLENNIA, CENTURIES, DECADES, YEARS, MONTHS, "
		    "WEEKS, DAYS, HALF_DAYS, HOURS, MINUTES, SECONDS, MILLIS, MICROS or NANOS (a "
		    "multiple of 1000), and an interval that fits the unit",
		    description, unit_parameter, unit);
	}
	if (interval.empty() || unit.empty()) {
		// Athena reads the interval and its unit only together; otherwise one of the format's finest field
		if (key.format->HasTimeField()) {
			throw InvalidInputException("Glue table '%s': %s and %s are required together: projection.%s.format '%s' "
			                            "has time fields, so the step does not default to 1 day, 1 month or 1 year",
			                            description, interval_parameter, unit_parameter, key.name,
			                            table.GetParameter(format_parameter));
		}
		unit = key.format->HasDayField() ? "DAYS" : key.format->HasMonthField() ? "MONTHS" : "YEARS";
		TryParseIntervalUnit(unit, 1, key.step);
	}
}

unique_ptr<PartitionProjection> PartitionProjection::Parse(const GlueTableInfo &table,
                                                           const vector<LogicalType> &key_types) {
	D_ASSERT(key_types.size() == table.partition_keys.size());
	auto result = make_uniq<PartitionProjection>();
	result->table_description = table.schema_name + "." + table.name;
	result->root_location = table.location;
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
			throw NotImplementedException("Glue table '%s': partition column '%s' uses the injected projection type "
			                              "(%s), which is not supported yet: Athena takes its values from the "
			                              "query's WHERE clause instead of the table parameters",
			                              result->table_description, key.name, type_parameter);
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

vector<PartitionProjection::ProjectedValue>
PartitionProjection::ProjectedValues(ClientContext &context, const PartitionProjectionKey &key) const {
	vector<ProjectedValue> result;
	switch (key.type) {
	case PartitionProjectionType::ENUM:
		for (auto &value : key.enum_values) {
			result.push_back(ProjectedValue {value, value});
		}
		break;
	case PartitionProjectionType::INTEGER: {
		auto current = key.range_start;
		while (current <= key.range_end) {
			auto text = FormatInteger(current, key.digits);
			result.push_back(ProjectedValue {text, text});
			context.InterruptCheck();
			int64_t next;
			if (!TryAddOperator::Operation(current, key.interval, next)) {
				break;
			}
			current = next;
		}
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
		auto current = start;
		while (current <= end) {
			auto directory = key.format->Format(current);
			// a finer step than the format spells a partition several times: keep one, valued as the directory reads
			if (result.empty() || result.back().directory != directory) {
				result.push_back(ProjectedValue {DateValue(key, ReadBack(key, directory)), directory});
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
	if (template_parts.empty()) {
		auto location = root_location;
		StringUtil::RTrim(location, "/");
		for (idx_t i = 0; i < keys.size(); i++) {
			location += "/" + keys[i].name + "=" + key_values[i][indexes[i]].directory;
		}
		return location;
	}
	string location;
	for (auto &part : template_parts) {
		if (part.key_index == DConstants::INVALID_INDEX) {
			location += part.literal;
			continue;
		}
		location += key_values[part.key_index][indexes[part.key_index]].directory;
	}
	return location;
}

vector<GluePartitionInfo> PartitionProjection::Derive(ClientContext &context) const {
	vector<vector<ProjectedValue>> key_values;
	for (auto &key : keys) {
		key_values.push_back(ProjectedValues(context, key));
		if (key_values.back().empty()) {
			return {};
		}
	}
	vector<GluePartitionInfo> partitions;
	// an odometer over the keys' values: the first key is the outermost
	vector<idx_t> indexes(keys.size(), 0);
	while (true) {
		GluePartitionInfo partition;
		for (idx_t i = 0; i < keys.size(); i++) {
			partition.values.push_back(key_values[i][indexes[i]].value);
		}
		partition.location = Location(key_values, indexes);
		partitions.push_back(std::move(partition));
		context.InterruptCheck();
		idx_t position = keys.size();
		while (position > 0) {
			position--;
			if (++indexes[position] < key_values[position].size()) {
				break;
			}
			indexes[position] = 0;
			if (position == 0) {
				return partitions;
			}
		}
	}
}

} // namespace duckdb
