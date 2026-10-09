#include "core/helpers.hpp"

#include "duckdb/common/types/value.hpp"

#include "utf8proc_wrapper.hpp"

namespace duckdb {

bool TryParseInteger(const string &value, int64_t min, int64_t max, int64_t &result) {
	if (value.empty()) {
		return false;
	}
	auto cast = Value(value).DefaultTryCastAs(LogicalType::BIGINT);
	if (!cast) {
		return false;
	}
	result = cast->GetValue<int64_t>();
	return result >= min && result <= max;
}

bool TryFirstCharacter(const string &value, string &result) {
	if (value.empty() || !Utf8Proc::IsValid(value.c_str(), value.size())) {
		return false;
	}
	int length;
	Utf8Proc::UTF8ToCodepoint(value.c_str(), length, value.size());
	result = value.substr(0, NumericCast<idx_t>(length));
	return true;
}

} // namespace duckdb
