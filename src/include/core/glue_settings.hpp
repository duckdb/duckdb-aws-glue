#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

//! The value of setting 'name' from a ClientContext or DatabaseInstance; false when it is unset or NULL
template <class SOURCE>
bool TryGetSetting(const SOURCE &source, const Identifier &name, Value &result) {
	return source.TryGetCurrentSetting(name, result) && !result.IsNull();
}

//! The value of setting 'name' as T, or 'default_value' when it is unset or NULL
template <class T, class SOURCE>
T GetSetting(const SOURCE &source, const Identifier &name, T default_value) {
	Value setting;
	if (TryGetSetting(source, name, setting)) {
		return setting.GetValue<T>();
	}
	return default_value;
}

} // namespace duckdb
