#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class TableFilter;

//! Extracts the name from a `<name column> = 'x'` or `lower(<name column>) = 'x'` filter pushed into a catalog scan.
//! Glue stores database and table names lowercased, so a single lookup of 'x' finds every entry either shape can
//! accept; the caller re-checks the result against the filter. `caller` names the scan in error messages.
bool TryGetNameEquality(optional_ptr<const TableFilter> filter, const char *caller, string &name);

} // namespace duckdb
