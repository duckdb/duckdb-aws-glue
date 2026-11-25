#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

#include <functional>

namespace duckdb {
class ClientContext;

//! An integer from 'min' to 'max', false when 'value' is not one
bool TryParseInteger(const string &value, int64_t min, int64_t max, int64_t &result);
//! The first character of a string, false when it is empty or not valid UTF-8
bool TryFirstCharacter(const string &value, string &result);
//! Run 'task' for every index below 'count' at the same time, as tasks on DuckDB's threads (e.g. one S3 listing
//! each); the first error stops those not yet started and is thrown
void RunInParallel(ClientContext &context, idx_t count, const std::function<void(idx_t)> &task);

} // namespace duckdb
