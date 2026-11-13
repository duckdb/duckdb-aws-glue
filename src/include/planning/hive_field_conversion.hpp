#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
class Expression;

//! The scalar function 'name' bound to 'children'
unique_ptr<Expression> BindFunction(ClientContext &context, const string &name,
                                    vector<unique_ptr<Expression>> children);

} // namespace duckdb
