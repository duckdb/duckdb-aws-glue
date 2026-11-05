#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
class Expression;
class Identifier;
class ParsedExpression;
struct HiveCSVOptions;
struct LogicalType;

//! The scalar function 'name' bound to 'children'
unique_ptr<Expression> BindFunction(ClientContext &context, const string &name,
                                    vector<unique_ptr<Expression>> children);

//! Make the reader's cast of a text field to its column's type the conversion HiveCSVOptions::FieldConversion names
void ConvertTextField(ClientContext &context, const HiveCSVOptions &options, unique_ptr<Expression> &expr);

//! The same conversion over the reader's column 'name', aliased as the column (the column itself for NONE), for the
//! reader call
unique_ptr<ParsedExpression> TextFieldReaderExpression(const HiveCSVOptions &options, const Identifier &name,
                                                       const LogicalType &type);

} // namespace duckdb
