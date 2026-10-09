#include "functions/glue_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/transaction.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "core/glue_types.hpp"

namespace duckdb {

namespace {

//! A resolved Glue table target (shared with glue_partition_functions.cpp via
//! same struct layout)
struct GlueColumnTarget {
	GlueCatalog *catalog = nullptr;
	GlueTableInfo table;

	string TableName() const {
		return table.database_name + "." + table.name;
	}
};

//! One-shot global state used by glue_replace_columns and glue_change_column
struct GlueColumnChangeState : public GlobalTableFunctionState {
	bool done = false;
};

unique_ptr<GlobalTableFunctionState> GlueColumnChangeInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlueColumnChangeState>();
}

GlueColumnTarget ResolveGlueColumn(ClientContext &context, const string &function_name, const Value &table_name,
                                   bool require_partitions = false) {
	auto name = ResolveGlueTableName(context, function_name, table_name.GetValue<string>());
	GlueColumnTarget result;
	result.catalog = &Catalog::GetCatalog(context, name.Catalog()).Cast<GlueCatalog>();
	if (!GlueAPI::GetTable(context, *result.catalog, name.Schema().GetIdentifierName(), name.Name().GetIdentifierName(),
	                       result.table)) {
		throw CatalogException("Table '%s.%s' does not exist in Glue catalog '%s'", name.Schema().GetIdentifierName(),
		                       name.Name().GetIdentifierName(), name.Catalog().GetIdentifierName());
	}
	if (result.table.IsView()) {
		throw BinderException("%s: '%s' is a Glue view, not a table", function_name, result.TableName());
	}
	if (result.table.GetFormat() != GlueTableFormat::HIVE) {
		throw NotImplementedException("%s only works on Hive tables, '%s' is a %s table", function_name,
		                              result.TableName(), result.table.GetFormatName());
	}
	return result;
}

//===--------------------------------------------------------------------===//
// glue_replace_columns: replace the data columns of a Hive table
//===--------------------------------------------------------------------===//
struct GlueReplaceColumnsBindData : public TableFunctionData {
	GlueColumnTarget target;
	vector<GlueColumn> columns;

	//! The checks in bind are against the table as it is then: a prepared
	//! statement binds again on every execution
	bool SupportStatementCache() const override {
		return false;
	}
};

//! A struct value whose fields are all strings (or NULL), e.g. {id: 'BIGINT'}
static bool IsStringStruct(const Value &value) {
	if (value.IsNull() || value.type().id() != LogicalTypeId::STRUCT) {
		return false;
	}
	for (auto &child : StructType::GetChildTypes(value.type())) {
		if (child.second.id() != LogicalTypeId::VARCHAR && child.second.id() != LogicalTypeId::SQLNULL) {
			return false;
		}
	}
	return true;
}

unique_ptr<FunctionData> GlueReplaceColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlueReplaceColumnsBindData>();
	result->target = ResolveGlueColumn(context, "glue_replace_columns", input.inputs[0], false);
	GlueCatalog::ThrowIfInExplicitTransaction(context);
	auto &table = result->target.table;
	auto &columns = input.inputs[1];
	if (!IsStringStruct(columns) || StructType::GetChildCount(columns.type()) == 0) {
		throw BinderException("glue_replace_columns: the columns must be a struct "
		                      "of column name to type, e.g. "
		                      "{id: 'BIGINT', name: 'VARCHAR'}");
	}
	optional_ptr<const Value> comments;
	// as in Hive, the comments of the columns that stay are dropped unless
	// keep_comments is set
	bool keep_comments = false;
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "comments") {
			comments = option.second;
		} else if (name == "keep_comments") {
			keep_comments = !option.second.IsNull() && option.second.GetValue<bool>();
		}
	}
	if (comments && !IsStringStruct(*comments)) {
		throw BinderException("glue_replace_columns: 'comments' must be a struct "
		                      "of column name to comment");
	}
	case_insensitive_map_t<idx_t> current;
	for (idx_t i = 0; i < table.columns.size(); i++) {
		current[table.columns[i].name] = i;
	}
	auto &column_names = StructType::GetChildTypes(columns.type());
	auto &column_types = StructValue::GetChildren(columns);
	// csv files are read by position, the other formats by name; other SerDes are
	// refused
	bool by_position = table.GetFileFormat() == HiveFileFormat::CSV;
	if (by_position && column_names.size() != table.columns.size()) {
		throw BinderException("glue_replace_columns: table '%s' is stored as csv, "
		                      "which is read by position, so it "
		                      "must keep its %d columns",
		                      result->target.TableName(), table.columns.size());
	}
	case_insensitive_map_t<idx_t> given;
	for (idx_t i = 0; i < column_names.size(); i++) {
		GlueColumn column;
		column.name = column_names[i].first.GetIdentifierName();
		if (column_types[i].IsNull()) {
			throw BinderException("glue_replace_columns: no type given for column '%s'", column.name);
		}
		column.type =
		    GlueTypes::FromLogicalType(TransformStringToLogicalType(column_types[i].GetValue<string>(), context));
		// the type as it reads back from Glue; a type that can not be read back
		// fails here, before Glue is changed
		auto type = GlueTypes::ToLogicalType(column.type);
		// a column that stays must keep a type its existing files can be read with,
		// as for ALTER COLUMN TYPE
		optional_ptr<const GlueColumn> previous;
		if (by_position) {
			previous = &table.columns[i];
		} else {
			auto existing = current.find(column.name);
			if (existing != current.end()) {
				previous = &table.columns[existing->second];
			}
		}
		if (previous) {
			// a column matched by name keeps its stored name: the json reader matches
			// field names case-sensitively
			if (!by_position) {
				column.name = previous->name;
			}
			if (keep_comments) {
				column.comment = previous->comment;
			}
			auto from = GlueTypes::ToLogicalType(previous->type);
			if (!GlueSchemaEntry::IsAllowedHiveTypeChange(from, type)) {
				throw BinderException("glue_replace_columns: can not change column "
				                      "'%s' of table '%s' from %s to %s: "
				                      "existing files keep their types, only widening "
				                      "changes are supported",
				                      previous->name, result->target.TableName(), from.ToString(), type.ToString());
			}
		}
		for (auto &key : table.partition_keys) {
			if (StringUtil::CIEquals(key.name, column.name)) {
				throw BinderException("glue_replace_columns: '%s' is a partition key "
				                      "of table '%s', the columns are the "
				                      "data columns only",
				                      column.name, result->target.TableName());
			}
		}
		given[column.name] = result->columns.size();
		result->columns.push_back(std::move(column));
	}
	if (comments) {
		auto &comment_names = StructType::GetChildTypes(comments->type());
		auto &comment_values = StructValue::GetChildren(*comments);
		for (idx_t i = 0; i < comment_names.size(); i++) {
			auto &name = comment_names[i].first.GetIdentifierName();
			auto column = given.find(name);
			if (column == given.end()) {
				throw BinderException("glue_replace_columns: 'comments' names '%s', "
				                      "which is not one of the columns",
				                      name);
			}
			// NULL removes the comment
			result->columns[column->second].comment =
			    comment_values[i].IsNull() ? string() : comment_values[i].GetValue<string>();
		}
	}
	auto require_kept = [&](const string &column) {
		if (given.find(column) == given.end()) {
			throw BinderException("glue_replace_columns: table '%s' is %s, the columns must keep '%s'",
			                      result->target.TableName(), table.DescribeBucketing(), column);
		}
	};
	for (auto &column : table.bucket_columns) {
		require_kept(column);
	}
	for (auto &column : table.sort_columns) {
		require_kept(column.name);
	}
	names = {"columns"};
	return_types = {LogicalType::LIST(LogicalType::STRUCT(
	    {{"name", LogicalType::VARCHAR}, {"type", LogicalType::VARCHAR}, {"comment", LogicalType::VARCHAR}}))};
	return std::move(result);
}

//! The columns glue_replace_columns and glue_change_column return: the table's
//! data columns after the change
static Value ColumnsValue(const vector<GlueColumn> &columns, const LogicalType &type) {
	vector<Value> values;
	for (auto &column : columns) {
		values.push_back(
		    Value::STRUCT({{"name", Value(column.name)},
		                   {"type", Value(column.type)},
		                   {"comment", column.comment.empty() ? Value(LogicalType::VARCHAR) : Value(column.comment)}}));
	}
	return Value::LIST(ListType::GetChildType(type), std::move(values));
}

void GlueReplaceColumnsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueColumnChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GlueReplaceColumnsBindData>();
	auto &catalog = *bind_data.target.catalog;
	auto &table = bind_data.target.table;
	// keeps the refreshed entry alive while its columns are read below
	Transaction::Get(context, catalog);
	GlueAPI::UpdateTableColumns(context, catalog, table.database_name, table.name, bind_data.columns);
	auto schema = catalog.GetSchemas().GetEntry(context, table.database_name);
	if (!schema) {
		throw CatalogException("Table \"%s\" was altered but its Glue database "
		                       "could not be found afterwards",
		                       bind_data.target.TableName());
	}
	auto &updated = schema->Cast<GlueSchemaEntry>().RefreshTable(context, table.name);
	output.data[0].Append(ColumnsValue(updated.table_info.columns, output.data[0].GetType()));
	output.CheckCardinality(1);
}

//===--------------------------------------------------------------------===//
// glue_change_column: Hive's ALTER TABLE ... CHANGE COLUMN
//===--------------------------------------------------------------------===//
struct GlueChangeColumnBindData : public TableFunctionData {
	GlueColumnTarget target;
	GlueColumnChange change;
};

unique_ptr<FunctionData> GlueChangeColumnBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlueChangeColumnBindData>();
	result->target = ResolveGlueColumn(context, "glue_change_column", input.inputs[0], false);
	GlueCatalog::ThrowIfInExplicitTransaction(context);
	for (idx_t i = 1; i < 4; i++) {
		if (input.inputs[i].IsNull()) {
			throw BinderException("glue_change_column: the column names and the type must not be NULL");
		}
	}
	auto &change = result->change;
	change.name = input.inputs[1].GetValue<string>();
	change.new_name = input.inputs[2].GetValue<string>();
	// as ALTER COLUMN TYPE, the type is checked against the column's when the
	// change is made
	change.new_type = TransformStringToLogicalType(input.inputs[3].GetValue<string>(), context);
	bool first = false;
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "comment") {
			// NULL removes the comment, as for COMMENT ON COLUMN
			change.comment = option.second.IsNull() ? string() : option.second.GetValue<string>();
		} else if (option.second.IsNull()) {
			continue;
		} else if (name == "first") {
			first = option.second.GetValue<bool>();
		} else if (name == "after") {
			change.position = GlueColumnPosition::AFTER;
			change.after = option.second.GetValue<string>();
		}
	}
	if (first) {
		if (change.position == GlueColumnPosition::AFTER) {
			throw BinderException("glue_change_column: a column goes either first or "
			                      "after another column, not both");
		}
		change.position = GlueColumnPosition::FIRST;
	}
	names = {"columns"};
	return_types = {LogicalType::LIST(LogicalType::STRUCT(
	    {{"name", LogicalType::VARCHAR}, {"type", LogicalType::VARCHAR}, {"comment", LogicalType::VARCHAR}}))};
	return std::move(result);
}

void GlueChangeColumnScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueColumnChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GlueChangeColumnBindData>();
	auto &catalog = *bind_data.target.catalog;
	auto &table = bind_data.target.table;
	// keeps the refreshed entry alive while its columns are read below
	Transaction::Get(context, catalog);
	auto schema = catalog.GetSchemas().GetEntry(context, table.database_name);
	if (!schema) {
		throw CatalogException("Glue database \"%s\" of table \"%s\" could not be found", table.database_name,
		                       bind_data.target.TableName());
	}
	auto &updated = schema->Cast<GlueSchemaEntry>().ChangeColumn(context, table.name, bind_data.change);
	output.data[0].Append(ColumnsValue(updated.table_info.columns, output.data[0].GetType()));
	output.CheckCardinality(1);
}

} // namespace

TableFunction GetGlueReplaceColumnsFunction() {
	TableFunction function("glue_replace_columns", {LogicalType::VARCHAR, LogicalType::ANY}, GlueReplaceColumnsScan,
	                       GlueReplaceColumnsBind, GlueColumnChangeInit);
	function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("comments", LogicalType::ANY);
		options.Add("keep_comments", LogicalType::BOOLEAN);
	});
	return function;
}

TableFunction GetGlueChangeColumnFunction() {
	TableFunction function("glue_change_column",
	                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                       GlueChangeColumnScan, GlueChangeColumnBind, GlueColumnChangeInit);
	function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("comment", LogicalType::VARCHAR);
		options.Add("first", LogicalType::BOOLEAN);
		options.Add("after", LogicalType::VARCHAR);
	});
	return function;
}
} // namespace duckdb
