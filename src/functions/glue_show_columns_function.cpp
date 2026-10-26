#include "functions/glue_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"

namespace duckdb {

namespace {

struct GlueShowColumnsBindData : public TableFunctionData {
	vector<string> column_names;
};

struct GlueShowColumnsState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

//! The column names of the table: data columns first, partition keys last. Read from the Glue definition, so it
//! works for every table format.
vector<string> GetColumnNames(ClientContext &context, const Value &input) {
	if (input.IsNull()) {
		throw BinderException("glue_show_columns: the table name can not be NULL");
	}
	auto name = ResolveGlueTableName(context, "glue_show_columns", input.GetValue<string>());
	auto &catalog = Catalog::GetCatalog(context, name.Catalog()).Cast<GlueCatalog>();
	auto database_name = name.Schema().GetIdentifierName();
	auto table_name = name.Name().GetIdentifierName();
	auto load = [&]() -> shared_ptr<const GlueTableInfo> {
		GlueTableInfo info;
		if (!GlueAPI::GetTable(context, catalog, database_name, table_name, info)) {
			return nullptr;
		}
		return make_shared_ptr<const GlueTableInfo>(std::move(info));
	};
	// Of() is null in a bare statement (nothing started the catalog's transaction); Get() starts it
	auto table = GlueTransaction::Get(context, catalog).cache->GetTable(database_name, table_name, load);
	if (!table) {
		throw CatalogException("Table '%s.%s' does not exist in Glue catalog '%s'", database_name, table_name,
		                       name.Catalog().GetIdentifierName());
	}
	vector<string> result;
	for (auto &column : table->columns) {
		result.push_back(column.name);
	}
	for (auto &key : table->partition_keys) {
		result.push_back(key.name);
	}
	return result;
}

unique_ptr<FunctionData> GlueShowColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlueShowColumnsBindData>();
	result->column_names = GetColumnNames(context, input.inputs[0]);
	names = {"field"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GlueShowColumnsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlueShowColumnsState>();
}

void GlueShowColumnsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<GlueShowColumnsBindData>();
	auto &state = data.global_state->Cast<GlueShowColumnsState>();
	idx_t count = 0;
	while (state.offset < bind_data.column_names.size() && count < STANDARD_VECTOR_SIZE) {
		output.data[0].Append(Value(bind_data.column_names[state.offset++]));
		count++;
	}
}

} // namespace

TableFunction GetGlueShowColumnsFunction() {
	TableFunction function("glue_show_columns", {LogicalType::VARCHAR}, GlueShowColumnsScan, GlueShowColumnsBind,
	                       GlueShowColumnsInit);
	return function;
}

} // namespace duckdb
