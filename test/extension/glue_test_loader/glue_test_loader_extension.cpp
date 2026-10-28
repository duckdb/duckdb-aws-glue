#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb_static_extension.h"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"

#include "catalog/glue_table_loader.hpp"

namespace duckdb {

namespace {

struct StagedTable {
	string table_json;
	string view_sql;
};

//! Loads the tables staged with PRAGMA glue_test_loader_stage and records every request
class TestGovernedGlueTableLoader : public GovernedGlueTableLoader {
public:
	optional_ptr<CatalogEntry> Load(ClientContext &context, const GlueLoadGovernedTableRequest &request,
	                                const build_entry_t &build_entry) override {
		lock_guard<mutex> guard(lock);
		requests.push_back(request);
		auto staged = tables.find(request.table_name);
		if (staged == tables.end()) {
			return nullptr;
		}
		auto &entry = build_entry(staged->second.table_json);
		if (staged->second.view_sql.empty()) {
			return &entry;
		}
		CreateViewInfo info(entry.ParentSchema(), Identifier(request.table_name));
		auto parser = Parser::GetBuiltinParser();
		info.query = CreateViewInfo::ParseSelect(parser, staged->second.view_sql);
		views.push_back(make_uniq<ViewCatalogEntry>(entry.ParentCatalog(), entry.ParentSchema(), info));
		return views.back().get();
	}

public:
	mutex lock;
	case_insensitive_map_t<StagedTable> tables;
	vector<GlueLoadGovernedTableRequest> requests;
	vector<unique_ptr<ViewCatalogEntry>> views;
};

TestGovernedGlueTableLoader &GetTestLoader(ClientContext &context) {
	for (auto &callback : ExtensionCallback::Iterate(context)) {
		auto loader = dynamic_cast<TestGovernedGlueTableLoader *>(callback.get());
		if (loader) {
			return *loader;
		}
	}
	throw InternalException("glue_test_loader is not registered");
}

//! PRAGMA glue_test_loader_stage('<table>', '<Glue Table JSON>'[, view = '<SELECT>']): the loader builds the table
//! from the JSON and returns it, or a view of the SELECT instead if one is given
void StageTable(ClientContext &context, const FunctionParameters &parameters) {
	auto &loader = GetTestLoader(context);
	StagedTable table;
	table.table_json = parameters.values[1].GetValue<string>();
	auto view = parameters.named_parameters.find(Identifier("view"));
	if (view != parameters.named_parameters.end()) {
		table.view_sql = view->second.GetValue<string>();
	}
	lock_guard<mutex> guard(loader.lock);
	loader.tables[parameters.values[0].GetValue<string>()] = std::move(table);
}

//! PRAGMA glue_test_loader_register: registers a second loader
void RegisterSecondLoader(ClientContext &context, const FunctionParameters &parameters) {
	ExtensionCallback::Register(DBConfig::GetConfig(context), make_shared_ptr<TestGovernedGlueTableLoader>());
}

struct RequestsBindData : public TableFunctionData {
	vector<GlueLoadGovernedTableRequest> requests;
};

struct RequestsState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

unique_ptr<FunctionData> RequestsBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &loader = GetTestLoader(context);
	auto result = make_uniq<RequestsBindData>();
	{
		lock_guard<mutex> guard(loader.lock);
		result->requests = loader.requests;
	}
	for (auto name :
	     {"catalog_name", "catalog_id", "database_name", "table_name", "region", "key_id", "secret", "session_token"}) {
		names.emplace_back(name);
		return_types.push_back(LogicalType::VARCHAR);
	}
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> RequestsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RequestsState>();
}

//! FROM glue_test_loader_requests(): the requests the loader received, in order
void RequestsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &requests = data.bind_data->Cast<RequestsBindData>().requests;
	auto &state = data.global_state->Cast<RequestsState>();
	idx_t count = 0;
	for (; state.offset < requests.size() && count < STANDARD_VECTOR_SIZE; state.offset++, count++) {
		auto &request = requests[state.offset];
		idx_t column = 0;
		for (auto &field : {request.catalog_name, request.catalog_id, request.database_name, request.table_name,
		                    request.region, request.key_id, request.secret, request.session_token}) {
			output.data[column++].Append(Value(field));
		}
	}
	output.CheckCardinality(count);
}

void LoadInternal(ExtensionLoader &loader) {
	ExtensionCallback::Register(DBConfig::GetConfig(loader.GetDatabaseInstance()),
	                            make_shared_ptr<TestGovernedGlueTableLoader>());
	auto stage = PragmaFunction::PragmaCall(Identifier("glue_test_loader_stage"), StageTable,
	                                        {LogicalType::VARCHAR, LogicalType::VARCHAR});
	stage.GetSignature().AddKeywordOnly(Identifier("view"), LogicalType::VARCHAR, Value(""));
	loader.RegisterFunction(stage);
	loader.RegisterFunction(
	    PragmaFunction::PragmaStatement(Identifier("glue_test_loader_register"), RegisterSecondLoader));
	loader.RegisterFunction(TableFunction("glue_test_loader_requests", {}, RequestsScan, RequestsBind, RequestsInit));
}

} // namespace

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(glue_test_loader, loader) {
	duckdb::LoadInternal(loader);
}

static int32_t glue_test_loader_describe(duckdb_extension_descriptor *descriptor) {
	descriptor->version = DUCKDB_EXTENSION_DESCRIPTOR_VERSION;
	descriptor->name = "glue_test_loader";
	descriptor->entry_cpp = reinterpret_cast<void (*)(void)>(glue_test_loader_duckdb_cpp_init);
	return 0;
}
}

// Linked into unittest only, which no generated loader registers it in: register before main
static const int32_t glue_test_loader_registered = duckdb_register_static_extension(glue_test_loader_describe);
