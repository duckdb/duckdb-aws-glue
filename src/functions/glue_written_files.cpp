#include "functions/glue_written_files.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

shared_ptr<GlueWrittenFilesState> GlueWrittenFilesState::Get(ClientContext &context) {
	return context.registered_state->GetOrCreate<GlueWrittenFilesState>("glue_written_files");
}

void GlueWrittenFilesState::BeginWrite() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	write_started = true;
}

void GlueWrittenFilesState::AddFiles(const vector<string> &written) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	pending_files.insert(pending_files.end(), written.begin(), written.end());
}

vector<string> GlueWrittenFilesState::GetFiles() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return files;
}

void GlueWrittenFilesState::QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (!write_started) {
		return;
	}
	write_started = false;
	if (!error || !error->HasError()) {
		files = std::move(pending_files);
	} else {
		files.clear();
	}
	pending_files.clear();
}

namespace {

struct GlueWrittenFilesGlobalState : public GlobalTableFunctionState {
	vector<string> files;
	idx_t offset = 0;
};

unique_ptr<FunctionData> GlueWrittenFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("file_path");
	return_types.push_back(LogicalType::VARCHAR);
	return make_uniq<TableFunctionData>();
}

unique_ptr<GlobalTableFunctionState> GlueWrittenFilesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<GlueWrittenFilesGlobalState>();
	// read when the query runs, so a prepared statement sees the latest write
	result->files = GlueWrittenFilesState::Get(context)->GetFiles();
	return std::move(result);
}

void GlueWrittenFilesScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueWrittenFilesGlobalState>();
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.files.size() - state.offset);
	auto writer = FlatVector::Writer<string_t>(output.data[0], count);
	for (idx_t i = 0; i < count; i++) {
		writer.WriteValue(string_t(state.files[state.offset + i]));
	}
	state.offset += count;
	output.SetCardinality(count);
}

} // namespace

TableFunction GetGlueWrittenFilesFunction() {
	return TableFunction("glue_written_files", {}, GlueWrittenFilesScan, GlueWrittenFilesBind, GlueWrittenFilesInit);
}

} // namespace duckdb
