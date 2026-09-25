#include "planning/glue_hive_insert.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_table.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Planning
//===--------------------------------------------------------------------===//
GlueHiveInsert::GlueHiveInsert(PhysicalPlan &physical_plan, LogicalOperator &op, GlueTable &table, bool discard)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, op.estimated_cardinality),
      table(table), discard(discard) {
}

static optional_ptr<CopyFunctionCatalogEntry> TryGetCopyFunction(DatabaseInstance &db, const string &name) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto entry = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, Identifier(name));
	if (!entry) {
		return nullptr;
	}
	return &entry->Cast<CopyFunctionCatalogEntry>();
}

//! The directory of a partition relative to the table location, empty if the partition is not below it
static string RelativePartitionPath(const string &table_location, const string &partition_location) {
	auto prefix = table_location + "/";
	if (!StringUtil::StartsWith(partition_location, prefix)) {
		return string();
	}
	// normalized like the copy does, so the directories of the written files match
	vector<string> components;
	for (auto &component : StringUtil::Split(partition_location.substr(prefix.size()), '/')) {
		if (!component.empty() && component != ".") {
			components.push_back(component);
		}
	}
	return StringUtil::Join(components, "/");
}

//! The PARTITION_PATH of the copy: the hive layout (<key>=<value>/...), except for the existing partitions registered
//! at other locations, which are written to their own location. 'partition_directories' receives the directories of
//! those partitions with their Glue values.
static unique_ptr<Expression> CreatePartitionPath(ClientContext &context, GlueTable &table,
                                                  const GlueTableInfo &table_info, const string &location,
                                                  const vector<Identifier> &names, const vector<LogicalType> &types,
                                                  const vector<idx_t> &partition_columns,
                                                  unordered_map<string, vector<string>> &partition_directories) {
	FunctionBinder function_binder(context);
	vector<unique_ptr<Expression>> components;
	for (idx_t i = 0; i < partition_columns.size(); i++) {
		auto column_index = partition_columns[i];
		vector<unique_ptr<Expression>> children;
		children.push_back(make_uniq<BoundConstantExpression>(Value(names[column_index].GetIdentifierName())));
		children.push_back(make_uniq<BoundReferenceExpression>(types[column_index], i));
		components.push_back(
		    function_binder.BindScalarFunction(HivePartitionComponentFun::GetFunction(), std::move(children)));
	}
	auto hive_path = function_binder.BindScalarFunction(PathJoinFun::GetFunction(), std::move(components));

	auto &glue_catalog = table.catalog.Cast<GlueCatalog>();
	auto partitions = GlueAPI::GetPartitions(context, glue_catalog, table_info.database_name, table_info.name);
	auto result = make_uniq<BoundCaseExpression>(LogicalType::VARCHAR);
	for (auto &partition : partitions) {
		if (partition.values.size() != partition_columns.size()) {
			throw InvalidInputException("Partition [%s] of Hive table '%s' has %d values, but the table has %d "
			                            "partition keys",
			                            StringUtil::Join(partition.values, ", "), table_info.name,
			                            partition.values.size(), partition_columns.size());
		}
		string hive_directory;
		vector<unique_ptr<Expression>> conditions;
		for (idx_t i = 0; i < partition_columns.size(); i++) {
			auto column_index = partition_columns[i];
			auto value = HivePartitioning::GetValue(context, table_info.partition_keys[i].name, partition.values[i],
			                                        types[column_index]);
			hive_directory += i > 0 ? "/" : "";
			hive_directory += HivePartitioning::Escape(names[column_index].GetIdentifierName()) + "=";
			hive_directory += value.IsNull() ? HivePartitioning::DEFAULT_PARTITION_NAME
			                                 : HivePartitioning::EscapeValue(value.ToString());
			conditions.push_back(BoundComparisonExpression::Create(
			    ExpressionType::COMPARE_NOT_DISTINCT_FROM, make_uniq<BoundReferenceExpression>(types[column_index], i),
			    make_uniq<BoundConstantExpression>(std::move(value))));
		}
		auto relative_path = RelativePartitionPath(location, partition.location);
		if (relative_path == hive_directory) {
			continue;
		}
		BoundCaseCheck check;
		if (conditions.size() == 1) {
			check.when_expr = std::move(conditions[0]);
		} else {
			auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
			conjunction->GetChildrenMutable() = std::move(conditions);
			check.when_expr = std::move(conjunction);
		}
		if (relative_path.empty()) {
			// the copy rejects a partition path that is not below its target
			check.then_expr = make_uniq<BoundConstantExpression>(Value(partition.location));
		} else {
			check.then_expr = make_uniq<BoundConstantExpression>(Value(relative_path));
			partition_directories.emplace(location + "/" + relative_path, partition.values);
		}
		result->CaseChecksMutable().push_back(std::move(check));
	}
	if (result->CaseChecks().empty()) {
		return hive_path;
	}
	result->ElseMutable() = std::move(hive_path);
	return std::move(result);
}

PhysicalOperator &GlueHiveInsert::PlanWrite(ClientContext &context, PhysicalPlanGenerator &planner, LogicalOperator &op,
                                            GlueTable &table, PhysicalOperator &plan, const vector<Identifier> &names,
                                            const vector<LogicalType> &types) {
	// Ask Glue for the current definition: the location (and the format) may have changed since the entry was
	// created, e.g. through ALTER TABLE ... SET LOCATION
	auto table_info = table.RefreshTableInfo(context);
	auto location = table_info.location;
	StringUtil::RTrim(location, "/");

	// The partition keys of the table, by position in the rows we are given (which may be in query order for
	// CREATE TABLE AS)
	vector<idx_t> partition_columns;
	for (auto &key : table_info.partition_keys) {
		bool found = false;
		for (idx_t i = 0; i < names.size(); i++) {
			if (StringUtil::CIEquals(names[i].GetIdentifierName(), key.name)) {
				partition_columns.push_back(i);
				found = true;
				break;
			}
		}
		if (!found) {
			throw InternalException("Partition key '%s' of Hive table '%s' not found in the rows to insert", key.name,
			                        table_info.name);
		}
	}

	// the files are written in the table's format (from its SerDe)
	auto file_format = table_info.GetFileFormat();
	auto format_name = HiveFileFormatToString(file_format);
	// the operator feeding the copy and the columns it produces
	optional_ptr<PhysicalOperator> source = &plan;
	vector<Identifier> copy_names = names;
	vector<LogicalType> copy_types = types;
	// the copy function and its options
	string copy_format = format_name;
	identifier_map_t<vector<Value>> copy_options;
	switch (file_format) {
	case HiveFileFormat::PARQUET:
		break;
	case HiveFileFormat::AVRO:
		ExtensionHelper::AutoLoadExtension(context, "avro");
		break;
	case HiveFileFormat::CSV:
		// Hive CSV files: the table's dialect, a header line only when the table says so
		copy_options[Identifier("header")] = {Value::BOOLEAN(table_info.HasHeader())};
		copy_options[Identifier("delimiter")] = {Value(table_info.GetFieldDelimiter())};
		copy_options[Identifier("quote")] = {Value(table_info.GetQuoteCharacter())};
		copy_options[Identifier("escape")] = {Value(table_info.GetEscapeCharacter())};
		break;
	case HiveFileFormat::JSON: {
		// DuckDB writes JSON the way COPY ... (FORMAT json) does: every row becomes one JSON object (to_json over a
		// struct of the data columns) and the objects are written line by line with the CSV writer. The partition
		// columns stay separate columns so the copy can partition on them.
		ExtensionHelper::AutoLoadExtension(context, "json");
		vector<unique_ptr<Expression>> struct_children;
		for (idx_t i = 0; i < names.size(); i++) {
			if (std::find(partition_columns.begin(), partition_columns.end(), i) != partition_columns.end()) {
				continue;
			}
			auto column_ref = make_uniq<BoundReferenceExpression>(types[i], i);
			column_ref->SetAlias(names[i]);
			struct_children.push_back(std::move(column_ref));
		}
		FunctionBinder function_binder(context);
		ErrorData error;
		auto struct_pack = function_binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier("struct_pack"),
		                                                      std::move(struct_children), error);
		if (!struct_pack) {
			error.Throw();
		}
		vector<unique_ptr<Expression>> to_json_children;
		to_json_children.push_back(std::move(struct_pack));
		auto to_json = function_binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier("to_json"),
		                                                  std::move(to_json_children), error);
		if (!to_json) {
			error.Throw();
		}
		vector<unique_ptr<Expression>> select_list;
		vector<LogicalType> projected_types;
		// to_json returns the JSON type (a VARCHAR alias): keep it as is, the executor checks the exact type
		auto json_type = to_json->GetReturnType();
		select_list.push_back(std::move(to_json));
		projected_types.push_back(json_type);
		copy_names = {Identifier("json")};
		copy_types = {json_type};
		for (idx_t i = 0; i < partition_columns.size(); i++) {
			auto column_index = partition_columns[i];
			select_list.push_back(make_uniq<BoundReferenceExpression>(types[column_index], column_index));
			projected_types.push_back(types[column_index]);
			copy_names.push_back(names[column_index]);
			copy_types.push_back(types[column_index]);
			// the partition columns follow the json column
			partition_columns[i] = 1 + i;
		}
		auto &projection = planner.Make<PhysicalProjection>(std::move(projected_types), std::move(select_list),
		                                                    op.estimated_cardinality);
		projection.children.push_back(plan);
		source = &projection;
		copy_format = "csv";
		copy_options[Identifier("quote")] = {Value("")};
		copy_options[Identifier("escape")] = {Value("")};
		copy_options[Identifier("delimiter")] = {Value("\n")};
		copy_options[Identifier("header")] = {Value::BOOLEAN(false)};
		break;
	}
	}
	auto copy_function = TryGetCopyFunction(*context.db, copy_format);
	if (!copy_function) {
		throw MissingExtensionException("Writing to Hive table '%s' requires the %s copy function", table_info.name,
		                                copy_format);
	}
	auto copy_info = make_uniq<CopyInfo>();
	copy_info->file_path = location;
	copy_info->format = copy_format;
	copy_info->is_from = false;
	copy_info->options = std::move(copy_options);

	// Hive convention: partition columns live in the directory names, not in the files
	CopyFunctionBindInput bind_input(*copy_info);
	auto names_to_write = LogicalCopyToFile::GetNamesWithoutPartitions(copy_names, partition_columns, false);
	auto types_to_write = LogicalCopyToFile::GetTypesWithoutPartitions(copy_types, partition_columns, false);
	auto function_data = copy_function->function.copy_to_bind(context, bind_input, names_to_write, types_to_write);

	auto &physical_copy = planner.Make<PhysicalCopyToFile>(
	    GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST), copy_function->function,
	    std::move(function_data), op.estimated_cardinality);
	auto &copy = physical_copy.Cast<PhysicalCopyToFile>();
	copy.use_tmp_file = false;
	// files of earlier inserts are kept, so every file needs a unique name
	copy.filename_pattern.SetFilenamePattern("{uuid}");
	unordered_map<string, vector<string>> partition_directories;
	if (!partition_columns.empty()) {
		copy.file_path = location;
		copy.partition_output = true;
		copy.partition_columns = partition_columns;
		copy.write_partition_columns = false;
		copy.hive_file_pattern = true;
		copy.partition_path_expression = CreatePartitionPath(context, table, table_info, location, copy_names,
		                                                     copy_types, partition_columns, partition_directories);
		// with partitioned output the copy must not initialize a single (partition-less) output file
		copy.write_empty_file = true;
		copy.per_thread_output = false;
	} else {
		copy.file_path = location;
		copy.partition_output = false;
		copy.write_empty_file = false;
		copy.per_thread_output = true;
	}
	copy.file_extension = format_name;
	copy.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	copy.return_type = CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST;
	copy.names = copy_names;
	copy.expected_types = copy_types;
	copy.children.push_back(*source);

	auto &insert = planner.Make<GlueHiveInsert>(op, table, false).Cast<GlueHiveInsert>();
	insert.partition_directories = std::move(partition_directories);
	insert.children.push_back(physical_copy);
	return insert;
}

PhysicalOperator &GlueHiveInsert::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                             GlueTable &table, optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into a Hive table");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for insertion into a Hive table");
	}
	D_ASSERT(plan);
	// bring the rows into the table's column order, filling in defaults (NULL) for unmentioned columns
	if (!op.column_index_map.empty()) {
		plan = &planner.ResolveDefaultsProjection(op, *plan);
	}
	vector<Identifier> names;
	vector<LogicalType> types;
	for (auto &column : table.GetColumns().Logical()) {
		names.push_back(column.Name());
		types.push_back(column.Type());
	}
	return PlanWrite(context, planner, op, table, *plan, names, types);
}

PhysicalOperator &GlueHiveInsert::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalCreateTable &op, PhysicalOperator &plan) {
	// Create the table in Glue first (Glue has no transactions, the table exists from here on even if the insert
	// fails), then write the query result into it
	auto &glue_catalog = op.schema.catalog.Cast<GlueCatalog>();
	auto transaction = glue_catalog.GetCatalogTransaction(context);
	auto entry = op.schema.CreateTable(transaction, *op.info);

	vector<Identifier> names;
	vector<LogicalType> types;
	for (auto &column : op.info->Base().columns.Logical()) {
		names.push_back(column.Name());
		types.push_back(column.Type());
	}
	if (!entry) {
		// CREATE TABLE IF NOT EXISTS on an existing table: nothing is created and nothing is inserted
		auto &base = op.info->Base();
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(base.GetTableName()));
		auto existing = op.schema.LookupEntry(transaction, lookup);
		auto &insert = planner.Make<GlueHiveInsert>(op, existing->Cast<GlueTable>(), true);
		insert.children.push_back(plan);
		return insert;
	}
	return PlanWrite(context, planner, op, entry->Cast<GlueTable>(), plan, names, types);
}

//===--------------------------------------------------------------------===//
// Execution
//===--------------------------------------------------------------------===//
struct GlueHiveInsertGlobalState : public GlobalSinkState {
	mutex lock;
	idx_t insert_count = 0;
	vector<string> written_files;
};

unique_ptr<GlobalSinkState> GlueHiveInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<GlueHiveInsertGlobalState>();
}

SinkResultType GlueHiveInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.global_state.Cast<GlueHiveInsertGlobalState>();
	lock_guard<mutex> guard(state.lock);
	if (discard) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	// the COPY reports (rows written, files written)
	for (idx_t r = 0; r < chunk.size(); r++) {
		state.insert_count += chunk.GetValue(0, r).GetValue<idx_t>();
		auto files = chunk.GetValue(1, r);
		if (files.IsNull()) {
			continue;
		}
		for (auto &file : ListValue::GetChildren(files)) {
			state.written_files.push_back(file.GetValue<string>());
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//! Case sensitive, like S3 paths
using FilePathToGluePartition = unordered_map<string, GluePartitionInput>;

SinkFinalizeType GlueHiveInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<GlueHiveInsertGlobalState>();
	auto &table_info = table.table_info;
	if (discard || table_info.partition_keys.empty() || state.written_files.empty()) {
		return SinkFinalizeType::READY;
	}

	// Register the partition directories the files were written to: the locations of existing partitions, or
	// <key>=<value> directories in partition key order below the table location
	FilePathToGluePartition partitions;
	for (auto &file : state.written_files) {
		auto directory = file.substr(0, file.find_last_of('/'));
		if (partitions.find(directory) != partitions.end()) {
			continue;
		}
		GluePartitionInput partition;
		partition.location = directory;
		auto existing = partition_directories.find(directory);
		if (existing != partition_directories.end()) {
			partition.values = existing->second;
			partitions.emplace(directory, std::move(partition));
			continue;
		}
		auto parsed = HivePartitioning::Parse(file);
		for (auto &key : table_info.partition_keys) {
			auto value = parsed.find(key.name);
			if (value == parsed.end()) {
				throw InternalException("Written file '%s' has no value for partition key '%s'", file, key.name);
			}
			partition.values.push_back(HivePartitioning::Unescape(value->second));
		}
		partitions.emplace(directory, std::move(partition));
	}
	vector<GluePartitionInput> to_register;
	for (auto &entry : partitions) {
		to_register.push_back(entry.second);
	}
	auto &glue_catalog = table.catalog.Cast<GlueCatalog>();
	GlueAPI::BatchCreatePartitions(context, glue_catalog, table_info.database_name, table_info.name, to_register);
	return SinkFinalizeType::READY;
}

SourceResultType GlueHiveInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &state = sink_state->Cast<GlueHiveInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(state.insert_count)));
	return SourceResultType::FINISHED;
}

string GlueHiveInsert::GetName() const {
	return "GLUE_HIVE_INSERT";
}

InsertionOrderPreservingMap<string> GlueHiveInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name.GetIdentifierName();
	return result;
}

} // namespace duckdb
