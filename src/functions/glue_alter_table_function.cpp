#include "functions/glue_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "core/glue_types.hpp"
#include "core/hive_location.hpp"

namespace duckdb {

namespace {

//! The location a partition gets when none is given: <table location>/<key>=<value>/...
string DefaultPartitionLocation(const GluePartitionTarget &target, const vector<string> &values) {
	auto location = target.table.location;
	StringUtil::RTrim(location, "/");
	if (location.empty()) {
		throw InvalidInputException("Table '%s' has no location in Glue, provide the partition location explicitly",
		                            target.TableName());
	}
	vector<string> keys;
	for (auto &key : target.table.partition_keys) {
		keys.push_back(key.name);
	}
	return Path::FromString(location).Join(HivePartitionDirectory(keys, values)).ToString();
}

//! The partition a change rewrites, throws a CatalogException if it does not exist
GluePartitionDetails FetchPartition(ClientContext &context, const GluePartitionTarget &target,
                                    const vector<string> &values) {
	GluePartitionDetails result;
	if (!GlueAPI::GetPartition(context, *target.catalog, target.table.schema_name, target.table.name, values, result)) {
		throw CatalogException("Partition [%s] does not exist in Glue table '%s'", StringUtil::Join(values, ", "),
		                       target.TableName());
	}
	return result;
}

//===--------------------------------------------------------------------===//
// glue_alter_table: the SQL partition DDL, several actions in one statement
//===--------------------------------------------------------------------===//
enum class GlueAlterAction : uint8_t { ADD, DROP, RENAME, SET_PARTITION_LOCATION, SET_TABLE_LOCATION };

struct GlueAlterStep {
	GlueAlterAction action;
	bool if_not_exists = false;
	bool if_exists = false;
	vector<string> values;
	vector<string> new_values;
	string location;
};

struct GlueAlterTableBindData : public TableFunctionData {
	GluePartitionTarget target;
	vector<GlueAlterStep> steps;
	//! glue_add_partition, glue_drop_partition, ...: report the value of their one action instead of a row per action
	bool single_action = false;

	//! The table is rewritten from the definition bind fetched: a prepared statement binds again on every execution
	bool SupportStatementCache() const override {
		return false;
	}
};

//! glue_alter_table reports one row per action and an ALTER TABLE may carry more actions than a DataChunk
//! holds, so the rows are collected once when the actions are applied and then paged out.
struct GlueAlterTableState : public GlobalTableFunctionState {
	struct EmittedRow {
		Value action;
		Value partition;
		Value location;
	};
	vector<EmittedRow> rows;
	//! per action, the value a single-action function reports
	vector<Value> results;
	idx_t offset = 0;
	//! the Glue calls happen on the first scan only; the later ones just hand out the remaining rows
	bool applied = false;
};

//! The adds and drops of one statement: consecutive adds go out as one BatchCreatePartition call, consecutive drops
//! as one BatchDeletePartition call. Glue refusing a partition that was checked before any action ran means another
//! client raced the statement, unless the statement itself created or deleted it in an earlier call.
class GluePartitionBatch {
public:
	GluePartitionBatch(ClientContext &context, const GluePartitionTarget &target, vector<Value> &results)
	    : context(context), target(target), results(results) {
	}

public:
	void QueueAdd(const GlueAlterStep &step) {
		FlushDrops();
		// a partition named twice is registered once, at the location of the first
		if (queued.insert(step.values).second) {
			GluePartitionInput partition;
			partition.values = step.values;
			partition.location = step.location;
			adds.push_back(std::move(partition));
		}
		if (!step.if_not_exists) {
			checked.insert(step.values);
		}
	}

	//! 'step_index' is the step whose result reports whether the partition was dropped
	void QueueDrop(const GlueAlterStep &step, idx_t step_index) {
		FlushAdds();
		// a partition named twice is deleted once
		if (queued.insert(step.values).second) {
			drops.push_back(step.values);
		}
		if (!step.if_exists) {
			checked.insert(step.values);
		}
		drop_steps.emplace_back(step_index, step.values);
	}

	void Flush() {
		FlushAdds();
		FlushDrops();
	}

private:
	void FlushAdds() {
		if (adds.empty()) {
			return;
		}
		set<vector<string>> refused;
		for (auto &partition : GlueAPI::BatchCreatePartitions(context, *target.catalog, target.table, adds)) {
			if (checked.find(partition.values) != checked.end() && created.find(partition.values) == created.end()) {
				throw CatalogException("Partition [%s] already exists in Glue table '%s': another client registered it "
				                       "after it was checked",
				                       StringUtil::Join(partition.values, ", "), target.TableName());
			}
			refused.insert(partition.values);
		}
		for (auto &partition : adds) {
			if (refused.find(partition.values) == refused.end()) {
				deleted.erase(partition.values);
				created.insert(partition.values);
			}
		}
		adds.clear();
		queued.clear();
		checked.clear();
	}

	void FlushDrops() {
		if (drops.empty()) {
			return;
		}
		auto missing = GlueAPI::BatchDeletePartitions(context, *target.catalog, target.table.schema_name,
		                                              target.table.name, drops);
		for (auto &values : missing) {
			if (checked.find(values) != checked.end() && deleted.find(values) == deleted.end()) {
				throw CatalogException("Partition [%s] does not exist in Glue table '%s': another client dropped it "
				                       "after it was checked",
				                       StringUtil::Join(values, ", "), target.TableName());
			}
		}
		for (auto &step : drop_steps) {
			results[step.first] = Value::BOOLEAN(missing.find(step.second) == missing.end());
		}
		for (auto &values : drops) {
			if (missing.find(values) == missing.end()) {
				created.erase(values);
				deleted.insert(values);
			}
		}
		drops.clear();
		drop_steps.clear();
		queued.clear();
		checked.clear();
	}

private:
	ClientContext &context;
	const GluePartitionTarget &target;
	vector<Value> &results;
	//! the partitions this statement created or deleted so far: Glue refusing them again is no race
	set<vector<string>> created;
	set<vector<string>> deleted;
	//! adds and drops are never queued at the same time, so 'queued' and 'checked' hold the partitions of either
	vector<GluePartitionInput> adds;
	vector<vector<string>> drops;
	vector<pair<idx_t, vector<string>>> drop_steps;
	set<vector<string>> queued;
	//! the queued partitions the statement checked: not there (adds) or there (drops) before any action ran
	set<vector<string>> checked;
};

unique_ptr<GlobalTableFunctionState> GlueAlterTableInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlueAlterTableState>();
}

//! A location on the file system of the table's location: S3 for an S3 table (or one without a location in Glue), else
//! the table's own file system. The table's own location may also move to S3.
string ParseLocation(const string &function_name, const GluePartitionTarget &target, const Value &location,
                     bool is_table_location = false) {
	if (location.IsNull()) {
		throw BinderException("%s: the location must not be NULL", function_name);
	}
	auto result = location.GetValue<string>();
	StringUtil::RTrim(result, "/");
	if (result.empty()) {
		throw BinderException("%s: the location must not be empty", function_name);
	}
	auto scheme = LocationScheme(result);
	if (scheme == "s3" || scheme == "s3a") {
		if (is_table_location || target.table.location.empty() || IsS3Scheme(LocationScheme(target.table.location))) {
			return result;
		}
	}
	if (target.table.location.empty() || IsS3Scheme(LocationScheme(target.table.location))) {
		throw BinderException("%s: the location must be an S3 location (s3://...), got '%s'", function_name, result);
	}
	auto table_path = Path::FromString(target.table.location);
	auto path = Path::FromString(result);
	bool same_file_system = table_path.IsLocal()
	                            ? path.IsLocal()
	                            : StringUtil::CIEquals(LocationScheme(result), LocationScheme(target.table.location));
	if (!same_file_system) {
		throw BinderException("%s: the location must be %son the file system of the table location '%s', got '%s'",
		                      function_name, is_table_location ? "an S3 location or " : "", target.table.location,
		                      result);
	}
	return result;
}

bool OptionalFlag(const Value &value) {
	return !value.IsNull() && value.GetValue<bool>();
}

//! A struct field by name (case-insensitive), NULL if the struct has no such field
Value GetStructField(const Value &value, const string &field_name) {
	auto &types = StructType::GetChildTypes(value.type());
	auto &children = StructValue::GetChildren(value);
	for (idx_t i = 0; i < types.size(); i++) {
		if (StringUtil::CIEquals(types[i].first.GetIdentifierName(), field_name)) {
			return children[i];
		}
	}
	return Value();
}

//! A partition given as a list of {key, value} structs (value NULL for the default partition)
vector<string> ParsePartitionPairsValue(const string &function_name, const GluePartitionTarget &target,
                                        const Value &pairs) {
	if (pairs.IsNull() || pairs.type().id() != LogicalTypeId::LIST) {
		throw BinderException("%s: a partition must be given as a list of {key, value} structs", function_name);
	}
	vector<pair<string, Value>> result;
	for (auto &entry : ListValue::GetChildren(pairs)) {
		if (entry.IsNull() || entry.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("%s: a partition must be given as a list of {key, value} structs", function_name);
		}
		auto key = GetStructField(entry, "key");
		auto value = GetStructField(entry, "value");
		if (key.IsNull()) {
			throw BinderException("%s: a partition entry needs a key", function_name);
		}
		// the SQL form gives the value as text, whatever its literal's type
		auto text = value.IsNull() ? Value(LogicalType::VARCHAR) : Value(value.DefaultCastAs(LogicalType::VARCHAR));
		result.emplace_back(key.GetValue<string>(), std::move(text));
	}
	return ParsePartitionPairs(function_name, target, result);
}

//! A partition added twice, with no drop, rename or new location of it in between, is registered where its first add
//! says: a second add at another location would be ignored
void CheckAddedLocations(const vector<GlueAlterStep> &steps) {
	map<vector<string>, string> added;
	for (auto &step : steps) {
		switch (step.action) {
		case GlueAlterAction::ADD: {
			auto found = added.find(step.values);
			if (found != added.end() && found->second != step.location) {
				throw BinderException("ALTER TABLE ADD PARTITION: partition [%s] is added twice with different "
				                      "locations, '%s' and '%s'",
				                      StringUtil::Join(step.values, ", "), found->second, step.location);
			}
			added.emplace(step.values, step.location);
			break;
		}
		case GlueAlterAction::DROP:
		case GlueAlterAction::SET_PARTITION_LOCATION:
			added.erase(step.values);
			break;
		case GlueAlterAction::RENAME:
			added.erase(step.values);
			added.erase(step.new_values);
			break;
		case GlueAlterAction::SET_TABLE_LOCATION:
			break;
		}
	}
}

unique_ptr<FunctionData> GlueAlterTableBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlueAlterTableBindData>();
	auto &actions = input.inputs[1];
	if (actions.IsNull() || actions.type().id() != LogicalTypeId::LIST) {
		throw BinderException("glue_alter_table expects a list of actions");
	}
	// table-level actions do not need a partitioned table
	bool needs_partitions = false;
	for (auto &action : ListValue::GetChildren(actions)) {
		auto name = StringUtil::Lower(GetStructField(action, "action").ToString());
		if (name != "set_table_location") {
			needs_partitions = true;
		}
	}
	result->target = ResolveGlueTable(context, "glue_alter_table", input.inputs[0], needs_partitions);
	GlueCatalog::ThrowIfInExplicitTransaction(context);
	for (auto &action : ListValue::GetChildren(actions)) {
		if (action.IsNull() || action.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("glue_alter_table: every action must be a struct");
		}
		GlueAlterStep step;
		auto name = StringUtil::Lower(GetStructField(action, "action").ToString());
		auto if_not_exists = GetStructField(action, "if_not_exists");
		auto if_exists = GetStructField(action, "if_exists");
		auto partition = GetStructField(action, "partition");
		auto new_partition = GetStructField(action, "new_partition");
		auto location = GetStructField(action, "location");
		step.if_not_exists = OptionalFlag(if_not_exists);
		step.if_exists = OptionalFlag(if_exists);
		if (name == "add") {
			step.action = GlueAlterAction::ADD;
			step.values = ParsePartitionPairsValue("ALTER TABLE ADD PARTITION", result->target, partition);
			if (location.IsNull()) {
				step.location = DefaultPartitionLocation(result->target, step.values);
			} else {
				step.location = ParseLocation("ALTER TABLE ADD PARTITION", result->target, location);
			}
		} else if (name == "drop") {
			step.action = GlueAlterAction::DROP;
			step.values = ParsePartitionPairsValue("ALTER TABLE DROP PARTITION", result->target, partition);
		} else if (name == "rename") {
			step.action = GlueAlterAction::RENAME;
			step.values = ParsePartitionPairsValue("ALTER TABLE RENAME PARTITION", result->target, partition);
			step.new_values = ParsePartitionPairsValue("ALTER TABLE RENAME PARTITION", result->target, new_partition);
		} else if (name == "set_partition_location") {
			step.action = GlueAlterAction::SET_PARTITION_LOCATION;
			step.values = ParsePartitionPairsValue("ALTER TABLE PARTITION SET LOCATION", result->target, partition);
			step.location = ParseLocation("ALTER TABLE PARTITION SET LOCATION", result->target, location);
		} else if (name == "set_table_location") {
			step.action = GlueAlterAction::SET_TABLE_LOCATION;
			step.location = ParseLocation("ALTER TABLE SET LOCATION", result->target, location, true);
		} else {
			throw BinderException("glue_alter_table: unknown action '%s'", name);
		}
		result->steps.push_back(std::move(step));
	}
	CheckAddedLocations(result->steps);
	names = {"action", "partition", "location"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(result);
}

string DescribeAction(GlueAlterAction action) {
	switch (action) {
	case GlueAlterAction::ADD:
		return "add partition";
	case GlueAlterAction::DROP:
		return "drop partition";
	case GlueAlterAction::RENAME:
		return "rename partition";
	case GlueAlterAction::SET_PARTITION_LOCATION:
		return "set partition location";
	case GlueAlterAction::SET_TABLE_LOCATION:
		return "set table location";
	}
	return "unknown";
}

//! Apply every action, once, and record the row each one reports. Kept separate from emitting them because
//! there can be more actions than a DataChunk holds, so the rows are handed out over several scan calls
//! while the Glue calls must happen exactly once.
void GlueAlterTableApply(ClientContext &context, TableFunctionInput &data, GlueAlterTableState &state) {
	auto &bind_data = data.bind_data->Cast<GlueAlterTableBindData>();
	auto &catalog = *bind_data.target.catalog;
	auto &table = bind_data.target.table;
	auto table_name = bind_data.target.TableName();
	state.results.resize(bind_data.steps.size());

	// Glue has no transactions: check everything that can fail against the catalog first, so that a statement
	// which fails half way registers or removes nothing. Which partitions exist is fetched together, each partition
	// once; the partitions a rename or a new location rewrites are fetched whole and kept from this check.
	set<vector<string>> lookups;
	for (auto &step : bind_data.steps) {
		switch (step.action) {
		case GlueAlterAction::ADD:
			if (!step.if_not_exists) {
				lookups.insert(step.values);
			}
			break;
		case GlueAlterAction::DROP:
			if (!step.if_exists) {
				lookups.insert(step.values);
			}
			break;
		case GlueAlterAction::RENAME:
			lookups.insert(step.new_values);
			break;
		case GlueAlterAction::SET_PARTITION_LOCATION:
		case GlueAlterAction::SET_TABLE_LOCATION:
			break;
		}
	}
	auto existing = GlueAPI::ExistingPartitions(context, catalog, table.schema_name, table.name,
	                                            vector<vector<string>>(lookups.begin(), lookups.end()));
	auto exists = [&](const vector<string> &values) {
		return existing.find(values) != existing.end();
	};
	vector<unique_ptr<GluePartitionDetails>> checked(bind_data.steps.size());
	for (idx_t i = 0; i < bind_data.steps.size(); i++) {
		auto &step = bind_data.steps[i];
		switch (step.action) {
		case GlueAlterAction::ADD:
			if (!step.if_not_exists && exists(step.values)) {
				throw CatalogException("Partition [%s] already exists in Glue table '%s'",
				                       StringUtil::Join(step.values, ", "), table_name);
			}
			break;
		case GlueAlterAction::DROP:
			if (!step.if_exists && !exists(step.values)) {
				throw CatalogException("Partition [%s] does not exist in Glue table '%s'",
				                       StringUtil::Join(step.values, ", "), table_name);
			}
			break;
		case GlueAlterAction::SET_PARTITION_LOCATION:
			if (!step.if_exists) {
				checked[i] = make_uniq<GluePartitionDetails>(FetchPartition(context, bind_data.target, step.values));
			}
			break;
		case GlueAlterAction::RENAME:
			checked[i] = make_uniq<GluePartitionDetails>(FetchPartition(context, bind_data.target, step.values));
			if (exists(step.new_values)) {
				throw CatalogException("Partition [%s] already exists in Glue table '%s'",
				                       StringUtil::Join(step.new_values, ", "), table_name);
			}
			break;
		case GlueAlterAction::SET_TABLE_LOCATION:
			break;
		}
	}

	bool table_updated = false;
	GlueTableInfo refetched_table;
	GluePartitionBatch batch(context, bind_data.target, state.results);
	for (idx_t i = 0; i < bind_data.steps.size(); i++) {
		auto &step = bind_data.steps[i];
		switch (step.action) {
		case GlueAlterAction::ADD:
			batch.QueueAdd(step);
			break;
		case GlueAlterAction::DROP:
			batch.QueueDrop(step, i);
			break;
		case GlueAlterAction::RENAME:
			batch.Flush();
			state.results[i] = Value(GlueAPI::RenamePartition(context, catalog, table.schema_name, table.name,
			                                                  *checked[i], step.new_values));
			break;
		case GlueAlterAction::SET_PARTITION_LOCATION:
			batch.Flush();
			// with if_exists no check ran, so the partition is fetched here
			if (!checked[i]) {
				checked[i] = make_uniq<GluePartitionDetails>(FetchPartition(context, bind_data.target, step.values));
			}
			GlueAPI::SetPartitionLocation(context, catalog, table.schema_name, table.name, *checked[i], step.location);
			break;
		case GlueAlterAction::SET_TABLE_LOCATION:
			batch.Flush();
			// UpdateTable is refused with the VersionId of the definition an earlier step already replaced
			if (table_updated && !GlueAPI::GetTable(context, catalog, table.schema_name, table.name, refetched_table)) {
				throw CatalogException("Glue table '%s' was dropped while it was altered", table_name);
			}
			GlueAPI::SetTableLocation(context, catalog, table_updated ? refetched_table : table, step.location);
			table_updated = true;
			break;
		}
		if (step.action != GlueAlterAction::RENAME && step.action != GlueAlterAction::DROP) {
			state.results[i] = Value(step.location);
		}
		GlueAlterTableState::EmittedRow row;
		row.action = Value(DescribeAction(step.action));
		row.partition = step.values.empty() ? Value(LogicalType::VARCHAR) : Value(StringUtil::Join(step.values, ", "));
		row.location = step.location.empty() ? Value(LogicalType::VARCHAR) : Value(step.location);
		state.rows.push_back(std::move(row));
	}
	batch.Flush();
}

void GlueAlterTableScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<GlueAlterTableBindData>();
	auto &state = data.global_state->Cast<GlueAlterTableState>();
	if (!state.applied) {
		state.applied = true;
		GlueAlterTableApply(context, data, state);
	}
	if (bind_data.single_action) {
		if (state.offset == 0) {
			state.offset++;
			output.data[0].Append(state.results[0]);
			output.CheckCardinality(1);
		}
		return;
	}
	// one row per action, so there can be more rows than a chunk holds: page them out
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset++];
		output.data[0].Append(row.action);
		output.data[1].Append(row.partition);
		output.data[2].Append(row.location);
		count++;
	}
}

//===--------------------------------------------------------------------===//
// glue_add_partition, glue_drop_partition, glue_rename_partition, glue_set_partition_location,
// glue_set_table_location: glue_alter_table with one action
//===--------------------------------------------------------------------===//
unique_ptr<GlueAlterTableBindData> BindSingleAction(ClientContext &context, const string &function_name,
                                                    const Value &table_name, GlueAlterAction action) {
	auto result = make_uniq<GlueAlterTableBindData>();
	result->target =
	    ResolveGlueTable(context, function_name, table_name, action != GlueAlterAction::SET_TABLE_LOCATION);
	GlueCatalog::ThrowIfInExplicitTransaction(context, function_name.c_str());
	result->single_action = true;
	GlueAlterStep step;
	step.action = action;
	result->steps.push_back(std::move(step));
	return result;
}

unique_ptr<FunctionData> GlueAddPartitionBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = BindSingleAction(context, "glue_add_partition", input.inputs[0], GlueAlterAction::ADD);
	auto &step = result->steps[0];
	step.values = ParsePartitionSpec("glue_add_partition", result->target, input.inputs[1]);
	for (auto &option : input.named_parameters) {
		auto &name = option.first.GetIdentifierName();
		if (StringUtil::CIEquals(name, "location") && !option.second.IsNull()) {
			step.location = ParseLocation("glue_add_partition", result->target, option.second);
		} else if (StringUtil::CIEquals(name, "if_not_exists")) {
			step.if_not_exists = OptionalFlag(option.second);
		}
	}
	if (step.location.empty()) {
		step.location = DefaultPartitionLocation(result->target, step.values);
	}
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

unique_ptr<FunctionData> GlueDropPartitionBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = BindSingleAction(context, "glue_drop_partition", input.inputs[0], GlueAlterAction::DROP);
	auto &step = result->steps[0];
	step.values = ParsePartitionSpec("glue_drop_partition", result->target, input.inputs[1]);
	for (auto &option : input.named_parameters) {
		if (StringUtil::CIEquals(option.first.GetIdentifierName(), "if_exists")) {
			step.if_exists = OptionalFlag(option.second);
		}
	}
	names = {"dropped"};
	return_types = {LogicalType::BOOLEAN};
	return std::move(result);
}

unique_ptr<FunctionData> GlueRenamePartitionBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = BindSingleAction(context, "glue_rename_partition", input.inputs[0], GlueAlterAction::RENAME);
	auto &step = result->steps[0];
	step.values = ParsePartitionSpec("glue_rename_partition", result->target, input.inputs[1]);
	step.new_values = ParsePartitionSpec("glue_rename_partition", result->target, input.inputs[2]);
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

unique_ptr<FunctionData> GlueSetPartitionLocationBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = BindSingleAction(context, "glue_set_partition_location", input.inputs[0],
	                               GlueAlterAction::SET_PARTITION_LOCATION);
	auto &step = result->steps[0];
	step.values = ParsePartitionSpec("glue_set_partition_location", result->target, input.inputs[1]);
	step.location = ParseLocation("glue_set_partition_location", result->target, input.inputs[2]);
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

unique_ptr<FunctionData> GlueSetTableLocationBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result =
	    BindSingleAction(context, "glue_set_table_location", input.inputs[0], GlueAlterAction::SET_TABLE_LOCATION);
	result->steps[0].location = ParseLocation("glue_set_table_location", result->target, input.inputs[1], true);
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

} // namespace

TableFunction GetGlueAddPartitionFunction() {
	TableFunction function("glue_add_partition", {LogicalType::VARCHAR, LogicalType::ANY}, GlueAlterTableScan,
	                       GlueAddPartitionBind, GlueAlterTableInit);
	function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("location", LogicalType::VARCHAR);
		options.Add("if_not_exists", LogicalType::BOOLEAN);
	});
	return function;
}

TableFunction GetGlueDropPartitionFunction() {
	TableFunction function("glue_drop_partition", {LogicalType::VARCHAR, LogicalType::ANY}, GlueAlterTableScan,
	                       GlueDropPartitionBind, GlueAlterTableInit);
	function.GetSignature().WithTypedKwargs(
	    "options", [](TypedKwargs &options) { options.Add("if_exists", LogicalType::BOOLEAN); });
	return function;
}

TableFunction GetGlueRenamePartitionFunction() {
	TableFunction function("glue_rename_partition", {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY},
	                       GlueAlterTableScan, GlueRenamePartitionBind, GlueAlterTableInit);
	return function;
}

TableFunction GetGlueSetPartitionLocationFunction() {
	TableFunction function("glue_set_partition_location",
	                       {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::VARCHAR}, GlueAlterTableScan,
	                       GlueSetPartitionLocationBind, GlueAlterTableInit);
	return function;
}

TableFunction GetGlueSetTableLocationFunction() {
	TableFunction function("glue_set_table_location", {LogicalType::VARCHAR, LogicalType::VARCHAR}, GlueAlterTableScan,
	                       GlueSetTableLocationBind, GlueAlterTableInit);
	return function;
}

TableFunction GetGlueAlterTableFunction() {
	TableFunction function("glue_alter_table", {LogicalType::VARCHAR, LogicalType::ANY}, GlueAlterTableScan,
	                       GlueAlterTableBind, GlueAlterTableInit);
	return function;
}

} // namespace duckdb
