#include "glue_grammar.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/grammar_change.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/parser/peg/transformer/parse_result.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

namespace {

bool IsRule(const ParseResult &parse_result, const char *name) {
	return StringUtil::CIEquals(parse_result.name, name);
}

unique_ptr<ParsedExpression> Constant(const Value &value) {
	return ConstantExpression::FromValue(value);
}

unique_ptr<ParsedExpression> Call(const char *function_name, vector<FunctionArgument> arguments) {
	return make_uniq<FunctionExpression>(Identifier(function_name), std::move(arguments));
}

//! GluePartitionSpec <- 'PARTITION' Parens(List(GluePartitionValue))
//! GluePartitionValue <- ColumnName '=' Expression
//! -> list_value(struct_pack(key := 'dt', value := CAST(<expr> AS VARCHAR)), ...)
unique_ptr<ParsedExpression> TransformPartitionSpec(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &spec = parse_result.Cast<ListParseResult>();
	auto entries = PEGTransformerFactory::ExtractParseResultsFromList(
	    PEGTransformerFactory::ExtractResultFromParens(spec.GetChild(1)));
	vector<FunctionArgument> pairs;
	for (auto &entry : entries) {
		auto &key_value = entry.get().Cast<ListParseResult>();
		// ColumnName is matched as a bare identifier: read it, it has no transform of its own
		auto &key_result = key_value.GetChild(0);
		auto &key_identifier = key_result.type == ParseResultType::IDENTIFIER
		                           ? key_result.Cast<IdentifierParseResult>()
		                           : key_result.Cast<ListParseResult>().Child<IdentifierParseResult>(0);
		auto key = key_identifier.identifier;
		auto value = transformer.Transform<unique_ptr<ParsedExpression>>(key_value.GetChild(2));
		vector<FunctionArgument> fields;
		fields.emplace_back(Identifier("key"), Constant(Value(key.GetIdentifierName())));
		fields.emplace_back(Identifier("value"), make_uniq<CastExpression>(LogicalType::VARCHAR, std::move(value)));
		pairs.emplace_back(Call("struct_pack", std::move(fields)));
	}
	return Call("list_value", std::move(pairs));
}

//! GlueLocation <- 'LOCATION' StringLiteral
unique_ptr<ParsedExpression> TransformLocation(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &location = parse_result.Cast<ListParseResult>();
	return Constant(Value(transformer.Transform<string>(location.GetChild(1))));
}

unique_ptr<ParsedExpression> MakeAction(const char *action, bool if_not_exists, bool if_exists,
                                        unique_ptr<ParsedExpression> partition,
                                        unique_ptr<ParsedExpression> new_partition,
                                        unique_ptr<ParsedExpression> location) {
	vector<FunctionArgument> fields;
	fields.emplace_back(Identifier("action"), Constant(Value(action)));
	fields.emplace_back(Identifier("if_not_exists"), Constant(Value::BOOLEAN(if_not_exists)));
	fields.emplace_back(Identifier("if_exists"), Constant(Value::BOOLEAN(if_exists)));
	fields.emplace_back(Identifier("partition"), partition ? std::move(partition) : Constant(Value()));
	fields.emplace_back(Identifier("new_partition"), new_partition ? std::move(new_partition) : Constant(Value()));
	fields.emplace_back(Identifier("location"), location ? std::move(location) : Constant(Value()));
	return Call("struct_pack", std::move(fields));
}

void TransformAction(PEGTransformer &transformer, ParseResult &action_result, vector<FunctionArgument> &actions) {
	// GluePartitionAction <- GlueAddPartitions / GlueDropPartitions / GlueRenamePartition / ...
	auto &action = action_result.Cast<ListParseResult>().Child<ChoiceParseResult>(0).GetResult();
	auto &list = action.Cast<ListParseResult>();
	if (IsRule(action, "GlueAddPartitions")) {
		// 'ADD' IfNotExists? GluePartitionWithLocation+
		bool if_not_exists = list.Child<OptionalParseResult>(1).HasResult();
		for (auto &entry : list.Child<RepeatParseResult>(2).GetChildren()) {
			// GluePartitionWithLocation <- GluePartitionSpec GlueLocation?
			auto &with_location = entry.get().Cast<ListParseResult>();
			auto partition = TransformPartitionSpec(transformer, with_location.GetChild(0));
			unique_ptr<ParsedExpression> location;
			auto &optional_location = with_location.Child<OptionalParseResult>(1);
			if (optional_location.HasResult()) {
				location = TransformLocation(transformer, optional_location.GetResult());
			}
			actions.emplace_back(
			    MakeAction("add", if_not_exists, false, std::move(partition), nullptr, std::move(location)));
		}
		return;
	}
	if (IsRule(action, "GlueDropPartitions")) {
		// 'DROP' IfExists? List(GluePartitionSpec)
		bool if_exists = list.Child<OptionalParseResult>(1).HasResult();
		for (auto &entry : PEGTransformerFactory::ExtractParseResultsFromList(list.GetChild(2))) {
			auto partition = TransformPartitionSpec(transformer, entry.get());
			actions.emplace_back(MakeAction("drop", false, if_exists, std::move(partition), nullptr, nullptr));
		}
		return;
	}
	if (IsRule(action, "GlueRenamePartition")) {
		// GluePartitionSpec 'RENAME' 'TO' GluePartitionSpec
		auto partition = TransformPartitionSpec(transformer, list.GetChild(0));
		auto new_partition = TransformPartitionSpec(transformer, list.GetChild(3));
		actions.emplace_back(
		    MakeAction("rename", false, false, std::move(partition), std::move(new_partition), nullptr));
		return;
	}
	if (IsRule(action, "GluePartitionSetLocation")) {
		// GluePartitionSpec 'SET' GlueLocation
		auto partition = TransformPartitionSpec(transformer, list.GetChild(0));
		auto location = TransformLocation(transformer, list.GetChild(2));
		actions.emplace_back(
		    MakeAction("set_partition_location", false, false, std::move(partition), nullptr, std::move(location)));
		return;
	}
	if (IsRule(action, "GlueTableSetLocation")) {
		// 'SET' GlueLocation
		auto location = TransformLocation(transformer, list.GetChild(1));
		actions.emplace_back(MakeAction("set_table_location", false, false, nullptr, nullptr, std::move(location)));
		return;
	}
	throw InternalException("Unknown Glue partition action rule '%s'", action.name);
}

//! GlueAlterTableStatement <- 'ALTER' 'TABLE' BaseTableName GluePartitionAction+
//! -> CALL glue_alter_table('<table>', [actions])
unique_ptr<TransformResultValue> FinalizeGlueAlterTable(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto table = transformer.Transform<unique_ptr<BaseTableRef>>(list.GetChild(2));
	vector<FunctionArgument> actions;
	for (auto &action : list.Child<RepeatParseResult>(3).GetChildren()) {
		TransformAction(transformer, action.get(), actions);
	}
	vector<FunctionArgument> arguments;
	arguments.emplace_back(Constant(Value(table->GetQualifiedName().ToString())));
	arguments.emplace_back(Call("list_value", std::move(actions)));
	auto statement = make_uniq<CallStatement>();
	statement->function = Call("glue_alter_table", std::move(arguments));
	unique_ptr<SQLStatement> result = std::move(statement);
	return make_uniq<TypedTransformResult<unique_ptr<SQLStatement>>>(std::move(result));
}

unique_ptr<TransformProcess> StartGlueAlterTableTransform(PEGTransformer &transformer, ParseResult &parse_result) {
	return make_uniq<FinalizeTransformProcess>(transformer, parse_result, FinalizeGlueAlterTable);
}

//! GlueCreateSchemaStatement <- 'CREATE' 'SCHEMA' IfNotExists? QualifiedName WithList
//! -> CALL glue_create_database('<catalog>.<database>', if_not_exists := ..., options := struct_pack(...))
//! WithList is DuckDB's own option list, the one CREATE TABLE and CREATE INDEX already take, so this adds no new
//! syntax vocabulary. The option list is passed through untouched: which keys mean something and which become Glue
//! database parameters is decided in one place, by glue_create_database.
unique_ptr<TransformResultValue> FinalizeGlueCreateSchema(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	bool if_not_exists = list.Child<OptionalParseResult>(2).HasResult();
	auto qualified_name = transformer.Transform<QualifiedName>(list.GetChild(3));
	auto options = transformer.Transform<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(list.GetChild(4));

	vector<FunctionArgument> fields;
	for (auto &option : options) {
		fields.emplace_back(Identifier(option.first), std::move(option.second));
	}
	// A named argument of a *table* function is carried as the alias of the argument expression, not as the name on
	// FunctionArgument -- Binder::BindTableFunctionParameters reads GetAlias() (bind_table_function.cpp). The name on
	// FunctionArgument is what scalar functions such as struct_pack above use.
	auto if_not_exists_argument = Constant(Value::BOOLEAN(if_not_exists));
	if_not_exists_argument->SetAlias(Identifier("if_not_exists"));
	auto options_argument = Call("struct_pack", std::move(fields));
	options_argument->SetAlias(Identifier("options"));

	vector<FunctionArgument> arguments;
	arguments.emplace_back(Constant(Value(qualified_name.ToString())));
	arguments.emplace_back(std::move(if_not_exists_argument));
	arguments.emplace_back(std::move(options_argument));
	auto statement = make_uniq<CallStatement>();
	statement->function = Call("glue_create_database", std::move(arguments));
	unique_ptr<SQLStatement> result = std::move(statement);
	return make_uniq<TypedTransformResult<unique_ptr<SQLStatement>>>(std::move(result));
}

unique_ptr<TransformProcess> StartGlueCreateSchemaTransform(PEGTransformer &transformer, ParseResult &parse_result) {
	return make_uniq<FinalizeTransformProcess>(transformer, parse_result, FinalizeGlueCreateSchema);
}

class GlueHiveDDLGrammar final : public GrammarExtension {
public:
	GlueHiveDDLGrammar()
	    : GrammarExtension("glue_hive_ddl", "Hive DDL for Glue: ALTER TABLE ... ADD / DROP PARTITION, RENAME "
	                                        "PARTITION, SET LOCATION, and CREATE SCHEMA ... WITH (...)") {
	}

	vector<GrammarChange> GetChanges() const override {
		vector<GrammarChange> changes;
		changes.push_back(
		    GrammarChange::AddRule("GlueAlterTableStatement <- 'ALTER' 'TABLE' BaseTableName GluePartitionAction+",
		                           StartGlueAlterTableTransform));
		changes.push_back(GrammarChange::AddRule(
		    "GluePartitionAction <- GlueAddPartitions / GlueDropPartitions / GlueRenamePartition / "
		    "GluePartitionSetLocation / GlueTableSetLocation"));
		changes.push_back(GrammarChange::AddRule("GlueAddPartitions <- 'ADD' IfNotExists? GluePartitionWithLocation+"));
		changes.push_back(GrammarChange::AddRule("GluePartitionWithLocation <- GluePartitionSpec GlueLocation?"));
		changes.push_back(GrammarChange::AddRule("GlueDropPartitions <- 'DROP' IfExists? List(GluePartitionSpec)"));
		changes.push_back(
		    GrammarChange::AddRule("GlueRenamePartition <- GluePartitionSpec 'RENAME' 'TO' GluePartitionSpec"));
		changes.push_back(GrammarChange::AddRule("GluePartitionSetLocation <- GluePartitionSpec 'SET' GlueLocation"));
		changes.push_back(GrammarChange::AddRule("GlueTableSetLocation <- 'SET' GlueLocation"));
		changes.push_back(GrammarChange::AddRule("GluePartitionSpec <- 'PARTITION' Parens(List(GluePartitionValue))"));
		changes.push_back(GrammarChange::AddRule("GluePartitionValue <- ColumnName '=' Expression"));
		changes.push_back(GrammarChange::AddRule("GlueLocation <- 'LOCATION' StringLiteral"));
		// tried before the built-in ALTER statement; it fails on anything that is not a partition action, so the
		// built-in ALTER TABLE forms are unaffected
		changes.push_back(GrammarChange::PrependChoice("Statement", "GlueAlterTableStatement"));

		// WithList is mandatory here (no '?'), so a plain CREATE SCHEMA cannot match this rule and falls through to
		// DuckDB's built-in CreateSchemaStmt -- the same way the ALTER rule above leaves the built-in forms alone
		changes.push_back(
		    GrammarChange::AddRule("GlueCreateSchemaStatement <- 'CREATE' 'SCHEMA' IfNotExists? QualifiedName WithList",
		                           StartGlueCreateSchemaTransform));
		changes.push_back(GrammarChange::PrependChoice("Statement", "GlueCreateSchemaStatement"));
		return changes;
	}
};

} // namespace

void RegisterGlueGrammarExtension(DatabaseInstance &db) {
	GrammarExtension::Register(db, make_shared_ptr<GlueHiveDDLGrammar>());
}

} // namespace duckdb
