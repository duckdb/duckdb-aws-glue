#include "planning/glue_hive_insert.hpp"
#include "planning/hive_field_conversion.hpp"

#include "api/glue_api_util.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
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
#include "catalog/glue_transaction.hpp"
#include "core/glue_types.hpp"
#include "core/hive_location.hpp"
#include "execution/hive_copy.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Planning
//===--------------------------------------------------------------------===//
GlueHiveInsert::GlueHiveInsert(PhysicalPlan &physical_plan, LogicalOperator &op, GlueCatalog &catalog,
                               GlueTableInfo table_info_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, op.estimated_cardinality),
      catalog(catalog), table_info(std::move(table_info_p)) {
}

//! A field delimiter as it is written in an error message: control characters as their octal escape ('\001')
static string DelimiterToString(const string &delimiter) {
	string result;
	for (auto c : delimiter) {
		auto byte = static_cast<uint8_t>(c);
		result += byte < 0x20 ? StringUtil::Format("\\%03o", byte) : string(1, c);
	}
	return result;
}

//! error(message), as a value of 'type'
static unique_ptr<Expression> RaiseError(ClientContext &context, const string &message, const LogicalType &type) {
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundConstantExpression>(Value(message)));
	return BoundCastExpression::AddCastToType(context, BindFunction(context, "error", std::move(children)), type);
}

//! The text of a value written to a Hive text file; an error when it holds a 'forbidden' character or is 'null_string'
static unique_ptr<Expression> CheckedTextField(ClientContext &context, unique_ptr<Expression> value,
                                               const string &forbidden, optional_ptr<const string> null_string,
                                               const string &error_message) {
	auto text = BoundCastExpression::AddCastToType(context, std::move(value), LogicalType::VARCHAR);
	string pattern = "[";
	for (auto c : forbidden) {
		pattern += StringUtil::Format("\\x{%02X}", static_cast<uint8_t>(c));
	}
	pattern += "]";
	vector<unique_ptr<Expression>> match_children;
	match_children.push_back(text->Copy());
	match_children.push_back(make_uniq<BoundConstantExpression>(Value(pattern)));
	auto refused = BindFunction(context, "regexp_matches", std::move(match_children));
	if (null_string) {
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
		conjunction->GetChildrenMutable().push_back(std::move(refused));
		conjunction->GetChildrenMutable().push_back(BoundComparisonExpression::Create(
		    ExpressionType::COMPARE_EQUAL, text->Copy(), make_uniq<BoundConstantExpression>(Value(*null_string))));
		refused = std::move(conjunction);
	}
	auto raise = RaiseError(context, error_message, LogicalType::VARCHAR);
	return make_uniq<BoundCaseExpression>(std::move(refused), std::move(raise), std::move(text));
}

//! A timestamp or a date as OpenCSVSerde reads it, the milliseconds (truncated) or days since the epoch; an error when
//! infinite
static unique_ptr<Expression> EpochField(ClientContext &context, unique_ptr<Expression> value,
                                         const string &error_message) {
	vector<unique_ptr<Expression>> infinite_children;
	infinite_children.push_back(value->Copy());
	auto infinite = BindFunction(context, "isinf", std::move(infinite_children));
	unique_ptr<Expression> number;
	vector<unique_ptr<Expression>> number_children;
	if (value->GetReturnType() == LogicalType::DATE) {
		number_children.push_back(std::move(value));
		number_children.push_back(make_uniq<BoundConstantExpression>(Value::DATE(date_t::epoch())));
		number = BindFunction(context, "-", std::move(number_children));
	} else {
		// epoch_ms rounds to the nearest millisecond, which can move a value into the next day
		vector<unique_ptr<Expression>> trunc_children;
		trunc_children.push_back(make_uniq<BoundConstantExpression>(Value("millisecond")));
		trunc_children.push_back(std::move(value));
		number_children.push_back(BindFunction(context, "date_trunc", std::move(trunc_children)));
		number = BindFunction(context, "epoch_ms", std::move(number_children));
	}
	auto raise = RaiseError(context, error_message, number->GetReturnType());
	return make_uniq<BoundCaseExpression>(std::move(infinite), std::move(raise), std::move(number));
}

//! A value's text with 'escape' put before each escape character and, unless empty, each 'delimiter': LazySimpleSerDe
//! reads both back as written, and OpenCSVSerde, which drops an unpaired escape character, reads a doubled one as one
static unique_ptr<Expression> EscapedTextField(ClientContext &context, unique_ptr<Expression> text,
                                               const string &escape, const string &delimiter) {
	vector<string> specials {escape};
	if (!delimiter.empty()) {
		specials.push_back(delimiter);
	}
	for (auto &special : specials) {
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(text));
		children.push_back(make_uniq<BoundConstantExpression>(Value(special)));
		children.push_back(make_uniq<BoundConstantExpression>(Value(escape + special)));
		text = BindFunction(context, "replace", std::move(children));
	}
	return text;
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

//! The directory of a partition relative to the table location, empty if the partition is not below it. s3a:// and
//! s3:// name the same bucket and key. 'writable' is false when the directory has an empty or '.' segment ('t//p=x'):
//! the copy drops those, so it would write the files to another directory than the one the partition is read from.
static string RelativePartitionPath(const string &table_location, const string &partition_location, bool &writable) {
	writable = true;
	auto prefix = ComparableLocation(table_location) + "/";
	auto location = ComparableLocation(partition_location);
	if (!StringUtil::StartsWith(location, prefix)) {
		return string();
	}
	vector<string> components;
	for (auto &component : StringUtil::Split(location.substr(prefix.size()), '/')) {
		if (component.empty() || component == ".") {
			writable = false;
		} else {
			components.push_back(component);
		}
	}
	return StringUtil::Join(components, "/");
}

//! A VARCHAR expression that fails the query with 'message' when it is evaluated
static unique_ptr<Expression> ErrorExpression(ClientContext &context, FunctionBinder &function_binder,
                                              const string &message) {
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundConstantExpression>(Value(message)));
	return BoundCastExpression::AddCastToType(
	    context, function_binder.BindScalarFunction(ErrorFun::GetFunction(), std::move(children)),
	    LogicalType::VARCHAR);
}

//! Fails rows whose partition value is not NULL but whose Glue value reads back as NULL
//! (GlueTypes::PartitionValueIsNull):
//! '__HIVE_DEFAULT_PARTITION__', the value Hive stores a NULL partition value as, and for a key that is not a string
//! also 'NULL' in any case and ''. Only strings and blobs have such a value.
static void AddNullValueChecks(ClientContext &context, FunctionBinder &function_binder, const GlueTableInfo &table_info,
                               const vector<Identifier> &names, const vector<LogicalType> &types,
                               const vector<idx_t> &partition_columns, BoundCaseExpression &result) {
	for (idx_t i = 0; i < partition_columns.size(); i++) {
		auto column_index = partition_columns[i];
		auto &type = types[column_index];
		if (type.id() != LogicalTypeId::VARCHAR && type.id() != LogicalTypeId::BLOB) {
			continue;
		}
		auto text = [&]() {
			return BoundCastExpression::AddCastToType(context, make_uniq<BoundReferenceExpression>(type, i),
			                                          LogicalType::VARCHAR);
		};
		auto equals = [&](unique_ptr<Expression> left, const string &right) {
			return BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, std::move(left),
			                                         make_uniq<BoundConstantExpression>(Value(right)));
		};
		string values = StringUtil::Format("'%s'", HivePartitioning::DEFAULT_PARTITION_NAME);
		BoundCaseCheck check;
		check.when_expr = equals(text(), HivePartitioning::DEFAULT_PARTITION_NAME);
		if (type.id() != LogicalTypeId::VARCHAR) {
			vector<unique_ptr<Expression>> lower_children;
			lower_children.push_back(text());
			auto lower = function_binder.BindScalarFunction(LowerFun::GetFunction(), std::move(lower_children));
			auto any = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
			any->GetChildrenMutable().push_back(std::move(check.when_expr));
			any->GetChildrenMutable().push_back(equals(std::move(lower), "null"));
			any->GetChildrenMutable().push_back(equals(text(), ""));
			check.when_expr = std::move(any);
			values += ", 'NULL' in any case, or ''";
		}
		check.then_expr = ErrorExpression(
		    context, function_binder,
		    StringUtil::Format("Writing to Hive table '%s.%s' is not supported: partition key '%s' has a value (%s) "
		                       "that Hive reads back as NULL",
		                       table_info.schema_name, table_info.name, names[column_index].GetIdentifierName(),
		                       values));
		result.CaseChecksMutable().push_back(std::move(check));
	}
}

//! 'result' with 'hive_path' as its ELSE, or 'hive_path' itself when 'result' has no checks
static unique_ptr<Expression> FinishPartitionPath(unique_ptr<BoundCaseExpression> result,
                                                  unique_ptr<Expression> hive_path) {
	if (result->CaseChecks().empty()) {
		return hive_path;
	}
	result->ElseMutable() = std::move(hive_path);
	return std::move(result);
}

//! The PARTITION_PATH of the copy: the hive layout (<key>=<value>/...), except for the existing partitions registered
//! at other locations, which are written to their own location, and the existing partitions whose column types differ
//! from the table's, which a row may not be written to: files of the table's types in them would not match what the
//! partition declares. 'planned_locations' receives the location of every existing partition by its Glue values, and
//! 'partition_directories' receives the directories of the partitions at other locations with their Glue values.
static unique_ptr<Expression> CreatePartitionPath(ClientContext &context, optional_ptr<GlueTable> existing_table,
                                                  const GlueTableInfo &table_info, const string &location,
                                                  const vector<Identifier> &names, const vector<LogicalType> &types,
                                                  const vector<idx_t> &partition_columns,
                                                  map<vector<string>, string> &planned_locations,
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

	auto result = make_uniq<BoundCaseExpression>(LogicalType::VARCHAR);
	AddNullValueChecks(context, function_binder, table_info, names, types, partition_columns, *result);

	// A table created by this CTAS cannot have existing partitions. Planning from
	// the local definition therefore needs no Glue call and writes every new
	// partition to the canonical <key>=<value> path below the table location.
	if (!existing_table) {
		return FinishPartitionPath(std::move(result), std::move(hive_path));
	}
	D_ASSERT(existing_table);
	auto &glue_catalog = existing_table->catalog.Cast<GlueCatalog>();
	auto partitions = GlueAPI::GetPartitions(context, glue_catalog, table_info.schema_name, table_info.name);
	// matched as the scan matches the files' columns: by position in csv files, by name otherwise
	auto columns_by_position = table_info.GetFileFormat() == HiveFileFormat::CSV;
	vector<string> keys;
	for (auto column_index : partition_columns) {
		keys.push_back(names[column_index].GetIdentifierName());
	}
	vector<pair<string, vector<string>>> custom_directories;
	for (auto &partition : partitions) {
		planned_locations.emplace(partition.values, partition.location);
		if (partition.values.size() != partition_columns.size()) {
			throw InvalidInputException("Partition [%s] of Hive table '%s' has %d values, but the table has %d "
			                            "partition keys",
			                            StringUtil::Join(partition.values, ", "), table_info.name,
			                            partition.values.size(), partition_columns.size());
		}
		// the values as this INSERT would register them ('1', not '01', for an integer key)
		vector<string> canonical_values;
		vector<unique_ptr<Expression>> conditions;
		for (idx_t i = 0; i < partition_columns.size(); i++) {
			auto column_index = partition_columns[i];
			auto value = GlueTypes::PartitionValue(context, table_info.partition_keys[i].name, partition.values[i],
			                                       types[column_index]);
			canonical_values.push_back(value.IsNull() ? HivePartitioning::DEFAULT_PARTITION_NAME : value.ToString());
			conditions.push_back(BoundComparisonExpression::Create(
			    ExpressionType::COMPARE_NOT_DISTINCT_FROM, make_uniq<BoundReferenceExpression>(types[column_index], i),
			    make_uniq<BoundConstantExpression>(std::move(value))));
		}
		auto mismatch = partition.FindColumnTypeMismatch(table_info.columns, columns_by_position);
		bool writable;
		auto relative_path = RelativePartitionPath(location, partition.location, writable);
		// otherwise the rows are written to the partition's location with its Glue values, so that the INSERT
		// does not register a second partition there
		if (!mismatch.IsValid() && relative_path == HivePartitionDirectory(keys, canonical_values).ToString() &&
		    writable && partition.values == canonical_values) {
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
		if (mismatch.IsValid()) {
			auto &table_column = table_info.columns[mismatch.GetIndex()];
			auto &partition_column =
			    *partition.FindPartitionColumn(table_info.columns, mismatch.GetIndex(), columns_by_position);
			auto message = StringUtil::Format(
			    "Writing to Hive table '%s.%s' failed: partition [%s] declares column '%s' as '%s', but the table "
			    "declares column '%s' as '%s'. The table schema changed after the partition was created, and "
			    "inserting into a partition whose column types differ from the table's is not supported: the new files "
			    "would not match the types the partition declares. Make the partition declare the table's columns "
			    "first, e.g. by dropping it with glue_drop_partition and adding it again with glue_add_partition, "
			    "which keeps its files",
			    table_info.schema_name, table_info.name, StringUtil::Join(partition.values, ", "),
			    partition_column.name, partition_column.type, table_column.name, table_column.type);
			check.then_expr = ErrorExpression(context, function_binder, message);
		} else if (!writable) {
			check.then_expr = ErrorExpression(
			    context, function_binder,
			    StringUtil::Format("INSERT into Hive table '%s.%s' is not supported: partition [%s] is registered at "
			                       "'%s', whose path has an empty or '.' segment, and DuckDB can not write to it",
			                       table_info.schema_name, table_info.name, StringUtil::Join(partition.values, ", "),
			                       partition.location));
		} else if (relative_path.empty()) {
			// the copy rejects a partition path that is not below its target
			check.then_expr = make_uniq<BoundConstantExpression>(Value(partition.location));
		} else {
			check.then_expr = make_uniq<BoundConstantExpression>(Value(relative_path));
			partition_directories.emplace(location + "/" + relative_path, partition.values);
			custom_directories.emplace_back(relative_path, partition.values);
		}
		result->CaseChecksMutable().push_back(std::move(check));
	}
	// after the checks of the existing partitions: rows whose own directory is the location of another partition
	// would be written there and read back as that partition
	for (auto &custom : custom_directories) {
		BoundCaseCheck check;
		check.when_expr = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, hive_path->Copy(),
		                                                    make_uniq<BoundConstantExpression>(Value(custom.first)));
		check.then_expr = ErrorExpression(
		    context, function_binder,
		    StringUtil::Format("INSERT into Hive table '%s.%s' is not supported: rows would be written to '%s', the "
		                       "location of partition [%s], and read back as that partition",
		                       table_info.schema_name, table_info.name, custom.first,
		                       StringUtil::Join(custom.second, ", ")));
		result->CaseChecksMutable().push_back(std::move(check));
	}
	return FinishPartitionPath(std::move(result), std::move(hive_path));
}

//! The part of a parquet file name before ".parquet" that names its codec, as parquet-mr names them ("snappy." for
//! snappy); empty for uncompressed files. 'codec' is the parquet.compression of the table, empty for DuckDB's default.
static string ParquetCodecExtension(const string &codec) {
	if (codec.empty() || codec == "snappy") {
		return "snappy.";
	}
	if (codec == "gzip") {
		return "gz.";
	}
	if (codec == "zstd") {
		return "zstd.";
	}
	if (codec == "brotli") {
		return "br.";
	}
	if (codec == "lz4" || codec == "lz4_raw") {
		// DuckDB writes LZ4 as LZ4_RAW
		return "lz4raw.";
	}
	return string();
}

void GlueHiveInsert::PlanWrite(ClientContext &context, PhysicalPlanGenerator &planner, LogicalOperator &op,
                               GlueHiveInsert &insert, optional_ptr<GlueTable> existing_table,
                               const GlueTableInfo &table_info, PhysicalOperator &plan, const vector<Identifier> &names,
                               const vector<LogicalType> &types, optional_ptr<GlueSchemaEntry> create_schema,
                               unique_ptr<BoundCreateTableInfo> create_info) {
	const bool creating_table = create_info != nullptr;
	D_ASSERT(creating_table == static_cast<bool>(create_schema));
	D_ASSERT(creating_table != static_cast<bool>(existing_table));
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

	// For INSERT, the table's SerDe determines the format.
	// For CTAS the table is freshly created and has no SerDe yet, so use the option from the CREATE TABLE statement.
	auto file_format = creating_table ? table_info.file_format : table_info.GetFileFormat();
	if (IsTextFileFormat(file_format)) {
		table_info.CheckTextSerdeSupported(file_format);
	}
	auto format_name = HiveFileFormatToString(file_format);
	// the operator feeding the copy and the columns it produces
	optional_ptr<PhysicalOperator> source = &plan;
	vector<Identifier> copy_names = names;
	vector<LogicalType> copy_types = types;
	// the copy function and its options
	string copy_format = format_name;
	identifier_map_t<vector<Value>> copy_options;
	auto codec = table_info.GetCodec(file_format);
	auto codec_option = file_format == HiveFileFormat::AVRO ? "codec" : "compression";
	if (!codec.empty()) {
		copy_options[Identifier(codec_option)] = {Value(codec)};
	}
	switch (file_format) {
	case HiveFileFormat::PARQUET: {
		// DuckDB's parquet writer takes a compression_level for zstd only
		auto level = table_info.GetCompressionLevel();
		if (codec == "zstd" && !level.empty()) {
			copy_options[Identifier("compression_level")] = {Value(level)};
		}
		break;
	}
	case HiveFileFormat::AVRO:
		ExtensionHelper::AutoLoadExtension(context, "avro");
		break;
	case HiveFileFormat::CSV: {
		// Hive CSV files: the table's dialect and NULL string, a header line only when the table says so
		auto header_lines = table_info.GetHeaderLineCount();
		if (header_lines > 1) {
			throw NotImplementedException("Writing to Hive table '%s.%s' is not supported: its files start with %d "
			                              "header lines, DuckDB writes at most one",
			                              table_info.schema_name, table_info.name, header_lines);
		}
		auto delimiter = table_info.GetFieldDelimiter();
		auto quoted = table_info.GetSerDe() == HiveSerDe::OPEN_CSV;
		auto null_string = table_info.GetNullFormat();
		if (delimiter.size() > 1) {
			throw NotImplementedException("Writing to Hive table '%s.%s' is not supported: DuckDB writes single-byte "
			                              "field delimiters only, the table's is '%s'",
			                              table_info.schema_name, table_info.name, delimiter);
		}
		copy_options[Identifier("header")] = {Value::BOOLEAN(header_lines == 1)};
		copy_options[Identifier("delimiter")] = {Value(delimiter)};
		copy_options[Identifier("quote")] = {Value(table_info.GetQuoteCharacter())};
		// LazySimpleSerDe's escape is written by EscapedTextField: DuckDB escapes only inside quotes
		auto escape = quoted ? string() : table_info.GetEscapeCharacter();
		copy_options[Identifier("escape")] = {Value(quoted ? table_info.GetEscapeCharacter() : string())};
		copy_options[Identifier("nullstr")] = {Value(null_string)};
		if (quoted) {
			// as Hive writes OpenCSVSerde files, so that an escape character in a value is escaped too
			copy_options[Identifier("force_quote")] = {Value("*")};
		}
		// every data column is written as text, refused where a field of the table's files can not hold it
		string forbidden = quoted || !escape.empty() ? "\n\r" : "\n\r" + delimiter;
		string reason;
		if (quoted) {
			reason = "holds a line break";
		} else if (!escape.empty()) {
			reason = StringUtil::Format("holds a line break or is the table's NULL string '%s'", null_string);
		} else {
			reason = StringUtil::Format("holds the field delimiter '%s' or a line break, or is the table's NULL string "
			                            "'%s'",
			                            DelimiterToString(delimiter), null_string);
		}
		vector<unique_ptr<Expression>> select_list;
		vector<LogicalType> projected_types;
		auto csv_options = table_info.GetCSVOptions();
		for (idx_t i = 0; i < names.size(); i++) {
			unique_ptr<Expression> column = make_uniq<BoundReferenceExpression>(types[i], i);
			if (std::find(partition_columns.begin(), partition_columns.end(), i) == partition_columns.end()) {
				auto message = StringUtil::Format("Writing to Hive table '%s.%s' failed: a value of column '%s' %s, "
				                                  "which the table's %s files can not hold in a field",
				                                  table_info.schema_name, table_info.name, names[i].GetIdentifierName(),
				                                  reason, quoted ? "OpenCSVSerde" : "LazySimpleSerDe");
				auto conversion = csv_options.FieldConversion(types[i]);
				if (conversion == HiveFieldConversion::EPOCH || conversion == HiveFieldConversion::TRY_EPOCH) {
					column = EpochField(
					    context, std::move(column),
					    StringUtil::Format(
					        "Writing to Hive table '%s.%s' failed: a value of column '%s' is infinite, which the "
					        "table's OpenCSVSerde files can not hold in a field",
					        table_info.schema_name, table_info.name, names[i].GetIdentifierName()));
				}
				// only unquoted fields can not tell the NULL string from NULL
				column =
				    CheckedTextField(context, std::move(column), forbidden, quoted ? nullptr : &null_string, message);
				if (!escape.empty()) {
					column = EscapedTextField(context, std::move(column), escape, delimiter);
				} else if (!csv_options.field_escape.empty()) {
					column = EscapedTextField(context, std::move(column), csv_options.field_escape, string());
				}
				copy_types[i] = LogicalType::VARCHAR;
			}
			projected_types.push_back(column->GetReturnType());
			select_list.push_back(std::move(column));
		}
		auto &projection = planner.Make<PhysicalProjection>(std::move(projected_types), std::move(select_list),
		                                                    op.estimated_cardinality);
		projection.children.push_back(plan);
		source = &projection;
		break;
	}
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
	CopyFunctionBindInput bind_input(*copy_info, copy_function->function.function_info);
	bind_input.file_extension = format_name;
	if (file_format == HiveFileFormat::PARQUET) {
		// named after the codec, as Spark and Hive name them (<name>.snappy.parquet): readers that list the files can
		// tell
		bind_input.file_extension = ParquetCodecExtension(codec) + format_name;
	}
	auto names_to_write = LogicalCopyToFile::GetNamesWithoutPartitions(copy_names, partition_columns, false);
	auto types_to_write = LogicalCopyToFile::GetTypesWithoutPartitions(copy_types, partition_columns, false);
	auto function_data = copy_function->function.copy_to_bind(context, bind_input, names_to_write, types_to_write);

	// Glue catalogs have no catalog version, so every execution plans again and gets its own write id
	auto write_id = UUID::ToString(UUIDv7::GenerateRandomUUID());
	auto return_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST);
	auto &copy = planner
	                 .Make<GlueHiveCopy>(std::move(return_types), copy_function->function, std::move(function_data),
	                                     op.estimated_cardinality, insert.catalog, create_schema,
	                                     std::move(create_info), table_info, write_id)
	                 .Cast<GlueHiveCopy>();
	// as the COPY binder does: without it every chunk becomes a batch of its own (a 2048-row parquet row group)
	if (copy.function.desired_batch_size) {
		copy.batch_size = copy.function.desired_batch_size(context, *copy.bind_data);
	}
	copy.use_tmp_file = false;
	// files of earlier inserts are kept, so every file needs a unique name; the write id finds those of a failed one
	copy.filename_pattern.SetFilenamePattern(write_id + "_{uuid}");
	map<vector<string>, string> planned_locations;
	unordered_map<string, vector<string>> partition_directories;
	if (!partition_columns.empty()) {
		copy.file_path = location;
		copy.partition_output = true;
		copy.partition_columns = partition_columns;
		copy.write_partition_columns = false;
		copy.hive_file_pattern = true;
		copy.partition_path_expression =
		    CreatePartitionPath(context, existing_table, table_info, location, copy_names, copy_types,
		                        partition_columns, planned_locations, partition_directories);
		// with partitioned output the copy must not initialize a single (partition-less) output file
		copy.write_empty_file = true;
		copy.per_thread_output = false;
	} else {
		copy.file_path = location;
		copy.partition_output = false;
		// false makes the first Sink open a writer at the location itself, leaving an object at the table key
		copy.write_empty_file = true;
		copy.per_thread_output = true;
	}
	copy.file_extension = bind_input.file_extension;
	copy.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	copy.return_type = CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST;
	copy.names = copy_names;
	copy.expected_types = copy_types;
	copy.children.push_back(*source);

	insert.planned_locations = std::move(planned_locations);
	insert.partition_directories = std::move(partition_directories);
	insert.write_id = std::move(write_id);
	insert.children.push_back(copy);
}

PhysicalOperator &GlueHiveInsert::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                             GlueTable &table, const GlueTableInfo &table_info,
                                             optional_ptr<PhysicalOperator> plan) {
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
	auto &catalog = table.catalog.Cast<GlueCatalog>();
	auto &insert = planner.Make<GlueHiveInsert>(op, catalog, table_info).Cast<GlueHiveInsert>();
	PlanWrite(context, planner, op, insert, table, table_info, *plan, names, types, nullptr, nullptr);
	return insert;
}

PhysicalOperator &GlueHiveInsert::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalCreateTable &op, PhysicalOperator &plan) {
	for (auto &option : op.info->Base().options) {
		if (GlueSchemaEntry::IsBucketingOption(option.first)) {
			throw NotImplementedException(
			    "CREATE TABLE ... AS with option '%s' is not supported: it creates a bucketed "
			    "Glue table, and DuckDB does not write a bucketed layout",
			    option.first);
		}
	}

	// no Glue call here: PREPARE and prepared statement rebinds plan more than once, the copy creates the table
	auto &schema = op.schema.Cast<GlueSchemaEntry>();
	if (op.info->Base().on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT &&
	    GlueSchemaEntry::ParseCreateTableOptions(context, op.info->Base()).overwrite) {
		// the files are deleted before the table is created, so a failed replace would leave neither
		throw NotImplementedException("CREATE OR REPLACE TABLE ... AS with option 'overwrite' is not supported: its "
		                              "files would be deleted before the table is replaced");
	}
	auto &catalog = schema.catalog.Cast<GlueCatalog>();
	// Catalog::CreateTable would check this, but the copy creates the table through the schema entry
	auto supported = catalog.SupportsCreateTable(*op.info);
	if (supported.HasError()) {
		supported.Throw();
	}
	auto table_info = schema.BuildTableInfo(context, op.info->Base());
	// the same gate as GetHiveTableForDML: the table would be created, written and then refused on read
	if (table_info.IsPartitionProjected()) {
		throw NotImplementedException(
		    "CREATE TABLE ... AS for Glue table '%s' is not supported: with 'projection.enabled' "
		    "on a partitioned table it would use Athena partition projection, which this "
		    "extension cannot read yet. Set projection.enabled to false; the partitions the "
		    "statement writes are then registered in Glue and readable",
		    table_info.name);
	}
	vector<Identifier> names;
	vector<LogicalType> types;
	for (auto &column : op.info->Base().columns.Logical()) {
		names.push_back(column.Name());
		types.push_back(column.Type());
	}
	auto &insert = planner.Make<GlueHiveInsert>(op, catalog, table_info).Cast<GlueHiveInsert>();
	PlanWrite(context, planner, op, insert, nullptr, table_info, plan, names, types, schema, std::move(op.info));
	return insert;
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
	// The COPY reports (rows written, files written)
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

//! Fails when a partition the files were written for already existed at another location, whose files are the ones
//! read. A partition the plan saw is compared with the location it had then; one it did not see was registered by
//! another writer since, and is looked up.
void GlueHiveInsert::CheckExistingPartitionLocations(ClientContext &context,
                                                     const vector<GluePartitionInput> &already_existing) const {
	auto check = [&](const GluePartitionInput &partition, const string &registered) {
		if (ComparableLocation(registered) != ComparableLocation(partition.location)) {
			throw IOException("rows of partition [%s] were written to '%s', but the partition is registered at '%s'",
			                  StringUtil::Join(partition.values, ", "), partition.location, registered);
		}
	};
	vector<reference<const GluePartitionInput>> unplanned;
	vector<vector<string>> unplanned_values;
	for (auto &partition : already_existing) {
		auto planned = planned_locations.find(partition.values);
		if (planned != planned_locations.end()) {
			check(partition, planned->second);
		} else {
			unplanned.push_back(partition);
			unplanned_values.push_back(partition.values);
		}
	}
	if (unplanned.empty()) {
		return;
	}
	auto locations =
	    GlueAPI::PartitionLocations(context, catalog, table_info.schema_name, table_info.name, unplanned_values);
	for (auto &entry : unplanned) {
		auto &partition = entry.get();
		auto registered = locations.find(partition.values);
		if (registered == locations.end()) {
			throw IOException("partition [%s] existed when its files were registered, but no longer does",
			                  StringUtil::Join(partition.values, ", "));
		}
		check(partition, registered->second);
	}
}

SinkFinalizeType GlueHiveInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<GlueHiveInsertGlobalState>();
	if (!state.written_files.empty()) {
		// an unpartitioned INSERT still changes files, so invalidate even with no new partition
		GlueTransactionCache::Invalidate(context, catalog);
	}
	auto &transaction = GlueTransaction::Get(context, catalog);
	if (table_info.partition_keys.empty() || state.written_files.empty()) {
		transaction.CompleteHiveWrite(write_id);
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
		// only the directories the copy added: the table location may hold a <key>=<value> of its own
		auto &keys = table_info.partition_keys;
		auto components = StringUtil::Split(directory, '/');
		if (components.size() < keys.size()) {
			throw InternalException("Written file '%s' has no directory for each partition key", file);
		}
		auto first = components.size() - keys.size();
		for (idx_t i = 0; i < keys.size(); i++) {
			auto &component = components[first + i];
			auto equals = component.find('=');
			if (equals == string::npos ||
			    !StringUtil::CIEquals(HivePartitioning::Unescape(component.substr(0, equals)), keys[i].name)) {
				throw InternalException("Written file '%s' has no value for partition key '%s'", file, keys[i].name);
			}
			partition.values.push_back(HivePartitioning::Unescape(component.substr(equals + 1)));
		}
		partitions.emplace(directory, std::move(partition));
	}
	vector<GluePartitionInput> to_register;
	for (auto &entry : partitions) {
		to_register.push_back(entry.second);
	}
	try {
		// an INSERT registers against the table definition it bound; a CREATE TABLE AS planned from the definition
		// it was about to create, so the table is fetched as created
		optional_ptr<const GlueTableInfo> registered = &table_info;
		GlueTableInfo created;
		if (!table_info.fetched) {
			if (!GlueAPI::GetTable(context, catalog, table_info.schema_name, table_info.name, created)) {
				throw CatalogException("Table \"%s.%s\" was created but could not be fetched afterwards",
				                       table_info.schema_name, table_info.name);
			}
			registered = &created;
		}
		// a cancel or timeout during registration would fail the statement with its rows in the table: undo the write
		// instead. BlockingInterruptCheck: InterruptCheck only looks at the deadline every 256 calls
		context.BlockingInterruptCheck();
		auto already_existing = GlueAPI::BatchCreatePartitions(context, catalog, *registered, to_register);
		CheckExistingPartitionLocations(context, already_existing);
		context.BlockingInterruptCheck();
	} catch (std::exception &ex) {
		// Undo the INSERT by deleting the files it wrote, so that running it again does not add its rows twice. The
		// partitions it registered stay, empty: another INSERT may have added rows to them in the meantime.
		ErrorData error(ex);
		try {
			// not the context's file system, which stops at the interrupt this may be handling
			DatabaseInstance::GetDatabase(context).GetFileSystem().RemoveFiles(state.written_files);
		} catch (std::exception &remove_ex) {
			throw IOException("INSERT into Hive table '%s.%s' failed: %s. Deleting the %llu files it wrote failed too "
			                  "(%s); remove them before running it again",
			                  table_info.schema_name, table_info.name, error.RawMessage(), state.written_files.size(),
			                  ErrorData(remove_ex).RawMessage());
		}
		// the files are gone; the write stays with the transaction, whose rollback drops a table CTAS created
		if (auto write = transaction.FindHiveWrite(write_id)) {
			write->files_deleted = true;
		}
		if (error.Type() == ExceptionType::INTERRUPT) {
			error.Throw();
		}
		throw IOException("INSERT into Hive table '%s.%s' was rolled back: %s. The files it wrote were deleted, so it "
		                  "can be run again",
		                  table_info.schema_name, table_info.name, error.RawMessage());
	}
	transaction.CompleteHiveWrite(write_id);
	return SinkFinalizeType::READY;
}

SourceResultType GlueHiveInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &state = sink_state->Cast<GlueHiveInsertGlobalState>();
	chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(state.insert_count)));
	chunk.CheckCardinality(1);
	return SourceResultType::FINISHED;
}

string GlueHiveInsert::GetName() const {
	return "GLUE_HIVE_INSERT";
}

InsertionOrderPreservingMap<string> GlueHiveInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table_info.name;
	return result;
}

} // namespace duckdb
