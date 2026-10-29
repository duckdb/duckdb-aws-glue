#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "functions/hive_reader_call.hpp"
#include "glue_test_util.hpp"

#include <filesystem>
#include <map>
#include <set>

using namespace duckdb;

namespace {

Value Scalar(Connection &con, const string &sql) {
	auto result = Run(con, sql);
	auto chunk = result->Fetch();
	REQUIRE(chunk != nullptr);
	REQUIRE(chunk->size() == 1);
	return chunk->GetValue(0, 0);
}

//! hash(t) makes the count read every column
int64_t Rows(Connection &con, const string &query) {
	return Scalar(con, "SELECT count(hash(t)) FROM (" + query + ") t").GetValue<int64_t>();
}

string ReadError(Connection &con, const string &query) {
	auto result = con.Query("SELECT count(hash(t)) FROM (" + query + ") t");
	return result->HasError() ? result->GetError() : string();
}

string Except(const string &a, const string &b) {
	return "(" + a + ") EXCEPT ALL (" + b + ")";
}

//! The rows in one result and not in the other, either way
int64_t Differences(Connection &con, const string &a, const string &b) {
	auto sql = "SELECT count(*) FROM ((" + Except(a, b) + ") UNION ALL (" + Except(b, a) + "))";
	auto differences = Scalar(con, sql).GetValue<int64_t>();
	if (differences != 0) {
		UNSCOPED_INFO("only in the first:\n" + Run(con, Except(a, b))->ToString());
		UNSCOPED_INFO("only in the second:\n" + Run(con, Except(b, a))->ToString());
		UNSCOPED_INFO("the second: " + b);
	}
	return differences;
}

struct TestDirectory {
	explicit TestDirectory(const string &name)
	    : path((std::filesystem::temp_directory_path() / ("unittest_glue_" + name + "_" + Unique())).string()) {
		std::filesystem::create_directories(path);
	}
	~TestDirectory() {
		std::error_code error;
		std::filesystem::remove_all(path, error);
	}
	string path;
};

void Write(Connection &con, const string &select, const string &path, const string &options) {
	if (!StringUtil::StartsWith(path, "s3://")) {
		std::filesystem::create_directories(std::filesystem::path(path).parent_path());
	}
	Run(con, "COPY (" + select + ") TO " + Quote(path) + " (" + options + ")");
}

void WriteLines(Connection &con, const vector<string> &lines, const string &path) {
	vector<string> rows;
	for (auto &line : lines) {
		rows.push_back("(" + Quote(line) + ")");
	}
	Write(con, "SELECT * FROM (VALUES " + StringUtil::Join(rows, ", ") + ") t(line)", path,
	      "FORMAT csv, HEADER false, QUOTE '', ESCAPE ''");
}

void CollectGets(LogicalOperator &op, vector<reference<LogicalGet>> &gets) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		gets.push_back(op.Cast<LogicalGet>());
	}
	for (auto &child : op.children) {
		CollectGets(*child, gets);
	}
}

//! A query's optimized plan, read inside the transaction that planned it
class PlannedScan {
public:
	PlannedScan(Connection &con_p, const string &query) : con(con_p) {
		con.BeginTransaction();
		plan = con.context->ExtractPlan(query);
		vector<reference<LogicalGet>> gets;
		CollectGets(*plan, gets);
		REQUIRE(gets.size() == 1);
		get = &gets[0].get();
	}
	~PlannedScan() {
		plan.reset();
		con.Query("COMMIT");
	}

public:
	const LogicalGet &Get() const {
		return *get;
	}
	vector<OpenFileInfo> Files() const {
		return get->bind_data->Cast<MultiFileBindData>().file_list->GetAllFiles();
	}
	//! Read 'column' as 'type', as the optimizer's type pushdown leaves a scan whose column a cast was pushed into
	void Retype(const string &column, const LogicalType &type) {
		auto &bind_data = get->bind_data->Cast<MultiFileBindData>();
		for (idx_t i = 0; i < bind_data.names.size(); i++) {
			if (bind_data.names[i].GetIdentifierName() == column) {
				bind_data.types[i] = type;
				return;
			}
		}
		FAIL("the scan has no column " + column);
	}

private:
	Connection &con;
	unique_ptr<LogicalOperator> plan;
	optional_ptr<LogicalGet> get;
};

HiveReaderCall Call(const PlannedScan &scan) {
	auto call = HiveScanGetReaderCall(scan.Get());
	REQUIRE(call.has_value());
	return std::move(*call);
}

std::set<string> ParameterNames(const HiveReaderCall &call) {
	std::set<string> names;
	for (auto &parameter : call.parameters) {
		names.insert(parameter.first.GetIdentifierName());
	}
	return names;
}

string Parameter(const HiveReaderCall &call, const string &name) {
	auto entry = call.parameters.find(Identifier(name));
	if (entry == call.parameters.end()) {
		FAIL("the call has no parameter " + name);
	}
	return entry->second.ToSQLString();
}

vector<string> Columns(const HiveReaderCall &call) {
	vector<string> columns;
	for (auto &column : call.columns) {
		columns.push_back(column->ToString());
	}
	return columns;
}

string SelectItem(const ParsedExpression &expression) {
	auto sql = expression.ToString();
	if (expression.GetAlias().empty()) {
		return sql;
	}
	return sql + " AS " + SQLIdentifier::ToString(expression.GetAlias());
}

//! The partition values of the file at 'path' as SQL, or "none"
string PartitionValuesSQL(const PlannedScan &scan, const string &path) {
	auto values = HiveScanGetFilePartitionValues(scan.Get(), path);
	if (!values) {
		return "none";
	}
	vector<string> items;
	for (auto &value : *values) {
		items.push_back(SelectItem(*value));
	}
	return StringUtil::Join(items, ", ");
}

string ReaderCallSQL(const HiveReaderCall &call, const vector<string> &files) {
	auto sql = call.function + "([" + StringUtil::Join(files, ", ") + "]";
	for (auto &parameter : call.parameters) {
		sql += ", " + SQLIdentifier::ToString(parameter.first) +
		       " := " + ConstantExpression::FromValue(parameter.second)->ToString();
	}
	return sql + ")";
}

//! The read an engine distributing the scan would send for every file of the plan: per partition, the reader call over
//! the partition's files, selecting the columns and the partition's values
string RenderRead(const PlannedScan &scan) {
	auto call = Call(scan);
	std::map<string, pair<vector<string>, vector<string>>> partitions;
	for (auto &file : scan.Files()) {
		vector<string> values;
		auto partition = HiveScanGetFilePartitionValues(scan.Get(), file.path);
		if (partition) {
			for (auto &value : *partition) {
				values.push_back(SelectItem(*value));
			}
		}
		auto &entry = partitions[StringUtil::Join(values, ", ")];
		entry.first = values;
		entry.second.push_back(Quote(file.path));
	}
	vector<string> reads;
	for (auto &partition : partitions) {
		vector<string> items;
		for (auto &column : call.columns) {
			items.push_back(SelectItem(*column));
		}
		items.insert(items.end(), partition.second.first.begin(), partition.second.first.end());
		reads.push_back("SELECT " + StringUtil::Join(items, ", ") + " FROM " +
		                ReaderCallSQL(call, partition.second.second));
	}
	return StringUtil::Join(reads, " UNION ALL ");
}

string Restate(Connection &con, const string &query) {
	PlannedScan scan(con, query);
	return RenderRead(scan);
}

} // namespace

TEST_CASE("A Parquet scan is restated as read_parquet over its files", "[hive_reader_call]") {
	DuckDB db(nullptr);
	Connection con(db);
	TestDirectory dir("parquet");
	Write(con,
	      "SELECT i, i::DOUBLE AS v, CASE WHEN i < 20 THEN '2024-01-01' ELSE '2024-01-02' END AS dt FROM range(0, 30) "
	      "t(i)",
	      dir.path + "/part", "FORMAT parquet, PARTITION_BY (dt)");
	Write(con, "SELECT i, i::DOUBLE AS v FROM range(30, 40) t(i)", dir.path + "/elsewhere/a.parquet", "FORMAT parquet");
	auto query = "SELECT * FROM hive_scan(" + Quote(dir.path + "/part") +
	             ", schema := {i: 'BIGINT', v: 'DOUBLE', dt: 'DATE'}, partitions := [{dt: '2024-01-01'}, "
	             "{dt: '2024-01-02'}, {dt: NULL, location: " +
	             Quote(dir.path + "/elsewhere") + "}])";
	{
		PlannedScan scan(con, query);
		auto call = Call(scan);
		REQUIRE(call.function == "read_parquet");
		REQUIRE(ParameterNames(call) ==
		        std::set<string> {"binary_as_string", "can_have_nan", "hive_partitioning", "schema"});
		REQUIRE(Parameter(call, "binary_as_string") == "true");
		REQUIRE(Parameter(call, "can_have_nan") == "true");
		REQUIRE(Parameter(call, "hive_partitioning") == "false");
		REQUIRE(Columns(call) == vector<string> {"i", "v"});
		bool null_partition = false;
		bool typed_partition = false;
		for (auto &file : scan.Files()) {
			auto values = HiveScanGetFilePartitionValues(scan.Get(), file.path);
			REQUIRE(values.has_value());
			REQUIRE(values->size() == 1);
			REQUIRE((*values)[0]->GetAlias().GetIdentifierName() == "dt");
			null_partition |= (*values)[0]->ToString() == "CAST(NULL AS DATE)";
			typed_partition |= (*values)[0]->ToString() == "CAST('2024-01-02' AS DATE)";
		}
		REQUIRE(null_partition);
		REQUIRE(typed_partition);
		REQUIRE(!HiveScanGetFilePartitionValues(scan.Get(), dir.path + "/part/not_listed.parquet").has_value());
		// another spelling of a listed file's path is the same file
		for (auto &file : scan.Files()) {
			auto respelled = StringUtil::Replace(file.path, dir.path, dir.path + "/.//");
			REQUIRE(respelled != file.path);
			REQUIRE(PartitionValuesSQL(scan, respelled) == PartitionValuesSQL(scan, file.path));
		}
		REQUIRE(PartitionValuesSQL(scan, dir.path + "/.//part/not_listed.parquet") == "none");
	}
	auto read = Restate(con, query);
	REQUIRE(Rows(con, read) == 40);
	REQUIRE(Differences(con, query, read) == 0);
	auto pruned = query + " WHERE dt IS NULL OR dt = DATE '2024-01-02'";
	REQUIRE(Rows(con, Restate(con, pruned)) == 20);
	REQUIRE(Differences(con, pruned, Restate(con, pruned)) == 0);
}

TEST_CASE("Strings a Parquet file stores as binary read as the scan reads them", "[hive_reader_call]") {
	DuckDB db(nullptr);
	Connection con(db);
	TestDirectory dir("binary");
	// Impala, older Hive and parquet-mr write a string column as binary without a string annotation
	Write(con, "SELECT i, encode('ü' || i) AS s FROM range(0, 4) t(i)", dir.path + "/text/a.parquet", "FORMAT parquet");
	auto query = "SELECT * FROM hive_scan(" + Quote(dir.path + "/text") + ", schema := {i: 'BIGINT', s: 'VARCHAR'})";
	auto read = Restate(con, query);
	REQUIRE(Scalar(con, "SELECT string_agg(s, ',' ORDER BY i) FROM (" + read + ")").ToString() == "ü0,ü1,ü2,ü3");
	REQUIRE(Differences(con, query, read) == 0);
	// with a binary column the scan decodes those string columns file by file, which no reader call says
	Write(con, "SELECT i, encode('ü' || i) AS s, '\\x01'::BLOB AS b FROM range(0, 4) t(i)",
	      dir.path + "/mixed/a.parquet", "FORMAT parquet");
	PlannedScan scan(con, "SELECT * FROM hive_scan(" + Quote(dir.path + "/mixed") +
	                          ", schema := {i: 'BIGINT', s: 'VARCHAR', b: 'BLOB'})");
	REQUIRE(!HiveScanGetReaderCall(scan.Get()).has_value());
}

TEST_CASE("A csv scan is restated as read_csv with the options it was bound with", "[hive_reader_call]") {
	DuckDB db(nullptr);
	Connection con(db);
	TestDirectory dir("csv");
	WriteLines(con, {"1|a|1.5", "2|NA|NA", "3|\"c|d\"|3.5"}, dir.path + "/text_d1/a.csv");
	WriteLines(con, {"4|e|4.5"}, dir.path + "/text_d2/a.csv");
	auto query = "SELECT * FROM hive_scan(" + Quote(dir.path + "/text") +
	             ", format := 'csv', delim := '|', quote := '\"', escape := '\"', nullstr := 'NA', schema := {id: "
	             "'INTEGER', name: 'VARCHAR', amount: 'DOUBLE', dt: 'DATE'}, partitions := [{dt: '2024-01-01', "
	             "location: " +
	             Quote(dir.path + "/text_d1") + "}, {dt: NULL, location: " + Quote(dir.path + "/text_d2") + "}])";
	{
		PlannedScan scan(con, query);
		auto call = Call(scan);
		REQUIRE(call.function == "read_csv");
		REQUIRE(Parameter(call, "delim") == "'|'");
		REQUIRE(Parameter(call, "nullstr") == "'NA'");
		REQUIRE(Parameter(call, "auto_detect") == "false");
		REQUIRE(Parameter(call, "header") == "false");
		REQUIRE(Parameter(call, "hive_partitioning") == "false");
		REQUIRE(StringUtil::Contains(Parameter(call, "columns"), "'amount': 'DOUBLE'"));
		REQUIRE(Columns(call) == vector<string> {"id", "\"name\"", "amount"});
	}
	auto read = Restate(con, query);
	REQUIRE(Rows(con, read) == 4);
	REQUIRE(Differences(con, query, read) == 0);
	// a column the scan reads as another type than the table's, which no reader call says: of a data column, and of a
	// partition key, whose values would be cast from Glue's text
	for (auto &column : {"name", "dt"}) {
		PlannedScan scan(con, query);
		scan.Retype(column, LogicalType::INTEGER);
		REQUIRE(!HiveScanGetReaderCall(scan.Get()).has_value());
		auto retypes_key = string(column) == "dt";
		for (auto &file : scan.Files()) {
			REQUIRE(HiveScanGetFilePartitionValues(scan.Get(), file.path).has_value() != retypes_key);
		}
	}
}

TEST_CASE("json and avro scans are restated", "[hive_reader_call]") {
	DuckDB db(nullptr);
	Connection con(db);
	TestDirectory dir("json_avro");
	WriteLines(con, {"{\"id\": 1, \"name\": \"a\"}", "not json", "{\"id\": 3, \"name\": \"c\", \"extra\": true}"},
	           dir.path + "/json/a.json");
	auto json = "SELECT * FROM hive_scan(" + Quote(dir.path + "/json") +
	            ", format := 'json', ignore_errors := true, schema := {id: 'INTEGER', name: 'VARCHAR'})";
	{
		PlannedScan scan(con, json);
		auto call = Call(scan);
		REQUIRE(call.function == "read_json");
		REQUIRE(Parameter(call, "format") == "'newline_delimited'");
		REQUIRE(Parameter(call, "ignore_errors") == "true");
	}
	REQUIRE(Rows(con, Restate(con, json)) == 3);
	REQUIRE(Differences(con, json, Restate(con, json)) == 0);
	// files of different columns, the first without one the second has, and a column no file has: every file reads
	// onto the table's columns by name
	Write(con, "SELECT 'a' AS flight, 1 AS delay", dir.path + "/avro/year=2020/a.avro", "FORMAT avro");
	Write(con, "SELECT 'b' AS flight, 'G1' AS gate, 2 AS delay", dir.path + "/avro/year=2020/b.avro", "FORMAT avro");
	Write(con, "SELECT 'c' AS flight, 3 AS delay", dir.path + "/avro/year=2021/a.avro", "FORMAT avro");
	auto avro = "SELECT * FROM hive_scan(" + Quote(dir.path + "/avro") +
	            ", format := 'avro', schema := {flight: 'VARCHAR', gate: 'VARCHAR', terminal: 'VARCHAR', delay: "
	            "'BIGINT', year: 'INTEGER'}, partitions := [{year: '2020'}, {year: '2021'}])";
	{
		PlannedScan scan(con, avro);
		auto call = Call(scan);
		REQUIRE(call.function == "read_avro");
		REQUIRE(ParameterNames(call) == std::set<string> {"hive_partitioning", "schema"});
	}
	REQUIRE(Rows(con, Restate(con, avro)) == 3);
	REQUIRE(Differences(con, avro, Restate(con, avro)) == 0);
}

TEST_CASE("A scan that is not a Hive scan has no reader call", "[hive_reader_call]") {
	DuckDB db(nullptr);
	Connection con(db);
	TestDirectory dir("other");
	Write(con, "SELECT 1 AS i", dir.path + "/a.parquet", "FORMAT parquet");
	{
		PlannedScan scan(con, "SELECT * FROM read_parquet(" + Quote(dir.path + "/a.parquet") + ")");
		REQUIRE(!HiveScanGetReaderCall(scan.Get()).has_value());
		REQUIRE(!HiveScanGetFilePartitionValues(scan.Get(), dir.path + "/a.parquet").has_value());
	}
	PlannedScan scan(con, "SELECT * FROM range(3)");
	REQUIRE(!HiveScanGetReaderCall(scan.Get()).has_value());
}

TEST_CASE("Glue tables of every SerDe are restated", "[hive_reader_call][glue_catalog]") {
	auto endpoint = LocalGlueEndpoint();
	if (endpoint.empty()) {
		WARN("skipped: GLUE_ENDPOINT does not name the local Glue (make glue-fixture, make test-cpp)");
		return;
	}
	DuckDB db(nullptr);
	Connection con(db);
	auto unique = Unique();
	auto location = Env("DEFAULT_S3_LOCATION", "s3://glue-test/warehouse") + "/unittest_glue/" + unique;
	AttachLocalGlue(con, endpoint, location);
	GlueTables tables(con, "urc_" + unique + "_");
	// parquet: a nested column, typed keys, a NULL partition and a partition outside the table location
	auto parquet = tables.Add("parquet");
	Run(con, "CREATE TABLE lake.default." + parquet +
	             " (id INTEGER, amount DECIMAL(10,2), s STRUCT(a INTEGER, b VARCHAR), dt DATE, k INTEGER) "
	             "PARTITIONED BY (dt, k)");
	Run(con, "INSERT INTO lake.default." + parquet +
	             " VALUES (1, 1.50, {a: 1, b: 'x'}, DATE '2016-05-14', 1), (2, 2.50, NULL, DATE '2016-05-14', 1), "
	             "(3, 3.50, {a: 3, b: NULL}, DATE '2016-05-15', NULL)");
	Write(con, "SELECT 4 AS id, 4.50::DECIMAL(10,2) AS amount, {a: 4, b: 'y'} AS s", location + "/imports/p/a.parquet",
	      "FORMAT parquet");
	Run(con, "CALL glue_add_partition(" + Quote("lake.default." + parquet) +
	             ", {dt: '2016-05-16', k: 2}, location := " + Quote(location + "/imports/p") + ")");
	auto query = "SELECT * FROM lake.default." + parquet;
	auto read = Restate(con, query);
	REQUIRE(Rows(con, read) == 4);
	REQUIRE(Differences(con, query, read) == 0);
	{
		PlannedScan scan(con, query);
		for (auto &file : scan.Files()) {
			auto respelled = StringUtil::Replace(file.path, location, location + "//./");
			REQUIRE(respelled != file.path);
			REQUIRE(PartitionValuesSQL(scan, file.path) != "none");
			REQUIRE(PartitionValuesSQL(scan, respelled) == PartitionValuesSQL(scan, file.path));
		}
	}
	auto pruned = query + " WHERE k IS NULL OR k = 2";
	REQUIRE(Differences(con, pruned, Restate(con, pruned)) == 0);
	// LazySimpleSerDe: '|', NULL is 'NA', two header lines, short rows padded, extra fields ignored, invalid fields
	// NULL
	auto lazy = tables.Add("lazy");
	Run(con, "CREATE TABLE lake.default." + lazy +
	             " (id INTEGER, name VARCHAR, amount DOUBLE, dt VARCHAR) PARTITIONED BY (dt) WITH (format = 'csv', "
	             "delimiter = '|', 'skip.header.line.count' = '2', 'serialization.null.format' = 'NA')");
	WriteLines(con, {"generated by a job", "id|name|amount", "1|a|1.5", "2|NA|NA", "3|c", "4|d|oops", "5|\"e\"|5.5|x"},
	           location + "/default/" + lazy + "/dt=d1/data.csv");
	Run(con, "CALL glue_add_partition(" + Quote("lake.default." + lazy) + ", {dt: 'd1'})");
	query = "SELECT * FROM lake.default." + lazy;
	{
		PlannedScan scan(con, query);
		auto call = Call(scan);
		REQUIRE(call.function == "read_csv");
		REQUIRE(Parameter(call, "skip") == "2");
		REQUIRE(Parameter(call, "nullstr") == "'NA'");
		REQUIRE(Parameter(call, "null_padding") == "true");
		REQUIRE(Parameter(call, "strict_mode") == "false");
		REQUIRE(Columns(call) == vector<string> {"TRY_CAST(__internal_barrier(id) AS INTEGER)", "\"name\"",
		                                         "TRY_CAST(__internal_barrier(amount) AS DOUBLE)"});
	}
	read = Restate(con, query);
	REQUIRE(Rows(con, read) == 5);
	REQUIRE(Differences(con, query, read) == 0);
	// a field read as another type than the table's: its conversion to that type would TRY_CAST a field a CAST fails on
	{
		PlannedScan scan(con, query);
		scan.Retype("name", LogicalType::INTEGER);
		REQUIRE(!HiveScanGetReaderCall(scan.Get()).has_value());
	}
	// OpenCSVSerde: quoted, one header line, no NULL, and a value that does not parse fails the query
	auto opencsv = tables.Add("opencsv");
	Run(con, "CREATE TABLE lake.default." + opencsv +
	             " (id INTEGER, name VARCHAR, amount DOUBLE) WITH (format = 'csv', quote = '\"', "
	             "'skip.header.line.count' = '1')");
	auto opencsv_location = location + "/default/" + opencsv;
	WriteLines(con, {"id,name,amount", "1,\"a,b\",1.5", "2,", "3,\"x \"\"y\"\"\",3.5,extra", "4"},
	           opencsv_location + "/data.csv");
	query = "SELECT * FROM lake.default." + opencsv;
	read = Restate(con, query);
	REQUIRE(StringUtil::Contains(read, "CAST(__internal_barrier(amount) AS DOUBLE)"));
	REQUIRE(!StringUtil::Contains(read, "TRY_CAST"));
	REQUIRE(Rows(con, read) == 4);
	REQUIRE(Differences(con, query, read) == 0);
	WriteLines(con, {"id,name,amount", "5,e,oops"}, opencsv_location + "/invalid.csv");
	REQUIRE(StringUtil::Contains(ReadError(con, query), "Could not convert string 'oops' to DOUBLE"));
	REQUIRE(StringUtil::Contains(ReadError(con, Restate(con, query)), "Could not convert string 'oops' to DOUBLE"));
	Run(con, "ALTER TABLE lake.default." + opencsv + " SET ('use.null.for.invalid.data' = 'true')");
	read = Restate(con, query);
	REQUIRE(StringUtil::Contains(read, "TRY_CAST(__internal_barrier(amount) AS DOUBLE)"));
	REQUIRE(Rows(con, read) == 5);
	REQUIRE(Differences(con, query, read) == 0);
	// a quoted line break: planned with hive_csv_parallel off, the read is sent to a session that has it on
	WriteLines(con, {"id,name,amount", "6,\"two\nlines\",6.5", "7"}, opencsv_location + "/line_break.csv");
	Run(con, "SET hive_csv_parallel = false");
	read = Restate(con, query);
	Run(con, "RESET hive_csv_parallel");
	REQUIRE(StringUtil::Contains(read, "\"parallel\" := false"));
	REQUIRE(StringUtil::Contains(ReadError(con, query), "parallel scanner does not support null_padding"));
	REQUIRE(Rows(con, read) == 7);
	// an OpenCSVSerde boolean is true for 'true' in any case and false otherwise, never an error
	auto flags = tables.Add("flags");
	Run(con, "CREATE TABLE lake.default." + flags + " (id INTEGER, ok BOOLEAN) WITH (format = 'csv', quote = '\"')");
	WriteLines(con, {"1,true", "2,TRUE", "3,false", "4,True.", "5,1", "6,yes", "7,", "8"},
	           location + "/default/" + flags + "/data.csv");
	query = "SELECT * FROM lake.default." + flags;
	{
		PlannedScan scan(con, query);
		REQUIRE(Columns(Call(scan)) ==
		        vector<string> {"CAST(__internal_barrier(id) AS INTEGER)", "(lower(__internal_barrier(ok)) = 'true')"});
	}
	read = Restate(con, query);
	REQUIRE(Scalar(con,
	               "SELECT string_agg(id || ':' || coalesce(ok::VARCHAR, 'NULL'), ' ' ORDER BY id) FROM (" + read + ")")
	            .ToString() == "1:true 2:true 3:false 4:false 5:false 6:false 7:false 8:NULL");
	REQUIRE(Differences(con, query, read) == 0);
	// json and avro
	auto json = tables.Add("json");
	Run(con, "CREATE TABLE lake.default." + json +
	             " (id INTEGER, name VARCHAR, s STRUCT(a INTEGER), dt VARCHAR) PARTITIONED BY (dt) WITH (format = "
	             "'json')");
	Run(con, "INSERT INTO lake.default." + json +
	             " VALUES (1, 'a', {a: 1}, 'd1'), (2, NULL, NULL, 'd1'), (3, 'c', {a: 3}, 'd2')");
	query = "SELECT * FROM lake.default." + json;
	read = Restate(con, query);
	REQUIRE(Rows(con, read) == 3);
	REQUIRE(Differences(con, query, read) == 0);
	auto avro = tables.Add("avro");
	Run(con, "CREATE TABLE lake.default." + avro +
	             " (flight VARCHAR, delay INTEGER, year INTEGER) PARTITIONED BY (year) WITH (format = 'avro')");
	Run(con, "INSERT INTO lake.default." + avro + " VALUES ('a', 1, 2020), ('b', NULL, 2020), ('c', 3, 2021)");
	query = "SELECT * FROM lake.default." + avro;
	read = Restate(con, query);
	REQUIRE(StringUtil::Contains(read, "read_avro("));
	REQUIRE(Rows(con, read) == 3);
	REQUIRE(Differences(con, query, read) == 0);
}
