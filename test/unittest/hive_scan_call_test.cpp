#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "functions/hive_scan_call.hpp"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <set>

using namespace duckdb;

namespace {

unique_ptr<QueryResult> Run(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		FAIL(result->GetError() + "\n" + sql);
	}
	return result;
}

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

//! The rows in one result and not in the other, either way
int64_t Differences(Connection &con, const string &a, const string &b) {
	auto sql = "SELECT count(*) FROM ((" + a + " EXCEPT ALL " + b + ") UNION ALL (" + b + " EXCEPT ALL " + a + "))";
	return Scalar(con, sql).GetValue<int64_t>();
}

string Env(const char *name, const string &fallback) {
	auto value = std::getenv(name);
	return value ? string(value) : fallback;
}

string Unique() {
	return std::to_string(std::chrono::system_clock::now().time_since_epoch().count() % 1000000000);
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

string Quote(const string &text) {
	return Value(text).ToSQLString();
}

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

private:
	Connection &con;
	unique_ptr<LogicalOperator> plan;
	optional_ptr<LogicalGet> get;
};

std::set<string> ArgumentNames(const HiveScanCall &call) {
	std::set<string> names;
	for (auto &argument : call.arguments) {
		names.insert(argument.first.GetIdentifierName());
	}
	return names;
}

string Argument(const HiveScanCall &call, const string &name) {
	auto entry = call.arguments.find(Identifier(name));
	if (entry == call.arguments.end()) {
		FAIL("the call has no argument " + name);
	}
	return entry->second.ToSQLString();
}

HiveScanCall Call(const PlannedScan &scan) {
	auto call = HiveScanGetCall(scan.Get());
	REQUIRE(call.has_value());
	return *call;
}

HiveScanFilePartition FilePartition(const PlannedScan &scan, const string &path) {
	auto partition = HiveScanGetFilePartition(scan.Get(), path);
	REQUIRE(partition.has_value());
	return *partition;
}

//! The call an engine distributing the scan would send, for every file of the plan under its partition
string RenderCall(const PlannedScan &scan) {
	auto call = Call(scan);
	auto sql = "SELECT * FROM hive_scan(" + Quote(call.root);
	for (auto &argument : call.arguments) {
		sql += ", " + argument.first.GetIdentifierName() + " := " + argument.second.ToSQLString();
	}
	auto files = scan.Files();
	if (call.partition_keys.empty()) {
		vector<string> entries;
		for (auto &file : files) {
			entries.push_back(file.ToValue().ToSQLString());
		}
		return sql + ", files := [" + StringUtil::Join(entries, ", ") + "])";
	}
	vector<pair<HiveScanFilePartition, vector<string>>> partitions;
	unordered_map<string, idx_t> partition_indexes;
	for (auto &file : files) {
		auto partition = FilePartition(scan, file.path);
		auto key = StringUtil::Join(partition.values, "\x1f") + "\x1e" + partition.location;
		auto entry = partition_indexes.find(key);
		if (entry == partition_indexes.end()) {
			entry = partition_indexes.emplace(key, partitions.size()).first;
			partitions.emplace_back(partition, vector<string>());
		}
		partitions[entry->second].second.push_back(file.ToValue().ToSQLString());
	}
	vector<string> entries;
	for (auto &partition : partitions) {
		string entry = "{";
		for (idx_t k = 0; k < call.partition_keys.size(); k++) {
			entry += Quote(call.partition_keys[k]) + ": " + Quote(partition.first.values[k]) + ", ";
		}
		entry += "'location': " + Quote(partition.first.location) + ", 'files': [" +
		         StringUtil::Join(partition.second, ", ") + "]}";
		entries.push_back(entry);
	}
	return sql + ", partitions := [" + StringUtil::Join(entries, ", ") + "])";
}

string Restate(Connection &con, const string &query) {
	PlannedScan scan(con, query);
	return RenderCall(scan);
}

void RequireSameCall(const PlannedScan &a, const PlannedScan &b) {
	auto call_a = Call(a);
	auto call_b = Call(b);
	REQUIRE(call_a.root == call_b.root);
	REQUIRE(ArgumentNames(call_a) == ArgumentNames(call_b));
	for (auto &name : ArgumentNames(call_a)) {
		REQUIRE(Argument(call_a, name) == Argument(call_b, name));
	}
	REQUIRE(call_a.partition_keys == call_b.partition_keys);
	auto files_a = a.Files();
	auto files_b = b.Files();
	REQUIRE(files_a.size() == files_b.size());
	for (idx_t i = 0; i < files_a.size(); i++) {
		REQUIRE(files_a[i].path == files_b[i].path);
		if (!call_a.partition_keys.empty()) {
			REQUIRE(FilePartition(a, files_a[i].path).values == FilePartition(b, files_b[i].path).values);
			REQUIRE(FilePartition(a, files_a[i].path).location == FilePartition(b, files_b[i].path).location);
		}
	}
}

vector<string> ColumnNames(const LogicalGet &get) {
	vector<string> names;
	for (auto &name : get.names) {
		names.push_back(name.GetIdentifierName());
	}
	return names;
}

vector<string> ColumnTypes(const LogicalGet &get) {
	vector<string> types;
	for (auto &type : get.returned_types) {
		types.push_back(type.ToString());
	}
	return types;
}

} // namespace

TEST_CASE("A partitioned hive_scan is restated with its partitions and files", "[hive_scan_call]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection other(db);
	TestDirectory dir("partitioned");
	auto root = dir.path + "/orders";
	auto outside = dir.path + "/imports/may16";
	Write(con, "SELECT * FROM (VALUES (1, 1.5::DOUBLE), (2, 2.5::DOUBLE)) t(id, amount)",
	      root + "/dt=2016-05-14/k=1/a.parquet", "FORMAT parquet");
	Write(con, "SELECT 3 AS id, 3.5::DOUBLE AS amount", root + "/dt=2016-05-14/k=1/b.parquet", "FORMAT parquet");
	Write(con, "SELECT 4 AS id, 4.5::DOUBLE AS amount", root + "/dt=2016-05-15/k=__HIVE_DEFAULT_PARTITION__/a.parquet",
	      "FORMAT parquet");
	Write(con, "SELECT 5 AS id, 5.5::DOUBLE AS amount", outside + "/a.parquet", "FORMAT parquet");
	auto query = "SELECT * FROM hive_scan(" + Quote(root + "/") +
	             ", schema := {id: 'INTEGER', amount: 'DOUBLE', dt: 'DATE', k: 'INTEGER'}, partitions := ["
	             "{dt: '2016-05-14', k: '1', location: NULL}, {dt: '2016-05-15', k: NULL, location: NULL}, "
	             "{dt: '2016-05-16', k: '2', location: " +
	             Quote(outside + "/") + "}])";
	{
		PlannedScan scan(con, query);
		auto call = Call(scan);
		REQUIRE(call.root == root);
		REQUIRE(ArgumentNames(call) == std::set<string> {"format", "partition_keys", "schema"});
		REQUIRE(Argument(call, "format") == "'parquet'");
		REQUIRE(Argument(call, "partition_keys") == "['dt', 'k']");
		REQUIRE(Argument(call, "schema") == "{'id': 'INTEGER', 'amount': 'DOUBLE', 'dt': 'DATE', 'k': 'INTEGER'}");
		REQUIRE(call.partition_keys == vector<string> {"dt", "k"});

		REQUIRE(scan.Files().size() == 4);
		auto partition = FilePartition(scan, root + "/dt=2016-05-14/k=1/b.parquet");
		REQUIRE(partition.values == vector<string> {"2016-05-14", "1"});
		REQUIRE(partition.location == root + "/dt=2016-05-14/k=1");
		partition = FilePartition(scan, root + "/dt=2016-05-15/k=__HIVE_DEFAULT_PARTITION__/a.parquet");
		REQUIRE(partition.values == vector<string> {"2016-05-15", "__HIVE_DEFAULT_PARTITION__"});
		partition = FilePartition(scan, outside + "/a.parquet");
		REQUIRE(partition.values == vector<string> {"2016-05-16", "2"});
		REQUIRE(partition.location == outside);
		REQUIRE_FALSE(HiveScanGetFilePartition(scan.Get(), root + "/dt=2016-05-14/k=1/c.parquet").has_value());
	}

	auto call = Restate(con, query);
	REQUIRE(Rows(con, call) == 5);
	REQUIRE(Differences(con, query, call) == 0);

	// a plan pruned by a filter restates only the partitions it reads
	auto pruned = query + " WHERE dt = DATE '2016-05-14' OR k IS NULL";
	auto pruned_call = Restate(con, pruned);
	REQUIRE_FALSE(StringUtil::Contains(pruned_call, "may16"));
	REQUIRE(Rows(con, pruned_call) == 4);
	REQUIRE(Differences(con, pruned, pruned_call) == 0);

	// restating the restated call gives the same call
	PlannedScan original(con, query);
	PlannedScan restated(other, call);
	RequireSameCall(original, restated);
}

TEST_CASE("The csv reading options travel with the call", "[hive_scan_call]") {
	DuckDB db(nullptr);
	Connection con(db);
	TestDirectory dir("csv");
	WriteLines(con, {"generated by a job", "id|name|amount", "1|\"a|b\"|1.5", "2|NA|NA", "3|c", "4|d|4.5|extra"},
	           dir.path + "/serde/data.csv");
	auto query = "SELECT * FROM hive_scan(" + Quote(dir.path + "/serde") +
	             ", format := 'csv', schema := {id: 'INTEGER', name: 'VARCHAR', amount: 'DOUBLE'}, delim := '|', "
	             "quote := '\"', escape := '\\', skip := 2, nullstr := 'NA', serde_fields := true, parallel := false)";
	{
		PlannedScan scan(con, query);
		auto call = Call(scan);
		REQUIRE(ArgumentNames(call) == std::set<string> {"delim", "escape", "format", "null_for_invalid_data",
		                                                 "nullstr", "parallel", "quote", "schema", "serde_fields",
		                                                 "skip"});
		REQUIRE(Argument(call, "format") == "'csv'");
		REQUIRE(Argument(call, "delim") == "'|'");
		REQUIRE(Argument(call, "quote") == "'\"'");
		REQUIRE(Argument(call, "escape") == "'\\'");
		REQUIRE(Argument(call, "skip") == "2");
		REQUIRE(Argument(call, "nullstr") == "'NA'");
		REQUIRE(Argument(call, "serde_fields") == "true");
		REQUIRE(Argument(call, "null_for_invalid_data") == "true");
		REQUIRE(Argument(call, "parallel") == "false");
	}
	auto call = Restate(con, query);
	REQUIRE(Rows(con, call) == 4);
	REQUIRE(Differences(con, query, call) == 0);

	// read_csv rules have no null_for_invalid_data to restate
	{
		PlannedScan scan(con, "SELECT * FROM hive_scan(" + Quote(dir.path + "/serde") +
		                          ", format := 'csv', schema := {id: 'INTEGER'}, delim := '|', skip := 2)");
		auto plain = Call(scan);
		REQUIRE(Argument(plain, "serde_fields") == "false");
		REQUIRE(plain.arguments.count(Identifier("null_for_invalid_data")) == 0);
	}

	// null_for_invalid_data := false: the call fails on a value that does not parse, as the scan does
	WriteLines(con, {"id|amount", "1|oops"}, dir.path + "/invalid/data.csv");
	auto invalid = "SELECT * FROM hive_scan(" + Quote(dir.path + "/invalid") +
	               ", format := 'csv', schema := {id: 'INTEGER', amount: 'DOUBLE'}, delim := '|', skip := 1, "
	               "serde_fields := true";
	auto strict_call = Restate(con, invalid + ", null_for_invalid_data := false)");
	REQUIRE(StringUtil::Contains(ReadError(con, strict_call), "Could not convert string 'oops' to DOUBLE"));
	auto lenient_call = Restate(con, invalid + ")");
	REQUIRE(Scalar(con, "SELECT amount IS NULL FROM (" + lenient_call + ")").GetValue<bool>());

	// parallel is what hive_csv_parallel was in the session that planned the scan, not in the one running the call
	WriteLines(con, {"1,\"two\nlines\"", "2,after", "3"}, dir.path + "/line_break/data.csv");
	auto line_break = "SELECT * FROM hive_scan(" + Quote(dir.path + "/line_break") +
	                  ", format := 'csv', serde_fields := true, schema := {id: 'INTEGER', name: 'VARCHAR'})";
	Run(con, "SET hive_csv_parallel = false");
	auto one_thread = Restate(con, line_break);
	Run(con, "SET hive_csv_parallel = true");
	REQUIRE(StringUtil::Contains(one_thread, "parallel := false"));
	REQUIRE(Rows(con, one_thread) == 3);
	auto several_threads = Restate(con, line_break);
	REQUIRE(StringUtil::Contains(ReadError(con, several_threads), "parallel scanner does not support null_padding"));
}

TEST_CASE("json and avro scans are restated", "[hive_scan_call]") {
	DuckDB db(nullptr);
	Connection con(db);
	TestDirectory dir("json_avro");
	WriteLines(con, {"{\"id\": 1, \"name\": \"a\"}", "not json", "{\"id\": 3, \"name\": \"c\"}"},
	           dir.path + "/json/data.json");
	auto json = "SELECT * FROM hive_scan(" + Quote(dir.path + "/json") +
	            ", format := 'json', schema := {id: 'INTEGER', name: 'VARCHAR'}, ignore_errors := true)";
	{
		PlannedScan scan(con, json);
		auto call = Call(scan);
		REQUIRE(ArgumentNames(call) == std::set<string> {"format", "ignore_errors", "schema"});
		REQUIRE(Argument(call, "format") == "'json'");
		REQUIRE(Argument(call, "ignore_errors") == "true");
	}
	auto json_call = Restate(con, json);
	REQUIRE(Rows(con, json_call) == 3);
	REQUIRE(Differences(con, json, json_call) == 0);

	Write(con, "SELECT * FROM (VALUES ('a', 1), ('b', NULL)) t(flight, delay)", dir.path + "/avro/year=2020/a.avro",
	      "FORMAT avro");
	Write(con, "SELECT 'c' AS flight, 3 AS delay", dir.path + "/avro/year=2021/a.avro", "FORMAT avro");
	auto avro = "SELECT * FROM hive_scan(" + Quote(dir.path + "/avro") +
	            ", format := 'avro', schema := {flight: 'VARCHAR', delay: 'INTEGER', year: 'INTEGER'}, "
	            "partitions := [{year: '2020'}, {year: '2021'}])";
	{
		PlannedScan scan(con, avro);
		auto call = Call(scan);
		REQUIRE(ArgumentNames(call) == std::set<string> {"format", "partition_keys", "schema"});
		REQUIRE(Argument(call, "format") == "'avro'");
	}
	auto avro_call = Restate(con, avro);
	REQUIRE(Rows(con, avro_call) == 3);
	REQUIRE(Differences(con, avro, avro_call) == 0);
}

TEST_CASE("Column names and nested types survive the schema argument", "[hive_scan_call]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection other(db);
	TestDirectory dir("names");
	Write(con,
	      "SELECT 1 AS \"Mixed Case\", 'x' AS \"a b\", {'x y': 1, 'z': ['p', 'q']} AS \"it's\", 1.25::DECIMAL(10,2) "
	      "AS d, MAP {'k': 1} AS m, '\\x01\\x02'::BLOB AS bin, TIMESTAMP '2016-05-14 10:00:00' AS ts",
	      dir.path + "/t/a.parquet", "FORMAT parquet");
	auto query = "SELECT * FROM hive_scan(" + Quote(dir.path + "/t") +
	             ", schema := {'Mixed Case': 'INTEGER', 'a b': 'VARCHAR', 'it''s': 'STRUCT(\"x y\" INTEGER, z "
	             "VARCHAR[])', d: 'DECIMAL(10,2)', m: 'MAP(VARCHAR, INTEGER)', bin: 'BLOB', ts: 'TIMESTAMP'})";
	auto call = Restate(con, query);
	{
		PlannedScan original(con, query);
		PlannedScan restated(other, call);
		REQUIRE(ColumnNames(restated.Get()) == ColumnNames(original.Get()));
		REQUIRE(ColumnTypes(restated.Get()) == ColumnTypes(original.Get()));
	}
	REQUIRE(Differences(con, query, call) == 0);
}

TEST_CASE("Given files keep the options they are opened with", "[hive_scan_call]") {
	DuckDB db(nullptr);
	Connection con(db);
	TestDirectory dir("given");
	auto path = dir.path + "/t/dt=d1/a.parquet";
	Write(con, "SELECT range AS id FROM range(3)", path, "FORMAT parquet");
	auto size = std::to_string(std::filesystem::file_size(path));
	auto query =
	    "SELECT * FROM hive_scan(" + Quote(dir.path + "/t") +
	    ", schema := {id: 'BIGINT', dt: 'VARCHAR'}, partitions := [{dt: 'd1', files: [{filename: " + Quote(path) +
	    ", file_size: " + size + "}]}])";
	auto call = Restate(con, query);
	REQUIRE(StringUtil::Contains(call, "{'filename': " + Quote(path) + ", 'file_size': " + size + "}"));
	REQUIRE(Rows(con, call) == 3);
	REQUIRE(Differences(con, query, call) == 0);
}

TEST_CASE("A scan that is not a Hive scan, and a table without partition keys", "[hive_scan_call]") {
	DuckDB db(nullptr);
	Connection con(db);
	TestDirectory dir("plain");
	auto path = dir.path + "/t/a.parquet";
	Write(con, "SELECT 1 AS id", path, "FORMAT parquet");
	{
		PlannedScan scan(con, "SELECT * FROM read_parquet(" + Quote(path) + ")");
		REQUIRE_FALSE(HiveScanGetCall(scan.Get()).has_value());
		REQUIRE_FALSE(HiveScanGetFilePartition(scan.Get(), path).has_value());
	}
	{
		PlannedScan scan(con, "SELECT * FROM range(3)");
		REQUIRE_FALSE(HiveScanGetCall(scan.Get()).has_value());
	}

	auto query = "SELECT * FROM hive_scan(" + Quote(dir.path + "/t") + ", schema := {id: 'INTEGER'})";
	{
		PlannedScan scan(con, query);
		auto call = Call(scan);
		REQUIRE(ArgumentNames(call) == std::set<string> {"format", "schema"});
		REQUIRE(call.partition_keys.empty());
		REQUIRE(scan.Files().size() == 1);
		REQUIRE_FALSE(HiveScanGetFilePartition(scan.Get(), path).has_value());
	}
	auto call = Restate(con, query);
	REQUIRE(StringUtil::Contains(call, ", files := ["));
	REQUIRE(Differences(con, query, call) == 0);
}

namespace {

//! Only the local moto of make glue-fixture: the Glue table tests create and drop tables
string LocalGlueEndpoint() {
	auto endpoint = Env("GLUE_ENDPOINT", "");
	if (StringUtil::StartsWith(endpoint, "http://localhost:") ||
	    StringUtil::StartsWith(endpoint, "http://127.0.0.1:")) {
		return endpoint;
	}
	return string();
}

//! Dropped when the test ends, also when it fails
class GlueTables {
public:
	GlueTables(Connection &con_p, string prefix_p) : con(con_p), prefix(std::move(prefix_p)) {
	}
	~GlueTables() {
		for (auto &name : names) {
			con.Query("DROP TABLE IF EXISTS lake.default." + name);
		}
	}

public:
	string Add(const string &suffix) {
		names.push_back(prefix + suffix);
		return names.back();
	}

private:
	Connection &con;
	string prefix;
	vector<string> names;
};

} // namespace

TEST_CASE("Glue tables of every SerDe are restated", "[hive_scan_call][glue_catalog]") {
	auto endpoint = LocalGlueEndpoint();
	if (endpoint.empty()) {
		WARN("skipped: GLUE_ENDPOINT does not name the local Glue (make glue-fixture, make test-cpp)");
		return;
	}
	DuckDB db(nullptr);
	Connection con(db);
	auto unique = Unique();
	// the S3 secret and catalog of test/configs/local_glue.json
	auto location = Env("DEFAULT_S3_LOCATION", "s3://glue-test/warehouse") + "/unittest_glue/" + unique;
	Run(con, "CREATE SECRET (TYPE S3, KEY_ID 'minio', SECRET 'minio123', REGION 'us-east-1', ENDPOINT " +
	             Quote(Env("S3_ENDPOINT", "localhost:9100")) + ", USE_SSL false, URL_STYLE 'path')");
	Run(con, "ATTACH " + Quote(Env("GLUE_CATALOG_ID", "123456789012")) + " AS lake (TYPE glue, ENDPOINT " +
	             Quote(endpoint) + ", DEFAULT_LOCATION " + Quote(location) + ")");
	GlueTables tables(con, "ugc_" + unique + "_");

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
	{
		PlannedScan scan(con, query);
		auto parquet_call = Call(scan);
		REQUIRE(ArgumentNames(parquet_call) == std::set<string> {"format", "partition_keys", "schema"});
		REQUIRE(Argument(parquet_call, "partition_keys") == "['dt', 'k']");
		bool null_partition = false;
		bool outside_partition = false;
		for (auto &file : scan.Files()) {
			auto partition = FilePartition(scan, file.path);
			null_partition |= partition.values == vector<string> {"2016-05-15", "__HIVE_DEFAULT_PARTITION__"};
			outside_partition |= partition.location == location + "/imports/p";
		}
		REQUIRE(null_partition);
		REQUIRE(outside_partition);
	}
	auto call = Restate(con, query);
	REQUIRE(Rows(con, call) == 4);
	REQUIRE(Differences(con, query, call) == 0);
	auto pruned = query + " WHERE k IS NULL OR k = 2";
	REQUIRE(Differences(con, pruned, Restate(con, pruned)) == 0);

	// LazySimpleSerDe: '|', NULL is 'NA', two header lines
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
		auto lazy_call = Call(scan);
		REQUIRE(Argument(lazy_call, "delim") == "'|'");
		REQUIRE(Argument(lazy_call, "quote") == "''");
		REQUIRE(Argument(lazy_call, "escape") == "''");
		REQUIRE(Argument(lazy_call, "skip") == "2");
		REQUIRE(Argument(lazy_call, "nullstr") == "'NA'");
		REQUIRE(Argument(lazy_call, "serde_fields") == "true");
		REQUIRE(Argument(lazy_call, "null_for_invalid_data") == "true");
		REQUIRE(Argument(lazy_call, "parallel") == "true");
	}
	call = Restate(con, query);
	REQUIRE(Rows(con, call) == 5);
	REQUIRE(Differences(con, query, call) == 0);

	// OpenCSVSerde: quoted, one header line, no NULL, and a value that does not parse fails the query
	auto opencsv = tables.Add("opencsv");
	Run(con, "CREATE TABLE lake.default." + opencsv +
	             " (id INTEGER, name VARCHAR, amount DOUBLE) WITH (format = 'csv', quote = '\"', "
	             "'skip.header.line.count' = '1')");
	auto opencsv_location = location + "/default/" + opencsv;
	WriteLines(con, {"id,name,amount", "1,\"a,b\",1.5", "2,", "3,\"x \"\"y\"\"\",3.5,extra", "4"},
	           opencsv_location + "/data.csv");
	query = "SELECT * FROM lake.default." + opencsv;
	{
		PlannedScan scan(con, query);
		auto opencsv_call = Call(scan);
		REQUIRE(Argument(opencsv_call, "delim") == "','");
		REQUIRE(Argument(opencsv_call, "quote") == "'\"'");
		REQUIRE(Argument(opencsv_call, "escape") == "'\"'");
		REQUIRE(Argument(opencsv_call, "nullstr") == Quote("\n"));
		REQUIRE(Argument(opencsv_call, "null_for_invalid_data") == "false");
		REQUIRE(Argument(opencsv_call, "parallel") == "true");
	}
	call = Restate(con, query);
	REQUIRE(Rows(con, call) == 4);
	REQUIRE(Differences(con, query, call) == 0);

	WriteLines(con, {"id,name,amount", "5,e,oops"}, opencsv_location + "/invalid.csv");
	REQUIRE(StringUtil::Contains(ReadError(con, Restate(con, query)), "Could not convert string 'oops' to DOUBLE"));
	Run(con, "ALTER TABLE lake.default." + opencsv + " SET ('use.null.for.invalid.data' = 'true')");
	call = Restate(con, query);
	REQUIRE(StringUtil::Contains(call, "null_for_invalid_data := true"));
	REQUIRE(Rows(con, call) == 5);
	REQUIRE(Differences(con, query, call) == 0);

	// a quoted line break: planned with hive_csv_parallel off, the call reads it in a session that has it on
	WriteLines(con, {"id,name,amount", "6,\"two\nlines\",6.5", "7"}, opencsv_location + "/line_break.csv");
	Run(con, "SET hive_csv_parallel = false");
	call = Restate(con, query);
	Run(con, "RESET hive_csv_parallel");
	REQUIRE(StringUtil::Contains(call, "parallel := false"));
	REQUIRE(StringUtil::Contains(ReadError(con, query), "parallel scanner does not support null_padding"));
	REQUIRE(Rows(con, call) == 7);

	// json and avro
	auto json = tables.Add("json");
	Run(con, "CREATE TABLE lake.default." + json +
	             " (id INTEGER, name VARCHAR, s STRUCT(a INTEGER), dt VARCHAR) PARTITIONED BY (dt) WITH (format = "
	             "'json')");
	Run(con, "INSERT INTO lake.default." + json +
	             " VALUES (1, 'a', {a: 1}, 'd1'), (2, NULL, NULL, 'd1'), (3, 'c', "
	             "{a: 3}, 'd2')");
	query = "SELECT * FROM lake.default." + json;
	{
		PlannedScan scan(con, query);
		auto json_call = Call(scan);
		REQUIRE(ArgumentNames(json_call) == std::set<string> {"format", "ignore_errors", "partition_keys", "schema"});
		REQUIRE(Argument(json_call, "ignore_errors") == "false");
	}
	call = Restate(con, query);
	REQUIRE(Rows(con, call) == 3);
	REQUIRE(Differences(con, query, call) == 0);

	auto avro = tables.Add("avro");
	Run(con, "CREATE TABLE lake.default." + avro +
	             " (flight VARCHAR, delay INTEGER, year INTEGER) PARTITIONED BY (year) WITH (format = 'avro')");
	Run(con, "INSERT INTO lake.default." + avro + " VALUES ('a', 1, 2020), ('b', NULL, 2020), ('c', 3, 2021)");
	query = "SELECT * FROM lake.default." + avro;
	call = Restate(con, query);
	REQUIRE(StringUtil::Contains(call, "format := 'avro'"));
	REQUIRE(Rows(con, call) == 3);
	REQUIRE(Differences(con, query, call) == 0);
}
