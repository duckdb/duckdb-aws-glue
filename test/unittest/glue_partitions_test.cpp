#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "core/glue_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "glue_test_util.hpp"

using namespace duckdb;

//! The first column of a query, one string per row
static vector<string> Column(Connection &con, const string &sql) {
	auto result = Run(con, sql);
	vector<string> rows;
	while (auto chunk = result->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			rows.push_back(chunk->GetValue(0, i).ToString());
		}
	}
	return rows;
}

// Registers a partition the way another tool would: with the strings as given, which glue_add_partition cannot do
// because it casts the values to the key types first
static void RegisterRawPartition(Connection &con, const string &table, const vector<string> &values,
                                 const string &location) {
	// the API needs a transaction; a failure rolls it back. No REQUIRE inside: Catch's failure exception is not a
	// std::exception, RunFunctionInTransaction would not see it and the transaction would stay open
	bool found = false;
	con.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con.context, Identifier("lake")).Cast<GlueCatalog>();
		GlueTableInfo table_info;
		found = GlueAPI::GetTable(*con.context, catalog, "default", table, table_info);
		if (!found) {
			return;
		}
		GluePartitionInput partition;
		partition.values = values;
		partition.location = location;
		GlueAPI::CreatePartition(*con.context, catalog, table_info, partition, false);
	});
	REQUIRE(found);
}

TEST_CASE("glue_partitions lists the stored values next to the typed keys", "[glue_partitions][glue_catalog]") {
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
	GlueTables tables(con, "gpv_" + unique + "_");
	auto table = tables.Add("ints");
	auto name = Quote("lake.default." + table);
	Run(con, "CREATE TABLE lake.default." + table + " (id INTEGER, dt VARCHAR, k INTEGER) PARTITIONED BY (dt, k)");
	Run(con, "CALL glue_add_partition(" + name + ", {dt: 'd1', k: 5})");
	Run(con, "CALL glue_add_partition(" + name + ", {dt: 'd1', k: NULL})");
	// a value an INTEGER cannot hold: the typed column reads NULL, as for the NULL partition
	RegisterRawPartition(con, table, {"d1", "abc"}, location + "/default/" + table + "/dt=d1/k=abc");

	// the stored strings, in partition key order, next to the typed columns
	auto listing = Column(con, "SELECT dt || '/' || coalesce(k::VARCHAR, 'NULL') || ' ' || partition_values::VARCHAR "
	                           "FROM glue_partitions(" +
	                               name + ") ORDER BY partition_values");
	REQUIRE(listing ==
	        vector<string> {"d1/5 [d1, 5]", "d1/NULL [d1, __HIVE_DEFAULT_PARTITION__]", "d1/NULL [d1, abc]"});
	// the NULL partition is the one whose stored value is the sentinel
	auto null_partition = Column(con, "SELECT location FROM glue_partitions(" + name +
	                                      ") WHERE k IS NULL AND partition_values[2] = '__HIVE_DEFAULT_PARTITION__'");
	REQUIRE(null_partition == vector<string> {location + "/default/" + table + "/dt=d1/k=__HIVE_DEFAULT_PARTITION__"});
	auto columns = Column(
	    con, "SELECT column_name || ' ' || column_type FROM (DESCRIBE SELECT * FROM glue_partitions(" + name + "))");
	REQUIRE(columns == vector<string> {"dt VARCHAR", "k INTEGER", "location VARCHAR", "partition_values VARCHAR[]"});
}
