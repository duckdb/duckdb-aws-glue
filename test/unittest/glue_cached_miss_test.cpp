#include "glue_test_util.hpp"

namespace duckdb {

TEST_CASE("A table miss from an API lookup does not outlive it", "[glue_catalog]") {
	auto endpoint = LocalGlueEndpoint();
	if (endpoint.empty()) {
		WARN("skipped: GLUE_ENDPOINT does not name the local Glue (make glue-fixture, make test-cpp)");
		return;
	}
	DuckDB db(nullptr);
	Connection con(db);
	Connection other(db);
	auto unique = Unique();
	auto location = Env("DEFAULT_S3_LOCATION", "s3://glue-test/warehouse") + "/unittest_glue/" + unique;
	AttachLocalGlue(con, endpoint, location);
	GlueTables tables(other, "acm_" + unique + "_");
	auto table = tables.Add("t");
	Run(con, "SET glue_metadata_cache_scope = 'connection'");

	// TableInfo looks the table up outside a query, so no QueryEnd follows it
	REQUIRE(!con.TableInfo(Identifier("lake"), Identifier("default"), Identifier(table)));
	Run(other, "CREATE TABLE lake.default." + table + " (id INTEGER)");
	Run(con, "DROP TABLE IF EXISTS lake.default." + table);

	auto response = other.Query("SELECT * FROM glue_get_table_response(" + Quote("lake.default." + table) + ")");
	REQUIRE(response->HasError());
	REQUIRE(StringUtil::Contains(response->GetError(), "does not exist"));
}

TEST_CASE("A table miss is not kept from one API lookup to the next", "[glue_catalog]") {
	auto endpoint = LocalGlueEndpoint();
	if (endpoint.empty()) {
		WARN("skipped: GLUE_ENDPOINT does not name the local Glue (make glue-fixture, make test-cpp)");
		return;
	}
	DuckDB db(nullptr);
	Connection con(db);
	Connection other(db);
	auto unique = Unique();
	auto location = Env("DEFAULT_S3_LOCATION", "s3://glue-test/warehouse") + "/unittest_glue/" + unique;
	AttachLocalGlue(con, endpoint, location);
	GlueTables tables(other, "aca_" + unique + "_");
	auto table = tables.Add("t");
	Run(con, "SET glue_metadata_cache_scope = 'connection'");

	REQUIRE(!con.TableInfo(Identifier("lake"), Identifier("default"), Identifier(table)));
	Run(other, "CREATE TABLE lake.default." + table + " (id INTEGER)");
	REQUIRE(con.TableInfo(Identifier("lake"), Identifier("default"), Identifier(table)));
}

TEST_CASE("A table miss from an API lookup in a transaction ends with it under the default scope", "[glue_catalog]") {
	auto endpoint = LocalGlueEndpoint();
	if (endpoint.empty()) {
		WARN("skipped: GLUE_ENDPOINT does not name the local Glue (make glue-fixture, make test-cpp)");
		return;
	}
	DuckDB db(nullptr);
	Connection con(db);
	Connection other(db);
	auto unique = Unique();
	auto location = Env("DEFAULT_S3_LOCATION", "s3://glue-test/warehouse") + "/unittest_glue/" + unique;
	AttachLocalGlue(con, endpoint, location);
	GlueTables tables(other, "act_" + unique + "_");
	auto table = tables.Add("t");

	Run(con, "BEGIN");
	REQUIRE(!con.TableInfo(Identifier("lake"), Identifier("default"), Identifier(table)));
	Run(other, "CREATE TABLE lake.default." + table + " (id INTEGER)");
	auto result = con.Query("SELECT count(*) FROM lake.default." + table);
	REQUIRE(!result->HasError());
	Run(con, "COMMIT");
}

} // namespace duckdb
