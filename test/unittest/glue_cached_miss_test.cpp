#include "glue_test_util.hpp"
#include "catalog/glue_transaction.hpp"

namespace duckdb {

TEST_CASE("A definition a listing stores replaces a kept miss but not a cached table", "[glue_catalog]") {
	GlueTransactionCache cache;
	auto none = []() -> shared_ptr<const GlueTableInfo> {
		return nullptr;
	};
	REQUIRE(!cache.GetTable("db", "m", none, 1));
	auto listed = make_shared_ptr<const GlueTableInfo>();
	cache.PutTable("db", "m", listed);
	REQUIRE(cache.GetTable("db", "m", none, 1) == listed);
	REQUIRE(cache.GetTable("db", "m", none, 2) == listed);

	auto cached = make_shared_ptr<const GlueTableInfo>();
	REQUIRE(cache.GetTable("db", "t", [&]() { return cached; }) == cached);
	cache.PutTable("db", "t", make_shared_ptr<const GlueTableInfo>());
	REQUIRE(cache.GetTable("db", "t", none) == cached);

	// a table never looked up is answered from what was stored, in any case
	auto fresh = make_shared_ptr<const GlueTableInfo>();
	cache.PutTable("db", "Fresh", fresh);
	REQUIRE(cache.GetTable("DB", "fresh", [&]() -> shared_ptr<const GlueTableInfo> {
		FAIL("loaded a table PutTable stored");
		return nullptr;
	}) == fresh);
}

TEST_CASE("A table lookup that throws is not kept and a miss only serves its own query", "[glue_catalog]") {
	GlueTransactionCache cache;
	auto table = make_shared_ptr<const GlueTableInfo>();
	REQUIRE_THROWS(
	    cache.GetTable("db", "t", []() -> shared_ptr<const GlueTableInfo> { throw IOException("throttled"); }));
	REQUIRE(!cache.IsTableResolved("db", "t"));
	REQUIRE(cache.GetTable("db", "t", [&]() { return table; }) == table);

	idx_t loads = 0;
	auto miss = [&]() -> shared_ptr<const GlueTableInfo> {
		loads++;
		return nullptr;
	};
	REQUIRE(!cache.GetTable("db", "m", miss, 1));
	REQUIRE(cache.IsTableResolved("DB", "M", 1));
	REQUIRE(!cache.GetTable("db", "m", miss, 1));
	REQUIRE(loads == 1);
	REQUIRE(!cache.IsTableResolved("db", "m", 2));
	REQUIRE(!cache.IsTableResolved("db", "m"));
	REQUIRE(cache.IsTableResolved("db", "t", 2));
	REQUIRE(!cache.GetTable("db", "m", miss, 2));
	REQUIRE(loads == 2);

	// outside a statement a miss is never kept
	REQUIRE(!cache.GetTable("db", "o", miss));
	REQUIRE(!cache.GetTable("db", "o", miss));
	REQUIRE(loads == 4);
}

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

	// TableInfo looks the table up outside a statement
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

TEST_CASE("A table miss is not kept from one API lookup to the next in an explicit transaction", "[glue_catalog]") {
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
	GlueTables tables(other, "acb_" + unique + "_");
	auto table = tables.Add("t");

	for (auto scope : {"connection", "transaction"}) {
		INFO("glue_metadata_cache_scope = " << scope);
		Run(con, string("SET glue_metadata_cache_scope = '") + scope + "'");
		other.Query("DROP TABLE IF EXISTS lake.default." + table);
		Run(con, "BEGIN");
		REQUIRE(!con.TableInfo(Identifier("lake"), Identifier("default"), Identifier(table)));
		Run(other, "CREATE TABLE lake.default." + table + " (id INTEGER)");
		REQUIRE(con.TableInfo(Identifier("lake"), Identifier("default"), Identifier(table)));
		Run(con, "COMMIT");
	}
}

} // namespace duckdb
