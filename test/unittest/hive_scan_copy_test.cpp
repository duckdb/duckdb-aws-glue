#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "glue_test_util.hpp"

using namespace duckdb;

//! The scans of a plan, in the order a depth-first walk meets them
static void CollectScans(LogicalOperator &op, vector<reference<LogicalGet>> &scans) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		scans.push_back(op.Cast<LogicalGet>());
	}
	for (auto &child : op.children) {
		CollectScans(*child, scans);
	}
}

static vector<reference<LogicalGet>> Scans(LogicalOperator &plan) {
	vector<reference<LogicalGet>> scans;
	CollectScans(plan, scans);
	return scans;
}

TEST_CASE("A copy of a plan reports the Glue table of its hive scans", "[hive_scan][glue_catalog]") {
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
	GlueTables tables(con, "hsc_" + unique + "_");
	auto table = "lake.default." + tables.Add("orders");
	Run(con, "CREATE TABLE " + table + " (id INTEGER, amount INTEGER, dt VARCHAR) PARTITIONED BY (dt)");
	Run(con, "INSERT INTO " + table + " VALUES (1, 10, 'd1'), (2, 20, 'd2'), (3, 30, 'd2')");

	// The optimizer copies a plan within the statement's transaction (CTE inlining copies a CTE's definition into each
	// reference), through a serialization round trip
	Run(con, "BEGIN");
	auto plan = con.ExtractPlan("SELECT id, amount FROM " + table);
	auto scans = Scans(*plan);
	REQUIRE(scans.size() == 1);
	auto scanned = scans[0].get().GetTable();
	REQUIRE(scanned);

	auto copy = plan->Copy(*con.context);
	auto copied = Scans(*copy);
	REQUIRE(copied.size() == 1);
	REQUIRE(copied[0].get().GetTable().get() == scanned.get());

	// a pruned scan is copied with its partitions and its table
	auto pruned_plan = con.ExtractPlan("SELECT id FROM " + table + " WHERE dt = 'd2'");
	auto pruned_copy = pruned_plan->Copy(*con.context);
	auto pruned = Scans(*pruned_copy);
	REQUIRE(pruned.size() == 1);
	REQUIRE(pruned[0].get().GetTable().get() == scanned.get());
	Run(con, "COMMIT");

	// a hive_scan() call reads a location, not a table: its copy has no table either
	Run(con, "BEGIN");
	auto location_plan = con.ExtractPlan("SELECT * FROM hive_scan(" + Quote(location + "/hive_scan_" + unique) +
	                                     ", schema := {id: 'INTEGER'})");
	auto location_scans = Scans(*location_plan);
	REQUIRE(location_scans.size() == 1);
	REQUIRE(!location_scans[0].get().GetTable());
	auto location_copy = location_plan->Copy(*con.context);
	auto location_copied = Scans(*location_copy);
	REQUIRE(location_copied.size() == 1);
	REQUIRE(!location_copied[0].get().GetTable());
	Run(con, "COMMIT");
}
