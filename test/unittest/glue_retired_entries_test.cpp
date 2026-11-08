#include "catalog/glue_transaction_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "glue_test_util.hpp"

using namespace duckdb;

static optional_ptr<LogicalGet> FindScan(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		return &op.Cast<LogicalGet>();
	}
	for (auto &child : op.children) {
		auto scan = FindScan(*child);
		if (scan) {
			return scan;
		}
	}
	return nullptr;
}

static GlueTransactionManager &Manager(Connection &con) {
	optional_ptr<GlueTransactionManager> manager;
	con.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con.context, Identifier("lake"));
		manager = &catalog.GetAttached().GetTransactionManager().Cast<GlueTransactionManager>();
	});
	return *manager;
}

TEST_CASE("An idle transaction does not keep the catalog entries other connections retire", "[glue_catalog]") {
	auto endpoint = LocalGlueEndpoint();
	if (endpoint.empty()) {
		WARN("skipped: GLUE_ENDPOINT does not name the local Glue (make glue-fixture, make test-cpp)");
		return;
	}
	DuckDB db(nullptr);
	Connection con(db);
	Connection idle(db);
	auto unique = Unique();
	auto location = Env("DEFAULT_S3_LOCATION", "s3://glue-test/warehouse") + "/unittest_glue/" + unique;
	AttachLocalGlue(con, endpoint, location);
	GlueTables tables(con, "ret_" + unique + "_");
	auto name = tables.Add("t");
	auto table = "lake.default." + name;
	Run(con, "CREATE TABLE " + table + " (id INTEGER)");
	auto &manager = Manager(con);

	// each ALTER replaces the cached entry of the table; the transaction left open between statements uses none
	Run(idle, "BEGIN");
	Run(idle, "SELECT count(*) FROM " + table);
	for (idx_t i = 0; i < 5; i++) {
		Run(con, "ALTER TABLE " + table + " ADD COLUMN c" + std::to_string(i) + " INTEGER");
	}
	REQUIRE(manager.RetiredEntryCount() == 0);

	// a plan bound outside a statement (a client API call) keeps the entries it refers to until the transaction ends
	auto plan = idle.ExtractPlan("SELECT id FROM " + table);
	auto scan = FindScan(*plan);
	REQUIRE(scan);
	auto scanned = scan->GetTable();
	REQUIRE(scanned);
	Run(con, "ALTER TABLE " + table + " ADD COLUMN c5 INTEGER");
	REQUIRE(manager.RetiredEntryCount() >= 1);
	// read through the entry the ALTER retired: freed memory here is what the pin prevents
	REQUIRE(scanned->name.GetIdentifierName() == name);
	Run(idle, "COMMIT");
	REQUIRE(manager.RetiredEntryCount() == 0);
}
