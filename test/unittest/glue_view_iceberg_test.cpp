#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"
#include "catalog/glue_view_names.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "glue_test_util.hpp"

using namespace duckdb;

namespace {

class TestIcebergCatalog : public DuckCatalog {
public:
	TestIcebergCatalog(AttachedDatabase &db, string warehouse) : DuckCatalog(db), warehouse(std::move(warehouse)) {
	}
	string GetCatalogType() override {
		return "iceberg";
	}
	string GetDBPath() override {
		return warehouse;
	}

private:
	string warehouse;
};

struct TestIcebergInfo : StorageExtensionInfo {
	string path;
};

class TestIcebergStorage : public StorageExtension {
public:
	explicit TestIcebergStorage(shared_ptr<TestIcebergInfo> info) {
		storage_info = std::move(info);
		attach = [](optional_ptr<StorageExtensionInfo> storage_info, ClientContext &, AttachedDatabase &db,
		            const string &, AttachInfo &info, AttachOptions &) -> unique_ptr<Catalog> {
			auto warehouse = info.path;
			info.path = static_cast<TestIcebergInfo &>(*storage_info).path;
			return make_uniq<TestIcebergCatalog>(db, warehouse);
		};
		create_transaction_manager = [](optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
		                                Catalog &) -> unique_ptr<TransactionManager> {
			return make_uniq<DuckTransactionManager>(db);
		};
	}
};

class IcebergViewFixture {
public:
	explicit IcebergViewFixture(bool map_iceberg = true, const string &iceberg_warehouse = "123456789012") {
		directory = "glue_view_iceberg_" + Unique();
		auto fs = FileSystem::CreateLocal();
		fs->CreateDirectory(directory);
		fs->CreateDirectory(directory + "/customers");
		auto iceberg = make_shared_ptr<TestIcebergInfo>();
		iceberg->path = directory + "/iceberg.duckdb";
		{
			DuckDB seed(iceberg->path);
			Connection con(seed);
			Run(con, "CREATE SCHEMA sales");
			Run(con, "CREATE TABLE sales.orders AS SELECT 1 AS id, 42 AS amount");
			Run(con, "CREATE TABLE sales.hive_in_glue AS SELECT 1 AS id");
			Run(con, "COPY (SELECT 1 AS id, 'Ada' AS name) TO " + Quote(directory + "/customers/data.parquet") +
			             " (FORMAT PARQUET)");
		}
		DBConfig config;
		StorageExtension::Register(config, "iceberg", make_shared_ptr<TestIcebergStorage>(iceberg));
		db = make_uniq<DuckDB>(nullptr, &config);
		con = make_uniq<Connection>(*db);
		Run(*con, "CREATE SECRET view_test (TYPE S3, KEY_ID 'test', SECRET 'test', REGION 'us-east-1')");
		Run(*con, "SET glue_metadata_cache_scope = 'connection'");
		Run(*con, "SET glue_batch_table_lookup = false");
		Run(*con, "ATTACH '123456789012' AS lake (TYPE GLUE, SECRET view_test, REGION 'us-east-1', "
		          "ENDPOINT 'http://127.0.0.1:1', VERIFY_CONNECTION false" +
		              string(map_iceberg ? ", ICEBERG_CATALOG 'lake_ice'" : "") + ")");
		Run(*con, "ATTACH " + Quote(iceberg_warehouse) + " AS lake_ice (TYPE iceberg)");
		Run(*con, "BEGIN");
		auto &catalog = Catalog::GetCatalog(*con->context, Identifier("lake")).Cast<GlueCatalog>();
		GlueDatabaseInfo schema;
		schema.name = "sales";
		schema.catalog_id = "123456789012";
		catalog.GetSchemas().CreateEntry(catalog.GetSchemas().CreateSchemaEntry(schema));
		Run(*con, "COMMIT");

		GlueTableInfo customers;
		customers.name = "customers";
		customers.schema_name = "sales";
		customers.glue_database_name = "sales";
		customers.table_type = GlueTableType::EXTERNAL_TABLE;
		customers.glue_table_type = "EXTERNAL_TABLE";
		customers.location = directory + "/customers";
		customers.serde_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
		customers.columns = {{"id", "int", ""}, {"name", "string", ""}};
		tables.push_back(customers);
		auto hive_in_glue = customers;
		hive_in_glue.name = "hive_in_glue";
		tables.push_back(hive_in_glue);
		auto orders = customers;
		orders.name = "orders";
		orders.parameters["table_type"] = "ICEBERG";
		orders.columns = {{"id", "bigint", ""}, {"outdated_amount", "string", ""}};
		tables.push_back(orders);
		AddView("orders_by_customer",
		        "SELECT c.name, o.amount FROM sales.customers c JOIN sales.orders o ON c.id = o.id");
		AddView("qualified_view", "SELECT sales.orders.amount, orders.id FROM sales.orders");
		AddView("customers_view", "SELECT name FROM sales.customers");
		Warm();
	}

	~IcebergViewFixture() {
		con.reset();
		db.reset();
		FileSystem::CreateLocal()->RemoveDirectory(directory);
	}

	void AddView(const string &name, const string &sql) {
		GlueTableInfo view;
		view.name = name;
		view.schema_name = "sales";
		view.glue_database_name = "sales";
		view.table_type = GlueTableType::VIRTUAL_VIEW;
		view.glue_table_type = "VIRTUAL_VIEW";
		view.parameters = {{"duckdb_view", "true"}, {"duckdb_view_default_database", "sales"}};
		view.view_original_text = sql;
		view.view_expanded_text = sql;
		view.version_id = "1";
		tables.push_back(std::move(view));
	}

	void Warm() {
		Run(*con, "BEGIN");
		auto &catalog = Catalog::GetCatalog(*con->context, Identifier("lake"));
		auto &transaction = GlueTransaction::Get(*con->context, catalog);
		for (auto &table : tables) {
			transaction.cache->GetTable("sales", table.name,
			                            [&]() { return make_shared_ptr<const GlueTableInfo>(table); });
		}
		Run(*con, "COMMIT");
	}

	//! The SQL CREATE VIEW would store for a body in database sales
	string Store(const string &sql) {
		auto parser = Parser::GetBuiltinParser();
		auto statement = CreateViewInfo::ParseSelect(parser, sql);
		auto &catalog = Catalog::GetCatalog(*con->context, Identifier("lake")).Cast<GlueCatalog>();
		GlueViewNames::QualifyForStorage(*con->context, catalog, Identifier("sales"), *statement->node);
		return statement->ToString();
	}

public:
	string directory;
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> con;
	vector<GlueTableInfo> tables;
};

string ErrorOf(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	REQUIRE(result->HasError());
	return result->GetError();
}

} // namespace

TEST_CASE("A Glue view stores the tables of ICEBERG_CATALOG under their Glue names", "[glue_view_iceberg]") {
	IcebergViewFixture fixture;
	auto &con = *fixture.con;
	auto create = con.Prepare("CREATE VIEW lake.sales.new_view AS SELECT c.name, o.amount FROM lake.sales.customers c "
	                          "JOIN lake_ice.sales.orders o ON c.id = o.id");
	REQUIRE_FALSE(create->HasError());

	Run(con, "BEGIN");
	auto stored = fixture.Store("SELECT c.name, o.amount FROM sales.customers c JOIN lake_ice.sales.orders o "
	                            "ON c.id = o.id");
	INFO(stored);
	REQUIRE(StringUtil::Contains(stored, "sales.customers"));
	REQUIRE(StringUtil::Contains(stored, "sales.orders"));
	REQUIRE(!StringUtil::Contains(stored, "lake_ice"));
	REQUIRE(!StringUtil::Contains(stored, "external:"));
	REQUIRE_THROWS_WITH(fixture.Store("SELECT * FROM lake_ice.sales.hive_in_glue"),
	                    Catch::Contains("has no Iceberg table sales.hive_in_glue"));
	REQUIRE_THROWS_WITH(fixture.Store("SELECT lake_ice.sales.orders.amount FROM lake_ice.sales.orders"),
	                    Catch::Contains("use a table alias"));
	Run(con, "COMMIT");

	// validation passes, then the fake endpoint fails the Glue call that replaces the view
	auto error = ErrorOf(con, "CREATE OR REPLACE VIEW lake.sales.orders_by_customer AS SELECT c.name, o.amount "
	                          "FROM lake.sales.customers c JOIN lake_ice.sales.orders o ON c.id = o.id");
	INFO(error);
	REQUIRE(StringUtil::Contains(error, "Could not connect to server"));
	REQUIRE(!StringUtil::Contains(error, "can not be read yet"));
}

TEST_CASE("A Glue view reads its Iceberg tables from ICEBERG_CATALOG", "[glue_view_iceberg]") {
	IcebergViewFixture fixture;
	auto &con = *fixture.con;
	auto joined = Run(con, "SELECT * FROM lake.sales.orders_by_customer")->Fetch();
	REQUIRE(joined->GetValue(0, 0).ToString() == "Ada");
	REQUIRE(joined->GetValue(1, 0).ToString() == "42");
	auto qualified = Run(con, "SELECT * FROM lake.sales.qualified_view")->Fetch();
	REQUIRE(qualified->GetValue(0, 0).ToString() == "42");
	auto described = Run(con, "DESCRIBE lake.sales.orders_by_customer")->Fetch();
	REQUIRE(described->size() == 2);
	REQUIRE(described->GetValue(1, 1).ToString() == "INTEGER");

	Run(con, "BEGIN");
	Run(con, "SELECT * FROM lake.sales.orders_by_customer");
	Run(con, "SELECT * FROM lake.sales.orders_by_customer");
	Run(con, "COMMIT");
	Run(con, "PREPARE p AS SELECT amount FROM lake.sales.orders_by_customer");
	REQUIRE(Run(con, "EXECUTE p")->Fetch()->GetValue(0, 0).ToString() == "42");
	REQUIRE(Run(con, "EXECUTE p")->Fetch()->GetValue(0, 0).ToString() == "42");
}

TEST_CASE("Column listings of a Glue view read its Iceberg tables from ICEBERG_CATALOG", "[glue_view_iceberg]") {
	IcebergViewFixture fixture;
	auto &con = *fixture.con;
	Run(con, "BEGIN");
	auto &catalog = Catalog::GetCatalog(*con.context, Identifier("lake")).Cast<GlueCatalog>();
	auto schema = catalog.GetSchemas().GetEntry(*con.context, "sales");
	EntryLookupInfo lookup(CatalogType::VIEW_ENTRY, QualifiedName(Identifier("orders_by_customer")));
	auto entry = schema->Cast<SchemaCatalogEntry>().LookupEntry(catalog.GetCatalogTransaction(*con.context), lookup);
	// what duckdb_columns() calls for each listed view
	auto &view = entry->Cast<ViewCatalogEntry>();
	view.BindView(*con.context);
	auto columns = view.GetColumnInfo();
	REQUIRE(columns);
	REQUIRE(columns->names.size() == 2);
	REQUIRE(columns->names[1] == Identifier("amount"));
	REQUIRE(columns->types[1] == LogicalType::INTEGER);
	Run(con, "COMMIT");
}

TEST_CASE("ICEBERG_CATALOG must read the same Glue catalog", "[glue_view_iceberg]") {
	IcebergViewFixture fixture(true, "999999999999");
	auto &con = *fixture.con;
	Run(con, "BEGIN");
	REQUIRE_THROWS_WITH(fixture.Store("SELECT * FROM lake_ice.sales.orders"),
	                    Catch::Contains("reads warehouse '999999999999', not Glue catalog"));
	Run(con, "COMMIT");
	auto error = ErrorOf(con, "SELECT * FROM lake.sales.orders_by_customer");
	INFO(error);
	REQUIRE(StringUtil::Contains(error, "reads warehouse '999999999999', not Glue catalog"));
}

TEST_CASE("Direct queries and DROP VIEW do not use ICEBERG_CATALOG", "[glue_view_iceberg]") {
	IcebergViewFixture fixture;
	auto &con = *fixture.con;
	REQUIRE(StringUtil::Contains(ErrorOf(con, "SELECT * FROM lake.sales.orders"), "can not be read yet"));
	fixture.Warm();
	REQUIRE(Run(con, "SELECT amount FROM lake_ice.sales.orders")->Fetch()->GetValue(0, 0).ToString() == "42");
	REQUIRE_FALSE(con.Prepare("DROP VIEW lake.sales.orders_by_customer")->HasError());
}

TEST_CASE("A Glue view of Iceberg tables needs the catalog ICEBERG_CATALOG names", "[glue_view_iceberg]") {
	IcebergViewFixture fixture;
	auto &con = *fixture.con;
	Run(con, "DETACH lake_ice");
	auto error = ErrorOf(con, "SELECT * FROM lake.sales.orders_by_customer");
	INFO(error);
	REQUIRE(StringUtil::Contains(error, "lake_ice"));
	fixture.Warm();
	REQUIRE(Run(con, "SELECT * FROM lake.sales.customers_view")->Fetch()->GetValue(0, 0).ToString() == "Ada");
}

TEST_CASE("Without ICEBERG_CATALOG a Glue view does not read Iceberg tables", "[glue_view_iceberg]") {
	IcebergViewFixture fixture(false);
	auto &con = *fixture.con;
	REQUIRE(StringUtil::Contains(ErrorOf(con, "SELECT * FROM lake.sales.orders_by_customer"), "can not be read yet"));
	fixture.Warm();
	Run(con, "BEGIN");
	REQUIRE_THROWS_WITH(fixture.Store("SELECT * FROM lake_ice.sales.orders"),
	                    Catch::Contains("experimental_glue_view_external_catalog_types"));
	Run(con, "COMMIT");
}

TEST_CASE("ICEBERG_CATALOG can not name the Glue catalog itself", "[glue_view_iceberg]") {
	IcebergViewFixture fixture;
	auto result = fixture.con->Query("ATTACH '123456789012' AS lake2 (TYPE GLUE, SECRET view_test, REGION 'us-east-1', "
	                                 "ENDPOINT 'http://127.0.0.1:1', VERIFY_CONNECTION false, "
	                                 "ICEBERG_CATALOG 'LAKE2')");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "not the Glue catalog \"lake2\" itself"));
}

TEST_CASE("A Glue view binds the Iceberg schema of the current transaction", "[glue_view_iceberg]") {
	IcebergViewFixture fixture;
	auto &con = *fixture.con;
	auto bind_columns = [&]() {
		Run(con, "BEGIN");
		auto &catalog = Catalog::GetCatalog(*con.context, Identifier("lake")).Cast<GlueCatalog>();
		auto schema = catalog.GetSchemas().GetEntry(*con.context, "sales");
		EntryLookupInfo lookup(CatalogType::VIEW_ENTRY, QualifiedName(Identifier("orders_by_customer")));
		auto entry =
		    schema->Cast<SchemaCatalogEntry>().LookupEntry(catalog.GetCatalogTransaction(*con.context), lookup);
		auto &view = entry->Cast<ViewCatalogEntry>();
		view.BindView(*con.context);
		auto type = view.GetColumnInfo()->types[1];
		Run(con, "COMMIT");
		return type;
	};
	REQUIRE(bind_columns() == LogicalType::INTEGER);
	Run(con, "CREATE OR REPLACE TABLE lake_ice.sales.orders AS SELECT 1 AS id, 42::BIGINT AS amount");
	REQUIRE(bind_columns() == LogicalType::BIGINT);
	auto result = Run(con, "SELECT amount FROM lake.sales.orders_by_customer");
	REQUIRE(result->GetTypes()[0] == LogicalType::BIGINT);
}

TEST_CASE("A Glue view follows a reattached ICEBERG_CATALOG within a transaction", "[glue_view_iceberg]") {
	IcebergViewFixture fixture;
	auto &con = *fixture.con;
	Run(con, "BEGIN");
	Run(con, "SELECT * FROM lake.sales.orders_by_customer");
	Run(con, "DETACH lake_ice");
	Run(con, "ATTACH '999999999999' AS lake_ice (TYPE iceberg)");
	auto error = ErrorOf(con, "SELECT * FROM lake.sales.orders_by_customer");
	INFO(error);
	REQUIRE(StringUtil::Contains(error, "reads warehouse '999999999999'"));
}

TEST_CASE("A Glue view checks experimental_glue_view_external_catalog_types on every read", "[glue_view_iceberg]") {
	IcebergViewFixture fixture;
	auto &con = *fixture.con;
	{
		DuckDB seed(fixture.directory + "/ext.duckdb");
		Connection seed_con(seed);
		Run(seed_con, "CREATE TABLE t AS SELECT 7 AS x");
	}
	Run(con, "ATTACH " + Quote(fixture.directory + "/ext.duckdb") + " AS ext");
	fixture.AddView("external_view", "SELECT o.amount, e.x FROM sales.orders o, \"external:duckdb:ext\".main.t e");
	fixture.Warm();
	Run(con, "SET GLOBAL experimental_glue_view_external_catalog_types = ['duckdb']");
	Run(con, "BEGIN");
	auto result = Run(con, "SELECT * FROM lake.sales.external_view")->Fetch();
	REQUIRE(result->GetValue(1, 0).ToString() == "7");
	Run(con, "RESET GLOBAL experimental_glue_view_external_catalog_types");
	auto error = ErrorOf(con, "SELECT * FROM lake.sales.external_view");
	INFO(error);
	REQUIRE(StringUtil::Contains(error, "experimental_glue_view_external_catalog_types"));
}
