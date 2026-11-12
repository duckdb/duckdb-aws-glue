#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_transaction.hpp"
#include "catalog/glue_view.hpp"
#include "catalog/glue_view_names.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "functions/glue_functions.hpp"
#include "glue_test_util.hpp"

using namespace duckdb;

namespace {

struct ViewReaderTestInfo : StorageExtensionInfo {
	string path;
	vector<Identifier> names;
	string catalog_id;
	unordered_map<string, Value> options;
	bool fail_attach = false;
};

class ViewReaderTestExtension : public StorageExtension {
public:
	explicit ViewReaderTestExtension(shared_ptr<ViewReaderTestInfo> info) {
		storage_info = std::move(info);
		attach = [](optional_ptr<StorageExtensionInfo> storage_info, ClientContext &, AttachedDatabase &db,
		            const string &, AttachInfo &info, AttachOptions &options) -> unique_ptr<Catalog> {
			auto &state = static_cast<ViewReaderTestInfo &>(*storage_info);
			REQUIRE(options.visibility == AttachVisibility::HIDDEN);
			REQUIRE(options.access_mode == AccessMode::READ_ONLY);
			state.names.push_back(info.name);
			state.catalog_id = info.path;
			state.options = info.options;
			if (state.fail_attach) {
				throw IOException("test Iceberg reader unavailable");
			}
			info.path = state.path;
			return make_uniq<DuckCatalog>(db);
		};
		create_transaction_manager = [](optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
		                                Catalog &) -> unique_ptr<TransactionManager> {
			return make_uniq<DuckTransactionManager>(db);
		};
	}
};

class ViewBindingFixture {
public:
	explicit ViewBindingFixture(bool explicit_secret = true) {
		directory = "glue_view_binding_" + Unique();
		auto fs = FileSystem::CreateLocal();
		fs->CreateDirectory(directory);
		fs->CreateDirectory(directory + "/customers");
		reader = make_shared_ptr<ViewReaderTestInfo>();
		reader->path = directory + "/reader.duckdb";
		{
			DuckDB seed(reader->path);
			Connection con(seed);
			Run(con, "CREATE SCHEMA sales");
			Run(con, "CREATE TABLE sales.orders AS SELECT 1 AS id, 42 AS amount");
			Run(con, "COPY (SELECT 1 AS id, 'Ada' AS name) TO " + Quote(directory + "/customers/data.parquet") +
			             " (FORMAT PARQUET)");
		}
		DBConfig config;
		StorageExtension::Register(config, "iceberg", make_shared_ptr<ViewReaderTestExtension>(reader));
		db = make_uniq<DuckDB>(nullptr, &config);
		con = make_uniq<Connection>(*db);
		Run(*con, "CREATE SECRET view_test (TYPE S3, KEY_ID 'test', SECRET 'test', REGION 'us-east-1')");
		Run(*con, "SET glue_metadata_cache_scope = 'connection'");
		Run(*con, "SET glue_batch_table_lookup = false");
		Run(*con, "ATTACH '123456789012' AS lake (TYPE GLUE, " + string(explicit_secret ? "SECRET view_test, " : "") +
		              "REGION 'us-east-1', ENDPOINT 'http://127.0.0.1:1', VERIFY_CONNECTION false)");
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
		customers.version_id = "1";
		customers.location = directory + "/customers";
		customers.serde_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
		customers.columns = {{"id", "int", ""}, {"name", "string", ""}};
		tables.push_back(customers);
		auto orders = customers;
		orders.name = "orders";
		orders.parameters["table_type"] = "ICEBERG";
		// Glue's copy can lag behind the schema held by the format implementation.
		orders.columns = {{"id", "bigint", ""}, {"outdated_amount", "string", ""}};
		tables.push_back(orders);
		AddView("orders_by_customer", "SELECT c.name, o.amount FROM sales.customers c "
		                              "JOIN sales.orders o ON c.id = o.id");
		Warm();
	}

	~ViewBindingFixture() {
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
		tables.push_back(std::move(view));
	}

	void Warm() {
		Warm(*con);
	}

	void Warm(Connection &connection) {
		Run(connection, "BEGIN");
		auto &catalog = Catalog::GetCatalog(*connection.context, Identifier("lake"));
		auto &transaction = GlueTransaction::Get(*connection.context, catalog);
		for (auto &table : tables) {
			transaction.cache->GetTable("sales", table.name,
			                            [&]() { return make_shared_ptr<const GlueTableInfo>(table); });
		}
		Run(connection, "COMMIT");
	}

public:
	string directory;
	shared_ptr<ViewReaderTestInfo> reader;
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> con;
	vector<GlueTableInfo> tables;
};

} // namespace

TEST_CASE("Glue view binding routes Iceberg and leaves Hive in Glue", "[glue_view_binding]") {
	ViewBindingFixture fixture;
	auto &con = *fixture.con;
	auto hive = Run(con, "SELECT name FROM lake.sales.customers")->Fetch();
	REQUIRE(hive->GetValue(0, 0).ToString() == "Ada");
	REQUIRE(fixture.reader->names.empty());

	auto result = Run(con, "SELECT * FROM lake.sales.orders_by_customer")->Fetch();
	REQUIRE(result->GetValue(0, 0).ToString() == "Ada");
	REQUIRE(result->GetValue(1, 0).ToString() == "42");
	REQUIRE(fixture.reader->names.size() == 1);
	REQUIRE(fixture.reader->catalog_id == "123456789012");
	REQUIRE(fixture.reader->options.at("secret").ToString() == "view_test");
	REQUIRE(fixture.reader->options.at("uri").ToString() == "http://127.0.0.1:1/iceberg");
	REQUIRE(fixture.reader->options.at("sigv4_region").ToString() == "us-east-1");
	Run(con, "SELECT * FROM lake.sales.orders_by_customer");
	REQUIRE(fixture.reader->names.size() == 1);
	auto databases =
	    Run(con, "SELECT count(*) FROM duckdb_databases() WHERE database_name LIKE '__glue_iceberg_%'")->Fetch();
	REQUIRE(databases->GetValue(0, 0).ToString() == "0");

	Run(con, "BEGIN");
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
	                       QualifiedName(Identifier("lake"), Identifier("sales"), Identifier("orders_by_customer")));
	auto &view = Catalog::GetEntry(*con.context, lookup).Cast<GlueView>();
	REQUIRE(view.GetQuery().ToString().find("__glue_iceberg_") == string::npos);
	REQUIRE(view.table_info.view_expanded_text == fixture.tables.back().view_expanded_text);
	view.BindView(*con.context);
	REQUIRE(view.GetColumnInfo()->types.size() == 2);
	Run(con, "COMMIT");

	auto reader_name = fixture.reader->names.back();
	Run(con, "DETACH lake");
	REQUIRE(!DatabaseManager::Get(*con.context).GetDatabase(reader_name));
}

TEST_CASE("Glue CREATE VIEW validates a copy without persisting the reader alias", "[glue_view_binding]") {
	ViewBindingFixture fixture;
	auto &con = *fixture.con;
	auto prepared = con.Prepare("CREATE VIEW lake.sales.new_view AS "
	                            "SELECT c.name, o.amount FROM lake.sales.customers c "
	                            "JOIN lake.sales.orders o ON c.id = o.id");
	REQUIRE_FALSE(prepared->HasError());
	REQUIRE(fixture.reader->names.size() == 1);
	auto bare = con.Prepare("CREATE VIEW lake.sales.bare_view AS SELECT c.name, o.amount "
	                        "FROM customers c JOIN orders o ON c.id = o.id");
	REQUIRE_FALSE(bare->HasError());

	Run(con, "BEGIN");
	auto parser = Parser::GetBuiltinParser();
	auto statement = CreateViewInfo::ParseSelect(parser, fixture.tables.back().view_expanded_text);
	auto &catalog = Catalog::GetCatalog(*con.context, Identifier("lake")).Cast<GlueCatalog>();
	GlueViewNames::QualifyForStorage(*con.context, catalog, Identifier("sales"), *statement->node);
	auto stored_sql = statement->ToString();
	vector<LogicalType> types;
	vector<Identifier> names;
	Binder::BindView(*con.context, *statement, Identifier("lake"), {Identifier("sales")}, nullptr, {}, types, names);
	REQUIRE(statement->ToString() == stored_sql);
	REQUIRE(stored_sql.find("__glue_iceberg_") == string::npos);
	REQUIRE(types.size() == 2);
	Run(con, "COMMIT");
}

TEST_CASE("Glue view binding respects CTEs, nested views and prepared statements", "[glue_view_binding]") {
	ViewBindingFixture fixture;
	fixture.AddView("cte_view", "WITH orders AS (SELECT 7 AS amount) SELECT amount FROM orders");
	fixture.AddView("nested_view", "SELECT * FROM sales.orders_by_customer");
	fixture.Warm();
	auto &con = *fixture.con;
	auto cte = Run(con, "SELECT * FROM lake.sales.cte_view")->Fetch();
	REQUIRE(cte->GetValue(0, 0).ToString() == "7");
	REQUIRE(fixture.reader->names.empty());
	Run(con, "PREPARE v AS SELECT * FROM lake.sales.nested_view WHERE amount = $1");
	auto result = Run(con, "EXECUTE v(42)")->Fetch();
	REQUIRE(result->GetValue(0, 0).ToString() == "Ada");
	Run(con, "EXECUTE v(42)");
	REQUIRE(fixture.reader->names.size() == 1);
}

TEST_CASE("Glue view reader is recreated after a failed bind or transaction rollback", "[glue_view_binding]") {
	ViewBindingFixture fixture;
	auto &con = *fixture.con;
	fixture.AddView("bad_view", "SELECT nonexistent FROM sales.orders");
	fixture.Warm();
	auto result = con.Query("SELECT * FROM lake.sales.bad_view");
	REQUIRE(result->HasError());
	REQUIRE(fixture.reader->names.size() == 1);
	auto first_reader = fixture.reader->names.back();
	REQUIRE(!DatabaseManager::Get(*con.context).GetDatabase(first_reader));
	fixture.Warm();
	Run(con, "BEGIN");
	Run(con, "SELECT * FROM lake.sales.orders_by_customer");
	Run(con, "ROLLBACK");
	REQUIRE(fixture.reader->names.size() == 2);
	fixture.Warm();
	Run(con, "SELECT * FROM lake.sales.orders_by_customer");
	REQUIRE(fixture.reader->names.size() == 3);
}

TEST_CASE("Glue view reader attachment can retry after an extension error", "[glue_view_binding]") {
	ViewBindingFixture fixture;
	auto &con = *fixture.con;
	fixture.reader->fail_attach = true;
	auto result = con.Query("SELECT * FROM lake.sales.orders_by_customer");
	REQUIRE(result->HasError());
	REQUIRE(result->GetError().find("test Iceberg reader unavailable") != string::npos);
	fixture.Warm();
	auto hive = Run(con, "SELECT name FROM lake.sales.customers")->Fetch();
	REQUIRE(hive->GetValue(0, 0).ToString() == "Ada");
	REQUIRE(fixture.reader->names.size() == 1);
	fixture.reader->fail_attach = false;
	Run(con, "SELECT * FROM lake.sales.orders_by_customer");
	REQUIRE(fixture.reader->names.size() == 2);
}

TEST_CASE("Glue view reader survives another connection rolling back its attachment", "[glue_view_binding]") {
	ViewBindingFixture fixture;
	auto &con = *fixture.con;
	Connection other(*fixture.db);
	Run(other, "SET glue_metadata_cache_scope = 'connection'");
	fixture.Warm(other);
	Run(con, "BEGIN");
	Run(con, "SELECT * FROM lake.sales.orders_by_customer");
	Run(other, "BEGIN");
	Run(other, "SELECT * FROM lake.sales.orders_by_customer");
	REQUIRE(fixture.reader->names.size() == 1);
	Run(con, "ROLLBACK");
	auto result = Run(other, "SELECT * FROM lake.sales.orders_by_customer")->Fetch();
	REQUIRE(result->GetValue(1, 0).ToString() == "42");
	Run(other, "COMMIT");
	fixture.Warm();
	Run(con, "SELECT * FROM lake.sales.orders_by_customer");
	REQUIRE(fixture.reader->names.size() == 2);
}

TEST_CASE("Glue view reader follows a change in the default Glue secret", "[glue_view_binding]") {
	ViewBindingFixture fixture(false);
	auto &con = *fixture.con;
	Run(con, "SELECT * FROM lake.sales.orders_by_customer");
	REQUIRE(fixture.reader->options.at("secret").ToString() == "view_test");
	auto first_reader = fixture.reader->names.back();
	Run(con, "CREATE SECRET (TYPE S3, KEY_ID 'replacement', SECRET 'replacement', REGION 'us-east-1')");
	Run(con, "SELECT * FROM lake.sales.orders_by_customer");
	REQUIRE(fixture.reader->options.at("secret").ToString() == "__default_s3");
	REQUIRE(fixture.reader->names.size() == 2);
	REQUIRE(!DatabaseManager::Get(*con.context).GetDatabase(first_reader));
}

TEST_CASE("Glue table names remain valid through an Iceberg redirect view", "[glue_table_binding]") {
	ViewBindingFixture fixture;
	auto result = Run(*fixture.con, "SELECT lake.sales.orders.amount FROM lake.sales.orders")->Fetch();
	REQUIRE(result);
	REQUIRE(result->GetValue(0, 0).ToString() == "42");
	auto schema = Run(*fixture.con, "DESCRIBE lake.sales.orders")->Fetch();
	REQUIRE(schema->GetValue(0, 1).ToString() == "amount");
	REQUIRE(schema->GetValue(1, 1).ToString() == "INTEGER");
	auto joined = Run(*fixture.con, "SELECT lake.sales.customers.name, lake.sales.orders.amount "
	                                "FROM lake.sales.customers JOIN lake.sales.orders "
	                                "ON lake.sales.customers.id = lake.sales.orders.id")
	                  ->Fetch();
	REQUIRE(joined->GetValue(0, 0).ToString() == "Ada");
	REQUIRE(joined->GetValue(1, 0).ToString() == "42");
	Run(*fixture.con, "EXPLAIN SELECT * FROM lake.sales.orders");
}

TEST_CASE("Hive scans retain their schema when another connection refreshes Glue metadata", "[glue_table_binding]") {
	ViewBindingFixture fixture;
	auto &con = *fixture.con;
	Connection other(*fixture.db);
	Run(other, "SET glue_metadata_cache_scope = 'connection'");
	fixture.Warm(other);
	Run(con, "BEGIN");
	auto prepared = con.Prepare("SELECT id, name FROM lake.sales.customers");
	REQUIRE_FALSE(prepared->HasError());

	Run(other, "BEGIN");
	auto &catalog = Catalog::GetCatalog(*other.context, Identifier("lake"));
	auto &cache = *GlueTransaction::Get(*other.context, catalog).cache;
	cache.Clear();
	auto updated = fixture.tables.front();
	updated.version_id = "2";
	updated.columns.front().type = "bigint";
	cache.GetTable("sales", "customers", [&]() { return make_shared_ptr<const GlueTableInfo>(updated); });
	auto refreshed = Run(other, "SELECT id FROM lake.sales.customers");
	REQUIRE(refreshed->GetTypes()[0] == LogicalType::BIGINT);
	Run(other, "COMMIT");

	auto result = prepared->Execute();
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->GetTypes()[0] == LogicalType::INTEGER);
	auto rows = result->Fetch();
	REQUIRE(rows->GetValue(0, 0).ToString() == "1");
	REQUIRE(rows->GetValue(1, 0).ToString() == "Ada");
	Run(con, "COMMIT");
	REQUIRE(fixture.reader->names.empty());
}

TEST_CASE("Glue redirects register the Iceberg catalog and bind its real table", "[glue_table_binding]") {
	ViewBindingFixture fixture;
	auto &con = *fixture.con;
	auto prepared = con.Prepare("SELECT * FROM lake.sales.orders_by_customer");
	REQUIRE_FALSE(prepared->HasError());
	REQUIRE(fixture.reader->names.size() == 1);
	auto reader_name = fixture.reader->names.back();
	auto &properties = prepared->GetStatementProperties();
	REQUIRE(properties.read_databases.count(Identifier("lake")) == 1);
	REQUIRE(properties.read_databases.count(reader_name) == 1);
	REQUIRE(properties.modified_databases.empty());

	Run(con, "BEGIN");
	auto plan = con.ExtractPlan("SELECT amount FROM lake.sales.orders");
	vector<reference<LogicalOperator>> pending;
	pending.emplace_back(*plan);
	idx_t scans = 0;
	while (!pending.empty()) {
		auto &op = pending.back().get();
		pending.pop_back();
		for (auto &child : op.children) {
			pending.emplace_back(*child);
		}
		if (op.type != LogicalOperatorType::LOGICAL_GET) {
			continue;
		}
		auto table = op.Cast<LogicalGet>().GetTable();
		REQUIRE(table);
		REQUIRE(table->ParentCatalog().GetName() == reader_name);
		REQUIRE(table->ParentSchema().name == Identifier("sales"));
		REQUIRE(table->name == Identifier("orders"));
		scans++;
	}
	REQUIRE(scans == 1);
	Run(con, "COMMIT");
}

TEST_CASE("Glue metadata operations do not require an Iceberg reader", "[glue_table_binding]") {
	ViewBindingFixture fixture;
	auto &con = *fixture.con;
	fixture.reader->fail_attach = true;

	auto drop = con.Prepare("DROP TABLE lake.sales.orders");
	REQUIRE_FALSE(drop->HasError());
	Run(con, "USE lake.sales");
	Run(con, "BEGIN");
	for (auto &name : {"orders", "sales.orders", "lake.sales.orders"}) {
		auto resolved = ResolveGlueTableName(*con.context, "glue_get_table_response", name);
		REQUIRE(resolved.ToString() == "lake.sales.orders");
	}
	Run(con, "COMMIT");
	auto columns = Run(con, "SELECT column_name, data_type FROM duckdb_columns() "
	                        "WHERE database_name = 'lake' AND schema_name = 'sales' AND table_name = 'orders' "
	                        "ORDER BY column_index")
	                   ->Fetch();
	REQUIRE(columns->size() == 2);
	REQUIRE(columns->GetValue(0, 1).ToString() == "outdated_amount");
	REQUIRE(columns->GetValue(1, 1).ToString() == "VARCHAR");
	REQUIRE(fixture.reader->names.empty());

	for (auto &sql : {"SELECT * FROM lake.sales.orders", "DESCRIBE lake.sales.orders"}) {
		auto result = con.Query(sql);
		REQUIRE(result->HasError());
		REQUIRE(result->GetError().find("test Iceberg reader unavailable") != string::npos);
		fixture.Warm();
	}
	REQUIRE(fixture.reader->names.size() == 2);
}

TEST_CASE("Glue diagnostic names and DROP bind even if the Iceberg table is missing", "[glue_table_binding]") {
	ViewBindingFixture fixture;
	auto missing = fixture.tables[1];
	missing.name = "missing_orders";
	fixture.tables.push_back(std::move(missing));
	fixture.Warm();
	auto &con = *fixture.con;
	// The mock has no Glue listing endpoint for suggestions in unrelated schemas.
	Run(con, "SET catalog_error_max_schemas = 0");

	auto result = con.Query("SELECT * FROM lake.sales.missing_orders");
	REQUIRE(result->HasError());
	INFO(result->GetError());
	REQUIRE(result->GetError().find("missing_orders") != string::npos);
	REQUIRE(result->GetError().find("does not exist") != string::npos);
	REQUIRE(fixture.reader->names.size() == 1);
	fixture.Warm();

	auto drop = con.Prepare("DROP TABLE lake.sales.missing_orders");
	REQUIRE_FALSE(drop->HasError());
	Run(con, "USE lake.sales");
	Run(con, "BEGIN");
	auto name = ResolveGlueTableName(*con.context, "glue_get_table_response", "sales.missing_orders");
	REQUIRE(name.ToString() == "lake.sales.missing_orders");
	Run(con, "COMMIT");
	REQUIRE(fixture.reader->names.size() == 1);
}

TEST_CASE("Binding an Iceberg scan does not replace the columns reported by Glue listings", "[glue_table_binding]") {
	ViewBindingFixture fixture;
	auto &con = *fixture.con;
	Run(con, "BEGIN");
	auto result = Run(con, "SELECT amount FROM lake.sales.orders")->Fetch();
	REQUIRE(result->GetValue(0, 0).ToString() == "42");
	auto columns = Run(con, "SELECT column_name, data_type FROM duckdb_columns() "
	                        "WHERE database_name = 'lake' AND schema_name = 'sales' AND table_name = 'orders' "
	                        "ORDER BY column_index")
	                   ->Fetch();
	REQUIRE(columns->size() == 2);
	REQUIRE(columns->GetValue(0, 1).ToString() == "outdated_amount");
	REQUIRE(columns->GetValue(1, 1).ToString() == "VARCHAR");
	auto described = Run(con, "DESCRIBE lake.sales.orders")->Fetch();
	REQUIRE(described->GetValue(0, 1).ToString() == "amount");
	REQUIRE(described->GetValue(1, 1).ToString() == "INTEGER");
	Run(con, "COMMIT");
}

TEST_CASE("The Iceberg redirect does not enable writes", "[glue_table_binding]") {
	ViewBindingFixture fixture;
	auto &con = *fixture.con;
	for (auto &sql : {"INSERT INTO lake.sales.orders VALUES (2, 99)", "DELETE FROM lake.sales.orders",
	                  "UPDATE lake.sales.orders SET amount = 99"}) {
		auto result = con.Query(sql);
		REQUIRE(result->HasError());
		fixture.Warm();
	}
	auto result = Run(con, "SELECT amount FROM lake.sales.orders")->Fetch();
	REQUIRE(result->GetValue(0, 0).ToString() == "42");
}

TEST_CASE("Unsupported Glue formats remain metadata-only tables", "[glue_table_binding]") {
	ViewBindingFixture fixture;
	auto unsupported = fixture.tables[1];
	unsupported.name = "delta_orders";
	unsupported.parameters["table_type"] = "DELTA";
	fixture.tables.push_back(std::move(unsupported));
	fixture.Warm();
	auto &con = *fixture.con;

	auto drop = con.Prepare("DROP TABLE lake.sales.delta_orders");
	REQUIRE_FALSE(drop->HasError());
	auto insert = con.Prepare("INSERT INTO lake.sales.delta_orders VALUES (2, '99')");
	REQUIRE(insert->HasError());
	REQUIRE(insert->GetError().find("only Hive tables can be written") != string::npos);
	fixture.Warm();
	auto result = con.Query("SELECT * FROM lake.sales.delta_orders");
	REQUIRE(result->HasError());
	REQUIRE(result->GetError().find("DELTA") != string::npos);
	REQUIRE(result->GetError().find("can not be read yet") != string::npos);
	REQUIRE(fixture.reader->names.empty());
}

TEST_CASE("Iceberg database resource links are rejected before attaching a reader", "[glue_table_binding]") {
	ViewBindingFixture fixture;
	auto &con = *fixture.con;
	Run(con, "BEGIN");
	auto &catalog = Catalog::GetCatalog(*con.context, Identifier("lake")).Cast<GlueCatalog>();
	GlueDatabaseInfo schema;
	schema.name = "sales";
	schema.is_resource_link = true;
	catalog.GetSchemas().CreateEntry(catalog.GetSchemas().CreateSchemaEntry(schema));
	Run(con, "COMMIT");
	auto drop = con.Prepare("DROP TABLE lake.sales.orders");
	REQUIRE_FALSE(drop->HasError());
	auto result = con.Query("SELECT * FROM lake.sales.orders");
	REQUIRE(result->HasError());
	REQUIRE(result->GetError().find("resource link") != string::npos);
	REQUIRE(fixture.reader->names.empty());
}

TEST_CASE("A prepared Glue query tracks the backing catalog's lifetime", "[glue_table_binding]") {
	ViewBindingFixture fixture;
	auto &con = *fixture.con;
	Run(con, "PREPARE p AS SELECT amount FROM lake.sales.orders");
	Run(con, "EXECUTE p");
	REQUIRE(fixture.reader->names.size() == 1);
	Run(con, "DETACH \"" + fixture.reader->names.back().GetIdentifierName() + "\"");
	auto result = Run(con, "EXECUTE p")->Fetch();
	REQUIRE(result->GetValue(0, 0).ToString() == "42");
	REQUIRE(fixture.reader->names.size() == 2);
}
