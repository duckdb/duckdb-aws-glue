#include "catch.hpp"
#include "duckdb.hpp"
#include "core/helpers.hpp"

using namespace duckdb;

TEST_CASE("Glue catalog ids are split into account and catalog", "[catalog_id]") {
	string account, catalog;
	SplitGlueCatalogId("111111111111", account, catalog);
	REQUIRE(account == "111111111111");
	REQUIRE(catalog.empty());
	SplitGlueCatalogId("111111111111:s3tablescatalog/sales", account, catalog);
	REQUIRE(account == "111111111111");
	REQUIRE(catalog == "s3tablescatalog/sales");
	SplitGlueCatalogId("s3tablescatalog/sales", account, catalog);
	REQUIRE(account.empty());
	REQUIRE(catalog == "s3tablescatalog/sales");
	// twelve digits lead an id only when a ':' or nothing follows them
	SplitGlueCatalogId("111111111111x", account, catalog);
	REQUIRE(account.empty());
	REQUIRE(catalog == "111111111111x");
	SplitGlueCatalogId("1111:sales", account, catalog);
	REQUIRE(account.empty());
	REQUIRE(catalog == "1111:sales");
	SplitGlueCatalogId("", account, catalog);
	REQUIRE(account.empty());
	REQUIRE(catalog.empty());
}

TEST_CASE("Glue catalog ids are compared with their accounts", "[catalog_id]") {
	// the same catalog, with and without the account, in any case
	REQUIRE(IsSameGlueCatalogId("111111111111:s3tablescatalog/sales", "111111111111:s3tablescatalog/sales"));
	REQUIRE(IsSameGlueCatalogId("111111111111:s3tablescatalog/sales", "s3tablescatalog/sales"));
	REQUIRE(IsSameGlueCatalogId("s3tablescatalog/sales", "111111111111:s3tablescatalog/sales"));
	REQUIRE(IsSameGlueCatalogId("111111111111:S3TablesCatalog/Sales", "111111111111:s3tablescatalog/sales"));
	REQUIRE(IsSameGlueCatalogId("111111111111", "111111111111"));
	// the same catalog name in another account is another catalog
	REQUIRE_FALSE(IsSameGlueCatalogId("111111111111:s3tablescatalog/sales", "222222222222:s3tablescatalog/sales"));
	REQUIRE_FALSE(IsSameGlueCatalogId("111111111111", "222222222222"));
	// a child catalog is not the Data Catalog of its account, which is what a server that ignores the id reports
	REQUIRE_FALSE(IsSameGlueCatalogId("111111111111:s3tablescatalog/sales", "111111111111"));
	REQUIRE_FALSE(IsSameGlueCatalogId("s3tablescatalog/sales", "111111111111"));
	REQUIRE_FALSE(IsSameGlueCatalogId("111111111111:s3tablescatalog/sales", ""));
	// nor its parent or a sibling
	REQUIRE_FALSE(IsSameGlueCatalogId("111111111111:s3tablescatalog/sales", "111111111111:s3tablescatalog"));
	REQUIRE_FALSE(IsSameGlueCatalogId("111111111111:s3tablescatalog/sales", "111111111111:s3tablescatalog/other"));
}
