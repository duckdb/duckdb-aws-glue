#define DUCKDB_EXTENSION_MAIN

#include "glue_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "glue_attach.hpp"
#include "glue_functions.hpp"
#include "glue_grammar.hpp"
#include "glue_http_client.hpp"
#include "duckdb/main/extension_helper.hpp"

#include <aws/core/Aws.h>
#include "storage/glue_catalog.hpp"
#include "storage/glue_transaction_manager.hpp"

namespace duckdb {

static unique_ptr<TransactionManager> CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                               AttachedDatabase &db, Catalog &catalog) {
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	return make_uniq<GlueTransactionManager>(db, glue_catalog);
}

class GlueStorageExtension : public StorageExtension {
public:
	GlueStorageExtension() {
		attach = GlueAttach::Attach;
		create_transaction_manager = CreateTransactionManager;
	}
};

static void InitAWSAPI() {
	static bool loaded = false;
	if (!loaded) {
		Aws::SDKOptions options;
		Aws::InitAPI(options); // Should only be called once.
		loaded = true;
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);

	config.AddExtensionOption("glue_network_calls_via_duckdb",
	                          "Route the Glue API calls of the AWS SDK through DuckDB's HTTP layer (httpfs) instead "
	                          "of the AWS SDK's own HTTP client, so they use DuckDB's proxy / certificate settings "
	                          "and show up in the HTTP log. Default true.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));

	config.AddExtensionOption("glue_get_partitions_segments",
	                          "How many GetPartitions requests to run at the same time when listing the partitions of "
	                          "a table (Glue's Segment API splits them over non overlapping segments). 0, the "
	                          "default, uses 8 against AWS and 1 against a Glue compatible server given with "
	                          "ENDPOINT. At most 10.",
	                          LogicalType::UBIGINT, Value::UBIGINT(0));

	config.AddExtensionOption("hive_partition_listing_threshold",
	                          "When a scan reads at least this many partitions below the table location, the location "
	                          "is listed once (recursively) instead of one listing per partition. Default 10.",
	                          LogicalType::UBIGINT, Value::UBIGINT(10));

	// The HTTP client factory has to be in place before the first AWS client is constructed
	InitAWSAPI();
	RegisterGlueHttpClientFactory(instance);

	// Hive tables are read with read_parquet
	ExtensionHelper::AutoLoadExtension(instance, "parquet");
	if (!instance.ExtensionIsLoaded("parquet")) {
		throw MissingExtensionException("The glue extension requires the parquet extension to be loaded!");
	}
	// ATTACH '<catalog id>' (TYPE GLUE)
	StorageExtension::Register(config, "glue", make_shared_ptr<GlueStorageExtension>());

	loader.RegisterFunction(GetGlueGetTableResponseFunction());
	loader.RegisterFunction(GetGlueGetDatabaseResponseFunction());
	loader.RegisterFunction(GetGlueCreateDatabaseFunction());
	loader.RegisterFunction(GetGlueSetDatabasePropertiesFunction());
	loader.RegisterFunction(GetGluePartitionsFunction());
	loader.RegisterFunction(GetGlueAddPartitionFunction());
	loader.RegisterFunction(GetGlueDropPartitionFunction());
	loader.RegisterFunction(GetGlueRenamePartitionFunction());
	loader.RegisterFunction(GetGlueSetPartitionLocationFunction());
	loader.RegisterFunction(GetGlueSetTableLocationFunction());
	loader.RegisterFunction(GetGlueAlterTableFunction());
	// ALTER TABLE ... ADD / DROP PARTITION etc., switched on with SET active_grammar_extensions = ['glue_hive_ddl']
	RegisterGlueGrammarExtension(instance);
	loader.RegisterFunction(GetHiveScanFunction(instance));
}

void GlueExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string GlueExtension::Name() {
	return "glue";
}

std::string GlueExtension::Version() const {
#ifdef EXT_VERSION_GLUE
	return EXT_VERSION_GLUE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(glue, loader) {
	duckdb::LoadInternal(loader);
}
}
