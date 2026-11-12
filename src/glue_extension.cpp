#define DUCKDB_EXTENSION_MAIN

#include "glue_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "catalog/glue_attach.hpp"
#include "core/readers/hive_csv_options.hpp"
#include "functions/glue_functions.hpp"
#include "functions/glue_repair_table.hpp"
#include "grammar/glue_grammar.hpp"
#include "api/glue_api.hpp"
#include "api/glue_http_client.hpp"
#include "core/glue_log_types.hpp"
#include "duckdb/main/extension_helper.hpp"

#include <aws/core/Aws.h>
#include <mutex>
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction_manager.hpp"

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
	// Should only be called once, and databases can load the extension concurrently
	static std::once_flag initialized;
	std::call_once(initialized, [] {
		Aws::SDKOptions options;
		Aws::InitAPI(options);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);

	config.AddExtensionOption("glue_partition_filter_pushdown",
	                          "Give the filters a query has on the partition columns of a Hive table to Glue, so that "
	                          "GetPartitions returns only the partitions they match instead of all of them. Default "
	                          "false.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption(
	    "glue_network_calls_via_duckdb",
	    "Route the Glue API calls of the AWS SDK through DuckDB's HTTP layer (httpfs) instead "
	    "of the AWS SDK's own HTTP client, so they use DuckDB's proxy / certificate settings "
	    "and show up in the HTTP log. Default true, and always on under WebAssembly, where the SDK "
	    "has no HTTP client of its own.",
	    LogicalType::BOOLEAN, Value::BOOLEAN(true));

	config.AddExtensionOption("glue_get_partitions_segments",
	                          "How many GetPartitions requests to run at the same time when listing the partitions of "
	                          "a table (Glue's Segment API splits them over non overlapping segments). 0, the "
	                          "default, uses 8 against AWS and 1 against a Glue compatible server given with "
	                          "ENDPOINT. At most 10.",
	                          LogicalType::UBIGINT, Value::UBIGINT(0));

	config.AddExtensionOption("glue_max_retries",
	                          "Set a custom retry limit for AWS SDK Glue API calls: the number of retries after the "
	                          "first attempt fails, 0 meaning no retries (single attempt), at most 10. -1, the "
	                          "default, leaves the SDK's own retry policy in place.",
	                          LogicalType::BIGINT, Value::BIGINT(-1));

	config.AddExtensionOption("glue_create_bucketed_tables",
	                          "Allow CREATE TABLE ... WITH (BucketColumns = [...], NumberOfBuckets = n, SortColumns = "
	                          "[...]) to create a bucketed (clustered) Hive table. DuckDB reads such a table but does "
	                          "not write one: INSERT into it and CREATE TABLE ... AS with these options are refused. "
	                          "Default false.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	// global only: a view entry is shared by all sessions and reads it without a ClientContext
	ExtensionOption external_catalog_types(
	    "Types of non-Glue catalogs (e.g. ['duckdb', 'iceberg']) whose tables Glue views may store, by type and alias, "
	    "and read; never catalogs in memory, temporary, system or hidden. Global only. Default empty.",
	    LogicalType::LIST(LogicalType::VARCHAR), nullptr, Value::LIST(LogicalType::VARCHAR, vector<Value>()),
	    SetScope::GLOBAL, false, false);
	external_catalog_types.supports_session = false;
	config.AddExtensionOption("experimental_glue_view_external_catalog_types", std::move(external_catalog_types));
	config.AddExtensionOption("glue_metadata_cache_scope",
	                          "How long resolved Glue metadata (tables, partitions) is reused: 'transaction' (default) "
	                          "re-resolves per transaction, so writes from other sessions become visible between "
	                          "statements; 'connection' serves both binds of a prepared statement and later statements "
	                          "from one cache until a write through this connection invalidates it. A change takes "
	                          "effect on the next transaction.",
	                          LogicalType::VARCHAR, Value("transaction"));

	config.AddExtensionOption("glue_batch_table_lookup",
	                          "After the first table of a database is looked up one at a time, fetch one GetTables "
	                          "page of the database into the metadata cache instead of a GetTable per table; larger "
	                          "databases keep per-table lookups for the remainder. Default true.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));

	config.AddExtensionOption(
	    "glue_cache_listings",
	    "Keep the S3 listing of a Glue table's directory for later statements of the connection "
	    "(within one statement every scan and the cardinality sample share a listing already). "
	    "Kept like the metadata cache (glue_metadata_cache_scope) and cleared by the same writes; "
	    "files added any other way (another writer, COPY TO the table location) are not seen "
	    "until then. Files removed any other way (compaction, an overwrite, dropping and recreating "
	    "the table at the same location) make scans under the cached listing fail, or read the "
	    "superseded files, until then. Default false.",
	    LogicalType::BOOLEAN, Value::BOOLEAN(false));

	config.AddExtensionOption("hive_partition_listing_threshold",
	                          "When set, a scan reading at least this many partitions below the table location lists "
	                          "the location once (recursively) instead of one listing per partition. Unset (default), "
	                          "the location is listed when that takes fewer rounds of requests than listing the "
	                          "partitions read, or as many rounds and a tenth of the requests.",
	                          LogicalType::UBIGINT, Value(LogicalType::UBIGINT));

	config.AddExtensionOption("hive_csv_parallel",
	                          "Read each file of an OpenCSVSerde table with several threads when a scan has at most "
	                          "twice `threads` files. False reads one thread per file, which a file that holds a line "
	                          "break inside a quoted field needs: the parallel scanner refuses such a field together "
	                          "with the NULL padding of short rows. Default true.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));

	config.AddExtensionOption("hive_csv_max_line_size",
	                          "The longest line, in bytes, a scan of a csv table reads; a longer line fails the query. "
	                          "Hive reads a line of any length. Up to 8000000 the reader's buffers and how a file is "
	                          "split between threads are unchanged; a larger limit makes each thread read at least "
	                          "this many bytes. Default 8000000.",
	                          LogicalType::UBIGINT, Value::UBIGINT(HIVE_CSV_DEFAULT_MAX_LINE_SIZE));

	config.AddExtensionOption(
	    "glue_debug_fail_partition_registration",
	    "For tests: '<Glue error code>:<n>' makes the n-th partition a write registers in Glue, "
	    "and every later one, fail with that error code; a transient error (ThrottlingException, "
	    "InternalServiceException, ...) fails the first attempt only. Empty, the default, injects "
	    "nothing.",
	    LogicalType::VARCHAR, Value(""), GlueAPI::CheckDebugFailPartitionRegistration, SetScope::SESSION, true);

	// The HTTP client factory has to be in place before the first AWS client is constructed
	InitAWSAPI();
	RegisterGlueHttpClientFactory();
	RegisterGlueLogTypes(instance);

	// Hive tables are read with read_parquet. A linked parquet is loaded as linked: autoloading would install it first
	if (!instance.ExtensionIsLoaded("parquet")) {
		DuckDB db(instance);
		if (ExtensionHelper::LoadExtension(db, "parquet") != ExtensionLoadResult::LOADED_EXTENSION) {
			ExtensionHelper::AutoLoadExtension(instance, "parquet");
		}
	}
	if (!instance.ExtensionIsLoaded("parquet")) {
		throw MissingExtensionException("The glue extension requires the parquet extension to be loaded!");
	}
	// ATTACH '<catalog id>' (TYPE GLUE)
	StorageExtension::Register(config, "glue", make_shared_ptr<GlueStorageExtension>());

	loader.RegisterFunction(GetGlueGetTableResponseFunction());
	loader.RegisterFunction(GetGlueGetDatabaseResponseFunction());
	loader.RegisterFunction(GetGlueGetPartitionResponseFunction());
	loader.RegisterFunction(GetGluePartitionsFunction());
	loader.RegisterFunction(GetGlueAddPartitionFunction());
	loader.RegisterFunction(GetGlueDropPartitionFunction());
	loader.RegisterFunction(GetGlueRenamePartitionFunction());
	loader.RegisterFunction(GetGlueSetPartitionLocationFunction());
	loader.RegisterFunction(GetGlueSetTableLocationFunction());
	loader.RegisterFunction(GetGlueReplaceColumnsFunction());
	loader.RegisterFunction(GetGlueAlterTableFunction());
	loader.RegisterFunction(GetGlueRepairTableFunction());
	loader.RegisterFunction(*GetGlueRequestsMacro());
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
