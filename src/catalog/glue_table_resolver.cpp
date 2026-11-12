#include "catalog/glue_table_resolver.hpp"

#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_table.hpp"
#include "catalog/glue_transaction.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

//! Compatibility with Iceberg's current catalog API.
class IcebergCatalogBridge {
public:
	explicit IcebergCatalogBridge(GlueCatalog &catalog) : catalog(catalog) {
	}
	TableCatalogEntry &Resolve(ClientContext &context, const GlueTable &table);
	void OnDetach(ClientContext &context);

private:
	Catalog &GetCatalog(ClientContext &context);

private:
	GlueCatalog &catalog;
	mutex lock;
	shared_ptr<AttachedDatabase> database;
	Identifier secret_name;
};

namespace {

class IcebergTableImplementation : public GlueTableImplementation {
public:
	IcebergTableImplementation(IcebergCatalogBridge &bridge, const ColumnList &columns)
	    : GlueTableImplementation(columns.Copy()), bridge(bridge) {
	}

	const ColumnList &GetColumns() const override {
		return table ? table->GetColumns() : GlueTableImplementation::GetColumns();
	}
	TableFunction BindScan(ClientContext &context, GlueTable &glue_table, unique_ptr<FunctionData> &bind_data,
	                       const EntryLookupInfo &lookup) override {
		if (!table) {
			auto &schema = glue_table.ParentSchema().Cast<GlueSchemaEntry>();
			if (schema.GetSchemaInfo(context).is_resource_link) {
				throw BinderException("Glue cannot automatically read Iceberg table \"%s.%s\" through a resource link",
				                      schema.name, glue_table.name);
			}
			table = &bridge.Resolve(context, glue_table);
		}
		EntryLookupInfo physical_lookup(
		    lookup, QualifiedName(table->ParentCatalog().GetName(), table->ParentSchema().name, table->name));
		return table->GetScanFunction(context, bind_data, physical_lookup);
	}
	virtual_column_map_t GetVirtualColumns() const override {
		return table ? table->GetVirtualColumns() : virtual_column_map_t {};
	}
	vector<column_t> GetRowIdColumns() const override {
		return table ? table->GetRowIdColumns() : vector<column_t> {};
	}

private:
	IcebergCatalogBridge &bridge;
	optional_ptr<TableCatalogEntry> table;
};

} // namespace

Catalog &IcebergCatalogBridge::GetCatalog(ClientContext &context) {
	lock_guard<mutex> guard(lock);
	auto &manager = DatabaseManager::Get(context);
	auto secret = GlueCatalog::GetStorageSecret(context, catalog.options.secret_name);
	auto selected_secret = secret->secret->GetName();
	if (database && secret_name != selected_secret && manager.GetDatabase(database->GetName()) == database) {
		manager.DetachDatabase(context, database->GetName(), OnEntryNotFound::RETURN_NULL);
	}
	if (database && manager.GetDatabase(database->GetName()) != database) {
		// A rollback can remove an attachment created while binding.
		database.reset();
	}
	if (!database) {
		static atomic<idx_t> next_reader_id {0};
		AttachInfo info;
		do {
			info.name = Identifier("__glue_iceberg_" + to_string(next_reader_id++));
		} while (manager.GetDatabase(info.name));
		auto &options = catalog.options;
		info.path = options.path;
		auto endpoint = options.endpoint.empty() ? "glue." + options.region + ".amazonaws.com" : options.endpoint;
		info.options = {{"type", Value("iceberg")},
		                {"endpoint_type", Value("glue")},
		                {"uri", Value(endpoint + "/iceberg")},
		                {"sigv4_region", Value(options.region)},
		                {"secret", Value(selected_secret.GetIdentifierName())}};
		AttachOptions attach_options(DBConfig::GetConfig(context).options);
		attach_options.db_type = "iceberg";
		attach_options.access_mode = AccessMode::READ_ONLY;
		attach_options.visibility = AttachVisibility::HIDDEN;
		database = manager.AttachDatabase(context, info, attach_options);
		if (!database) {
			throw InternalException("Could not attach the Iceberg reader for Glue catalog \"%s\"",
			                        catalog.GetName().GetIdentifierName());
		}
		secret_name = selected_secret;
	}
	// Keep it alive if another connection rolls back the transaction that attached it.
	return MetaTransaction::Get(context).UseDatabase(database).GetCatalog();
}

TableCatalogEntry &IcebergCatalogBridge::Resolve(ClientContext &context, const GlueTable &table) {
	auto &reader = GetCatalog(context);
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
	                       QualifiedName(reader.GetName(), table.ParentSchema().name, table.name));
	auto entry = Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		throw CatalogException(
		    "Iceberg table \"%s.%s\" is registered in Glue but is not returned by its Iceberg endpoint",
		    table.table_info.schema_name, table.table_info.name);
	}
	return entry->Cast<TableCatalogEntry>();
}

void IcebergCatalogBridge::OnDetach(ClientContext &context) {
	lock_guard<mutex> guard(lock);
	if (!database) {
		return;
	}
	auto &manager = DatabaseManager::Get(context);
	auto name = database->GetName();
	if (manager.GetDatabase(name) == database) {
		manager.DetachDatabase(context, name, OnEntryNotFound::RETURN_NULL);
	}
	database.reset();
}

GlueTableResolver::GlueTableResolver(GlueCatalog &catalog)
    : catalog(catalog), iceberg(make_uniq<IcebergCatalogBridge>(catalog)) {
}

GlueTableResolver::~GlueTableResolver() {
}

GlueTable &GlueTableResolver::Resolve(ClientContext &context, GlueTable &table) {
	auto &transaction = GlueTransaction::Get(context, catalog);
	auto existing = transaction.resolved_tables.find(table);
	if (existing != transaction.resolved_tables.end()) {
		return *existing->second;
	}
	unique_ptr<GlueTableImplementation> implementation;
	switch (table.table_info.GetFormat().format) {
	case GlueFormat::HIVE:
		implementation = GlueTableImplementation::CreateHive(table.GetColumns());
		break;
	case GlueFormat::ICEBERG:
		implementation = make_uniq<IcebergTableImplementation>(*iceberg, table.GetColumns());
		break;
	default:
		implementation = GlueTableImplementation::CreateUnsupported(table.GetColumns());
		break;
	}
	auto &schema = table.ParentSchema();
	CreateTableInfo info(schema, table.name);
	info.columns = implementation->GetColumns().Copy();
	info.comment = table.comment;
	auto resolved = make_uniq<GlueTable>(catalog, schema, info, table.table_info, std::move(implementation));
	resolved->oid = table.oid;
	resolved->tags = table.tags;
	auto &result = *resolved;
	transaction.resolved_tables.emplace(table, std::move(resolved));
	return result;
}

void GlueTableResolver::OnDetach(ClientContext &context) {
	iceberg->OnDetach(context);
}

} // namespace duckdb
