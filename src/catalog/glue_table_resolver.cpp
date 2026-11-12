#include "catalog/glue_table_resolver.hpp"

#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_table.hpp"
#include "catalog/glue_transaction.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

//! Owns the Iceberg catalog delegated to by Glue.
class IcebergCatalogBridge {
public:
	explicit IcebergCatalogBridge(GlueCatalog &catalog) : catalog(catalog) {
	}
	Catalog &GetCatalog(ClientContext &context);
	void OnDetach(ClientContext &context);

private:
	GlueCatalog &catalog;
	mutex lock;
	shared_ptr<AttachedDatabase> database;
	Identifier secret_name;
};

namespace {

class IcebergRedirectView : public ViewCatalogEntry {
public:
	IcebergRedirectView(ClientContext &context, GlueTable &table, IcebergCatalogBridge &bridge, CreateViewInfo &info)
	    : ViewCatalogEntry(table.ParentCatalog(), table.ParentSchema(), info), context(context), table(table),
	      bridge(bridge) {
	}

	const SelectStatement &GetQuery() override {
		auto &schema = table.ParentSchema().Cast<GlueSchemaEntry>();
		if (schema.GetSchemaInfo(context).is_resource_link) {
			throw BinderException("Glue cannot automatically read Iceberg table \"%s.%s\" through a resource link",
			                      schema.name, table.name);
		}
		auto &reader = bridge.GetCatalog(context);
		auto ref = make_uniq<BaseTableRef>();
		ref->SetQualifiedName(reader.GetName(), schema.name, table.name);
		auto select = make_uniq<SelectNode>();
		select->select_list.push_back(make_uniq<StarExpression>());
		select->from_table = std::move(ref);
		query = make_uniq<SelectStatement>();
		query->node = std::move(select);
		return *query;
	}

	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override {
		auto info = GetInfo();
		return make_uniq<IcebergRedirectView>(context, table, bridge, info->Cast<CreateViewInfo>());
	}

private:
	// Redirect entries live in the Glue transaction of this context.
	ClientContext &context;
	GlueTable &table;
	IcebergCatalogBridge &bridge;
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

CatalogEntry &GlueTableResolver::Resolve(ClientContext &context, GlueTable &table) {
	auto &transaction = GlueTransaction::Get(context, catalog);
	auto existing = transaction.resolved_tables.find(table);
	if (existing != transaction.resolved_tables.end()) {
		return *existing->second;
	}
	auto &schema = table.ParentSchema();
	unique_ptr<CatalogEntry> resolved;
	auto format = table.table_info.GetFormat().format;
	if (format == GlueFormat::ICEBERG) {
		CreateViewInfo info(schema, table.name);
		resolved = make_uniq<IcebergRedirectView>(context, table, *iceberg, info);
	} else {
		auto implementation = format == GlueFormat::HIVE
		                          ? GlueTableImplementation::CreateHive(table.GetColumns())
		                          : GlueTableImplementation::CreateUnsupported(table.GetColumns());
		CreateTableInfo info(schema, table.name);
		info.columns = table.GetColumns().Copy();
		resolved = make_uniq<GlueTable>(catalog, schema, info, table.table_info, std::move(implementation));
	}
	resolved->oid = table.oid;
	resolved->comment = table.comment;
	resolved->tags = table.tags;
	auto &result = *resolved;
	transaction.resolved_tables.emplace(table, std::move(resolved));
	return result;
}

void GlueTableResolver::OnDetach(ClientContext &context) {
	iceberg->OnDetach(context);
}

} // namespace duckdb
