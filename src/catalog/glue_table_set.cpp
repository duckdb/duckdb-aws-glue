#include "catalog/glue_table_set.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include "core/glue_types.hpp"
#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

GlueTableSet::GlueTableSet(GlueSchemaEntry &schema) : schema(schema), catalog(schema.catalog.Cast<GlueCatalog>()) {
}

unique_ptr<CatalogEntry> GlueTableSet::CreateEntry(const GlueTableInfo &table) {
	if (table.IsView()) {
		return GlueView::FromTableInfo(catalog, schema, table);
	}
	CreateTableInfo info(schema, Identifier(table.name));
	auto is_iceberg = table.GetFormat() == GlueTableFormat::ICEBERG;
	for (auto &column : table.columns) {
		LogicalType type;
		try {
			type = GlueTypes::ToLogicalType(column.type);
		} catch (std::exception &) {
			if (!is_iceberg) {
				throw;
			}
			// only listed: scans and DESCRIBE of an Iceberg table use the schema of the Iceberg catalog
			type = LogicalType::UNKNOWN;
		}
		info.columns.AddColumn(ColumnDefinition(Identifier(column.name), std::move(type)));
	}
	// Hive tables store their partition columns separately, they are regular (trailing) columns for a scan
	for (auto &column : table.partition_keys) {
		info.columns.AddColumn(ColumnDefinition(Identifier(column.name), GlueTypes::ToLogicalType(column.type)));
	}
	auto entry = make_uniq<GlueTable>(catalog, schema, info, table);
	SetTableTypeTag(*entry);
	return entry;
}

void GlueTableSet::SetTableTypeTag(GlueTable &entry) {
	// exposed through duckdb_tables().tags['table_type'] (ICEBERG / DELTA / HIVE / UNKNOWN)
	entry.tags["table_type"] = GlueTableFormatToString(entry.table_info.GetFormat());
}

void GlueTableSet::LoadEntries(ClientContext &context) {
	if (is_loaded) {
		return;
	}
	auto tables = GlueAPI::GetTables(context, catalog, schema.database_info.name);
	for (auto &table : tables) {
		if (entries.find(table.name) != entries.end()) {
			// already loaded through a direct lookup
			continue;
		}
		unique_ptr<CatalogEntry> entry;
		try {
			entry = CreateEntry(table);
		} catch (std::exception &ex) {
			// A table whose Glue definition we can not turn into a DuckDB table (e.g. an unsupported column type)
			// must not break listing the other tables: leave it out and log why. Looking the table up by name
			// still reports the error to the user.
			ErrorData error(ex);
			DUCKDB_LOG_ERROR(context, "Glue table '%s.%s' is not listed: %s", schema.database_info.name, table.name,
			                 error.RawMessage());
			continue;
		}
		entries.emplace(table.name, std::move(entry));
	}
	is_loaded = true;
}

optional_ptr<CatalogEntry> GlueTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	auto &name = lookup.GetEntryName();
	// the reader's Glue transaction keeps the entry alive while it is used (GlueTransactionManager::RetireEntry); it
	// is started before entry_lock is taken, the order RetireEntry takes the locks in
	Transaction::Get(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	auto entry = entries.find(name);
	if (entry != entries.end()) {
		return entry->second.get();
	}
	// not cached, ask Glue for this table directly
	GlueTableInfo table;
	if (!GlueAPI::GetTable(context, catalog, schema.database_info.name, name, table)) {
		return nullptr;
	}
	auto result = entries.emplace(table.name, CreateEntry(table));
	return result.first->second.get();
}

void GlueTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	// tables and views share the set and a scan of either type yields both, as in DuckDB's own schema: the catalog
	// functions filter by entry type themselves, and duckdb_columns() collects the columns of views through a
	// TABLE_ENTRY scan
	Transaction::Get(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	LoadEntries(context);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> GlueTableSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> guard(entry_lock);
	auto name = entry->name.GetIdentifierName();
	RetireEntry(name);
	auto result = entries.emplace(name, std::move(entry));
	return result.first->second.get();
}

void GlueTableSet::RemoveEntry(const string &name) {
	lock_guard<mutex> guard(entry_lock);
	RetireEntry(name);
}

void GlueTableSet::InvalidateEntry(const string &name) {
	lock_guard<mutex> guard(entry_lock);
	RetireEntry(name);
	is_loaded = false;
}

void GlueTableSet::RetireEntry(const string &name) {
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return;
	}
	catalog.GetAttached().GetTransactionManager().Cast<GlueTransactionManager>().RetireEntry(std::move(entry->second));
	entries.erase(entry);
}

} // namespace duckdb
