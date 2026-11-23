#include "execution/glue_written_files.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

bool GlueWrittenFiles::IsEnabled(ClientContext &context) {
	Value enabled;
	return context.TryGetCurrentSetting(SETTING, enabled) && !enabled.IsNull() && BooleanValue::Get(enabled);
}

void GlueWrittenFiles::Append(ClientContext &context, const string &table, const vector<string> &files) {
	auto &temp_catalog = Catalog::GetCatalog(context, Identifier::TempCatalog());
	QualifiedName name(Identifier::TempCatalog(), Identifier::DefaultSchema(), Identifier(TABLE));
	auto info = make_uniq<CreateTableInfo>(name);
	info->temporary = true;
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	info->columns.AddColumn(ColumnDefinition("query_id", LogicalType::UBIGINT));
	info->columns.AddColumn(ColumnDefinition("table", LogicalType::VARCHAR));
	info->columns.AddColumn(ColumnDefinition("file_path", LogicalType::VARCHAR));
	temp_catalog.CreateTable(context, std::move(info));

	auto &entry = temp_catalog.GetEntry<TableCatalogEntry>(context, name);
	InternalAppender appender(context, entry);
	auto query_id = context.transaction.GetActiveQuery();
	for (auto &file : files) {
		appender.BeginRow();
		appender.Append<uint64_t>(query_id);
		appender.Append(string_t(table));
		appender.Append(string_t(file));
		appender.EndRow();
	}
	appender.Close();
}

} // namespace duckdb
