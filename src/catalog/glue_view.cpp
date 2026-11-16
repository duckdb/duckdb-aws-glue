#include "catalog/glue_view.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"
#include "catalog/glue_view_names.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

GlueView::GlueView(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info, GlueTableInfo table_info_p,
                   string select_sql_p, string unsupported_reason_p)
    : ViewCatalogEntry(catalog, schema, info), table_info(std::move(table_info_p)), select_sql(std::move(select_sql_p)),
      is_duckdb_view(StringUtil::CIEquals(table_info.GetParameter("duckdb_view"), "true")),
      unsupported_reason(std::move(unsupported_reason_p)) {
	this->internal = false;
}

const SelectStatement &GlueView::GetQuery() {
	string signature;
	return ResolveQuery(signature);
}

const SelectStatement &GlueView::ResolveQuery(string &signature) {
	signature.clear();
	if (!IsSupported()) {
		throw BinderException("Glue view \"%s.%s\" can not be queried: %s", table_info.schema_name, table_info.name,
		                      unsupported_reason);
	}
	lock_guard<mutex> guard(parse_lock);
	if (!parsed) {
		unique_ptr<SelectStatement> statement;
		try {
			auto parser = Parser::GetBuiltinParser();
			statement = CreateViewInfo::ParseSelect(parser, select_sql);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			throw BinderException("Glue view \"%s.%s\" could not be parsed: %s", table_info.schema_name,
			                      table_info.name, error.RawMessage());
		}
		// set together: a failure here must not leave a parse without its ids
		referenced_ids = GlueViewNames::ReferencedCatalogIds(*statement->node);
		if (referenced_ids.empty()) {
			GlueViewNames::PinToCatalog(*statement->node, catalog.GetName(), schema.name);
		}
		parsed = std::move(statement);
	}
	if (referenced_ids.empty()) {
		// only the view's own catalog, which DuckDB binds the view in
		return *parsed;
	}
	// a non-Glue catalog's table needs its type listed; the smallest refused id, for a stable error
	auto external_types = GlueViewNames::ExternalCatalogTypes(catalog.GetDatabase());
	optional_ptr<const Identifier> refused;
	for (auto &id : referenced_ids) {
		if (GlueViewNames::IsExternal(id) && !external_types.count(GlueViewNames::ExternalType(id)) &&
		    (!refused || id < *refused)) {
			refused = &id;
		}
	}
	if (refused) {
		auto type = GlueViewNames::ExternalType(*refused);
		throw BinderException("Glue view \"%s.%s\" reads catalog \"%s\" of type %s, which is not in "
		                      "experimental_glue_view_external_catalog_types: add '%s' to it",
		                      table_info.schema_name, table_info.name, refused->GetIdentifierName(), type, type);
	}
	// map stored catalog ids to the aliases attached now
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	auto aliases = GlueViewNames::AttachedAliases(glue_catalog, referenced_ids);
	for (auto &entry : aliases) {
		signature += entry.first.GetIdentifierName() + "=" + entry.second.GetIdentifierName() + ";";
	}
	auto existing = resolved.find(signature);
	if (existing != resolved.end()) {
		return *existing->second;
	}
	auto copy = unique_ptr_cast<SQLStatement, SelectStatement>(parsed->Copy());
	GlueViewNames::ResolveForReading(glue_catalog, table_info.schema_name + "." + table_info.name, aliases,
	                                 *copy->node);
	GlueViewNames::PinToCatalog(*copy->node, catalog.GetName(), schema.name);
	auto &result = *copy;
	resolved.emplace(signature, std::move(copy));
	return result;
}

CatalogEntry &GlueView::ForReading(ClientContext &context) {
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	if (!IsSupported() || glue_catalog.options.iceberg_catalog.empty()) {
		return *this;
	}
	string signature;
	optional_ptr<const SelectStatement> query;
	try {
		query = &ResolveQuery(signature);
	} catch (std::exception &) {
		// binding the view reports the error
		return *this;
	}
	auto iceberg_catalog = Catalog::GetCatalogEntry(context, glue_catalog.options.iceberg_catalog);
	signature += "iceberg=" + (iceberg_catalog ? to_string(iceberg_catalog->GetOid()) : string());
	auto &transaction = GlueTransaction::Get(context, catalog);
	{
		lock_guard<mutex> guard(transaction.iceberg_views_lock);
		auto views = transaction.iceberg_views.find(*this);
		if (views != transaction.iceberg_views.end()) {
			auto existing = views->second.find(signature);
			if (existing != views->second.end()) {
				return existing->second ? *existing->second : *this;
			}
		}
	}
	unique_ptr<ViewCatalogEntry> view;
	auto mapped = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	if (GlueViewNames::MapIcebergTables(context, glue_catalog, table_info.schema_name + "." + table_info.name,
	                                    *mapped->node)) {
		auto info = GetInfo();
		auto &view_info = info->Cast<CreateViewInfo>();
		view_info.query = std::move(mapped);
		view_info.types.clear();
		view_info.names.clear();
		view = make_uniq<ViewCatalogEntry>(catalog, schema, view_info);
	}
	lock_guard<mutex> guard(transaction.iceberg_views_lock);
	auto &stored = transaction.iceberg_views[*this].emplace(signature, std::move(view)).first->second;
	return stored ? *stored : *this;
}

void GlueView::BindView(ClientContext &context, BindViewAction action) {
	auto &reading = ForReading(context);
	if (&reading == this) {
		ViewCatalogEntry::BindView(context, action);
		return;
	}
	auto &view = reading.Cast<ViewCatalogEntry>();
	view.BindView(context, action);
	auto columns = view.GetColumnInfo();
	if (columns) {
		UpdateBinding(columns->types, columns->names);
	}
}

string GlueView::ToSQL() const {
	return sql;
}

unique_ptr<CatalogEntry> GlueView::Copy(ClientContext &context) const {
	return FromTableInfo(catalog, schema, table_info);
}

string GlueView::RenderViewSql(const CreateViewInfo &info) {
	return info.query->ToString();
}

unique_ptr<GlueView> GlueView::FromTableInfo(Catalog &catalog, SchemaCatalogEntry &schema, const GlueTableInfo &table) {
	// shown: ViewOriginalText; executed: ViewExpandedText, by DuckDB views only (others are refused below)
	auto sql = table.view_expanded_text.empty() ? table.view_original_text : table.view_expanded_text;
	auto display_sql = table.view_original_text.empty() ? table.view_expanded_text : table.view_original_text;

	// Why the view can not be used, if it can not: decided here so that listing and DROP work on any view and only
	// SELECT / DESCRIBE report it
	string unsupported_reason;
	if (!StringUtil::CIEquals(table.GetParameter("duckdb_view"), "true")) {
		unsupported_reason = "it was not written by DuckDB (no duckdb_view parameter); query it from the engine that "
		                     "wrote it";
	} else {
		// The unqualified names in the SQL belong to the database the view was written in. DuckDB binds a view in
		// the database that holds it, so a view moved or copied to another database would bind against the wrong
		// tables; refuse it instead. Through a resource link the view is held by the link's target
		auto written_for = table.GetParameter("duckdb_view_default_database");
		auto &held_by = table.glue_database_name.empty() ? table.schema_name : table.glue_database_name;
		if (!written_for.empty() && !StringUtil::CIEquals(written_for, held_by)) {
			unsupported_reason = StringUtil::Format("it was written for database \"%s\" and is now in database \"%s\"; "
			                                        "DuckDB binds a view in the database that holds it",
			                                        written_for, held_by);
		}
	}

	// unbound on purpose: the binder derives names and types from the query on first use; the stored columns
	// carry the column alias list of CREATE VIEW
	CreateViewInfo info(schema, Identifier(table.name));
	bool has_column_alias_list = StringUtil::CIEquals(table.GetParameter("duckdb_view_column_alias_list"), "true");
	string alias_list;
	for (auto &column : table.columns) {
		if (!has_column_alias_list) {
			continue;
		}
		info.aliases.emplace_back(column.name);
		if (!alias_list.empty()) {
			alias_list += ", ";
		}
		alias_list += SQLIdentifier::ToString(column.name);
	}
	if (StringUtil::CIEquals(table.GetParameter("duckdb_view_secure"), "true")) {
		info.security_type = ViewSecurityType::SECURE_VIEW;
	}
	string header = string("CREATE ") + (info.security_type == ViewSecurityType::SECURE_VIEW ? "SECURE " : "") +
	                "VIEW " + SQLIdentifier::ToString(table.schema_name) + "." + SQLIdentifier::ToString(table.name);
	if (!alias_list.empty()) {
		header += " (" + alias_list + ")";
	}
	info.sql = header + " AS " + display_sql + ";";
	if (!table.description.empty()) {
		info.comment = Value(table.description);
	}
	for (auto &parameter : table.parameters) {
		info.tags[parameter.first] = parameter.second;
	}
	return make_uniq<GlueView>(catalog, schema, info, table, std::move(sql), std::move(unsupported_reason));
}

} // namespace duckdb
