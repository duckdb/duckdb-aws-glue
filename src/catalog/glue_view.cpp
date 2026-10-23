#include "catalog/glue_view.hpp"
#include "catalog/glue_catalog.hpp"
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
			GlueViewNames::PinToCatalog(*statement->node, catalog.GetName());
		}
		parsed = std::move(statement);
	}
	if (referenced_ids.empty()) {
		// only the view's own catalog, which DuckDB binds the view in
		return *parsed;
	}
	// map stored catalog ids to the aliases attached now
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	auto aliases = GlueViewNames::AttachedAliases(glue_catalog, referenced_ids);
	string signature;
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
	GlueViewNames::PinToCatalog(*copy->node, catalog.GetName());
	auto &result = *copy;
	resolved.emplace(std::move(signature), std::move(copy));
	return result;
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
	// carry the column alias list of CREATE VIEW (none for a view created with DEFER_BINDING)
	CreateViewInfo info(schema, Identifier(table.name));
	for (auto &column : table.columns) {
		info.aliases.emplace_back(column.name);
	}
	if (StringUtil::CIEquals(table.GetParameter("duckdb_view_secure"), "true")) {
		info.security_type = ViewSecurityType::SECURE_VIEW;
	}
	info.sql = string("CREATE ") + (info.security_type == ViewSecurityType::SECURE_VIEW ? "SECURE " : "") + "VIEW " +
	           SQLIdentifier::ToString(table.name) + " AS " + display_sql + ";";
	if (!table.description.empty()) {
		info.comment = Value(table.description);
	}
	for (auto &parameter : table.parameters) {
		info.tags[parameter.first] = parameter.second;
	}
	return make_uniq<GlueView>(catalog, schema, info, table, std::move(sql), std::move(unsupported_reason));
}

} // namespace duckdb
