#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {
class ClientContext;
class DatabaseInstance;
class GlueCatalog;

//! ViewExpandedText names tables of the view's own catalog <db>.<table>, of other Glue catalogs
//! "<catalog id>".<db>.<table>, of other kinds of catalogs (experimental_glue_view_external_catalog_types)
//! "external:<type>:<alias>".<schema>.<table>; readers map these to attached catalogs
struct GlueViewNames {
	//! CREATE VIEW: qualify bare table names with their database; a table of another catalog, named with its catalog,
	//! goes to its Glue catalog id or "external:<type>:<alias>". Throws for catalog-qualified columns, table functions,
	//! a bare name outside the view's catalog, another region or endpoint, no reported id, an unlisted or unusable
	//! non-Glue catalog
	static void QualifyForStorage(ClientContext &context, GlueCatalog &view_catalog, const Identifier &view_schema,
	                              QueryNode &query);
	//! the catalog ids the stored body names tables by
	static identifier_set_t ReferencedCatalogIds(QueryNode &query);
	//! experimental_glue_view_external_catalog_types (global only)
	static case_insensitive_set_t ExternalCatalogTypes(DatabaseInstance &db);
	//! "external:<type>:<alias>", the prefix in any case
	static bool IsExternal(const Identifier &id);
	//! for an IsExternal id
	static string ExternalType(const Identifier &id);
	//! id -> alias, for the given ids: an attached Glue catalog in the view's region and endpoint, or an attached
	//! usable catalog with the stored type and alias
	static identifier_tree_t<Identifier> AttachedAliases(GlueCatalog &view_catalog, const identifier_set_t &ids);
	//! reading: ids -> aliases; throws for an id not attached
	static void ResolveForReading(GlueCatalog &view_catalog, const string &view_name,
	                              const identifier_tree_t<Identifier> &aliases, QueryNode &query);
	//! reading: db.t -> catalog.db.t, as DuckDB binds x.t to a catalog x when one is attached under that name; a bare
	//! t (not a CTE) -> catalog.schema.t
	static void PinToCatalog(QueryNode &query, const Identifier &catalog_name, const Identifier &schema_name);
	//! reading: catalog.db.t of an Iceberg table in Glue -> iceberg_catalog.db.t; whether any was mapped
	static bool MapIcebergTables(ClientContext &context, GlueCatalog &view_catalog, const string &view_name,
	                             QueryNode &query);
};

} // namespace duckdb
