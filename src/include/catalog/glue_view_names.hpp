#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {
class ClientContext;
class DatabaseInstance;
class GlueCatalog;
class Value;

//! A stored table read from an Iceberg catalog, and that catalog's attach oid when it was checked
struct IcebergTarget {
	Identifier catalog;
	idx_t catalog_oid;
};
struct QualifiedNameHash {
	hash_t operator()(const QualifiedName &name) const {
		return name.Hash();
	}
};
//! stored table name (case-insensitive) -> where it is read from
using IcebergTargets = unordered_map<QualifiedName, IcebergTarget, QualifiedNameHash>;
//! experimental_glue_view_iceberg_catalogs: (Glue catalog id, Iceberg catalog alias)
using ViewIcebergCatalogs = vector<pair<string, string>>;

//! ViewExpandedText names tables of the view's own catalog <db>.<table>, of other Glue catalogs
//! "<catalog id>".<db>.<table>, of other kinds of catalogs (experimental_glue_view_external_catalog_types)
//! "external:<type>:<alias>".<schema>.<table>; readers map these to attached catalogs
struct GlueViewNames {
	//! CREATE VIEW: qualify bare table names with their database; a table of another catalog, named with its catalog,
	//! goes to its Glue catalog id or "external:<type>:<alias>". Throws for catalog-qualified columns, table functions,
	//! a bare name outside the view's catalog, another region or endpoint, no reported id, an unlisted or unusable
	//! non-Glue catalog. A table stored by its Glue name through a mapped Iceberg catalog is named so in `shown` too
	static void QualifyForStorage(ClientContext &context, GlueCatalog &view_catalog, const Identifier &view_schema,
	                              QueryNode &query, optional_ptr<QueryNode> shown = nullptr);
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
	//! reading: ids -> aliases, and an Iceberg target -> its Iceberg catalog; throws for an id not attached
	static void ResolveForReading(GlueCatalog &view_catalog, const string &view_name,
	                              const identifier_tree_t<Identifier> &aliases, const IcebergTargets &iceberg_targets,
	                              QueryNode &query);
	//! reading: db.t -> catalog.db.t, as DuckDB binds x.t to a catalog x when one is attached under that name; a bare
	//! t (not a CTE) -> catalog.schema.t
	static void PinToCatalog(QueryNode &query, const Identifier &catalog_name, const Identifier &schema_name);

	//! the setting's entries; throws for a NULL or empty id or alias, or ids differing only in case
	static ViewIcebergCatalogs ReadViewIcebergCatalogs(const Value &setting);
	//! reading: Iceberg tables of mapped ids (all tables of an id no attached Glue catalog has) -> their catalog
	static IcebergTargets FindIcebergTargets(ClientContext &context, GlueCatalog &view_catalog, QueryNode &stored);
};

} // namespace duckdb
