#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {
class Catalog;
class ClientContext;
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

//! experimental_glue_view_iceberg_catalogs: a Glue view reads the Iceberg tables of a mapped Glue catalog id through
//! the attached Iceberg catalog mapped to it, once that catalog is checked to serve the id
struct GlueViewIcebergCatalogs {
	//! the setting's entries; throws for a NULL or empty id or alias, or ids differing only in case
	static ViewIcebergCatalogs Read(const Value &setting);
	//! CREATE VIEW: the Glue catalog id an Iceberg catalog is mapped from and serves, empty if it is mapped from none;
	//! throws if it serves none of the ids it is mapped from
	static string ServedCatalogId(ClientContext &context, GlueCatalog &view_catalog, Catalog &iceberg);
	//! reading: Iceberg tables of mapped ids (all tables of an id no attached Glue catalog has) -> their catalog
	static IcebergTargets FindIcebergTargets(ClientContext &context, GlueCatalog &view_catalog, QueryNode &stored);
};

} // namespace duckdb
