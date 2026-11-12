#pragma once

#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class CatalogEntry;
class ClientContext;
class GlueCatalog;
class GlueTable;
class IcebergCatalogBridge;

class GlueTableResolver {
public:
	explicit GlueTableResolver(GlueCatalog &catalog);
	~GlueTableResolver();

	CatalogEntry &Resolve(ClientContext &context, GlueTable &table);
	void OnDetach(ClientContext &context);

private:
	GlueCatalog &catalog;
	unique_ptr<IcebergCatalogBridge> iceberg;
};

} // namespace duckdb
