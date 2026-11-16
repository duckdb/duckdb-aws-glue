#include "api/glue_api_util.hpp"
#include "api/glue_http_client.hpp"
#include "api/glue_retry_strategy.hpp"

#include "duckdb/common/string_util.hpp"

#include <aws/glue/model/Catalog.h>
#include <aws/glue/model/GetCatalogRequest.h>
#include <aws/glue/model/GetCatalogsRequest.h>

namespace duckdb {

static GlueCatalogInfo ToCatalogInfo(const Aws::Glue::Model::Catalog &catalog) {
	GlueCatalogInfo result;
	result.catalog_id = ToStdString(catalog.GetCatalogId());
	result.name = ToStdString(catalog.GetName());
	result.description = ToStdString(catalog.GetDescription());
	if (catalog.FederatedCatalogHasBeenSet()) {
		result.connection_name = ToStdString(catalog.GetFederatedCatalog().GetConnectionName());
	}
	if (catalog.CatalogPropertiesHasBeenSet()) {
		result.catalog_type =
		    ToStdString(catalog.GetCatalogProperties().GetDataLakeAccessProperties().GetCatalogType());
	}
	result.targets_redshift = catalog.TargetRedshiftCatalogHasBeenSet();
	return result;
}

bool GlueAPI::GetCatalog(ClientContext &context, GlueCatalog &catalog, const string &catalog_id,
                         GlueCatalogInfo &result) {
	GlueHttpClientContextScope http_scope(context);
	// a lookup falls back to this call after a database was not found, on endpoints that may not have it
	GlueUnknownCallScope unknown_call_scope;
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetCatalogRequest request;
	request.SetCatalogId(catalog_id);
	auto outcome = client->GetCatalog(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			return false;
		}
		ThrowGlueError(outcome, StringUtil::Format("GetCatalog '%s'", catalog_id));
	}
	result = ToCatalogInfo(outcome.GetResult().GetCatalog());
	return true;
}

vector<GlueCatalogInfo> GlueAPI::GetCatalogs(ClientContext &context, GlueCatalog &catalog,
                                             const string &parent_catalog_id) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	vector<GlueCatalogInfo> result;
	// without a parent Glue lists the catalogs below the Data Catalog of the current account
	auto &parent = parent_catalog_id.empty() ? catalog.options.catalog_id : parent_catalog_id;
	Aws::String next_token;
	do {
		Aws::Glue::Model::GetCatalogsRequest request;
		if (!parent.empty()) {
			request.SetParentCatalogId(parent);
		}
		request.SetRecursive(true);
		if (!next_token.empty()) {
			request.SetNextToken(next_token);
		}
		auto outcome = client->GetCatalogs(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, parent.empty()
			                            ? string("GetCatalogs (below the Data Catalog of the current account)")
			                            : StringUtil::Format("GetCatalogs (below catalog '%s')", parent));
		}
		for (auto &child : outcome.GetResult().GetCatalogList()) {
			result.push_back(ToCatalogInfo(child));
		}
		next_token = outcome.GetResult().GetNextToken();
	} while (!next_token.empty());
	return result;
}

} // namespace duckdb
