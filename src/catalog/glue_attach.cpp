#include "catalog/glue_attach.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_options.hpp"
#include "catalog/glue_catalog.hpp"

#include <regex>

namespace duckdb {

namespace {

//! Validate the ATTACH path, which identifies the Glue catalog
//! Same formats as Glue's own catalog id parameter (':' = default catalog of the current account)
void SanityCheckGlueCatalogPath(const string &path) {
	const std::regex patterns[] = {
	    std::regex("^:$"),                  // Default catalog ":" in current account
	    std::regex("^\\d{12}$"),            // Default catalog in a specific account
	    std::regex("^\\d{12}:[^:/]+$"),     // Specific catalog in a specific account
	    std::regex("^[^:]+/[^:]+$"),        // Nested catalog in the current account
	    std::regex("^\\d{12}:[^/]+/[^:]+$") // Nested catalog in a specific account
	};

	for (const auto &pattern : patterns) {
		if (std::regex_match(path, pattern)) {
			return;
		}
	}

	throw InvalidConfigurationException(
	    "Invalid Glue Catalog Format: '%s'. Expected format: ':', '12-digit account ID', "
	    "'catalog1/catalog2', or '12-digit accountId:catalog1/catalog2'.",
	    path);
}

} // namespace

unique_ptr<Catalog> GlueAttach::Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &name, AttachInfo &info,
                                       AttachOptions &options) {
	GlueAttachOptions attach_options;
	attach_options.name = name;
	attach_options.path = info.path;
	SanityCheckGlueCatalogPath(attach_options.path);
	if (attach_options.path != ":") {
		// ':' means the default catalog of the current account, which is what Glue uses without a CatalogId
		attach_options.catalog_id = attach_options.path;
	}

	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type" || lower_name == "read_only") {
			continue;
		}
		if (lower_name == "secret") {
			attach_options.secret_name = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "region") {
			attach_options.region = entry.second.ToString();
		} else if (lower_name == "endpoint") {
			attach_options.endpoint = entry.second.ToString();
			StringUtil::RTrim(attach_options.endpoint, "/");
		} else if (lower_name == "default_location") {
			attach_options.default_location = entry.second.ToString();
			StringUtil::RTrim(attach_options.default_location, "/");
		} else if (lower_name == "default_schema") {
			attach_options.default_schema = Identifier(entry.second.ToString());
		} else if (lower_name == "verify_connection") {
			attach_options.verify_connection = entry.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else {
			throw InvalidConfigurationException("Unrecognized option for Glue attach: '%s'", entry.first);
		}
	}

	// The region defaults to the region of the storage secret (an error is thrown if there is no secret)
	auto secret_entry = GlueCatalog::GetStorageSecret(context, attach_options.secret_name);
	if (attach_options.region.empty()) {
		auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
		auto region = kv_secret.TryGetValue("region");
		if (region.IsNull()) {
			throw InvalidConfigurationException(
			    "Secret '%s' used for Glue catalog '%s' does not have a region, provide one with the REGION option",
			    secret_entry->secret->GetName().GetIdentifierName(), name);
		}
		attach_options.region = region.ToString();
	}

	auto catalog = make_uniq<GlueCatalog>(db, options.access_mode, std::move(attach_options));
	// Fail early when the catalog can not be reached with these credentials (unless opted out)
	if (catalog->options.verify_connection) {
		GlueAPI::VerifyConnection(context, *catalog);
	}
	if (!catalog->options.default_schema.empty()) {
		GlueDatabaseInfo database;
		if (!GlueAPI::GetDatabase(context, *catalog, catalog->options.default_schema.GetIdentifierName(), database)) {
			throw InvalidConfigurationException("default_schema '%s' does not exist in Glue catalog '%s'",
			                                    catalog->options.default_schema.GetIdentifierName(),
			                                    catalog->options.path);
		}
	}
	catalog->AttachIcebergCatalog(context);
	return std::move(catalog);
}

} // namespace duckdb
