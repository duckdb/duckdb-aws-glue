#include "catalog/glue_view_iceberg_catalogs.hpp"

#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_table.hpp"
#include "catalog/glue_transaction.hpp"
#include "catalog/glue_view_names.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {

namespace {

constexpr char VIEW_ICEBERG_CATALOGS[] = "experimental_glue_view_iceberg_catalogs";

//! the mapping as of the transaction's first use of a Glue catalog: a change applies from the next transaction
const ViewIcebergCatalogs &Mappings(ClientContext &context, GlueCatalog &view_catalog) {
	return GlueTransaction::Get(context, view_catalog).view_iceberg_catalogs;
}

string AttachOption(const unordered_map<string, Value> &options, const string &name) {
	for (auto &option : options) {
		if (StringUtil::CIEquals(option.first, name)) {
			return option.second.ToString();
		}
	}
	return string();
}

//! the region of [https://]glue[-fips].<region>.amazonaws.com[/iceberg], empty if the URI is not that
string GlueIcebergUriRegion(const string &uri) {
	auto rest = StringUtil::Lower(uri);
	if (StringUtil::StartsWith(rest, "https://")) {
		rest = rest.substr(8);
	}
	while (StringUtil::EndsWith(rest, "/")) {
		rest.pop_back();
	}
	auto slash = rest.find('/');
	auto host = rest.substr(0, slash);
	if (slash != string::npos && rest.substr(slash) != "/iceberg") {
		return string();
	}
	const string suffix = ".amazonaws.com";
	if (!StringUtil::EndsWith(host, suffix)) {
		return string();
	}
	host = host.substr(0, host.size() - suffix.size());
	for (auto prefix : {"glue.", "glue-fips."}) {
		if (StringUtil::StartsWith(host, prefix)) {
			auto region = host.substr(strlen(prefix));
			auto valid = !region.empty() && std::all_of(region.begin(), region.end(), [](char c) {
				return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-';
			});
			return valid ? region : string();
		}
	}
	return string();
}

//! why a catalog (type, ATTACH path and options) does not serve the Iceberg tables of a Glue catalog id in a region.
//! Coupled to duckdb-iceberg's ATTACH options by name, as an attached catalog reports only its type, path and raw
//! options, and this extension does not link duckdb-iceberg. A renamed URI or ENDPOINT there makes this refuse; a
//! renamed ENDPOINT_TYPE or PREFIX, or a new option that changes where requests go, would not be seen
string WhyNotServing(const string &catalog_type, const string &warehouse,
                     const unordered_map<string, Value> &attach_options, const string &catalog_id,
                     const string &region) {
	if (catalog_type != "iceberg") {
		return StringUtil::Format("it is a catalog of type %s, not iceberg", catalog_type);
	}
	// mirrors duckdb-iceberg's ATTACH: requests go to URI (deprecated: ENDPOINT), else to the Glue endpoint of the
	// SECRET's region as of ATTACH, which can not be read back; under /v1/<PREFIX> if given; for the ATTACH path
	auto endpoint_type = AttachOption(attach_options, "endpoint_type");
	if (!endpoint_type.empty() && !StringUtil::CIEquals(endpoint_type, "glue")) {
		return StringUtil::Format("it is attached with ENDPOINT_TYPE '%s'", endpoint_type);
	}
	auto uri = AttachOption(attach_options, "uri");
	if (uri.empty()) {
		uri = AttachOption(attach_options, "endpoint");
	}
	if (uri.empty()) {
		return "its region can not be checked: attach it with URI 'https://glue.<region>.amazonaws.com/iceberg'";
	}
	auto uri_region = GlueIcebergUriRegion(uri);
	if (uri_region.empty()) {
		return "its URI is not https://glue.<region>.amazonaws.com/iceberg";
	}
	if (!AttachOption(attach_options, "prefix").empty()) {
		return "it is attached with a PREFIX, which can name another catalog";
	}
	if (!StringUtil::CIEquals(uri_region, region)) {
		return StringUtil::Format("it is in region %s, the Glue catalog in region %s", uri_region, region);
	}
	if (!StringUtil::CIEquals(warehouse, catalog_id)) {
		return StringUtil::Format("it serves Glue catalog id '%s', not '%s'", warehouse, catalog_id);
	}
	return string();
}

//! WhyNotServing for an attached catalog and the view's Glue catalog
string WhyNotServingView(GlueCatalog &view_catalog, const string &catalog_id, Catalog &iceberg) {
	if (iceberg.GetCatalogType() == "iceberg" && !view_catalog.options.endpoint.empty()) {
		return "the Glue catalog is attached with an ENDPOINT, which an Iceberg catalog can not be matched with";
	}
	auto warehouse = iceberg.GetCatalogType() == "iceberg" ? iceberg.GetDBPath() : string();
	return WhyNotServing(iceberg.GetCatalogType(), warehouse, iceberg.GetAttached().GetAttachOptions(), catalog_id,
	                     view_catalog.options.region);
}

//! the Iceberg catalog mapped to a Glue catalog id, null if none; throws if it is not attached or does not serve it
optional_ptr<Catalog> MappedIcebergCatalog(ClientContext &context, GlueCatalog &view_catalog,
                                           const string &catalog_id) {
	for (auto &entry : Mappings(context, view_catalog)) {
		if (!StringUtil::CIEquals(entry.first, catalog_id)) {
			continue;
		}
		auto iceberg = Catalog::GetCatalogEntry(context, Identifier(entry.second));
		if (!iceberg) {
			throw BinderException("%s maps Glue catalog id '%s' to catalog \"%s\", which is not attached",
			                      VIEW_ICEBERG_CATALOGS, entry.first, entry.second);
		}
		auto why = WhyNotServingView(view_catalog, entry.first, *iceberg);
		if (!why.empty()) {
			throw BinderException("%s maps Glue catalog id '%s' to catalog \"%s\", which does not serve it: %s",
			                      VIEW_ICEBERG_CATALOGS, entry.first, entry.second, why);
		}
		return iceberg;
	}
	return nullptr;
}

} // namespace

ViewIcebergCatalogs GlueViewIcebergCatalogs::Read(const Value &setting) {
	ViewIcebergCatalogs result;
	if (setting.IsNull()) {
		return result;
	}
	case_insensitive_set_t ids;
	for (auto &entry : MapValue::GetChildren(setting)) {
		auto &key_value = StructValue::GetChildren(entry);
		if (key_value[0].IsNull() || key_value[1].IsNull() || key_value[0].ToString().empty() ||
		    key_value[1].ToString().empty()) {
			throw InvalidInputException("%s: a Glue catalog id and a catalog alias can not be NULL or empty",
			                            VIEW_ICEBERG_CATALOGS);
		}
		if (!ids.insert(key_value[0].ToString()).second) {
			throw InvalidInputException("%s: Glue catalog id '%s' is mapped more than once", VIEW_ICEBERG_CATALOGS,
			                            key_value[0].ToString());
		}
		result.emplace_back(key_value[0].ToString(), key_value[1].ToString());
	}
	return result;
}

IcebergTargets GlueViewIcebergCatalogs::FindIcebergTargets(ClientContext &context, GlueCatalog &view_catalog,
                                                           QueryNode &stored) {
	IcebergTargets targets;
	auto &mappings = Mappings(context, view_catalog);
	if (mappings.empty()) {
		return targets;
	}
	auto aliases = GlueViewNames::AttachedAliases(view_catalog, GlueViewNames::ReferencedCatalogIds(stored));
	// per stored catalog part: its mapped id, the attached Glue catalog of it (to find Iceberg tables), the catalog
	struct MappedId {
		string catalog_id;
		optional_ptr<GlueCatalog> glue;
		optional_ptr<Catalog> iceberg;
	};
	case_insensitive_map_t<MappedId> mapped_ids;
	GlueViewNames::ForEachTable(stored, [&](BaseTableRef &ref) {
		auto &name = ref.GetQualifiedName();
		if (name.Schema().empty() || GlueViewNames::IsExternal(name.Catalog())) {
			return;
		}
		auto stored_catalog = name.Catalog().GetIdentifierName();
		auto known = mapped_ids.find(stored_catalog);
		if (known == mapped_ids.end()) {
			MappedId mapped;
			auto catalog_id = stored_catalog.empty() ? view_catalog.CatalogId(context) : stored_catalog;
			for (auto &mapping : mappings) {
				if (!catalog_id.empty() && StringUtil::CIEquals(mapping.first, catalog_id)) {
					mapped.catalog_id = catalog_id;
					break;
				}
			}
			if (stored_catalog.empty()) {
				mapped.glue = &view_catalog;
			} else if (!mapped.catalog_id.empty()) {
				auto alias = aliases.find(name.Catalog());
				auto glue = alias == aliases.end() ? nullptr : Catalog::GetCatalogEntry(context, alias->second);
				if (glue && glue->GetCatalogType() == "glue") {
					mapped.glue = &glue->Cast<GlueCatalog>();
				}
			}
			known = mapped_ids.emplace(stored_catalog, mapped).first;
		}
		auto &mapped = known->second;
		if (mapped.catalog_id.empty()) {
			return;
		}
		if (mapped.glue) {
			// the Glue definition, from the table set rather than LookupEntry
			auto schema = mapped.glue->GetSchema(context, name.Schema(), OnEntryNotFound::RETURN_NULL);
			if (!schema) {
				return;
			}
			auto table = schema->Cast<GlueSchemaEntry>().tables.GetEntry(
			    context, EntryLookupInfo(CatalogType::TABLE_ENTRY, QualifiedName(name.Name())));
			if (!table || table->type != CatalogType::TABLE_ENTRY ||
			    table->Cast<GlueTable>().table_info.GetFormat().format != GlueFormat::ICEBERG) {
				return;
			}
		}
		// checked only once a table needs it, so that a mapping no table uses can not fail the view
		if (!mapped.iceberg) {
			mapped.iceberg = MappedIcebergCatalog(context, view_catalog, mapped.catalog_id);
		}
		targets[name] = IcebergTarget {mapped.iceberg->GetName(), mapped.iceberg->GetAttached().oid};
	});
	return targets;
}

string GlueViewIcebergCatalogs::ServedCatalogId(ClientContext &context, GlueCatalog &view_catalog, Catalog &iceberg) {
	string refused;
	for (auto &entry : Mappings(context, view_catalog)) {
		if (!StringUtil::CIEquals(entry.second, iceberg.GetName().GetIdentifierName())) {
			continue;
		}
		auto why = WhyNotServingView(view_catalog, entry.first, iceberg);
		if (why.empty()) {
			return entry.first;
		}
		if (refused.empty()) {
			refused = StringUtil::Format("%s maps Glue catalog id '%s' to catalog \"%s\", which does not serve it: %s",
			                             VIEW_ICEBERG_CATALOGS, entry.first, entry.second, why);
		}
	}
	if (!refused.empty()) {
		throw BinderException(refused);
	}
	return string();
}

} // namespace duckdb
