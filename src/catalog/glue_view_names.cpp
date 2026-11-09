#include "catalog/glue_view_names.hpp"

#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_table.hpp"
#include "catalog/glue_transaction.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {

namespace {

//! called per base table, with the CTE names in scope
using BaseTableCallback = std::function<void(BaseTableRef &ref, const case_insensitive_set_t &visible_ctes)>;
//! called per column reference and star
using ExpressionCallback = std::function<void(ParsedExpression &expr)>;

//! Walks a view body with SQL CTE scoping; refuses what a Glue view can not store
class ViewBodyWalker {
public:
	explicit ViewBodyWalker(BaseTableCallback callback_p, ExpressionCallback expression_callback_p = nullptr)
	    : callback(std::move(callback_p)), expression_callback(std::move(expression_callback_p)) {
	}

	void Walk(QueryNode &node) {
		case_insensitive_set_t visible;
		VisitNode(node, visible);
	}

private:
	void VisitNode(QueryNode &node, const case_insensitive_set_t &outer) {
		// a CTE is visible in its query, in later CTEs and, if recursive, in its own recursive term
		case_insensitive_set_t visible = outer;
		for (auto &entry : node.cte_map.map) {
			if (entry.second->query_node) {
				VisitNode(*entry.second->query_node, visible);
			}
			// stored from here, bound from the recursive node's copy
			VisitKeyTargets(entry.second->key_targets);
			visible.insert(entry.first.GetIdentifierName());
		}
		switch (node.type) {
		case QueryNodeType::SELECT_NODE: {
			auto &select = node.Cast<SelectNode>();
			if (select.from_table) {
				VisitRef(*select.from_table, visible);
			}
			for (auto &expr : select.select_list) {
				VisitExpression(expr, visible);
			}
			for (auto &expr : select.groups.group_expressions) {
				VisitExpression(expr, visible);
			}
			VisitExpression(select.where_clause, visible);
			VisitExpression(select.having, visible);
			VisitExpression(select.qualify, visible);
			break;
		}
		case QueryNodeType::SET_OPERATION_NODE:
			for (auto &child : node.Cast<SetOperationNode>().children) {
				VisitNode(*child, visible);
			}
			break;
		case QueryNodeType::RECURSIVE_CTE_NODE: {
			auto &recursive = node.Cast<RecursiveCTENode>();
			// the anchor reads the table (or an outer CTE) of the CTE's name, the recursive term the CTE
			VisitNode(*recursive.left, visible);
			auto with_self = visible;
			with_self.insert(recursive.ctename.GetIdentifierName());
			VisitNode(*recursive.right, with_self);
			VisitKeyTargets(recursive.key_targets);
			break;
		}
		default:
			throw BinderException("Glue views can not store a query of type %s", EnumUtil::ToString(node.type));
		}
		ParsedExpressionIterator::EnumerateQueryNodeModifiers(
		    node, [&](unique_ptr<ParsedExpression> &child) { VisitExpression(child, visible); });
	}

	void VisitRef(TableRef &ref, const case_insensitive_set_t &visible) {
		switch (ref.type) {
		case TableReferenceType::BASE_TABLE:
			callback(ref.Cast<BaseTableRef>(), visible);
			break;
		case TableReferenceType::JOIN: {
			auto &join = ref.Cast<JoinRef>();
			VisitRef(*join.left, visible);
			VisitRef(*join.right, visible);
			VisitExpression(join.condition, visible);
			VisitExpression(join.ranking_expression, visible);
			break;
		}
		case TableReferenceType::SUBQUERY:
			VisitNode(*ref.Cast<SubqueryRef>().subquery->node, visible);
			break;
		case TableReferenceType::PIVOT: {
			auto &pivot = ref.Cast<PivotRef>();
			VisitRef(*pivot.source, visible);
			for (auto &aggregate : pivot.aggregates) {
				VisitExpression(aggregate, visible);
			}
			for (auto &column : pivot.pivots) {
				for (auto &expr : column.pivot_expressions) {
					VisitExpression(expr, visible);
				}
				for (auto &entry : column.entries) {
					VisitExpression(entry.expr, visible);
				}
			}
			break;
		}
		case TableReferenceType::EXPRESSION_LIST:
			for (auto &row : ref.Cast<ExpressionListRef>().values) {
				for (auto &value : row) {
					VisitExpression(value, visible);
				}
			}
			break;
		case TableReferenceType::EMPTY_FROM:
			break;
		case TableReferenceType::TABLE_FUNCTION:
			// a source outside the Glue catalog
			throw BinderException("Glue views can not read from table functions (\"%s\")", ref.ToString());
		default:
			throw BinderException("Glue views can not store a FROM clause of type %s", EnumUtil::ToString(ref.type));
		}
	}

	void VisitExpression(unique_ptr<ParsedExpression> &expr, const case_insensitive_set_t &visible) {
		if (!expr) {
			return;
		}
		if (expression_callback && (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF ||
		                            expr->GetExpressionClass() == ExpressionClass::STAR)) {
			expression_callback(*expr);
		}
		if (expr->GetExpressionClass() == ExpressionClass::SUBQUERY) {
			auto &subquery = expr->Cast<SubqueryExpression>().SubqueryMutable();
			if (subquery && subquery->node) {
				VisitNode(*subquery->node, visible);
			}
		}
		ParsedExpressionIterator::EnumerateChildren(
		    *expr, [&](unique_ptr<ParsedExpression> &child) { VisitExpression(child, visible); });
	}

	//! USING KEY is bound without a parent binder: no CTE is in scope
	void VisitKeyTargets(vector<unique_ptr<ParsedExpression>> &targets) {
		for (auto &target : targets) {
			VisitExpression(target, case_insensitive_set_t());
		}
	}

	BaseTableCallback callback;
	ExpressionCallback expression_callback;
};

//! Column references are not rewritten: one qualified with a catalog the stored view does not name could mean that
//! catalog's table, or a table, alias or column of the same name, which binds differently once the catalog is renamed
void RefuseCatalogQualifiedColumns(QueryNode &query, const identifier_set_t &catalogs) {
	auto refuse = [](const string &reference, const Identifier &catalog) {
		throw BinderException("Glue view references \"%s\", whose \"%s\" is also the name of a catalog the view reads: "
		                      "use a table alias with another name",
		                      reference, catalog.GetIdentifierName());
	};
	ViewBodyWalker walker([](BaseTableRef &, const case_insensitive_set_t &) {},
	                      [&](ParsedExpression &expr) {
		                      if (expr.GetExpressionClass() == ExpressionClass::STAR) {
			                      auto &star = expr.Cast<StarExpression>();
			                      for (auto &name : star.ExcludeList()) {
				                      if (catalogs.count(name.catalog)) {
					                      refuse(name.ToString(), name.catalog);
				                      }
			                      }
			                      for (auto &entry : star.RenameList()) {
				                      if (catalogs.count(entry.first.catalog)) {
					                      refuse(entry.first.ToString(), entry.first.catalog);
				                      }
			                      }
			                      return;
		                      }
		                      auto &names = expr.Cast<ColumnRefExpression>().ColumnNames();
		                      if (names.size() > 1 && catalogs.count(names[0])) {
			                      refuse(expr.ToString(), names[0]);
		                      }
	                      });
	walker.Walk(query);
}

//! The stored catalog part of a non-Glue catalog's table: "external:<type>:<alias>", the alias it was written with.
//! No Glue catalog id has the prefix in any case (an id is an account id, optionally followed by ":<catalog>")
constexpr char EXTERNAL_PREFIX[] = "external:";

string ExternalIdentity(Catalog &catalog) {
	return EXTERNAL_PREFIX + catalog.GetCatalogType() + ":" + catalog.GetName().GetIdentifierName();
}

optional_ptr<CatalogEntry> LookupTable(ClientContext &context, QualifiedName name) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, std::move(name));
	return Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
}

//! a catalog id names a catalog only within one region and endpoint
bool SameGlueService(const GlueCatalog &a, const GlueCatalog &b) {
	return StringUtil::CIEquals(a.options.region, b.options.region) &&
	       StringUtil::CIEquals(a.options.endpoint, b.options.endpoint);
}

string ServiceName(const GlueCatalog &catalog) {
	return catalog.options.endpoint.empty()
	           ? StringUtil::Format("region %s", catalog.options.region)
	           : StringUtil::Format("region %s at endpoint %s", catalog.options.region, catalog.options.endpoint);
}

//! not in memory (temporary is too), system or hidden (InMemory last: the system catalog throws)
bool UsableExternalCatalog(Catalog &catalog) {
	auto &db = catalog.GetAttached();
	return !db.IsSystem() && db.GetVisibility() != AttachVisibility::HIDDEN && !catalog.InMemory();
}

constexpr char VIEW_ICEBERG_CATALOGS[] = "experimental_glue_view_iceberg_catalogs";

//! the mapping as of the transaction's first use of the view's catalog: a change applies from the next transaction
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

//! why a catalog (type, ATTACH path and options) does not serve the Iceberg tables of a Glue catalog id in a region
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

//! the Glue catalog id an Iceberg catalog is mapped from and serves, empty if it is mapped from none; throws if it
//! serves none of the ids it is mapped from
string ServedCatalogId(ClientContext &context, GlueCatalog &view_catalog, Catalog &iceberg) {
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

//! a table's catalog part as stored: none for the view's own catalog
Identifier StoredCatalog(ClientContext &context, GlueCatalog &view_catalog, const string &catalog_id) {
	return StringUtil::CIEquals(catalog_id, view_catalog.CatalogId(context)) ? Identifier() : Identifier(catalog_id);
}

} // namespace

ViewIcebergCatalogs GlueViewNames::ReadViewIcebergCatalogs(const Value &setting) {
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

IcebergTargets GlueViewNames::FindIcebergTargets(ClientContext &context, GlueCatalog &view_catalog, QueryNode &stored) {
	IcebergTargets targets;
	auto &mappings = Mappings(context, view_catalog);
	if (mappings.empty()) {
		return targets;
	}
	auto aliases = AttachedAliases(view_catalog, ReferencedCatalogIds(stored));
	// per stored catalog part: its mapped id, the attached Glue catalog of it (to find Iceberg tables), the catalog
	struct MappedId {
		string catalog_id;
		optional_ptr<GlueCatalog> glue;
		optional_ptr<Catalog> iceberg;
	};
	case_insensitive_map_t<MappedId> mapped_ids;
	ViewBodyWalker walker([&](BaseTableRef &ref, const case_insensitive_set_t &) {
		auto &name = ref.GetQualifiedName();
		if (name.Schema().empty() || IsExternal(name.Catalog())) {
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
	walker.Walk(stored);
	return targets;
}

case_insensitive_set_t GlueViewNames::ExternalCatalogTypes(DatabaseInstance &db) {
	case_insensitive_set_t types;
	Value setting;
	if (!db.TryGetCurrentSetting("experimental_glue_view_external_catalog_types", setting) || setting.IsNull()) {
		return types;
	}
	for (auto &type : ListValue::GetChildren(setting)) {
		if (!type.IsNull()) {
			types.insert(StringValue::Get(type));
		}
	}
	return types;
}

bool GlueViewNames::IsExternal(const Identifier &id) {
	return id.StartsWith(EXTERNAL_PREFIX);
}

string GlueViewNames::ExternalType(const Identifier &id) {
	auto rest = id.GetIdentifierName().substr(sizeof(EXTERNAL_PREFIX) - 1);
	return rest.substr(0, rest.find(':'));
}

void GlueViewNames::QualifyForStorage(ClientContext &context, GlueCatalog &view_catalog, const Identifier &view_schema,
                                      QueryNode &query, optional_ptr<QueryNode> shown) {
	auto view_catalog_name = view_catalog.GetName();
	// catalogs named in the body, which the stored view does not keep
	identifier_set_t catalogs {view_catalog_name};
	// (position in walk order, Glue name) of the tables stored by their Glue name, for `shown`, a copy of `query`
	vector<pair<idx_t, QualifiedName>> shown_names;
	idx_t position = 0;
	// a table reached through the Iceberg catalog of a mapped Glue catalog id is stored as a table of that id
	auto store_by_served_id = [&](BaseTableRef &ref, CatalogEntry &entry) {
		auto &catalog = entry.ParentCatalog();
		auto catalog_id =
		    catalog.GetCatalogType() == "glue" ? string() : ServedCatalogId(context, view_catalog, catalog);
		if (catalog_id.empty()) {
			return false;
		}
		ref.SetQualifiedName(StoredCatalog(context, view_catalog, catalog_id), entry.ParentSchema().name, entry.name);
		shown_names.emplace_back(position, ref.GetQualifiedName());
		return true;
	};
	ViewBodyWalker walker([&](BaseTableRef &ref, const case_insensitive_set_t &visible_ctes) {
		position++;
		auto name = ref.GetQualifiedName();
		auto catalog_name = name.Catalog();
		if (catalog_name.empty() && !name.Schema().empty()) {
			// x.t names catalog x when one is attached under that name, as DuckDB binds it
			auto schema_name = name.Schema();
			Binder::BindSchemaOrCatalog(context, catalog_name, schema_name);
			if (!catalog_name.empty()) {
				name = QualifiedName(catalog_name, Identifier(), name.Name());
			}
		}
		if (!catalog_name.empty()) {
			// core has stripped the view's own catalog name, so this is another catalog: stored by its Glue catalog id
			// or "external:<type>:<alias>"
			auto entry = LookupTable(context, name);
			if (!entry) {
				throw BinderException("Glue view references table \"%s\", which was not found", name.ToString());
			}
			auto &catalog = entry->ParentCatalog();
			if (store_by_served_id(ref, *entry)) {
				catalogs.insert(catalog_name);
				return;
			}
			if (catalog.GetCatalogType() != "glue") {
				if (!ExternalCatalogTypes(DatabaseInstance::GetDatabase(context)).count(catalog.GetCatalogType())) {
					throw BinderException("Glue view references \"%s\" in catalog \"%s\" of type %s, which is not in "
					                      "experimental_glue_view_external_catalog_types: add '%s' to it",
					                      name.ToString(), catalog.GetName().GetIdentifierName(),
					                      catalog.GetCatalogType(), catalog.GetCatalogType());
				}
				if (!UsableExternalCatalog(catalog)) {
					throw BinderException("Glue view references \"%s\" in catalog \"%s\", which is in memory, "
					                      "temporary, a system catalog or hidden: no other session can read it",
					                      name.ToString(), catalog.GetName().GetIdentifierName());
				}
				catalogs.insert(catalog_name);
				ref.SetQualifiedName(Identifier(ExternalIdentity(catalog)), entry->ParentSchema().name, entry->name);
				return;
			}
			auto &glue_catalog = catalog.Cast<GlueCatalog>();
			if (!SameGlueService(glue_catalog, view_catalog)) {
				throw BinderException(
				    "Glue view in %s references \"%s\" in Glue catalog \"%s\" of %s: a Glue view can only "
				    "reference tables of Glue catalogs in its own region",
				    ServiceName(view_catalog), name.ToString(), catalog.GetName().GetIdentifierName(),
				    ServiceName(glue_catalog));
			}
			auto catalog_id = glue_catalog.CatalogId(context);
			if (catalog_id.empty()) {
				throw BinderException("Glue view references \"%s\" in Glue catalog \"%s\", whose catalog id Glue does "
				                      "not report (it has no database of its own); attach it by its account id",
				                      name.ToString(), catalog.GetName().GetIdentifierName());
			}
			if (StringUtil::CIEquals(catalog_id, view_catalog.CatalogId(context))) {
				// another alias of the view's own catalog
				catalog_id.clear();
			}
			catalogs.insert(catalog_name);
			ref.SetQualifiedName(Identifier(catalog_id), entry->ParentSchema().name, entry->name);
			return;
		}
		if (!name.Schema().empty() || visible_ctes.count(name.Name().GetIdentifierName())) {
			// db.t of the view's own catalog (PinToCatalog keeps it there when read), or a CTE in scope
			return;
		}
		// a bare name binds through the session search path too: store it as db.t, resolved as DuckDB binds a view
		// (a temporary table first)
		auto entry =
		    LookupTable(context, QualifiedName(Identifier(TEMP_CATALOG), Identifier(DEFAULT_SCHEMA), name.Name()));
		if (!entry) {
			entry = LookupTable(context, QualifiedName(view_catalog_name, view_schema, name.Name()));
		}
		if (!entry) {
			entry = LookupTable(context, name);
		}
		if (!entry) {
			throw BinderException("Glue view references table \"%s\", which was not found", name.ToString());
		}
		if (&entry->ParentCatalog() == &view_catalog) {
			ref.SetQualifiedName(Identifier(), entry->ParentSchema().name, entry->name);
		} else if (!store_by_served_id(ref, *entry)) {
			throw BinderException("Glue view references table \"%s\", which is not in the view's Glue catalog but in "
			                      "catalog \"%s\" (through the search path)",
			                      name.ToString(), entry->ParentCatalog().GetName().GetIdentifierName());
		}
	});
	walker.Walk(query);
	RefuseCatalogQualifiedColumns(query, catalogs);
	if (!shown || shown_names.empty()) {
		return;
	}
	// walked only now, so that errors come in the order of the walk of `query`
	position = 0;
	idx_t next = 0;
	ViewBodyWalker([&](BaseTableRef &ref, const case_insensitive_set_t &) {
		position++;
		if (next < shown_names.size() && shown_names[next].first == position) {
			ref.SetQualifiedName(shown_names[next++].second);
		}
	}).Walk(*shown);
}

identifier_set_t GlueViewNames::ReferencedCatalogIds(QueryNode &query) {
	identifier_set_t ids;
	ViewBodyWalker walker([&](BaseTableRef &ref, const case_insensitive_set_t &) {
		auto &name = ref.GetQualifiedName();
		if (!name.Catalog().empty()) {
			ids.insert(name.Catalog());
		}
	});
	walker.Walk(query);
	return ids;
}

identifier_tree_t<Identifier> GlueViewNames::AttachedAliases(GlueCatalog &view_catalog, const identifier_set_t &ids) {
	// id -> alias, same region and endpoint only; the first alias alphabetically
	identifier_tree_t<Identifier> aliases;
	for (auto &db : DatabaseManager::Get(view_catalog.GetAttached()).GetDatabases()) {
		if (db->IsSystem() || db->IsTemporary()) {
			continue;
		}
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "glue") {
			// an identity names one alias
			auto id = Identifier(ExternalIdentity(catalog));
			if (ids.count(id) && UsableExternalCatalog(catalog)) {
				aliases[id] = catalog.GetName();
			}
			continue;
		}
		auto &glue_catalog = catalog.Cast<GlueCatalog>();
		auto id = Identifier(glue_catalog.KnownCatalogId());
		if (!ids.count(id) || !SameGlueService(glue_catalog, view_catalog)) {
			continue;
		}
		auto alias = catalog.GetName();
		auto existing = aliases.find(id);
		if (existing == aliases.end() || alias < existing->second) {
			aliases[id] = alias;
		}
	}
	return aliases;
}

void GlueViewNames::ResolveForReading(GlueCatalog &view_catalog, const string &view_name,
                                      const identifier_tree_t<Identifier> &aliases,
                                      const IcebergTargets &iceberg_targets, QueryNode &query) {
	ViewBodyWalker walker([&](BaseTableRef &ref, const case_insensitive_set_t &) {
		auto &name = ref.GetQualifiedName();
		auto target = iceberg_targets.find(name);
		if (target != iceberg_targets.end()) {
			ref.SetQualifiedName(target->second.catalog, name.Schema(), name.Name());
			return;
		}
		if (name.Catalog().empty()) {
			return;
		}
		auto &id = name.Catalog();
		auto alias = aliases.find(id);
		if (alias == aliases.end() && IsExternal(id)) {
			throw BinderException(
			    "Glue view \"%s\" reads %s: no usable catalog of that type is attached under that alias", view_name,
			    name.ToString());
		}
		if (alias == aliases.end()) {
			auto &endpoint = view_catalog.options.endpoint;
			throw BinderException("Glue view \"%s\" reads %s from Glue catalog %s, which is not attached in %s "
			                      "(or is attached with VERIFY_CONNECTION false and its id was not fetched yet); "
			                      "ATTACH '%s' (TYPE GLUE, REGION '%s'%s) to read it",
			                      view_name, name.ToString(), id.GetIdentifierName(), ServiceName(view_catalog),
			                      id.GetIdentifierName(), view_catalog.options.region,
			                      endpoint.empty() ? "" : ", ENDPOINT '" + endpoint + "'");
		}
		ref.SetQualifiedName(alias->second, name.Schema(), name.Name());
	});
	walker.Walk(query);
}

void GlueViewNames::PinToCatalog(QueryNode &query, const Identifier &catalog_name, const Identifier &schema_name) {
	ViewBodyWalker walker([&](BaseTableRef &ref, const case_insensitive_set_t &visible_ctes) {
		auto name = ref.GetQualifiedName();
		if (name.Catalog().empty() && !name.Schema().empty()) {
			ref.SetQualifiedName(catalog_name, name.Schema(), name.Name());
		} else if (name.Catalog().empty() && !visible_ctes.count(name.Name().GetIdentifierName())) {
			// a bare name (not stored by DuckDB) would bind through the search path
			ref.SetQualifiedName(catalog_name, schema_name, name.Name());
		}
	});
	walker.Walk(query);
}

} // namespace duckdb
