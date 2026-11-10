#include "catalog/glue_view_names.hpp"

#include "catalog/glue_catalog.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enum_util.hpp"
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

} // namespace

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
                                      QueryNode &query) {
	auto view_catalog_name = view_catalog.GetName();
	// catalogs named in the body, which the stored view does not keep
	identifier_set_t catalogs {view_catalog_name};
	ViewBodyWalker walker([&](BaseTableRef &ref, const case_insensitive_set_t &visible_ctes) {
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
			// asks Glue if the ATTACH probe did not; else the account id it was attached by
			auto catalog_id_of = [&](GlueCatalog &catalog) {
				auto id = catalog.FetchCatalogId(context);
				return id.empty() ? catalog.KnownCatalogId() : id;
			};
			auto catalog_id = catalog_id_of(glue_catalog);
			if (catalog_id.empty()) {
				throw BinderException("Glue view references \"%s\" in Glue catalog \"%s\", whose catalog id Glue does "
				                      "not report (it has no database of its own); attach it by its account id",
				                      name.ToString(), catalog.GetName().GetIdentifierName());
			}
			if (StringUtil::CIEquals(catalog_id, catalog_id_of(view_catalog))) {
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
		if (&entry->ParentCatalog() != &view_catalog) {
			throw BinderException("Glue view references table \"%s\", which is not in the view's Glue catalog but in "
			                      "catalog \"%s\" (through the search path)",
			                      name.ToString(), entry->ParentCatalog().GetName().GetIdentifierName());
		}
		ref.SetQualifiedName(Identifier(), entry->ParentSchema().name, entry->name);
	});
	walker.Walk(query);
	RefuseCatalogQualifiedColumns(query, catalogs);
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
                                      const identifier_tree_t<Identifier> &aliases, QueryNode &query) {
	ViewBodyWalker walker([&](BaseTableRef &ref, const case_insensitive_set_t &) {
		auto &name = ref.GetQualifiedName();
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
