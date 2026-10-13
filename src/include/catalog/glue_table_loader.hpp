//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/glue_table_loader.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/planner/extension_callback.hpp"

#include <functional>

namespace duckdb {

class CatalogEntry;
class ClientContext;

//! A table to load through the registered loader, and where and as whom to call AWS for it
struct GlueLoadGovernedTableRequest {
	//! The DuckDB catalog the table is loaded into
	string catalog_name;
	//! The Glue catalog id, empty for the caller's default catalog
	string catalog_id;
	string database_name;
	string table_name;
	string region;
	string key_id;
	string secret;
	string session_token;
};

//! Loads the tables of a catalog attached with GOVERNANCE true, in place of GetTable. Another extension registers
//! it as an ExtensionCallback; without one, or with more than one, such a catalog refuses to load tables.
class GovernedGlueTableLoader : public ExtensionCallback {
public:
	//! Builds the entry of the table (or Glue view) from the Glue Table JSON; the catalog keeps it for the rest of the
	//! transaction
	using build_entry_t = std::function<CatalogEntry &(const string &table_json)>;

	//! Loads the table and returns the entry to bind for it: the entry 'build_entry' builds, or a view over it that
	//! lives until the transaction ends. Returns nullptr if the table does not exist. The table's data is readable for
	//! the rest of the transaction. Calls 'build_entry' at most once, and must not look up the table it is loading.
	virtual optional_ptr<CatalogEntry> Load(ClientContext &context, const GlueLoadGovernedTableRequest &request,
	                                        const build_entry_t &build_entry) = 0;

	//! The registered loader, nullptr if there is none; throws if more than one is registered
	static optional_ptr<GovernedGlueTableLoader> Find(ClientContext &context) {
		optional_ptr<GovernedGlueTableLoader> result;
		for (auto &callback : ExtensionCallback::Iterate(context)) {
			auto loader = dynamic_cast<GovernedGlueTableLoader *>(callback.get());
			if (!loader) {
				continue;
			}
			if (result) {
				throw InvalidConfigurationException("More than one extension registered a GovernedGlueTableLoader");
			}
			result = loader;
		}
		return result;
	}
};

} // namespace duckdb
