//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/glue_file_credentials.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/extension_callback.hpp"

#include "core/glue_info.hpp"

namespace duckdb {

class GlueTable;

//! Gives the scans of a Glue table the credentials to list and open its files with, as the name of a file secret
//! (SecretManager::RegisterFileSecret). An extension registers one as an ExtensionCallback; without one, every file
//! is read with the secret that matches its path.
class GlueFileCredentialsCallback : public ExtensionCallback {
public:
	//! The file secret for a scan of `table`, whose definition Glue returned as `table_info` for this query, or "" to
	//! read its files by path. Asked each time a scan of the table is bound: register one file secret per table and
	//! transaction, and return its name for each of the table's scans in it, which then share their listings.
	virtual string GetFileSecret(ClientContext &context, const GlueTable &table, const GlueTableInfo &table_info) = 0;

	//! What the registered callback returns; "" when none is registered. Throws when more than one is.
	static string Get(ClientContext &context, const GlueTable &table, const GlueTableInfo &table_info) {
		optional_ptr<GlueFileCredentialsCallback> found;
		for (auto &callback : ExtensionCallback::Iterate(context)) {
			auto credentials_callback = dynamic_cast<GlueFileCredentialsCallback *>(callback.get());
			if (!credentials_callback) {
				continue;
			}
			if (found) {
				throw InvalidConfigurationException("More than one extension registered a GlueFileCredentialsCallback");
			}
			found = credentials_callback;
		}
		return found ? found->GetFileSecret(context, table, table_info) : string();
	}
};

} // namespace duckdb
