#include "catalog/glue_file_credentials.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

optional_ptr<GlueFileCredentialsCallback> GlueFileCredentialsCallback::Find(ClientContext &context) {
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
	return found;
}

bool GlueFileCredentialsCallback::Has(ClientContext &context, const GlueTable &table, const GlueTableInfo &table_info) {
	auto found = Find(context);
	return found && found->HasFileCredentials(context, table, table_info);
}

string GlueFileCredentialsCallback::Get(ClientContext &context, const GlueTable &table,
                                        const GlueTableInfo &table_info) {
	auto found = Find(context);
	return found ? found->GetFileSecret(context, table, table_info) : string();
}

} // namespace duckdb
