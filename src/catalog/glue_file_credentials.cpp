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

string GlueFileCredentialsCallback::Get(ClientContext &context, const GlueTable &table) {
	auto found = Find(context);
	return found ? found->GetFileSecret(context, table) : string();
}

} // namespace duckdb
