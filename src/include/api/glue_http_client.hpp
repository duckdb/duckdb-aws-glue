#pragma once

#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class ClientContext;
class DatabaseInstance;

//! Install an AWS SDK HttpClientFactory that routes all AWS SDK HTTP traffic through DuckDB's HTTPUtil (httpfs).
//! This unifies proxy / certificate handling and makes the Glue API calls show up in DuckDB's HTTP log
//! (`CALL enable_logging('HTTP')`). Call at extension load, after Aws::InitAPI and before any AWS client is
//! constructed; only the first call installs it. NOTE: the factory is process global, every AWS SDK client in the
//! process uses it, and only clients built inside a GlueHttpClientContextScope use DuckDB's HTTP layer. Where the SDK
//! has no transport of its own (wasm), clients built outside a scope go through `db` (the last database to load).
void RegisterGlueHttpClientFactory(DatabaseInstance &db);

//! Whether the bridge is enabled for the current connection (see the 'glue_network_calls_via_duckdb' setting)
bool GlueNetworkCallsViaDuckDB(DatabaseInstance &db);

//! The AWS SDK creates HTTP requests without a DuckDB ClientContext, but the HTTP log belongs to a connection.
//! Wrap every SDK call in this scope so the bridge can log the requests to the calling connection.
class GlueHttpClientContextScope {
public:
	explicit GlueHttpClientContextScope(ClientContext &context);
	~GlueHttpClientContextScope();

	static optional_ptr<ClientContext> Current();

private:
	optional_ptr<ClientContext> previous;
};

} // namespace duckdb
