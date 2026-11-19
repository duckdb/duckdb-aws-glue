#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/profiler.hpp"

#include "api/glue_http_client.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;

//! Profiler metrics of the Glue requests a query makes
struct GlueRequestMetrics {
	static constexpr const char *REQUESTS = "glue.requests";
	static constexpr const char *REQUEST_TIME = "glue.request_time";
	static constexpr const char *RETRIES = "glue.retries";

	static void Register(ExtensionLoader &loader);
	static void AddCount(ClientContext &context, const char *key) noexcept;
};

//! Times a Glue API call for `glue.request_time`, retries and backoff included
class GlueRequestTimer {
public:
	explicit GlueRequestTimer(ClientContext &context);
	~GlueRequestTimer();

private:
	optional_ptr<ClientContext> context;
	Profiler timer;
};

//! Wraps a Glue API call: the calling connection's context for the HTTP layer, and the call's timer
struct GlueCallScope {
	explicit GlueCallScope(ClientContext &context) : http_scope(context), timer(context) {
	}

	GlueHttpClientContextScope http_scope;
	GlueRequestTimer timer;
};

} // namespace duckdb
