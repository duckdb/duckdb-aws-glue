#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/profiler.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;

//! Profiler metrics of the Glue requests a query makes
struct GlueRequestMetrics {
	static constexpr const char *REQUESTS = "glue.requests";
	static constexpr const char *REQUEST_TIME = "glue.request_time";
	static constexpr const char *RETRIES = "glue.retries";

	static void Register(ExtensionLoader &loader);
	//! Never throws: the AWS SDK calls it
	static void AddCount(ClientContext &context, const char *key) noexcept;
};

//! Times a Glue API call for `glue.request_time`, retries and backoff included; a timer nested in another does nothing
class GlueRequestTimer {
public:
	explicit GlueRequestTimer(ClientContext &context) noexcept;
	~GlueRequestTimer();

private:
	optional_ptr<ClientContext> context;
	Profiler timer;
};

} // namespace duckdb
