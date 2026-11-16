#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;

//! Profiler metrics of what a query asked of Glue. Only counted while profiling is enabled.
struct GlueRequestMetrics {
	static constexpr const char *REQUESTS = "glue.requests";
	static constexpr const char *REQUEST_TIME = "glue.request_time";
	static constexpr const char *RETRIES = "glue.retries";

	static void Register(ExtensionLoader &loader);
	//! Each of these does nothing unless profiling is enabled, and never throws
	static void RecordRequest(ClientContext &context) noexcept;
	static void RecordRetry(ClientContext &context) noexcept;
};

} // namespace duckdb
