#pragma once

#include "duckdb/common/common.hpp"

namespace Aws {
namespace Http {
class HttpRequest;
}
} // namespace Aws

namespace duckdb {

class ClientContext;
class ExtensionLoader;

//! Profiler metrics of what a query asked of Glue: the requests the AWS SDK sent, the time they took and the ones it
//! sent again. Only counted while profiling is enabled.
struct GlueRequestMetrics {
	//! Glue requests the AWS SDK sent, retries included
	static constexpr const char *REQUESTS = "glue.requests";
	//! Time the Glue requests took, summed over requests
	static constexpr const char *REQUEST_TIME = "glue.request_time";
	//! Glue requests the AWS SDK sent again after a failed attempt
	static constexpr const char *RETRIES = "glue.retries";

	static void Register(ExtensionLoader &loader);
	//! True if `request` is a Glue request of a query that is being profiled, so its time is worth measuring.
	//! `context` is the query the request belongs to, if any. Never throws.
	static bool ShouldRecord(ClientContext *context, const Aws::Http::HttpRequest &request) noexcept;
	//! Counts a request, taking `elapsed_ns`. Call only if ShouldRecord returned true. Never throws.
	static void Record(ClientContext &context, idx_t elapsed_ns) noexcept;
	//! Counts a request sent again. Never throws.
	static void RecordRetry(ClientContext &context) noexcept;
};

} // namespace duckdb
