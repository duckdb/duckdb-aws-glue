#include "api/glue_request_metrics.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_profiler.hpp"

#include <aws/core/http/HttpRequest.h>

#include <cstring>

namespace duckdb {

static constexpr const char *TARGET_HEADER = "x-amz-target";
static constexpr const char *GLUE_TARGET_PREFIX = "AWSGlue.";

void GlueRequestMetrics::Register(ExtensionLoader &loader) {
	loader.RegisterMetric({REQUESTS, "uint64", "Glue requests the query made, retries included", "requests"});
	loader.RegisterMetric(
	    {REQUEST_TIME, "double", "Time the query's Glue requests took, summed over requests", "seconds"});
	loader.RegisterMetric(
	    {RETRIES, "uint64", "Glue requests the AWS SDK sent again after a failed attempt", "requests"});
}

bool GlueRequestMetrics::ShouldRecord(ClientContext *context, const Aws::Http::HttpRequest &request) noexcept {
	try {
		if (!context || !QueryProfiler::Get(*context).IsEnabled() || !request.HasHeader(TARGET_HEADER)) {
			return false;
		}
		// the header of the Glue API is 'AWSGlue.<Operation>'; other services, such as the credential calls, differ
		auto &target = request.GetHeaderValue(TARGET_HEADER);
		return strncmp(target.c_str(), GLUE_TARGET_PREFIX, strlen(GLUE_TARGET_PREFIX)) == 0;
	} catch (...) { // NOLINT: the AWS SDK must not see an exception from a metric
		return false;
	}
}

void GlueRequestMetrics::Record(ClientContext &context, idx_t elapsed_ns) noexcept {
	try {
		auto &profiler = QueryProfiler::Get(context);
		profiler.AddToMetricCounter(REQUESTS, 1);
		profiler.AddToMetricTiming(REQUEST_TIME, elapsed_ns);
	} catch (...) { // NOLINT: the AWS SDK must not see an exception from a metric
	}
}

void GlueRequestMetrics::RecordRetry(ClientContext &context) noexcept {
	try {
		auto &profiler = QueryProfiler::Get(context);
		if (profiler.IsEnabled()) {
			profiler.AddToMetricCounter(RETRIES, 1);
		}
	} catch (...) { // NOLINT: the AWS SDK must not see an exception from a metric
	}
}

} // namespace duckdb
