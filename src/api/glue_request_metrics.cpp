#include "api/glue_request_metrics.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

void GlueRequestMetrics::Register(ExtensionLoader &loader) {
	loader.RegisterMetric({REQUESTS, "uint64", "Glue requests the AWS SDK attempted, retries included", "requests"});
	loader.RegisterMetric({REQUEST_TIME, "double",
	                       "Time the query spent in Glue API calls, retries and backoff included, summed over calls "
	                       "that run at the same time on different threads",
	                       "seconds"});
	loader.RegisterMetric(
	    {RETRIES, "uint64", "Glue requests the AWS SDK attempted again after a failed attempt", "requests"});
}

void GlueRequestMetrics::RecordRequest(ClientContext &context) noexcept {
	try {
		auto &profiler = QueryProfiler::Get(context);
		if (profiler.IsEnabled()) {
			profiler.AddToMetricCounter(REQUESTS, 1);
		}
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
