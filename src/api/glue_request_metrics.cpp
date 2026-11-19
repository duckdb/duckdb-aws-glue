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

void GlueRequestMetrics::AddCount(ClientContext &context, const char *key) noexcept {
	try {
		QueryProfiler::Get(context).AddToMetricCounter(key, 1);
	} catch (...) { // NOLINT: the AWS SDK must not see an exception from a metric
	}
}

static thread_local bool glue_call_timed = false;

GlueRequestTimer::GlueRequestTimer(ClientContext &context_p) noexcept {
	if (glue_call_timed) {
		return;
	}
	try {
		if (QueryProfiler::Get(context_p).IsEnabled()) {
			context = &context_p;
			glue_call_timed = true;
			timer.Start();
		}
	} catch (...) { // NOLINT: a metric must not fail the call
	}
}

GlueRequestTimer::~GlueRequestTimer() {
	if (!context) {
		return;
	}
	glue_call_timed = false;
	try {
		timer.End();
		QueryProfiler::Get(*context).AddToMetricTiming(GlueRequestMetrics::REQUEST_TIME, timer.ElapsedNanos());
	} catch (...) { // NOLINT: a destructor must not throw
	}
}

} // namespace duckdb
