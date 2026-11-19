#include "api/glue_retry_strategy.hpp"
#include "api/glue_http_client.hpp"
#include "api/glue_request_metrics.hpp"
#include "core/glue_log_types.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"

#include <aws/core/client/AWSError.h>
#include <aws/core/client/CoreErrors.h>

namespace duckdb {

GlueRetryStrategy::GlueRetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> strategy_p)
    : strategy(std::move(strategy_p)) {
}

void GlueRetryStrategy::ReportRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &error,
                                    long attempted_retries) const {
	auto context = GlueHttpClientContextScope::Current();
	if (!context) {
		return;
	}
	GlueRequestMetrics::AddCount(*context, GlueRequestMetrics::RETRIES);
	if (Logger::Get(*context).ShouldLog(GlueRetryLogType::NAME, GlueRetryLogType::LEVEL)) {
		GlueRetryEvent event;
		event.error = error.GetExceptionName().c_str();
		auto status = static_cast<int>(error.GetResponseCode());
		if (status > 0) {
			event.status = NumericCast<idx_t>(status);
		}
		event.retry = NumericCast<idx_t>(attempted_retries + 1);
		event.request_id = error.GetRequestId().c_str();
		DUCKDB_LOG(*context, GlueRetryLogType, event);
	}
}

bool GlueRetryStrategy::ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &error,
                                    long attempted_retries) const {
	auto retry = strategy->ShouldRetry(error, attempted_retries);
	if (retry) {
		ReportRetry(error, attempted_retries);
	}
	return retry;
}

long GlueRetryStrategy::CalculateDelayBeforeNextRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &error,
                                                      long attempted_retries) const {
	return strategy->CalculateDelayBeforeNextRetry(error, attempted_retries);
}

long GlueRetryStrategy::GetMaxAttempts() const {
	return strategy->GetMaxAttempts();
}

void GlueRetryStrategy::GetSendToken() {
	strategy->GetSendToken();
}

bool GlueRetryStrategy::HasSendToken() {
	return strategy->HasSendToken();
}

// the SDK calls exactly one RequestBookkeeping() after each attempt it makes, with either HTTP client
void GlueRetryStrategy::ReportAttempt() const {
	if (auto context = GlueHttpClientContextScope::Current()) {
		GlueRequestMetrics::AddCount(*context, GlueRequestMetrics::REQUESTS);
	}
}

void GlueRetryStrategy::RequestBookkeeping(const Aws::Client::HttpResponseOutcome &outcome) {
	ReportAttempt();
	strategy->RequestBookkeeping(outcome);
}

void GlueRetryStrategy::RequestBookkeeping(const Aws::Client::HttpResponseOutcome &outcome,
                                           const Aws::Client::AWSError<Aws::Client::CoreErrors> &last_error) {
	ReportAttempt();
	strategy->RequestBookkeeping(outcome, last_error);
}

const char *GlueRetryStrategy::GetStrategyName() const {
	return strategy->GetStrategyName();
}

} // namespace duckdb
