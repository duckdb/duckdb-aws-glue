#include "api/glue_retry_strategy.hpp"
#include "api/glue_http_client.hpp"
#include "core/glue_log_types.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"

#include <aws/core/client/AWSError.h>
#include <aws/core/client/CoreErrors.h>

namespace duckdb {

GlueLoggingRetryStrategy::GlueLoggingRetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> strategy_p)
    : strategy(std::move(strategy_p)) {
}

bool GlueLoggingRetryStrategy::ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &error,
                                           long attempted_retries) const {
	auto retry = strategy->ShouldRetry(error, attempted_retries);
	auto context = GlueHttpClientContextScope::Current();
	if (retry && context && Logger::Get(*context).ShouldLog(GlueRetryLogType::NAME, GlueRetryLogType::LEVEL)) {
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
	return retry;
}

long GlueLoggingRetryStrategy::CalculateDelayBeforeNextRetry(
    const Aws::Client::AWSError<Aws::Client::CoreErrors> &error, long attempted_retries) const {
	return strategy->CalculateDelayBeforeNextRetry(error, attempted_retries);
}

long GlueLoggingRetryStrategy::GetMaxAttempts() const {
	return strategy->GetMaxAttempts();
}

void GlueLoggingRetryStrategy::GetSendToken() {
	strategy->GetSendToken();
}

bool GlueLoggingRetryStrategy::HasSendToken() {
	return strategy->HasSendToken();
}

void GlueLoggingRetryStrategy::RequestBookkeeping(const Aws::Client::HttpResponseOutcome &outcome) {
	strategy->RequestBookkeeping(outcome);
}

void GlueLoggingRetryStrategy::RequestBookkeeping(const Aws::Client::HttpResponseOutcome &outcome,
                                                  const Aws::Client::AWSError<Aws::Client::CoreErrors> &last_error) {
	strategy->RequestBookkeeping(outcome, last_error);
}

const char *GlueLoggingRetryStrategy::GetStrategyName() const {
	return strategy->GetStrategyName();
}

} // namespace duckdb
