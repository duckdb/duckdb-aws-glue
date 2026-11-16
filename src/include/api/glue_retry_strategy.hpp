#pragma once

#include <aws/core/client/RetryStrategy.h>

#include <memory>

namespace duckdb {

//! Wraps the client's retry strategy to log each retry it decides on as GlueRetry, to the connection making the call
class GlueLoggingRetryStrategy : public Aws::Client::RetryStrategy {
public:
	explicit GlueLoggingRetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> strategy_p);

	bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &error,
	                 long attempted_retries) const override;
	long CalculateDelayBeforeNextRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &error,
	                                   long attempted_retries) const override;
	long GetMaxAttempts() const override;
	void GetSendToken() override;
	bool HasSendToken() override;
	void RequestBookkeeping(const Aws::Client::HttpResponseOutcome &outcome) override;
	void RequestBookkeeping(const Aws::Client::HttpResponseOutcome &outcome,
	                        const Aws::Client::AWSError<Aws::Client::CoreErrors> &last_error) override;
	const char *GetStrategyName() const override;

private:
	std::shared_ptr<Aws::Client::RetryStrategy> strategy;
};

} // namespace duckdb
