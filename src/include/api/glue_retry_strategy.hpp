#pragma once

#include <aws/core/client/RetryStrategy.h>

#include <memory>

namespace duckdb {

//! Counts each attempt in `glue.requests`, and each retry in `glue.retries` and as a GlueRetry log event
class GlueRetryStrategy : public Aws::Client::RetryStrategy {
public:
	explicit GlueRetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> strategy_p);

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
	//! Reports a retry the wrapped strategy has just allowed
	void ReportRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &error, long attempted_retries) const;
	void ReportAttempt() const;

	std::shared_ptr<Aws::Client::RetryStrategy> strategy;
};

} // namespace duckdb
