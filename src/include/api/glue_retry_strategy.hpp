#pragma once

#include <aws/core/client/RetryStrategy.h>

#include <memory>

namespace duckdb {

//! While one is alive on a thread, a call of that thread is not retried when the server answers with something that
//! is not a Glue error. A Glue stand-in without the call answers every attempt that way, and the backoff of the SDK's
//! retries adds up to tens of seconds.
class GlueUnknownCallScope {
public:
	GlueUnknownCallScope();
	~GlueUnknownCallScope();
	static bool Active();

private:
	bool previous;
};

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
