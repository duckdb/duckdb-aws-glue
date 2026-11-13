#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/logging/log_type.hpp"

namespace duckdb {
class DatabaseInstance;
struct CreateMacroInfo;

//! What a scan of a Glue Hive table spends its time on, one entry per 'event': 'partitions' (loading them),
//! 'plan' (the listings to do), 'listing' (one of them) and 'sample' (the cardinality sample). 'scan_id' tells the
//! scans of one query apart. Fields an event does not set are NULL.
//! One step of a Hive scan, logged as GlueScan. Its timer starts when the event is made; Finish() records the time
//! since.
struct GlueScanEvent {
	GlueScanEvent(string table_p, idx_t scan_id_p, string event_p)
	    : table(std::move(table_p)), scan_id(scan_id_p), event(std::move(event_p)) {
		timer.Start();
	}

	//! Sets duration_ms to the time since the event was made
	void Finish() {
		timer.End();
		duration_ms = timer.Elapsed() * 1000;
	}

	string table;
	idx_t scan_id;
	string event;
	optional_idx partitions;
	optional_idx partitions_read;
	optional_idx listings;
	optional_idx files;
	double duration_ms = -1;
	string detail;

private:
	Profiler timer;
};

class GlueScanLogType : public LogType {
public:
	static constexpr const char *NAME = "GlueScan";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	GlueScanLogType();

	static LogicalType GetLogType();
	static string ConstructLogMessage(const GlueScanEvent &event);
};

//! A Glue API call the AWS SDK's retry strategy decided to retry, logged as GlueRetry before the wait. 'status' is NULL
//! when the request got no response, 'error' and 'request_id' when the SDK has none (e.g. a dropped connection).
struct GlueRetryEvent {
	string error;
	optional_idx status;
	idx_t retry = 0;
	string request_id;
};

class GlueRetryLogType : public LogType {
public:
	static constexpr const char *NAME = "GlueRetry";
	static constexpr LogLevel LEVEL = LogLevel::LOG_WARNING;

	GlueRetryLogType();

	static LogicalType GetLogType();
	static string ConstructLogMessage(const GlueRetryEvent &event);
};

void RegisterGlueLogTypes(DatabaseInstance &db);
//! The Glue requests in the HTTP log: glue_requests()
unique_ptr<CreateMacroInfo> GetGlueRequestsMacro();

} // namespace duckdb
