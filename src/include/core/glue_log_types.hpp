#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/logging/log_type.hpp"

#include <chrono>

namespace duckdb {
class DatabaseInstance;
struct CreateMacroInfo;

//! What a scan of a Glue Hive table spends its time on, one entry per 'event': 'partitions' (loading them),
//! 'plan' (the listings to do), 'listing' (one of them) and 'sample' (the cardinality sample). 'scan_id' tells the
//! scans of one query apart. Fields an event does not set are NULL.
struct GlueScanEvent {
	GlueScanEvent(string table_p, idx_t scan_id_p, string event_p)
	    : table(std::move(table_p)), scan_id(scan_id_p), event(std::move(event_p)) {
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
};

class GlueScanLogType : public LogType {
public:
	static constexpr const char *NAME = "GlueScan";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	GlueScanLogType();

	static LogicalType GetLogType();
	static string ConstructLogMessage(const GlueScanEvent &event);
};

void RegisterGlueLogTypes(DatabaseInstance &db);
//! The Glue requests in the HTTP log: glue_requests()
unique_ptr<CreateMacroInfo> GetGlueRequestsMacro();

class GlueLogTimer {
public:
	GlueLogTimer() : start(std::chrono::steady_clock::now()) {
	}
	double ElapsedMillis() const {
		return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
	}

private:
	std::chrono::steady_clock::time_point start;
};

} // namespace duckdb
