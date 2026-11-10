//! Helpers shared by the Catch tests of test/unittest: environment, SQL quoting, the local Glue of make glue-fixture
#pragma once

#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"

#include <chrono>
#include <cstdlib>

namespace duckdb {

inline unique_ptr<QueryResult> Run(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		FAIL(result->GetError() + "\n" + sql);
	}
	return result;
}

inline string Env(const char *name, const string &fallback) {
	auto value = std::getenv(name);
	return value ? string(value) : fallback;
}

inline string Unique() {
	return std::to_string(std::chrono::system_clock::now().time_since_epoch().count() % 1000000000);
}

inline string Quote(const string &text) {
	return Value(text).ToSQLString();
}

//! Only the local moto of make glue-fixture: the Glue table tests create and drop tables
inline string LocalGlueEndpoint() {
	auto endpoint = Env("GLUE_ENDPOINT", "");
	if (StringUtil::StartsWith(endpoint, "http://localhost:") ||
	    StringUtil::StartsWith(endpoint, "http://127.0.0.1:")) {
		return endpoint;
	}
	return string();
}

//! An S3 secret for the local S3 of make glue-fixture and a Glue catalog 'lake' on the local Glue, whose tables are
//! created under 'location'
inline void AttachLocalGlue(Connection &con, const string &endpoint, const string &location) {
	Run(con, "CREATE SECRET (TYPE S3, KEY_ID 'minio', SECRET 'minio123', REGION 'us-east-1', ENDPOINT " +
	             Quote(Env("S3_ENDPOINT", "localhost:9100")) + ", USE_SSL false, URL_STYLE 'path')");
	Run(con, "ATTACH " + Quote(Env("GLUE_CATALOG_ID", "123456789012")) + " AS lake (TYPE glue, ENDPOINT " +
	             Quote(endpoint) + ", DEFAULT_LOCATION " + Quote(location) + ")");
}

//! Dropped when the test ends, also when it fails
class GlueTables {
public:
	GlueTables(Connection &con_p, string prefix_p) : con(con_p), prefix(std::move(prefix_p)) {
	}
	~GlueTables() {
		for (auto &name : names) {
			con.Query("DROP TABLE IF EXISTS lake.default." + name);
		}
	}

public:
	string Add(const string &suffix) {
		names.push_back(prefix + suffix);
		return names.back();
	}

private:
	Connection &con;
	string prefix;
	vector<string> names;
};

} // namespace duckdb
