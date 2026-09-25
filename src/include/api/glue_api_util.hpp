#pragma once

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "duckdb/common/exception.hpp"

#include <aws/glue/GlueClient.h>
#include <aws/glue/GlueErrors.h>
#include <aws/glue/model/Column.h>
#include <aws/glue/model/Database.h>
#include <aws/glue/model/Table.h>

namespace duckdb {

string ToStdString(const Aws::String &input);
unordered_map<string, string> ToStdMap(const Aws::Map<Aws::String, Aws::String> &input);
vector<GlueColumn> ToColumns(const Aws::Vector<Aws::Glue::Model::Column> &input);
GlueDatabaseInfo ToDatabaseInfo(const Aws::Glue::Model::Database &database);
GlueTableInfo ToTableInfo(const Aws::Glue::Model::Table &table);
Aws::Map<Aws::String, Aws::String> ToAwsMap(const unordered_map<string, string> &input);
Aws::Vector<Aws::Glue::Model::Column> ToAwsColumns(const vector<GlueColumn> &input);
Aws::Vector<Aws::String> ToAwsValues(const vector<string> &values);
vector<string> ToStdValues(const Aws::Vector<Aws::String> &values);
string PartitionValuesToString(const vector<string> &values);
//! The numRows, numFiles and totalSize parameters; false unless all three are set to a valid count
bool TryGetBasicStatistics(const GlueParameters &parameters, GlueBasicStatistics &result);
void SetBasicStatistics(GlueParameters &parameters, const GlueBasicStatistics &statistics);
void RemoveBasicStatistics(GlueParameters &parameters);
void CheckWritable(const GlueCatalog &catalog, const string &operation);

template <class OUTCOME>
[[noreturn]] void ThrowGlueError(const OUTCOME &outcome, const string &operation) {
	auto &error = outcome.GetError();
	throw IOException("Glue %s failed: %s (%s)", operation, ToStdString(error.GetMessage()),
	                  ToStdString(error.GetExceptionName()));
}

template <class OUTCOME>
bool IsEntityNotFound(const OUTCOME &outcome) {
	return outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ENTITY_NOT_FOUND;
}

template <class OUTCOME>
bool IsAlreadyExists(const OUTCOME &outcome) {
	return outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ALREADY_EXISTS;
}

template <class REQUEST>
void SetCatalogId(REQUEST &request, const GlueCatalog &catalog) {
	if (!catalog.options.catalog_id.empty()) {
		request.SetCatalogId(catalog.options.catalog_id);
	}
}

} // namespace duckdb
