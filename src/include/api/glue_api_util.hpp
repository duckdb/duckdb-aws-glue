#pragma once

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "duckdb/common/exception.hpp"

#include <aws/glue/GlueClient.h>
#include <aws/glue/GlueErrors.h>
#include <aws/glue/model/Column.h>
#include <aws/glue/model/Database.h>
#include <aws/glue/model/Partition.h>
#include <aws/glue/model/Table.h>

namespace duckdb {

//! The opaque handles core/glue_info.hpp declares
struct GlueFetchedTable {
	explicit GlueFetchedTable(Aws::Glue::Model::Table raw_p) : raw(std::move(raw_p)) {
	}
	Aws::Glue::Model::Table raw;
};
struct GlueFetchedPartition {
	explicit GlueFetchedPartition(Aws::Glue::Model::Partition raw_p) : raw(std::move(raw_p)) {
	}
	Aws::Glue::Model::Partition raw;
};

//! The Table a GlueTableInfo was projected from; an InternalException if it was not fetched from Glue
const Aws::Glue::Model::Table &FetchedTable(const GlueTableInfo &table);
//! The Partition a GluePartitionDetails was projected from; an InternalException if it was not fetched from Glue
const Aws::Glue::Model::Partition &FetchedPartition(const GluePartitionDetails &partition, const string &database_name,
                                                    const string &table_name);

string ToStdString(const Aws::String &input);
unordered_map<string, string> ToStdMap(const Aws::Map<Aws::String, Aws::String> &input);
vector<GlueColumn> ToColumns(const Aws::Vector<Aws::Glue::Model::Column> &input);
GlueDatabaseInfo ToDatabaseInfo(const Aws::Glue::Model::Database &database);
//! Project a fetched Table; the result keeps the Table in 'fetched'
GlueTableInfo ToTableInfo(const Aws::Glue::Model::Table &table, const string &schema_name);
Aws::Map<Aws::String, Aws::String> ToAwsMap(const unordered_map<string, string> &input);
Aws::Vector<Aws::Glue::Model::Column> ToAwsColumns(const vector<GlueColumn> &input);
Aws::Vector<Aws::String> ToAwsValues(const vector<string> &values);
string PartitionValuesToString(const vector<string> &values);
void CheckWritable(const GlueCatalog &catalog, const string &operation);

template <class OUTCOME>
[[noreturn]] void ThrowGlueError(const OUTCOME &outcome, const string &operation) {
	auto &error = outcome.GetError();
	auto message = Exception::ConstructMessage("Glue %s failed: %s (%s)", operation, ToStdString(error.GetMessage()),
	                                           ToStdString(error.GetExceptionName()));
	throw GlueException(message, error.GetErrorType() == Aws::Glue::GlueErrors::ACCESS_DENIED);
}

template <class OUTCOME>
bool IsEntityNotFound(const OUTCOME &outcome) {
	return outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ENTITY_NOT_FOUND;
}

template <class OUTCOME>
bool IsAccessDenied(const OUTCOME &outcome) {
	return outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ACCESS_DENIED;
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
