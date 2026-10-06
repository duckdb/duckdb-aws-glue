#include "api/glue_api_util.hpp"
#include "api/glue_http_client.hpp"

#include "duckdb/common/string_util.hpp"

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/glue/model/CreateTableRequest.h>
#include <aws/glue/model/DeleteTableRequest.h>
#include <aws/glue/model/GetTableRequest.h>
#include <aws/glue/model/GetTablesRequest.h>
#include <aws/glue/model/Order.h>
#include <aws/glue/model/SerDeInfo.h>
#include <aws/glue/model/UpdateTableRequest.h>

#include <functional>

namespace duckdb {

vector<GlueTableInfo> GlueAPI::GetTables(ClientContext &context, GlueCatalog &catalog, const string &database_name) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	vector<GlueTableInfo> result;
	Aws::String next_token;
	do {
		Aws::Glue::Model::GetTablesRequest request;
		SetCatalogId(request, catalog);
		request.SetDatabaseName(database_name);
		if (!next_token.empty()) {
			request.SetNextToken(next_token);
		}
		auto outcome = client->GetTables(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, StringUtil::Format("GetTables (database '%s')", database_name));
		}
		auto &tables = outcome.GetResult();
		for (auto &table : tables.GetTableList()) {
			result.push_back(ToTableInfo(table));
		}
		next_token = tables.GetNextToken();
	} while (!next_token.empty());
	return result;
}

bool GlueAPI::GetAnyTableName(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              string &table_name) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::String next_token;
	do {
		Aws::Glue::Model::GetTablesRequest request;
		SetCatalogId(request, catalog);
		request.SetDatabaseName(database_name);
		request.SetMaxResults(1);
		if (!next_token.empty()) {
			request.SetNextToken(next_token);
		}
		auto outcome = client->GetTables(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, StringUtil::Format("GetTables (database '%s')", database_name));
		}
		auto &tables = outcome.GetResult();
		if (!tables.GetTableList().empty()) {
			table_name = ToStdString(tables.GetTableList()[0].GetName());
			return true;
		}
		next_token = tables.GetNextToken();
	} while (!next_token.empty());
	return false;
}

bool GlueAPI::GetTable(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                       const string &table_name, GlueTableInfo &result, string *raw_json) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetTableRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetName(table_name);
	auto outcome = client->GetTable(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			return false;
		}
		ThrowGlueError(outcome, StringUtil::Format("GetTable '%s.%s'", database_name, table_name));
	}
	auto &table = outcome.GetResult().GetTable();
	result = ToTableInfo(table);
	if (raw_json) {
		*raw_json = ToStdString(table.Jsonize().View().WriteReadable());
	}
	return true;
}

void GlueAPI::CreateHiveTable(ClientContext &context, GlueCatalog &catalog, const GlueTableInfo &table) {
	CheckWritable(catalog, "CreateTable");
	if (table.location.empty()) {
		throw InvalidInputException("Can not create Hive table '%s.%s' without a location", table.database_name,
		                            table.name);
	}
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);

	// An external table, described the way Hive / Athena / Spark expect it for the file format
	Aws::Glue::Model::SerDeInfo serde_info;
	Aws::Glue::Model::StorageDescriptor storage_descriptor;
	auto parameters = ToAwsMap(table.parameters);
	switch (table.file_format) {
	case HiveFileFormat::PARQUET:
		serde_info.SetSerializationLibrary("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
		serde_info.AddParameters("serialization.format", "1");
		storage_descriptor.SetInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
		storage_descriptor.SetOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
		parameters.emplace("classification", "parquet");
		break;
	case HiveFileFormat::CSV:
		if (table.csv_quote.empty() && table.csv_escape.empty()) {
			// Hive's "ROW FORMAT DELIMITED FIELDS TERMINATED BY '<delimiter>'": no quoting
			serde_info.SetSerializationLibrary("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
			serde_info.AddParameters("field.delim", table.csv_delimiter);
			serde_info.AddParameters("serialization.format", table.csv_delimiter);
		} else {
			// quoted fields: Hive's "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH
			// SERDEPROPERTIES (...)", the escape character defaults to the quote character like DuckDB's COPY
			auto quote = table.csv_quote.empty() ? "\"" : table.csv_quote;
			auto escape = table.csv_escape.empty() ? quote : table.csv_escape;
			serde_info.SetSerializationLibrary("org.apache.hadoop.hive.serde2.OpenCSVSerde");
			serde_info.AddParameters("separatorChar", table.csv_delimiter);
			serde_info.AddParameters("quoteChar", quote);
			serde_info.AddParameters("escapeChar", escape);
		}
		storage_descriptor.SetInputFormat("org.apache.hadoop.mapred.TextInputFormat");
		storage_descriptor.SetOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
		parameters.emplace("classification", "csv");
		parameters.emplace("delimiter", table.csv_delimiter);
		break;
	case HiveFileFormat::JSON:
		// one JSON object per line
		serde_info.SetSerializationLibrary("org.apache.hive.hcatalog.data.JsonSerDe");
		storage_descriptor.SetInputFormat("org.apache.hadoop.mapred.TextInputFormat");
		storage_descriptor.SetOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
		parameters.emplace("classification", "json");
		break;
	case HiveFileFormat::AVRO:
		// Avro container files, the schema travels in the files
		serde_info.SetSerializationLibrary("org.apache.hadoop.hive.serde2.avro.AvroSerDe");
		storage_descriptor.SetInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
		storage_descriptor.SetOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
		parameters.emplace("classification", "avro");
		break;
	}
	storage_descriptor.SetLocation(table.location);
	storage_descriptor.SetColumns(ToAwsColumns(table.columns));
	storage_descriptor.SetSerdeInfo(serde_info);
	storage_descriptor.SetCompressed(false);
	storage_descriptor.SetNumberOfBuckets(table.number_of_buckets);
	if (!table.bucket_columns.empty()) {
		storage_descriptor.SetBucketColumns(ToAwsValues(table.bucket_columns));
	}
	for (auto &sort_column : table.sort_columns) {
		Aws::Glue::Model::Order order;
		order.SetColumn(sort_column.name);
		// Glue's SortOrder is 1 for ascending, 0 for descending
		order.SetSortOrder(sort_column.sort_order == GlueSortOrder::DESCENDING ? 0 : 1);
		storage_descriptor.AddSortColumns(std::move(order));
	}
	parameters.emplace("EXTERNAL", "TRUE");

	Aws::Glue::Model::TableInput table_input;
	table_input.SetName(table.name);
	table_input.SetTableType("EXTERNAL_TABLE");
	table_input.SetStorageDescriptor(storage_descriptor);
	if (!table.partition_keys.empty()) {
		table_input.SetPartitionKeys(ToAwsColumns(table.partition_keys));
	}
	table_input.SetParameters(parameters);

	Aws::Glue::Model::CreateTableRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(table.database_name);
	request.SetTableInput(table_input);
	auto outcome = client->CreateTable(request);
	if (!outcome.IsSuccess()) {
		if (IsAlreadyExists(outcome)) {
			throw CatalogException("Table with name \"%s\" already exists in Glue database \"%s\"", table.name,
			                       table.database_name);
		}
		ThrowGlueError(outcome, StringUtil::Format("CreateTable '%s.%s' (location '%s')", table.database_name,
		                                           table.name, table.location));
	}
}

//! The parameters that mark a view as written by DuckDB
static void SetViewParameters(Aws::Map<Aws::String, Aws::String> &parameters, const GlueViewInfo &view) {
	parameters["duckdb_view"] = "true";
	parameters["duckdb_view_version"] = "1";
	// the database the unqualified names in the SQL belong to (the one the view is created in)
	parameters["duckdb_view_default_database"] = view.database_name;
	if (view.secure) {
		parameters["duckdb_view_secure"] = "true";
	} else {
		parameters.erase("duckdb_view_secure");
	}
}

//! Glue's (Hive's) two view fields: the SQL as written and the SQL with names resolved. Both are free text and readers
//! differ in which one they parse, so both carry the same DuckDB SQL - there is no separate expansion
static void SetViewDefinition(Aws::Glue::Model::TableInput &table_input, const GlueViewInfo &view) {
	table_input.SetTableType("VIRTUAL_VIEW");
	table_input.SetViewOriginalText(view.sql);
	table_input.SetViewExpandedText(view.sql);
	Aws::Glue::Model::StorageDescriptor storage_descriptor;
	if (!view.columns.empty()) {
		storage_descriptor.SetColumns(ToAwsColumns(view.columns));
	}
	table_input.SetStorageDescriptor(storage_descriptor);
}

static Aws::Glue::Model::TableInput ToViewTableInput(const GlueViewInfo &view) {
	Aws::Glue::Model::TableInput table_input;
	table_input.SetName(view.name);
	SetViewDefinition(table_input, view);
	Aws::Map<Aws::String, Aws::String> parameters;
	SetViewParameters(parameters, view);
	table_input.SetParameters(parameters);
	return table_input;
}

static void UpdateGlueTable(const std::shared_ptr<Aws::Glue::GlueClient> &client, GlueCatalog &catalog,
                            const string &database_name, const string &table_name,
                            const std::function<bool(Aws::Glue::Model::TableInput &)> &modify);

void GlueAPI::CreateView(ClientContext &context, GlueCatalog &catalog, const GlueViewInfo &view) {
	CheckWritable(catalog, "CreateView");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);

	Aws::Glue::Model::CreateTableRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(view.database_name);
	request.SetTableInput(ToViewTableInput(view));
	auto outcome = client->CreateTable(request);
	if (!outcome.IsSuccess()) {
		if (IsAlreadyExists(outcome)) {
			throw CatalogException("View with name \"%s\" already exists in Glue database \"%s\"", view.name,
			                       view.database_name);
		}
		ThrowGlueError(outcome, StringUtil::Format("CreateView '%s.%s'", view.database_name, view.name));
	}
}

void GlueAPI::UpdateView(ClientContext &context, GlueCatalog &catalog, const GlueViewInfo &view) {
	CheckWritable(catalog, "UpdateView");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	// CREATE OR REPLACE keeps what is not the definition: the description, owner and parameters other engines or
	// users set stay, the SQL, the columns and our own parameters are replaced
	UpdateGlueTable(client, catalog, view.database_name, view.name, [&](Aws::Glue::Model::TableInput &table_input) {
		SetViewDefinition(table_input, view);
		auto parameters = table_input.GetParameters();
		SetViewParameters(parameters, view);
		table_input.SetParameters(parameters);
		return true;
	});
}

//! UpdateTable replaces the whole definition: fetch the current one, let 'modify' change the TableInput built from
//! it, and send it back unless 'modify' returns false
static void UpdateGlueTable(const std::shared_ptr<Aws::Glue::GlueClient> &client, GlueCatalog &catalog,
                            const string &database_name, const string &table_name,
                            const std::function<bool(Aws::Glue::Model::TableInput &)> &modify) {
	// UpdateTable replaces the whole definition, so start from the current one and change only the columns
	Aws::Glue::Model::GetTableRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetDatabaseName(database_name);
	get_request.SetName(table_name);
	auto get_outcome = client->GetTable(get_request);
	if (!get_outcome.IsSuccess()) {
		if (IsEntityNotFound(get_outcome)) {
			throw CatalogException("Table with name \"%s\" does not exist in Glue database \"%s\"", table_name,
			                       database_name);
		}
		ThrowGlueError(get_outcome, StringUtil::Format("GetTable '%s.%s'", database_name, table_name));
	}
	auto &table = get_outcome.GetResult().GetTable();

	Aws::Glue::Model::TableInput table_input;
	table_input.SetName(table.GetName());
	if (table.DescriptionHasBeenSet()) {
		table_input.SetDescription(table.GetDescription());
	}
	if (table.OwnerHasBeenSet()) {
		table_input.SetOwner(table.GetOwner());
	}
	if (table.LastAccessTimeHasBeenSet()) {
		table_input.SetLastAccessTime(table.GetLastAccessTime());
	}
	if (table.LastAnalyzedTimeHasBeenSet()) {
		table_input.SetLastAnalyzedTime(table.GetLastAnalyzedTime());
	}
	if (table.RetentionHasBeenSet()) {
		table_input.SetRetention(table.GetRetention());
	}
	if (table.PartitionKeysHasBeenSet()) {
		table_input.SetPartitionKeys(table.GetPartitionKeys());
	}
	if (table.ViewOriginalTextHasBeenSet()) {
		table_input.SetViewOriginalText(table.GetViewOriginalText());
	}
	if (table.ViewExpandedTextHasBeenSet()) {
		table_input.SetViewExpandedText(table.GetViewExpandedText());
	}
	if (table.TableTypeHasBeenSet()) {
		table_input.SetTableType(table.GetTableType());
	}
	if (table.ParametersHasBeenSet()) {
		table_input.SetParameters(table.GetParameters());
	}
	if (table.TargetTableHasBeenSet()) {
		table_input.SetTargetTable(table.GetTargetTable());
	}
	table_input.SetStorageDescriptor(table.GetStorageDescriptor());
	if (!modify(table_input)) {
		return;
	}

	Aws::Glue::Model::UpdateTableRequest update_request;
	SetCatalogId(update_request, catalog);
	update_request.SetDatabaseName(database_name);
	update_request.SetTableInput(table_input);
	auto update_outcome = client->UpdateTable(update_request);
	if (!update_outcome.IsSuccess()) {
		ThrowGlueError(update_outcome, StringUtil::Format("UpdateTable '%s.%s'", database_name, table_name));
	}
}

void GlueAPI::UpdateTableColumns(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                 const string &table_name, const vector<GlueColumn> &columns) {
	CheckWritable(catalog, "UpdateTable");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	UpdateGlueTable(client, catalog, database_name, table_name, [&](Aws::Glue::Model::TableInput &table_input) {
		auto storage_descriptor = table_input.GetStorageDescriptor();
		storage_descriptor.SetColumns(ToAwsColumns(columns));
		table_input.SetStorageDescriptor(storage_descriptor);
		return true;
	});
}

void GlueAPI::SetTableLocation(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                               const string &table_name, const string &location) {
	CheckWritable(catalog, "UpdateTable");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	UpdateGlueTable(client, catalog, database_name, table_name, [&](Aws::Glue::Model::TableInput &table_input) {
		auto storage_descriptor = table_input.GetStorageDescriptor();
		storage_descriptor.SetLocation(location);
		table_input.SetStorageDescriptor(storage_descriptor);
		// the statistics describe the files at the old location
		auto parameters = ToStdMap(table_input.GetParameters());
		RemoveBasicStatistics(parameters);
		table_input.SetParameters(ToAwsMap(parameters));
		return true;
	});
}

void GlueAPI::AddTableStatistics(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                 const string &table_name, const GlueBasicStatistics &statistics) {
	CheckWritable(catalog, "UpdateTable");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	UpdateGlueTable(client, catalog, database_name, table_name, [&](Aws::Glue::Model::TableInput &table_input) {
		auto parameters = ToStdMap(table_input.GetParameters());
		GlueBasicStatistics current;
		if (!TryGetBasicStatistics(parameters, current)) {
			// unknown statistics stay unknown
			return false;
		}
		current.num_rows += statistics.num_rows;
		current.num_files += statistics.num_files;
		current.total_size += statistics.total_size;
		SetBasicStatistics(parameters, current);
		table_input.SetParameters(ToAwsMap(parameters));
		return true;
	});
}

void GlueAPI::UpdateTableParameters(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                    const string &table_name, const vector<pair<string, string>> &set,
                                    const vector<string> &unset) {
	// Reached only through ALTER TABLE, which the binder refuses on a read-only attach before it gets here.
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	UpdateGlueTable(client, catalog, database_name, table_name, [&](Aws::Glue::Model::TableInput &table_input) {
		auto parameters = table_input.GetParameters();
		for (auto &key : unset) {
			parameters.erase(key);
		}
		for (auto &entry : set) {
			parameters[entry.first] = entry.second;
		}
		table_input.SetParameters(parameters);
		return true;
	});
}

void GlueAPI::DeleteTable(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                          const string &table_name) {
	CheckWritable(catalog, "DeleteTable");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::DeleteTableRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetName(table_name);
	auto outcome = client->DeleteTable(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			throw CatalogException("Table with name \"%s\" does not exist in Glue database \"%s\"", table_name,
			                       database_name);
		}
		ThrowGlueError(outcome, StringUtil::Format("DeleteTable '%s.%s'", database_name, table_name));
	}
}

} // namespace duckdb
