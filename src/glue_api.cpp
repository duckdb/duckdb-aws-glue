#include "glue_api.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "glue_http_client.hpp"
#include "storage/glue_catalog.hpp"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/glue/GlueClient.h>
#include <aws/glue/GlueErrors.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/glue/model/GetDatabaseRequest.h>
#include <aws/glue/model/GetDatabasesRequest.h>
#include <aws/glue/model/GetTableRequest.h>
#include <aws/glue/model/GetTablesRequest.h>
#include <aws/glue/model/CreateDatabaseRequest.h>
#include <aws/glue/model/CreateTableRequest.h>
#include <aws/glue/model/DeleteDatabaseRequest.h>
#include <aws/glue/model/DeleteTableRequest.h>
#include <aws/glue/model/UpdateDatabaseRequest.h>
#include <aws/glue/model/UpdateTableRequest.h>
#include <aws/glue/model/BatchCreatePartitionRequest.h>
#include <aws/glue/model/GetPartitionsRequest.h>
#include <aws/glue/model/Segment.h>
#include <aws/glue/model/GetPartitionRequest.h>
#include <aws/glue/model/CreatePartitionRequest.h>
#include <aws/glue/model/DeletePartitionRequest.h>
#include <aws/glue/model/UpdatePartitionRequest.h>
#include <aws/glue/model/SerDeInfo.h>

#include <sys/stat.h>
#include <functional>

namespace duckdb {

//===--------------------------------------------------------------------===//
// GlueTableInfo
//===--------------------------------------------------------------------===//
string GlueTableFormatToString(GlueTableFormat format) {
	switch (format) {
	case GlueTableFormat::ICEBERG:
		return "ICEBERG";
	case GlueTableFormat::DELTA:
		return "DELTA";
	case GlueTableFormat::HUDI:
		return "HUDI";
	case GlueTableFormat::HIVE:
		return "HIVE";
	default:
		return "UNKNOWN";
	}
}

string GlueTableInfo::GetParameter(const string &key) const {
	for (auto &entry : parameters) {
		if (StringUtil::CIEquals(entry.first, key)) {
			return entry.second;
		}
	}
	return string();
}

string HiveFileFormatToString(HiveFileFormat format) {
	switch (format) {
	case HiveFileFormat::PARQUET:
		return "parquet";
	case HiveFileFormat::CSV:
		return "csv";
	case HiveFileFormat::JSON:
		return "json";
	case HiveFileFormat::AVRO:
		return "avro";
	}
	throw InternalException("Unknown HiveFileFormat");
}

HiveFileFormat HiveFileFormatFromString(const string &format) {
	auto lower = StringUtil::Lower(format);
	if (lower == "parquet") {
		return HiveFileFormat::PARQUET;
	}
	if (lower == "csv") {
		return HiveFileFormat::CSV;
	}
	if (lower == "json") {
		return HiveFileFormat::JSON;
	}
	if (lower == "avro") {
		return HiveFileFormat::AVRO;
	}
	throw BinderException("Unknown Hive file format '%s', expected 'parquet', 'csv', 'json' or 'avro'", format);
}

string GlueTableInfo::GetSerdeParameter(const string &key) const {
	for (auto &entry : serde_parameters) {
		if (StringUtil::CIEquals(entry.first, key)) {
			return entry.second;
		}
	}
	return string();
}

HiveFileFormat GlueTableInfo::GetFileFormat() const {
	auto serde = StringUtil::Lower(serde_library);
	if (StringUtil::Contains(serde, "parquet")) {
		return HiveFileFormat::PARQUET;
	}
	if (StringUtil::Contains(serde, "lazysimpleserde") || StringUtil::Contains(serde, "opencsvserde")) {
		return HiveFileFormat::CSV;
	}
	if (StringUtil::Contains(serde, "json")) {
		return HiveFileFormat::JSON;
	}
	if (StringUtil::Contains(serde, "avro")) {
		return HiveFileFormat::AVRO;
	}
	throw NotImplementedException("Hive table '%s.%s' uses SerDe '%s', only parquet (ParquetHiveSerDe), csv "
	                              "(LazySimpleSerDe, OpenCSVSerde), json (JsonSerDe) and avro (AvroSerDe) tables are "
	                              "supported",
	                              database_name, name, serde_library);
}

string GlueTableInfo::GetFieldDelimiter() const {
	// LazySimpleSerDe: field.delim, OpenCSVSerde: separatorChar
	auto delimiter = GetSerdeParameter("field.delim");
	if (delimiter.empty()) {
		delimiter = GetSerdeParameter("separatorChar");
	}
	if (delimiter.empty()) {
		return ",";
	}
	return delimiter;
}

bool GlueTableInfo::HasHeader() const {
	return GetParameter("skip.header.line.count") == "1";
}

string GlueTableInfo::GetQuoteCharacter() const {
	// OpenCSVSerde: quoteChar. LazySimpleSerDe does not quote at all, but DuckDB writes (and reads) quoted fields
	// with the '"' it defaults to, which is also OpenCSVSerde's default
	auto quote = GetSerdeParameter("quoteChar");
	if (quote.empty()) {
		return "\"";
	}
	return quote;
}

string GlueTableInfo::GetEscapeCharacter() const {
	auto escape = GetSerdeParameter("escapeChar");
	if (escape.empty()) {
		return GetQuoteCharacter();
	}
	return escape;
}

GlueTableFormat GlueTableInfo::GetFormat() const {
	// Open table formats register themselves through the 'table_type' parameter
	auto table_type = StringUtil::Upper(GetParameter("table_type"));
	if (table_type == "ICEBERG") {
		return GlueTableFormat::ICEBERG;
	}
	if (table_type == "DELTA") {
		return GlueTableFormat::DELTA;
	}
	if (table_type == "HUDI") {
		return GlueTableFormat::HUDI;
	}
	// Spark registers Delta tables through the data source provider
	auto provider = StringUtil::Lower(GetParameter("spark.sql.sources.provider"));
	if (provider == "delta") {
		return GlueTableFormat::DELTA;
	}
	if (provider == "iceberg") {
		return GlueTableFormat::ICEBERG;
	}
	if (provider == "hudi") {
		return GlueTableFormat::HUDI;
	}
	if (!GetMetadataLocation().empty()) {
		return GlueTableFormat::ICEBERG;
	}
	if (!input_format.empty() || !location.empty()) {
		// Regular (Hive style) table with a storage descriptor
		return GlueTableFormat::HIVE;
	}
	return GlueTableFormat::UNKNOWN;
}

string GlueTableInfo::GetFormatName() const {
	auto format = GetFormat();
	if (format == GlueTableFormat::HIVE) {
		return StringUtil::Format("HIVE (input format '%s')", input_format);
	}
	if (format == GlueTableFormat::UNKNOWN && !glue_table_type.empty()) {
		return StringUtil::Format("UNKNOWN (glue table type '%s')", glue_table_type);
	}
	return GlueTableFormatToString(format);
}

string GlueTableInfo::GetMetadataLocation() const {
	return GetParameter("metadata_location");
}

//===--------------------------------------------------------------------===//
// Client creation
//===--------------------------------------------------------------------===//
namespace {

//! Grab the first path that exists, from a list of well-known CA bundle locations
string SelectCURLCertPath() {
	static const char *cert_file_locations[] = {// Arch, Debian-based, Gentoo
	                                            "/etc/ssl/certs/ca-certificates.crt",
	                                            // RedHat 7 based
	                                            "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
	                                            // Redhat 6 based
	                                            "/etc/pki/tls/certs/ca-bundle.crt",
	                                            // OpenSUSE
	                                            "/etc/ssl/ca-bundle.pem",
	                                            // Alpine
	                                            "/etc/ssl/cert.pem"};
	for (auto &ca_file : cert_file_locations) {
		struct stat buf;
		if (stat(ca_file, &buf) == 0) {
			return ca_file;
		}
	}
	return string();
}

const string &GetCURLCertPath() {
	static string cert_path = SelectCURLCertPath();
	return cert_path;
}

string ToStdString(const Aws::String &input) {
	return string(input.c_str(), input.size());
}

unordered_map<string, string> ToStdMap(const Aws::Map<Aws::String, Aws::String> &input) {
	unordered_map<string, string> result;
	for (auto &entry : input) {
		result.emplace(ToStdString(entry.first), ToStdString(entry.second));
	}
	return result;
}

vector<GlueColumn> ToColumns(const Aws::Vector<Aws::Glue::Model::Column> &input) {
	vector<GlueColumn> result;
	for (auto &column : input) {
		GlueColumn glue_column;
		glue_column.name = ToStdString(column.GetName());
		glue_column.type = ToStdString(column.GetType());
		glue_column.comment = ToStdString(column.GetComment());
		result.push_back(std::move(glue_column));
	}
	return result;
}

GlueDatabaseInfo ToDatabaseInfo(const Aws::Glue::Model::Database &database) {
	GlueDatabaseInfo result;
	result.name = ToStdString(database.GetName());
	result.description = ToStdString(database.GetDescription());
	result.location_uri = ToStdString(database.GetLocationUri());
	result.parameters = ToStdMap(database.GetParameters());
	return result;
}

GlueTableInfo ToTableInfo(const Aws::Glue::Model::Table &table) {
	GlueTableInfo result;
	result.name = ToStdString(table.GetName());
	result.database_name = ToStdString(table.GetDatabaseName());
	result.glue_table_type = ToStdString(table.GetTableType());
	auto &storage_descriptor = table.GetStorageDescriptor();
	result.location = ToStdString(storage_descriptor.GetLocation());
	result.input_format = ToStdString(storage_descriptor.GetInputFormat());
	result.output_format = ToStdString(storage_descriptor.GetOutputFormat());
	result.serde_library = ToStdString(storage_descriptor.GetSerdeInfo().GetSerializationLibrary());
	result.serde_parameters = ToStdMap(storage_descriptor.GetSerdeInfo().GetParameters());
	result.columns = ToColumns(storage_descriptor.GetColumns());
	result.partition_keys = ToColumns(table.GetPartitionKeys());
	result.parameters = ToStdMap(table.GetParameters());
	return result;
}

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

string PartitionValuesToString(const vector<string> &values) {
	return StringUtil::Join(values, ", ");
}

Aws::Vector<Aws::String> ToAwsValues(const vector<string> &values) {
	return Aws::Vector<Aws::String>(values.begin(), values.end());
}

void CheckWritable(const GlueCatalog &catalog, const string &operation) {
	if (catalog.access_mode == AccessMode::READ_ONLY) {
		throw InvalidInputException("Cannot execute Glue %s on database %s which is attached in read-only mode!",
		                            operation, catalog.GetName());
	}
}

template <class REQUEST>
void SetCatalogId(REQUEST &request, const GlueCatalog &catalog) {
	if (!catalog.options.catalog_id.empty()) {
		request.SetCatalogId(catalog.options.catalog_id);
	}
}

} // namespace

std::shared_ptr<Aws::Glue::GlueClient> GlueAPI::GetClient(ClientContext &context, GlueCatalog &catalog) {
	// Take the credentials from the DuckDB secret. The secret is looked up on every call so a refreshed
	// (credential_chain / sts) secret is picked up automatically.
	auto secret_entry = GlueCatalog::GetStorageSecret(context, catalog.options.secret_name);
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	auto key_id_val = kv_secret.TryGetValue("key_id");
	auto secret_val = kv_secret.TryGetValue("secret");
	auto session_token_val = kv_secret.TryGetValue("session_token");
	string key_id = key_id_val.IsNull() ? "" : key_id_val.GetValue<string>();
	string secret = secret_val.IsNull() ? "" : secret_val.GetValue<string>();
	string session_token = session_token_val.IsNull() ? "" : session_token_val.GetValue<string>();
	if (key_id.empty() || secret.empty()) {
		throw InvalidConfigurationException(
		    "Secret '%s' does not contain AWS credentials (key_id / secret), can not connect to Glue catalog '%s'",
		    secret_entry->secret->GetName().GetIdentifierName(), catalog.options.name);
	}

	auto &region = catalog.options.region;
	// The HTTP transport is chosen when the SDK client is built, so a changed setting needs a new client
	auto via_duckdb = GlueNetworkCallsViaDuckDB(DatabaseInstance::GetDatabase(context));
	auto &endpoint = catalog.options.endpoint;
	auto cache_key = key_id + "\x1f" + session_token + "\x1f" + region + "\x1f" + endpoint + "\x1f" +
	                 (via_duckdb ? "duckdb" : "sdk");

	lock_guard<mutex> guard(catalog.client_lock);
	if (catalog.glue_client && catalog.client_cache_key == cache_key) {
		return catalog.glue_client;
	}

	// NOTE: without a region in the environment (AWS_DEFAULT_REGION / AWS_REGION) or the profile file, the SDK's
	// configuration constructor asks the EC2 instance metadata service for one before the region below is set;
	// off EC2 that is a connect timeout with retries. Set AWS_EC2_METADATA_DISABLED=true in such environments.
	Aws::Glue::GlueClientConfiguration config;
	config.region = region;
	if (!endpoint.empty()) {
		// a Glue compatible server elsewhere (e.g. moto for tests): 'http://host:port' or 'https://host:port'
		auto lower = StringUtil::Lower(endpoint);
		if (StringUtil::StartsWith(lower, "http://")) {
			config.scheme = Aws::Http::Scheme::HTTP;
			config.endpointOverride = endpoint.substr(7);
		} else if (StringUtil::StartsWith(lower, "https://")) {
			config.scheme = Aws::Http::Scheme::HTTPS;
			config.endpointOverride = endpoint.substr(8);
		} else {
			config.endpointOverride = endpoint;
		}
	}
	auto &cert_path = GetCURLCertPath();
	if (!cert_path.empty()) {
		config.caFile = cert_path;
	}
	auto &db_config = DBConfig::GetConfig(context);
	config.userAgent = db_config.UserAgent();

	Aws::Auth::AWSCredentials credentials(key_id, secret, session_token);
	auto provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(credentials);
	catalog.glue_client = std::make_shared<Aws::Glue::GlueClient>(provider, nullptr, config);
	catalog.client_cache_key = cache_key;
	return catalog.glue_client;
}

//===--------------------------------------------------------------------===//
// Read API
//===--------------------------------------------------------------------===//
void GlueAPI::VerifyConnection(ClientContext &context, GlueCatalog &catalog) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetDatabasesRequest request;
	SetCatalogId(request, catalog);
	request.SetMaxResults(1);
	auto outcome = client->GetDatabases(request);
	if (!outcome.IsSuccess()) {
		ThrowGlueError(outcome, StringUtil::Format("GetDatabases (catalog '%s', region '%s')", catalog.options.path,
		                                           catalog.options.region));
	}
}

vector<GlueDatabaseInfo> GlueAPI::GetDatabases(ClientContext &context, GlueCatalog &catalog) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	vector<GlueDatabaseInfo> result;
	Aws::String next_token;
	do {
		Aws::Glue::Model::GetDatabasesRequest request;
		SetCatalogId(request, catalog);
		if (!next_token.empty()) {
			request.SetNextToken(next_token);
		}
		auto outcome = client->GetDatabases(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, "GetDatabases");
		}
		auto &databases = outcome.GetResult();
		for (auto &database : databases.GetDatabaseList()) {
			result.push_back(ToDatabaseInfo(database));
		}
		next_token = databases.GetNextToken();
	} while (!next_token.empty());
	return result;
}

bool GlueAPI::GetDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                          GlueDatabaseInfo &result, string *raw_json) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetDatabaseRequest request;
	SetCatalogId(request, catalog);
	request.SetName(database_name);
	auto outcome = client->GetDatabase(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			return false;
		}
		ThrowGlueError(outcome, StringUtil::Format("GetDatabase '%s'", database_name));
	}
	auto &database = outcome.GetResult().GetDatabase();
	result = ToDatabaseInfo(database);
	if (raw_json) {
		*raw_json = ToStdString(database.Jsonize().View().WriteReadable());
	}
	return true;
}

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

bool GlueAPI::DatabaseHasTables(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                string *first_table_name) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetTablesRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	// one entry is enough to answer the question; GetTables would otherwise page to the end and parse every table
	request.SetMaxResults(1);
	auto outcome = client->GetTables(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			throw CatalogException("Glue database with name \"%s\" does not exist", database_name);
		}
		ThrowGlueError(outcome, StringUtil::Format("GetTables (database '%s')", database_name));
	}
	// Glue returns views (TableType VIRTUAL_VIEW) from GetTables as well, so they count as occupants here
	auto &tables = outcome.GetResult().GetTableList();
	if (tables.empty()) {
		return false;
	}
	if (first_table_name) {
		*first_table_name = ToStdString(tables.front().GetName());
	}
	return true;
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

} // namespace duckdb

//===--------------------------------------------------------------------===//
// Write API
//===--------------------------------------------------------------------===//
namespace duckdb {

namespace {

template <class OUTCOME>
bool IsAlreadyExists(const OUTCOME &outcome) {
	return outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ALREADY_EXISTS;
}

Aws::Map<Aws::String, Aws::String> ToAwsMap(const unordered_map<string, string> &input) {
	Aws::Map<Aws::String, Aws::String> result;
	for (auto &entry : input) {
		result.emplace(entry.first, entry.second);
	}
	return result;
}

Aws::Vector<Aws::Glue::Model::Column> ToAwsColumns(const vector<GlueColumn> &input) {
	Aws::Vector<Aws::Glue::Model::Column> result;
	for (auto &column : input) {
		Aws::Glue::Model::Column aws_column;
		aws_column.SetName(column.name);
		aws_column.SetType(column.type);
		if (!column.comment.empty()) {
			aws_column.SetComment(column.comment);
		}
		result.push_back(std::move(aws_column));
	}
	return result;
}

} // namespace

void GlueAPI::CreateDatabase(ClientContext &context, GlueCatalog &catalog, const GlueDatabaseInfo &database) {
	CheckWritable(catalog, "CreateDatabase");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::DatabaseInput input;
	input.SetName(database.name);
	if (!database.description.empty()) {
		input.SetDescription(database.description);
	}
	if (!database.location_uri.empty()) {
		input.SetLocationUri(database.location_uri);
	}
	if (!database.parameters.empty()) {
		input.SetParameters(ToAwsMap(database.parameters));
	}
	Aws::Glue::Model::CreateDatabaseRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseInput(input);
	auto outcome = client->CreateDatabase(request);
	if (!outcome.IsSuccess()) {
		if (IsAlreadyExists(outcome)) {
			throw CatalogException("Glue database with name \"%s\" already exists", database.name);
		}
		ThrowGlueError(outcome, StringUtil::Format("CreateDatabase '%s'", database.name));
	}
}

void GlueAPI::DeleteDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name) {
	CheckWritable(catalog, "DeleteDatabase");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::DeleteDatabaseRequest request;
	SetCatalogId(request, catalog);
	request.SetName(database_name);
	auto outcome = client->DeleteDatabase(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			throw CatalogException("Glue database with name \"%s\" does not exist", database_name);
		}
		ThrowGlueError(outcome, StringUtil::Format("DeleteDatabase '%s'", database_name));
	}
}

//! UpdateDatabase replaces the whole definition: fetch the current one, let 'modify' change the DatabaseInput built
//! from it, and send it back. Without this round trip every field the caller does not set would be wiped.
static void UpdateGlueDatabase(const std::shared_ptr<Aws::Glue::GlueClient> &client, GlueCatalog &catalog,
                               const string &database_name,
                               const std::function<void(Aws::Glue::Model::DatabaseInput &)> &modify) {
	Aws::Glue::Model::GetDatabaseRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetName(database_name);
	auto get_outcome = client->GetDatabase(get_request);
	if (!get_outcome.IsSuccess()) {
		if (IsEntityNotFound(get_outcome)) {
			throw CatalogException("Glue database with name \"%s\" does not exist", database_name);
		}
		ThrowGlueError(get_outcome, StringUtil::Format("GetDatabase '%s'", database_name));
	}
	auto &database = get_outcome.GetResult().GetDatabase();

	Aws::Glue::Model::DatabaseInput input;
	input.SetName(database.GetName());
	if (database.DescriptionHasBeenSet()) {
		input.SetDescription(database.GetDescription());
	}
	if (database.LocationUriHasBeenSet()) {
		input.SetLocationUri(database.GetLocationUri());
	}
	if (database.ParametersHasBeenSet()) {
		input.SetParameters(database.GetParameters());
	}
	if (database.CreateTableDefaultPermissionsHasBeenSet()) {
		input.SetCreateTableDefaultPermissions(database.GetCreateTableDefaultPermissions());
	}
	if (database.TargetDatabaseHasBeenSet()) {
		input.SetTargetDatabase(database.GetTargetDatabase());
	}
	if (database.FederatedDatabaseHasBeenSet()) {
		input.SetFederatedDatabase(database.GetFederatedDatabase());
	}
	modify(input);

	Aws::Glue::Model::UpdateDatabaseRequest update_request;
	SetCatalogId(update_request, catalog);
	update_request.SetName(database_name);
	update_request.SetDatabaseInput(input);
	auto update_outcome = client->UpdateDatabase(update_request);
	if (!update_outcome.IsSuccess()) {
		ThrowGlueError(update_outcome, StringUtil::Format("UpdateDatabase '%s'", database_name));
	}
}

void GlueAPI::SetDatabaseProperties(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                    const unordered_map<string, string> &parameters) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	UpdateGlueDatabase(client, catalog, database_name, [&](Aws::Glue::Model::DatabaseInput &input) {
		// Hive / Athena SET DBPROPERTIES merges: the listed keys are added or overwritten, the rest are kept
		auto merged = input.GetParameters();
		for (auto &parameter : ToAwsMap(parameters)) {
			merged[parameter.first] = parameter.second;
		}
		input.SetParameters(merged);
	});
}

void GlueAPI::SetDatabaseDescription(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                     const string &description) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	UpdateGlueDatabase(client, catalog, database_name,
	                   [&](Aws::Glue::Model::DatabaseInput &input) { input.SetDescription(description); });
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
	storage_descriptor.SetNumberOfBuckets(-1);
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

//! UpdateTable replaces the whole definition: fetch the current one, let 'modify' change the TableInput built from
//! it, and send it back
static void UpdateGlueTable(const std::shared_ptr<Aws::Glue::GlueClient> &client, GlueCatalog &catalog,
                            const string &database_name, const string &table_name,
                            const std::function<void(Aws::Glue::Model::TableInput &)> &modify) {
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
	modify(table_input);

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
	});
}

void GlueAPI::SetPartitionLocation(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                   const string &table_name, const vector<string> &values, const string &location) {
	CheckWritable(catalog, "UpdatePartition");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetPartitionRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetDatabaseName(database_name);
	get_request.SetTableName(table_name);
	get_request.SetPartitionValues(ToAwsValues(values));
	auto get_outcome = client->GetPartition(get_request);
	if (!get_outcome.IsSuccess()) {
		if (IsEntityNotFound(get_outcome)) {
			throw CatalogException("Partition [%s] does not exist in Glue table '%s.%s'",
			                       PartitionValuesToString(values), database_name, table_name);
		}
		ThrowGlueError(get_outcome, StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                               PartitionValuesToString(values)));
	}
	auto &partition = get_outcome.GetResult().GetPartition();
	auto storage_descriptor = partition.GetStorageDescriptor();
	storage_descriptor.SetLocation(location);
	Aws::Glue::Model::PartitionInput input;
	input.SetValues(partition.GetValues());
	input.SetStorageDescriptor(storage_descriptor);
	input.SetParameters(partition.GetParameters());
	Aws::Glue::Model::UpdatePartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionValueList(ToAwsValues(values));
	request.SetPartitionInput(input);
	auto outcome = client->UpdatePartition(request);
	if (!outcome.IsSuccess()) {
		ThrowGlueError(outcome, StringUtil::Format("UpdatePartition '%s.%s' [%s]", database_name, table_name,
		                                           PartitionValuesToString(values)));
	}
}

bool GlueAPI::GetPartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                           const string &table_name, const vector<string> &values, GluePartitionInfo &result) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetPartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionValues(ToAwsValues(values));
	auto outcome = client->GetPartition(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			return false;
		}
		ThrowGlueError(outcome, StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                           PartitionValuesToString(values)));
	}
	auto &partition = outcome.GetResult().GetPartition();
	result.values.clear();
	for (auto &value : partition.GetValues()) {
		result.values.push_back(ToStdString(value));
	}
	result.location = ToStdString(partition.GetStorageDescriptor().GetLocation());
	return true;
}

bool GlueAPI::CreatePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              const string &table_name, const GluePartitionInput &partition, bool if_not_exists) {
	CheckWritable(catalog, "CreatePartition");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);

	// A partition carries its own StorageDescriptor: the table's, with the partition's location
	Aws::Glue::Model::GetTableRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetDatabaseName(database_name);
	get_request.SetName(table_name);
	auto get_outcome = client->GetTable(get_request);
	if (!get_outcome.IsSuccess()) {
		ThrowGlueError(get_outcome, StringUtil::Format("GetTable '%s.%s'", database_name, table_name));
	}
	auto descriptor = get_outcome.GetResult().GetTable().GetStorageDescriptor();
	descriptor.SetLocation(partition.location);

	Aws::Glue::Model::PartitionInput input;
	input.SetValues(ToAwsValues(partition.values));
	input.SetStorageDescriptor(descriptor);
	Aws::Glue::Model::CreatePartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionInput(input);
	auto outcome = client->CreatePartition(request);
	if (!outcome.IsSuccess()) {
		if (IsAlreadyExists(outcome)) {
			if (if_not_exists) {
				return false;
			}
			throw CatalogException("Partition [%s] already exists in Glue table '%s.%s'",
			                       PartitionValuesToString(partition.values), database_name, table_name);
		}
		ThrowGlueError(outcome, StringUtil::Format("CreatePartition '%s.%s' [%s]", database_name, table_name,
		                                           PartitionValuesToString(partition.values)));
	}
	return true;
}

bool GlueAPI::DeletePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              const string &table_name, const vector<string> &values) {
	CheckWritable(catalog, "DeletePartition");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::DeletePartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionValues(ToAwsValues(values));
	auto outcome = client->DeletePartition(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			return false;
		}
		ThrowGlueError(outcome, StringUtil::Format("DeletePartition '%s.%s' [%s]", database_name, table_name,
		                                           PartitionValuesToString(values)));
	}
	return true;
}

void GlueAPI::RenamePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              const string &table_name, const vector<string> &values,
                              const vector<string> &new_values) {
	CheckWritable(catalog, "UpdatePartition");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);

	// the partition to rename, with everything it carries (location, SerDe, parameters)
	Aws::Glue::Model::GetPartitionRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetDatabaseName(database_name);
	get_request.SetTableName(table_name);
	get_request.SetPartitionValues(ToAwsValues(values));
	auto get_outcome = client->GetPartition(get_request);
	if (!get_outcome.IsSuccess()) {
		if (IsEntityNotFound(get_outcome)) {
			throw CatalogException("Partition [%s] does not exist in Glue table '%s.%s'",
			                       PartitionValuesToString(values), database_name, table_name);
		}
		ThrowGlueError(get_outcome, StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                               PartitionValuesToString(values)));
	}
	auto &partition = get_outcome.GetResult().GetPartition();

	// the new values must be free
	Aws::Glue::Model::GetPartitionRequest check_request;
	SetCatalogId(check_request, catalog);
	check_request.SetDatabaseName(database_name);
	check_request.SetTableName(table_name);
	check_request.SetPartitionValues(ToAwsValues(new_values));
	auto check_outcome = client->GetPartition(check_request);
	if (check_outcome.IsSuccess()) {
		throw CatalogException("Partition [%s] already exists in Glue table '%s.%s'",
		                       PartitionValuesToString(new_values), database_name, table_name);
	}
	if (!IsEntityNotFound(check_outcome)) {
		ThrowGlueError(check_outcome, StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                                 PartitionValuesToString(new_values)));
	}

	Aws::Glue::Model::PartitionInput input;
	input.SetValues(ToAwsValues(new_values));
	input.SetStorageDescriptor(partition.GetStorageDescriptor());
	input.SetParameters(partition.GetParameters());
	Aws::Glue::Model::UpdatePartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionValueList(ToAwsValues(values));
	request.SetPartitionInput(input);
	auto outcome = client->UpdatePartition(request);
	if (!outcome.IsSuccess()) {
		ThrowGlueError(outcome,
		               StringUtil::Format("UpdatePartition '%s.%s' [%s] -> [%s]", database_name, table_name,
		                                  PartitionValuesToString(values), PartitionValuesToString(new_values)));
	}
}

//! The partitions Glue returns per request. 1000 is the maximum the API allows.
static constexpr int GLUE_PARTITIONS_PAGE_SIZE = 1000;
//! The requests to run at the same time when the catalog is AWS and the setting leaves the choice open
static constexpr idx_t GLUE_DEFAULT_PARTITION_SEGMENTS = 8;
//! The maximum Glue accepts for Segment::TotalSegments
static constexpr idx_t GLUE_MAX_PARTITION_SEGMENTS = 10;

//! One chain of GetPartitions requests: pages through the partitions of segment 'segment_number' (the whole table
//! when 'total_segments' is 1) and appends them to 'result'
static void FetchPartitionSegment(Aws::Glue::GlueClient &client, const GlueCatalog &catalog,
                                  const string &database_name, const string &table_name, int segment_number,
                                  int total_segments, vector<GluePartitionInfo> &result) {
	Aws::String next_token;
	do {
		Aws::Glue::Model::GetPartitionsRequest request;
		SetCatalogId(request, catalog);
		request.SetDatabaseName(database_name);
		request.SetTableName(table_name);
		// Only the values and the location of a partition are used. The column schema every partition repeats is a
		// large part of the response, and fewer bytes per partition means more partitions per page.
		request.SetExcludeColumnSchema(true);
		request.SetMaxResults(GLUE_PARTITIONS_PAGE_SIZE);
		if (total_segments > 1) {
			Aws::Glue::Model::Segment segment;
			segment.SetSegmentNumber(segment_number);
			segment.SetTotalSegments(total_segments);
			request.SetSegment(segment);
		}
		if (!next_token.empty()) {
			request.SetNextToken(next_token);
		}
		auto outcome = client.GetPartitions(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, StringUtil::Format("GetPartitions '%s.%s'", database_name, table_name));
		}
		auto &partitions = outcome.GetResult();
		for (auto &partition : partitions.GetPartitions()) {
			GluePartitionInfo info;
			for (auto &value : partition.GetValues()) {
				info.values.push_back(ToStdString(value));
			}
			info.location = ToStdString(partition.GetStorageDescriptor().GetLocation());
			result.push_back(std::move(info));
		}
		next_token = partitions.GetNextToken();
	} while (!next_token.empty());
}

//! The number of GetPartitions requests to run at the same time: the setting when it is given, otherwise one request
//! against a server given with ENDPOINT (moto ignores Segment and answers every segment with the whole table) and
//! GLUE_DEFAULT_PARTITION_SEGMENTS against AWS.
static int GetPartitionSegmentCount(ClientContext &context, const GlueCatalog &catalog) {
	idx_t segments = 0;
	Value setting;
	if (context.TryGetCurrentSetting("glue_get_partitions_segments", setting) && !setting.IsNull()) {
		segments = setting.GetValue<idx_t>();
	}
	if (segments == 0) {
		segments = catalog.options.endpoint.empty() ? GLUE_DEFAULT_PARTITION_SEGMENTS : 1;
	}
	if (segments > GLUE_MAX_PARTITION_SEGMENTS) {
		segments = GLUE_MAX_PARTITION_SEGMENTS;
	}
	return static_cast<int>(segments);
}

vector<GluePartitionInfo> GlueAPI::GetPartitions(ClientContext &context, GlueCatalog &catalog,
                                                 const string &database_name, const string &table_name) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	auto total_segments = GetPartitionSegmentCount(context, catalog);
	vector<vector<GluePartitionInfo>> segment_results(NumericCast<idx_t>(total_segments));

	if (total_segments == 1) {
		FetchPartitionSegment(*client, catalog, database_name, table_name, 0, 1, segment_results[0]);
	} else {
		// The segments do not overlap, so their requests can run at the same time. Every worker sets the context
		// scope itself: it is thread local, and without it the requests would use the database level HTTP settings
		// and stay out of this connection's HTTP log.
		vector<ErrorData> errors(NumericCast<idx_t>(total_segments));
		vector<thread> workers;
		for (int segment = 1; segment < total_segments; segment++) {
			workers.emplace_back([&, segment]() {
				GlueHttpClientContextScope worker_scope(context);
				try {
					FetchPartitionSegment(*client, catalog, database_name, table_name, segment, total_segments,
					                      segment_results[NumericCast<idx_t>(segment)]);
				} catch (std::exception &ex) {
					errors[NumericCast<idx_t>(segment)] = ErrorData(ex);
				}
			});
		}
		try {
			FetchPartitionSegment(*client, catalog, database_name, table_name, 0, total_segments, segment_results[0]);
		} catch (std::exception &ex) {
			errors[0] = ErrorData(ex);
		}
		for (auto &worker : workers) {
			worker.join();
		}
		for (auto &error : errors) {
			if (error.HasError()) {
				error.Throw();
			}
		}
	}

	vector<GluePartitionInfo> result;
	unordered_set<string> seen;
	for (auto &segment_result : segment_results) {
		for (auto &partition : segment_result) {
			// A Glue compatible server that ignores Segment answers every segment with the whole table: the values
			// identify the partition, so what was seen already is dropped here
			if (total_segments > 1 && !seen.insert(StringUtil::Join(partition.values, "\x1f")).second) {
				continue;
			}
			result.push_back(std::move(partition));
		}
	}
	return result;
}

void GlueAPI::BatchCreatePartitions(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                    const string &table_name, const vector<GluePartitionInput> &partitions) {
	CheckWritable(catalog, "BatchCreatePartition");
	if (partitions.empty()) {
		return;
	}
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);

	// A partition carries its own StorageDescriptor: the table's, with the partition's location
	Aws::Glue::Model::GetTableRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetDatabaseName(database_name);
	get_request.SetName(table_name);
	auto get_outcome = client->GetTable(get_request);
	if (!get_outcome.IsSuccess()) {
		ThrowGlueError(get_outcome, StringUtil::Format("GetTable '%s.%s'", database_name, table_name));
	}
	auto table_descriptor = get_outcome.GetResult().GetTable().GetStorageDescriptor();

	// BatchCreatePartition accepts at most 100 partitions per call
	constexpr idx_t BATCH_SIZE = 100;
	for (idx_t offset = 0; offset < partitions.size(); offset += BATCH_SIZE) {
		Aws::Vector<Aws::Glue::Model::PartitionInput> inputs;
		for (idx_t i = offset; i < MinValue<idx_t>(offset + BATCH_SIZE, partitions.size()); i++) {
			auto &partition = partitions[i];
			Aws::Glue::Model::PartitionInput input;
			Aws::Vector<Aws::String> values(partition.values.begin(), partition.values.end());
			input.SetValues(values);
			auto descriptor = table_descriptor;
			descriptor.SetLocation(partition.location);
			input.SetStorageDescriptor(descriptor);
			inputs.push_back(std::move(input));
		}
		Aws::Glue::Model::BatchCreatePartitionRequest request;
		SetCatalogId(request, catalog);
		request.SetDatabaseName(database_name);
		request.SetTableName(table_name);
		request.SetPartitionInputList(inputs);
		auto outcome = client->BatchCreatePartition(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, StringUtil::Format("BatchCreatePartition '%s.%s'", database_name, table_name));
		}
		for (auto &error : outcome.GetResult().GetErrors()) {
			auto code = ToStdString(error.GetErrorDetail().GetErrorCode());
			if (code == "AlreadyExistsException") {
				// appending to an existing partition
				continue;
			}
			vector<string> values;
			for (auto &value : error.GetPartitionValues()) {
				values.push_back(ToStdString(value));
			}
			throw IOException("Glue BatchCreatePartition '%s.%s' failed for partition [%s]: %s (%s)", database_name,
			                  table_name, StringUtil::Join(values, ", "),
			                  ToStdString(error.GetErrorDetail().GetErrorMessage()), code);
		}
	}
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
