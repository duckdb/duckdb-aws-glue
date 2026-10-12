#include "api/glue_api_util.hpp"
#include "api/glue_http_client.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/glue/model/GetDatabasesRequest.h>

#include <sys/stat.h>

namespace duckdb {

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

} // namespace

Aws::Auth::AWSCredentials GetClientCredentials(ClientContext &context, GlueCatalog &catalog) {
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
	return Aws::Auth::AWSCredentials(key_id, secret, session_token);
}

std::shared_ptr<Aws::Glue::GlueClient> GlueAPI::GetClient(ClientContext &context, GlueCatalog &catalog) {
	auto credentials = GetClientCredentials(context, catalog);

	auto &region = catalog.options.region;
	// The HTTP transport is chosen when the SDK client is built, so a changed setting needs a new client
	auto via_duckdb = GlueNetworkCallsViaDuckDB(DatabaseInstance::GetDatabase(context));
	auto &endpoint = catalog.options.endpoint;
	auto cache_key = ToStdString(credentials.GetAWSAccessKeyId()) + "\x1f" +
	                 ToStdString(credentials.GetSessionToken()) + "\x1f" + region + "\x1f" + endpoint + "\x1f" +
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

	auto provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(credentials);
	catalog.glue_client = std::make_shared<Aws::Glue::GlueClient>(provider, nullptr, config);
	catalog.client_cache_key = cache_key;
	return catalog.glue_client;
}

//===--------------------------------------------------------------------===//
// Read API
//===--------------------------------------------------------------------===//
string GlueAPI::GetReportedCatalogId(ClientContext &context, GlueCatalog &catalog) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	// the catalog id of the first database that is not a resource link (a link's target is another catalog)
	Aws::String next_token;
	do {
		Aws::Glue::Model::GetDatabasesRequest request;
		SetCatalogId(request, catalog);
		request.SetMaxResults(20);
		if (!next_token.empty()) {
			request.SetNextToken(next_token);
		}
		auto outcome = client->GetDatabases(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, StringUtil::Format("GetDatabases (catalog '%s', region '%s')", catalog.options.path,
			                                           catalog.options.region));
		}
		for (auto &database : outcome.GetResult().GetDatabaseList()) {
			auto info = ToDatabaseInfo(database);
			if (!info.is_resource_link && !info.catalog_id.empty()) {
				return info.catalog_id;
			}
		}
		next_token = outcome.GetResult().GetNextToken();
	} while (!next_token.empty());
	return string();
}

} // namespace duckdb
