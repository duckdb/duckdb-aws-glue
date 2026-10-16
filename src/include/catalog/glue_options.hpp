#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

//! Options parsed from ATTACH '<catalog id>' (TYPE GLUE, ...)
struct GlueAttachOptions {
	//! The name of the attached database
	string name;
	//! The raw ATTACH path (':' | '<account id>' | '<account id>:<catalog>' | '<catalog>/<nested>' ...)
	string path;
	//! The Glue CatalogId sent with every request, empty means the default catalog of the current account
	string catalog_id;
	//! AWS region the Glue Data Catalog lives in
	string region;
	//! Name of the DuckDB (s3 / aws) secret to take credentials from, empty selects the default secret
	string secret_name;
	//! Optional Glue endpoint override, e.g. 'http://localhost:5000' for a local moto server; empty means AWS
	string endpoint;
	//! Optional S3 prefix for new databases, and for tables created without a location in databases that have no
	//! LocationUri. Empty means: databases are created without a LocationUri and tables must be given a location
	string default_location;
	//! Optional default schema
	Identifier default_schema;
	//! Run the ATTACH-time GetDatabases reachability probe (default true); false defers the first Glue call
	bool verify_connection = true;
	//! Name of an attached Iceberg catalog (TYPE ICEBERG) fronting the same Glue Data Catalog. When set, a lookup of
	//! an Iceberg table in this catalog is redirected to '<iceberg_catalog>.<database>.<table>'
	//! (Catalog::TryRedirectEntry), so the iceberg extension binds, scans and writes it. Empty: Iceberg tables are
	//! listed but can not be read.
	string iceberg_catalog;
};

} // namespace duckdb
