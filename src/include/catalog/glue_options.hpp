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
	//! Load tables through the GovernedGlueTableLoader another extension registers
	bool governance = false;
};

} // namespace duckdb
