#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! glue_get_table_response('<catalog>.<schema>.<table>'): the Glue GetTable response for a table of an attached
//! Glue catalog, for inspecting what Glue knows about a table. One row with the most useful fields as columns and
//! the complete Glue Table object as VARIANT.
TableFunction GetGlueGetTableResponseFunction();

//! glue_get_database_response('<catalog>.<database>'): the Glue GetDatabase response for a database (a DuckDB
//! schema) of an attached Glue catalog. One row with comment (Glue Description), location (LocationUri) and
//! parameters (Athena's DBPROPERTIES) as columns, plus the complete Glue Database object as VARIANT. These three
//! fields have no other SQL surface -- duckdb_schemas() carries none of them.
TableFunction GetGlueGetDatabaseResponseFunction();

//! glue_create_database('<catalog>.<database>', options := {comment: '...', location: '...', <property>: '...'},
//! if_not_exists := false): CREATE DATABASE with the fields DuckDB's own CREATE SCHEMA cannot express. 'comment'
//! becomes the Glue Description, 'location' the LocationUri, every other key a database parameter (DBPROPERTIES).
//! The options may be a struct or a MAP. This is what the glue_hive_ddl grammar turns CREATE SCHEMA ... WITH (...)
//! into.
TableFunction GetGlueCreateDatabaseFunction();
//! glue_set_database_properties('<catalog>.<database>', {key: 'value', ...}): ALTER DATABASE SET DBPROPERTIES. Merges,
//! so listed keys are added or overwritten and unlisted ones are kept. 'comment' sets the Glue Description instead of
//! a parameter.
TableFunction GetGlueSetDatabasePropertiesFunction();

//! glue_partitions('<catalog>.<schema>.<table>'): the partitions of a Hive table as registered in Glue, one row
//! per partition with a typed column per partition key and the partition's location
TableFunction GetGluePartitionsFunction();
//! glue_add_partition(table, {key: value, ...}, location := '...', if_not_exists := false): ALTER TABLE ADD
//! PARTITION. Without 'location' the partition lives at <table location>/<key>=<value>/...
TableFunction GetGlueAddPartitionFunction();
//! glue_drop_partition(table, {key: value, ...}, if_exists := false): ALTER TABLE DROP PARTITION, the data files
//! are left in place
TableFunction GetGlueDropPartitionFunction();
//! glue_rename_partition(table, {key: value, ...}, {key: new_value, ...}): ALTER TABLE ... RENAME TO PARTITION,
//! changes the partition values and keeps the location
TableFunction GetGlueRenamePartitionFunction();
//! glue_set_partition_location(table, {key: value, ...}, location): ALTER TABLE ... PARTITION (...) SET LOCATION
TableFunction GetGlueSetPartitionLocationFunction();
//! glue_set_table_location(table, location): ALTER TABLE ... SET LOCATION; existing partitions keep theirs
TableFunction GetGlueSetTableLocationFunction();
//! glue_alter_table(table, [{action, if_not_exists, if_exists, partition, new_partition, location}, ...]): the
//! partition DDL of one ALTER TABLE statement (what the glue_hive_ddl grammar extension turns the SQL into).
//! 'partition' and 'new_partition' are lists of {key, value}; the actions are validated against Glue before any
//! of them is applied, consecutive adds go out as one BatchCreatePartition call.
TableFunction GetGlueAlterTableFunction();

//! hive_scan('s3://root', schema := {col: 'TYPE', ...}, partitions := [{key: value, ..., location: '...'}, ...],
//! partition_keys := [...]): read a parquet Hive table without a catalog. The same scan as for a Glue Hive table,
//! with the schema and the partitions (values and locations) given as arguments.
TableFunction GetHiveScanFunction(DatabaseInstance &db);

} // namespace duckdb
