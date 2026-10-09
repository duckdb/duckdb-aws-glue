#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

//! Resolve the table argument of a glue_* function to a fully qualified name in a Glue catalog; a partially qualified
//! name is resolved like in a query (search path, default catalog)
QualifiedName ResolveGlueTableName(ClientContext &context, const string &function_name, const string &table_name);

//! glue_get_table_response('<catalog>.<schema>.<table>'): the Glue GetTable response for a table of an attached
//! Glue catalog (a partially qualified name is resolved like in a query), for inspecting what Glue knows about a table.
//! One row with the most useful fields as columns and the complete Glue Table object as VARIANT.
TableFunction GetGlueGetTableResponseFunction();
//! glue_get_database_response('<catalog>.<database>'): the Glue GetDatabase response for a database (schema) of an
//! attached Glue catalog (an unqualified name is resolved like in a query). One row with the description, location and
//! parameters as columns and the complete Glue Database object as VARIANT.
TableFunction GetGlueGetDatabaseResponseFunction();

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
//! glue_replace_columns(table, {name: 'TYPE', ...}, comments := {name: '...'}, keep_comments := false): Hive's
//! ALTER TABLE ... REPLACE COLUMNS, replaces all data columns of the table; the partition keys are kept, the bucketing
//! and sort columns must be given
TableFunction GetGlueReplaceColumnsFunction();
//! glue_change_column: rename, retype, comment and move one data column of a Hive table (Hive's CHANGE COLUMN)
TableFunction GetGlueChangeColumnFunction();
//! glue_alter_table(table, [{action, if_not_exists, if_exists, partition, new_partition, location}, ...]): the
//! partition DDL of one ALTER TABLE statement (what the glue_hive_ddl grammar extension turns the SQL into).
//! 'partition' and 'new_partition' are lists of {key, value}; the actions are validated against Glue before any
//! of them is applied, consecutive adds go out as one BatchCreatePartition call.
TableFunction GetGlueAlterTableFunction();

//! hive_scan('s3://root', schema := {col: 'TYPE', ...}, partitions := [{key: value, ..., location: '...'}, ...],
//! partition_keys := [...]): read a parquet Hive table without a catalog. The same scan as for a Glue Hive table,
//! with the schema and the partitions (values and locations) given as arguments.
TableFunctionSet GetHiveScanFunction(DatabaseInstance &db);

} // namespace duckdb
