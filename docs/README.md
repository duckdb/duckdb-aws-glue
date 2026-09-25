# DuckDB Glue extension

Experimental extension that exposes an AWS Glue Data Catalog as a DuckDB catalog. It talks to Glue through the AWS
SDK Glue client and works with Hive (Glue native) tables stored as parquet on S3. Tables of other formats that Glue
registers (Iceberg, Delta, ...) are listed, with the columns Glue reports, but can not be read or written.

```sql
CREATE SECRET (TYPE S3, PROVIDER credential_chain, REGION 'eu-central-1');
ATTACH '<account_id>' AS my_datalake (TYPE GLUE);
SHOW ALL TABLES;
SELECT * FROM my_datalake.default.some_table;
```

Attach options:

| option             | description                                                                 |
|--------------------|-----------------------------------------------------------------------------|
| `SECRET`           | name of the s3/aws secret to take credentials from (default: default secret) |
| `REGION`           | AWS region of the catalog (default: region of the secret)                    |
| `ENDPOINT`         | Glue endpoint override, e.g. `http://localhost:5000` for a local moto server (default: AWS) |
| `DEFAULT_LOCATION` | optional S3 prefix for new databases and for tables created without an explicit location (takes precedence over the Glue database LocationUri) |
| `DEFAULT_SCHEMA`   | Glue database to use as the default schema                                   |
| `READ_ONLY`        | refuse everything that changes the catalog: DML, DDL and the mutating `glue_*` partition functions |

## Reading

Hive tables stored as parquet (ParquetHiveSerDe) are scanned with `read_parquet` through a custom
`MultiFileReader` (`HiveMultiFileReader`) with these read semantics:

- The data files are those below the location of every partition Glue lists (`GetPartitions`), at any depth, or
  below the table location for an unpartitioned table. Partition locations need not follow the `<key>=<value>`
  layout. Files and directories named `_*` or `.*` are skipped. When one partition's location lies inside another's,
  a file belongs to the deepest one. A table without data files (just created) scans as empty.
- Partition column values are the values Glue stores for the partition, not the directory names, typed as Glue's
  partition keys. Files are listed lazily: filters on partition columns are applied to the partition values first,
  so only the partitions a query reads are listed (EXPLAIN shows the partitions kept as `Scanning Files`), and
  planning a query does not touch S3. When a query reads at least `hive_partition_listing_threshold` (default
  10) partitions below the table location, the location is listed once, recursively (one S3 request per 1000
  keys), and the files are matched to their partitions by prefix; fewer partitions, and partitions at custom
  locations, are listed one directory each.
- The schema is Glue's, data columns first and partition keys last, in `PARTITIONED BY` order. Files are matched
  by column name: a column a file does not have (added after the file was written) reads as NULL, a column with
  a different type in the file is cast, and file columns Glue does not list are ignored.

The SerDe of the Glue table decides the reader: ParquetHiveSerDe reads with `read_parquet`, LazySimpleSerDe and
OpenCSVSerde with `read_csv` (columns by position, no header unless `skip.header.line.count` is 1, delimiter
from `field.delim` / `separatorChar`, `,` otherwise) and JsonSerDe with `read_json` (one object per line, keys by
name) and AvroSerDe with `read_avro` from the avro extension, which is loaded on demand. Other SerDes (ORC, Ion,
...) are not supported.

## Writing

- `CREATE SCHEMA` creates a Glue database with LocationUri `<DEFAULT_LOCATION>/<schema>`, or without a LocationUri
  when the catalog was attached without `DEFAULT_LOCATION`.
- `CREATE TABLE ... [PARTITIONED BY (col, ...)] [WITH (format = 'parquet' | 'csv' | 'json' | 'avro', location = '...',
  <property> = '...')]` creates a parquet (default), csv (LazySimpleSerDe, `,` delimited, no header), json
  (JsonSerDe, one object per line) or avro (AvroSerDe)
  Hive table at `location`, else `<DEFAULT_LOCATION>/<database>/<table>`, else `<database LocationUri>/<table>`;
  without any of these the statement fails. Partition keys must be plain column names; they become Glue
  PartitionKeys and are listed last in the table's columns. Unknown `WITH` keys are stored as Glue table parameters.
  For csv, `delimiter = '|'` sets the field delimiter (`field.delim`), `header = true` makes every file start with a
  header line (`skip.header.line.count`), and `quote = '"'` / `escape = '\'` switch the table to OpenCSVSerde with
  `separatorChar` / `quoteChar` / `escapeChar` (the escape character defaults to the quote character).
- `INSERT INTO` and `CREATE TABLE ... AS` write files in the table's format into the table location (one file per partition
  touched, partition columns are not stored in the files) and register new partition directories in Glue with
  BatchCreatePartition. New partitions get `<key>=<value>` directories; rows of an existing partition are written to
  its registered location, which may be any directory below the table location (e.g. `<table>/2024/01`). Inserting
  into a partition whose location is not below the table location fails; the rows of other partitions can still be
  inserted. Because the partition keys are the last columns of the table, `INSERT ... VALUES` without a
  column list must list them last. `CREATE TABLE ... AS` creates the Glue table before the query runs; if the query
  fails the (empty) table stays. Writes to bucketed (clustered) tables, i.e. tables with `BucketColumns`, are refused;
  they can be read.
- Parquet writes keep Hive's basic statistics, the `numRows`, `numFiles` and `totalSize` parameters Athena, Trino
  and Spark plan with, current: a new partition gets the statistics of its files, an existing partition with
  statistics (at the directory written to) gets them added with BatchUpdatePartition. For unpartitioned tables they
  are table parameters, which `CREATE TABLE` without `location` sets to `0` and every insert adds to. Tables and
  partitions without valid statistics (e.g. created elsewhere, added with `glue_add_partition`, or `-1`) are left
  alone, and moving a table or partition removes them. Two concurrent writers can lose an update, and files left
  behind by a dropped table at the same location are not counted. csv, json and avro writes record no statistics.
- `ALTER TABLE ... ADD COLUMN` (appended last, no defaults), `DROP COLUMN` (not the last data column, not a
  partition key) and `ALTER COLUMN ... TYPE` update the Glue definition with UpdateTable. Existing parquet files
  keep their types, so only widening type changes are allowed: integer widening (TINYINT to BIGINT), FLOAT to
  DOUBLE, and anything to VARCHAR; partition keys can not be retyped.
- `DROP TABLE` and `DROP SCHEMA` delete the Glue entries but leave the data files in S3. Glue deletes all tables of
  a database when the database is dropped.

Glue has no transactions: DDL takes effect immediately, files are visible as soon as they are written, and nothing
is rolled back on failure. `DELETE`, `UPDATE` and `MERGE INTO` are not supported.

## Reading without a catalog: hive_scan

The same scan is available as a table function, for parquet Hive tables that are not (or not yet) registered in
Glue. The schema and the partitions are given as arguments; nothing is looked up in a catalog.

```sql
SELECT * FROM hive_scan('s3://bucket/warehouse/orders',
    schema := {id: 'INTEGER', amount: 'DOUBLE', dt: 'VARCHAR', country: 'VARCHAR'},
    partitions := [
        {dt: '2016-05-14', country: 'IN', location: NULL},
        {dt: '2016-05-16', country: 'IN', location: 's3://bucket/imports/INDIA_16_May_2016'}
    ]);
```

- `schema` (required): a struct of column name to DuckDB type name, data columns and partition columns.
- `format`: `'parquet'` (default), `'csv'`, `'json'` or `'avro'`. For csv, `header := true`, `delim := '|'`,
  `quote := '"'` and `escape := '\'` describe the files; csv columns are matched by position, json keys by name.
- `partitions`: one struct per partition with a value for every partition key and an optional `location`; without
  a location the partition lives at `<root>/<key>=<value>/...`. The partition keys are the struct fields other than
  `location`, in that order, unless `partition_keys := [...]` names them. Without `partitions` the table is
  unpartitioned and all files below the root are read; `partition_keys` without `partitions` is an error, and
  `partitions := []` with `partition_keys` reads no rows.
- A root or partition without data files scans as zero rows.

## Partitions

DuckDB has no `ALTER TABLE ... PARTITION` syntax, so the Hive partition statements are table functions. The
partition is given as a struct naming every partition key; values are stored as strings in Glue, in partition key
order.

| function | Hive statement |
|----------|------------------|
| `glue_partitions('cat.db.t')` | `SHOW PARTITIONS`: one row per registered partition, a typed column per partition key plus `location` and `parameters` (e.g. `numRows`) |
| `CALL glue_add_partition('cat.db.t', {dt: '2016-05-14', country: 'IN'}, location := 's3://...', if_not_exists := false)` | `ALTER TABLE ADD [IF NOT EXISTS] PARTITION (...) [LOCATION ...]`; without `location` the partition lives at `<table location>/dt=2016-05-14/country=IN` |
| `CALL glue_drop_partition('cat.db.t', {dt: '2016-05-14', country: 'IN'}, if_exists := false)` | `ALTER TABLE DROP [IF EXISTS] PARTITION (...)`; the data files stay in S3 |
| `CALL glue_rename_partition('cat.db.t', {dt: '2016-05-14', country: 'IN'}, {dt: '2016-05-15', country: 'IN'})` | `ALTER TABLE PARTITION (...) RENAME TO PARTITION (...)`; changes the values, keeps the location |
| `CALL glue_set_partition_location('cat.db.t', {dt: '2016-05-14', country: 'IN'}, 's3://...')` | `ALTER TABLE PARTITION (...) SET LOCATION '...'` |
| `CALL glue_set_table_location('cat.db.t', 's3://...')` | `ALTER TABLE SET LOCATION '...'`; existing partitions keep their locations, new ones land under the new location |

The Hive SQL forms are available as well, through the `glue_hive_ddl` grammar extension the extension registers.
Grammar extensions are switched on per connection:

```sql
SET active_grammar_extensions = ['glue_hive_ddl'];
ALTER TABLE my_datalake.default.orders ADD IF NOT EXISTS
    PARTITION (dt = '2016-05-14', country = 'IN')
    PARTITION (dt = '2016-06-02', country = 'IN') LOCATION 's3://bucket/imports/INDIA_02_June_2016/';
ALTER TABLE my_datalake.default.orders DROP IF EXISTS PARTITION (dt = '2016-05-14', country = 'IN'), PARTITION (dt = '2016-05-15', country = 'IN');
ALTER TABLE my_datalake.default.orders PARTITION (dt = '2016-05-15', country = 'IN') RENAME TO PARTITION (dt = '2016-05-16', country = 'IN');
ALTER TABLE my_datalake.default.orders PARTITION (dt = '2016-05-16', country = 'IN') SET LOCATION 's3://bucket/other/';
ALTER TABLE my_datalake.default.orders SET LOCATION 's3://bucket/orders_v2/';
```

Actions can be chained in one statement (`ADD PARTITION (...) LOCATION '...' ADD PARTITION (...) ...`). The statement
becomes `CALL glue_alter_table(table, [actions])`: every action is checked against Glue before any is applied, so a
statement that fails changes nothing, and consecutive adds go out as one `BatchCreatePartition` call. The table name
may be partially qualified; it is resolved like in a query.

Listing the partitions of a table - `glue_partitions`, and the binding of every scan of a partitioned table - pages
through Glue's `GetPartitions`. The pages are asked for in parallel with Glue's Segment API:
`glue_get_partitions_segments` requests run at the same time, each over a segment of the partitions that does not
overlap with the others. `0`, the default, uses 8 requests against AWS and 1 against a Glue compatible server given
with `ENDPOINT` (moto ignores `Segment` and answers every segment with the whole table, so the partitions a segment
already returned are dropped); 10 is the maximum Glue accepts. The responses leave out the column schema every
partition would otherwise repeat, which is not used: only the partition values and the location are.

Against AWS, listing the 2526 partitions of a TPC-H SF1 `lineitem` took ~2.1s before and ~0.45s with the default of
8 segments; `SET glue_get_partitions_segments = 1` restores one request at a time.

## Inspecting tables

Every table entry carries its format in `duckdb_tables().tags['table_type']` (`HIVE`, `ICEBERG`, `DELTA` or
`UNKNOWN`). The full Glue definition of a table is available through a table function:

```sql
SELECT * FROM glue_get_table_response('my_datalake.default.some_table');
SELECT response.StorageDescriptor.Location FROM glue_get_table_response('my_datalake.default.some_table');
```

It returns one row with the classification, the Glue table type, location, SerDe, columns, partition keys and
parameters as columns, plus the complete Glue `Table` object as a VARIANT in `response`.

## HTTP transport and logging

The AWS SDK's Glue calls are routed through DuckDB's HTTP layer (httpfs), so they honor DuckDB's proxy and
certificate settings and appear in the HTTP log:

```sql
CALL enable_logging('HTTP', storage='memory');
-- ... run queries ...
SELECT request.type, request.url, request.headers['x-amz-target'], response.status FROM duckdb_logs_parsed('HTTP');
```

`SET glue_network_calls_via_duckdb = false` switches back to the SDK's own HTTP client.

## Testing

The tests are written against two `--test-config` files, which decide where the catalog and the storage are:

- `test/configs/local_glue.json`: [moto](https://github.com/getmoto/moto) serving the Glue API and
  [SeaweedFS](https://github.com/seaweedfs/seaweedfs) serving S3, both from `scripts/docker-compose.yml`, which also
  creates the bucket, the Glue database `default` and the bucketed tables `default.fixture_bucketed` and
  `default.fixture_bucketed_multi` (the extension can not create those).
- `test/configs/cloud_glue.json`: a live AWS Glue Data Catalog, with credentials from the AWS credential chain.

A config creates the S3 secret (`on_init`) and sets `GLUE_CATALOG_ID`, `GLUE_ENDPOINT` and `DEFAULT_S3_LOCATION`,
which the tests use in their ATTACH; tests are skipped without a config (`require-env GLUE_CATALOG_ID`). Tests under
`test/sql/cloud/` read tables of the live account that the tests do not create and only run with the cloud config.

```sh
make glue-fixture        # docker compose up (creates the bucket and the 'default' database)
make test-local          # unittest --test-config test/configs/local_glue.json 'test/sql/*'
make glue-fixture-down

AWS_PROFILE=... AWS_CONFIG_FILE=~/.aws/config AWS_SHARED_CREDENTIALS_FILE=~/.aws/credentials make test-cloud
```

Both targets set `AWS_EC2_METADATA_DISABLED=true`: the test runner hides `~/.aws`, and without a region from the
environment or a profile the AWS SDK asks the EC2 instance metadata service for one, which off EC2 hangs for
minutes per client. A test config can not export process environment variables, so this stays on the command.

Every test creates the tables it needs and writes under its own `{TEST_DIR}` prefix, so runs do not interfere with
each other; `make glue-fixture-down` throws the containers and their data away.

The benchmarks under `benchmark/` read from the same local servers. They build their tables in the `load` step and
use `debug_fs_delay_mean_ms` to add latency to every file open and read, standing in for the S3 round trip the local
SeaweedFS does not have (`make glue-fixture` first; the benchmark runner needs a build with `BUILD_BENCHMARK=1`):

```sh
AWS_EC2_METADATA_DISABLED=true ./build/relassert/benchmark/benchmark_runner benchmark/heavily_partitioned_table.benchmark
```

`benchmark/tpch/sf1/` runs the 22 TPC-H queries at SF1 against Hive tables in the Glue database `bench_tpch_sf1`
(`lineitem` partitioned by `l_shipdate`, `orders` by `o_orderdate`) and checks the answers. The first run generates
the data with `dbgen` and writes it with CTAS, which takes a while; later runs reuse
`duckdb_benchmark_data/glue_tpch_sf1.duckdb`, which `make glue-fixture` removes:

```sh
AWS_EC2_METADATA_DISABLED=true ./build/relassert/benchmark/benchmark_runner 'benchmark/tpch/sf1/.*'
```

`benchmark/tpcds/sf1/` does the same for the 99 TPC-DS queries at SF1, with the Glue database `bench_tpcds_sf1`: the
fact tables are partitioned by their date key (`ss_sold_date_sk`, `sr_returned_date_sk`, `cs_sold_date_sk`,
`cr_returned_date_sk`, `ws_sold_date_sk`, `wr_returned_date_sk`, `inv_date_sk`), the dimension tables are not. Its
cache is `duckdb_benchmark_data/glue_tpcds_sf1.duckdb`:

```sh
AWS_EC2_METADATA_DISABLED=true ./build/release/benchmark/benchmark_runner 'benchmark/tpcds/sf1/.*'
```

The TPC-DS load needs a build without assertions (`BUILD_BENCHMARK=1 make release`): its fact tables have rows with a
NULL date key, and writing such a partition trips a `D_ASSERT` in DuckDB's partitioned copy (see the header of
`benchmark/tpcds/sf1/tpcds_sf1.benchmark.in` for a repro without Glue).

Both loads create their Glue tables with `CREATE TABLE IF NOT EXISTS ... AS`, so changing a load (e.g. the scale
factor) has no effect while the tables are in Glue: rebuild the fixture with
`make glue-fixture-down && make glue-fixture` first.

`.github/workflows/Regression.yml` runs them for a PR and for its merge base and compares the timings.

## Building

```sh
VCPKG_TOOLCHAIN_PATH='<path to vcpkg>/scripts/buildsystems/vcpkg.cmake' GEN=ninja make relassert
ASAN_OPTIONS=detect_container_overflow=0 ./build/relassert/test/unittest test/sql/test_glue_attach.test
```
