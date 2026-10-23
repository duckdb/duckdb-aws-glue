# DuckDB Glue extension

**Experimental** extension that exposes an AWS Glue Data Catalog as a DuckDB catalog. It talks to Glue through the AWS
SDK Glue client and works with Hive (Glue native) tables stored as parquet, csv, json or avro on S3. Tables of other
formats that Glue registers (Iceberg, Delta, ...) are listed, with the columns Glue reports, but can not be read or
written.

Use DDL/DML statements in this extension in production at your own risk. There is no guarantee that all incompatibilities between DuckDB and Trino/Spark engines writing to hive tables in glue have been discovered.

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

A table's format is read from its parameters (`table_type`, `spark.sql.sources.provider`, `metadata_location`) and
otherwise from its InputFormat: Hudi tables (`HoodieParquetInputFormat`, ...) and symlink manifest tables
(`SymlinkTextInputFormat`, e.g. a Delta `_symlink_format_manifest`) register a Parquet or text SerDe, so only the
InputFormat tells them apart from a Hive table. They are listed but not read or written, like Iceberg and Delta.

The SerDe of the Glue table decides the reader: ParquetHiveSerDe reads with `read_parquet` (columns by name),
LazySimpleSerDe and OpenCSVSerde with `read_csv` (columns by position), the Hive and the OpenX JsonSerDe with
`read_json` (one object per line, keys by name; SerDe properties see below) and AvroSerDe with `read_avro`
(columns by name) from the avro extension, which is loaded on demand. Other SerDes (ORC, Ion, ...) are not supported.
A parquet `BYTE_ARRAY` column without a `STRING` annotation (as Impala and older Hive writers produce) is read as
UTF-8 text when Glue declares the column `string` (bytes that are not UTF-8 are an error), and as bytes when it
declares it `binary`, whatever the session's `binary_as_string`. In a table that declares a `binary` column (or field),
such bytes inside an `array`, `struct` or `map` column keep the escaped form the reader gives a BLOB.
Text tables are read the way their SerDe reads them:

- LazySimpleSerDe: the delimiter is `field.delim`, else `serialization.format`, else `\001`; a number from -128 to 127
  is a byte code (`'1'` is `\001`, `'9'` a tab). NULL is `serialization.null.format`, `\N` by default. Fields are not
  quoted.
- OpenCSVSerde: `separatorChar` (`,`), `quoteChar` (`"`) and `escapeChar` (the quote character, which reads the
  doubled quotes Hive writes). An empty field is an empty string; there is no NULL. A line break inside a quoted field
  fails a scan of at most twice `threads` files, which reads each file with several threads (`The parallel scanner
  does not support null_padding in conjunction with quoted new lines`); with more files DuckDB reads one thread per
  file and the field is read as one value, as CSV defines it. `SET hive_csv_parallel = false` reads one thread per
  file in every scan. Hive and Athena v3 split the record at the line break first (TextInputFormat) and return two
  broken rows, a difference this extension does not reproduce.
- Short rows are padded with NULL, extra fields are ignored, and a value that does not parse as its column type is
  NULL, except in OpenCSVSerde tables, where it fails the query unless `use.null.for.invalid.data` is `true`. Fields
  are converted with `TRY_CAST` (`CAST` in OpenCSVSerde tables without the property), which differs from Hive in
  details: surrounding spaces are ignored, `'1.5'` in an `int` column is 2 (Hive: 1), and blank lines are skipped
  (Hive: a row of NULLs).
- `skip.header.line.count` lines are skipped at the start of every file.
- OpenX JsonSerDe: with `ignore.malformed.json` or `use.null.for.invalid.data` set to `true` the files are read with
  `read_json(ignore_errors := true)`: a line that is not valid JSON is a row of NULLs and a value that does not fit
  its column is NULL; either property enables both. Other JsonSerDe properties that change how the files are read
  (OpenX `mapping.*`, `dots.in.keys` and `case.insensitive=true`, Hive `timestamp.formats`) are not applied: the read
  succeeds and a warning is logged (`CALL enable_logging(level := 'warning')`).
- JSON keys are matched to the columns exactly, as with OpenX `case.insensitive=false`; both SerDes match them
  ignoring case by default, so a key `userId` does not fill the column `userid`.
- Writes produce files Hive reads back unchanged: LazySimpleSerDe values are unquoted and refused if they hold the
  delimiter or a line break, or equal the NULL string; OpenCSVSerde values are all quoted and refused if they hold a
  line break.
- Not supported, for reads and writes: footer lines (`skip.footer.line.count`), header lines in JsonSerDe tables,
  nested columns, multi-byte quote or escape characters, and for LazySimpleSerDe `escape.delim`, a non-UTF-8
  `serialization.encoding`, `serialization.last.column.takes.rest`, NUL or line-break delimiters and a delimiter in the
  NULL string. Writes need a single-byte delimiter.
- As in Hive, a table property overrides the SerDe property of the same name and keys are case-sensitive. Line counts
  and byte codes are DuckDB integer casts (`'1.5'` is 2). Partitions are read with the table's SerDe properties, not
  their own. `hive_scan()` uses plain `read_csv` rules.

Every format is scanned through a custom `MultiFileReader` (`HiveMultiFileReader`) with these read semantics:

- The data files are those below the location of every partition Glue lists (`GetPartitions`), at any depth, or
  below the table location for an unpartitioned table. Partition locations need not follow the `<key>=<value>`
  layout. Files and directories named `_*` or `.*`, `*_$folder$` directory markers and objects the listing reports as
  zero bytes are skipped.
  When one partition's location lies inside another's,
  a file belongs to the deepest one. A table without data files (just created) scans as empty.
- A partitioned table with Athena partition projection (`projection.enabled = true`) is refused on read and write
  rather than scanned: Athena derives its partitions from the `projection.*` parameters and ignores the registered
  ones, which such a table usually has none of, so a scan would silently read nothing. To read it, set
  `projection.enabled` to `false` and register its partitions (`ALTER TABLE ... ADD PARTITION`, `glue_add_partition`).
- Partition column values are the values Glue stores for the partition, not the directory names, typed as Glue's
  partition keys. Files are listed lazily: filters on partition columns, including those a join derives from its
  build side when the scan starts, are applied to the partition values first, so only the partitions a query reads
  are listed (EXPLAIN shows the partitions kept as `Scanning Files`). A filter the partition values decide is
  true of every row the scan then reads, and is dropped from the plan, as for `read_parquet` with hive
  partitioning. When a query reads at least
  `hive_partition_listing_threshold` (default 10) partitions below the table location, the location is listed once,
  recursively (one S3 request per 1000 keys), and the files are matched to their partitions by prefix; fewer
  partitions, and partitions at custom locations, are listed one directory each. A directory is listed once per
  query, shared by every scan of it and the cardinality sample. With `SET glue_cache_listings = true` (default
  false) the listing is kept for later statements of the connection as well, held and cleared like the metadata
  cache (`glue_metadata_cache_scope`, writes through the extension, rollback, DETACH) — so files added any other way
  (another writer, `COPY TO` the table location) appear only after the next clear. Files removed any other way
  (compaction, an overwrite, dropping and recreating the table at the same location) are worse than unseen: scans
  under the cached listing fail trying to open the dead paths, or read the superseded files, until the next clear.
  It suits hosts that run one query per connection.
- To estimate a scan's row count, planning lists one directory per table and query (the first partition a scan
  of the table reads, or the location of an unpartitioned table; when the scan lists the table location, the
  first page of that listing, which the scan then continues) and reads the row count of its largest file: the
  parquet footer, or the lines of a 64 KiB prefix for csv and json. Every scan of the table in the query scales
  that one measurement by the files and bytes of the partitions it reads: those the listing holds completely
  count with their own size, the others with the average of those. A scan that lists the same directory reuses
  the listing. Avro tables are not measured, so planning them lists nothing.
- The Glue table definition and the partition list are fetched once per query and table, however often the
  query scans the table.
- The schema is Glue's, data columns first and partition keys last, in `PARTITIONED BY` order. Files are matched
  by column name: a column a file does not have (added after the file was written) reads as NULL, a column with
  a different type in the file is cast, and file columns Glue does not list are ignored.
- Besides `filename`, the scan has three of the metadata columns of Trino's Hive connector, which `SELECT *` leaves out:
  `"$path"` (the data file, as `filename`), `"$file_size"` (`BIGINT`) and `"$file_modified_time"`
  (`TIMESTAMP WITH TIME ZONE`), as the listing reports them. `hive_scan()` has them too.

The SerDe of the Glue table decides the reader: ParquetHiveSerDe reads with `read_parquet`, LazySimpleSerDe and
OpenCSVSerde with `read_csv` (columns by position, no header unless `skip.header.line.count` is 1, delimiter
from `field.delim` / `separatorChar`, `,` otherwise) and JsonSerDe with `read_json` (one object per line, keys by
name) and AvroSerDe with `read_avro` from the avro extension, which is loaded on demand. Other SerDes (ORC, Ion,
...) are not supported.

Compression: every csv and json file is read with the codec DuckDB tells from its name (`.gz`, `.zst`), whatever the
table records, so a table whose files use more than one codec is read correctly and a stale `write.compression` or
`compressionType` does not matter. The codec a table records is what writes to it use. Parquet and avro files carry
their codec themselves.

## Writing

- `CREATE SCHEMA [IF NOT EXISTS] ... [WITH (comment = '...', location = '...', <property> = '...')]` creates a Glue
  database. `comment` is its Description, `location` its LocationUri (default `<DEFAULT_LOCATION>/<schema>`, or none
  when the catalog was attached without `DEFAULT_LOCATION`), and any other key a database parameter (Hive's
  `DBPROPERTIES`). A `DEFAULT_LOCATION` on ATTACH still decides where new tables go, over the database's LocationUri.
- `ALTER SCHEMA ... SET (<key> = '...', ...)` merges options into the Glue database (UpdateDatabase), with the same
  keys as `CREATE SCHEMA`; `ALTER SCHEMA ... RESET (<key>, ...)` removes them. Keys are case-insensitive.
- `CREATE TABLE ... [PARTITIONED BY (col, ...)] [WITH (format = 'parquet' | 'csv' | 'json' | 'avro', location = '...',
  <property> = '...')]` creates a parquet (default), csv (LazySimpleSerDe, `,` delimited, no header), json
  (JsonSerDe, one object per line) or avro (AvroSerDe)
  Hive table at `location`, else `<DEFAULT_LOCATION>/<database>/<table>`, else `<database LocationUri>/<table>`;
  without any of these the statement fails. Partition keys must be plain column names; they become Glue
  PartitionKeys and are listed last in the table's columns. Generated columns, column defaults and collated columns
  (`COLLATE`, also from a `CREATE TABLE ... AS` query) are refused. Unknown `WITH` keys are stored as Glue table
  parameters. Column types are stored as Hive types; DuckDB types without one are refused, e.g. `UBIGINT`, `HUGEINT` and
  `TIMESTAMP_NS`/`_MS`/`_S` (Hive's `timestamp` is `TIMESTAMP`, in microseconds).
  For csv, `delimiter = '|'` sets the field delimiter (`field.delim`), `header = true` makes every file start with a
  header line (`skip.header.line.count`), and `quote = '"'` / `escape = '\'` switch the table to OpenCSVSerde with
  `separatorChar` / `quoteChar` / `escapeChar` (the escape character defaults to the quote character).
  With `SET glue_create_bucketed_tables = true`, `BucketColumns = ['col', ...]`, `NumberOfBuckets = n` and
  `SortColumns = [{'Column': 'col', 'SortOrder': 1}, ...]` (1 ascending, 0 descending) create a bucketed (clustered)
  table, Hive's `CLUSTERED BY (...) SORTED BY (...) INTO n BUCKETS`: bucket and sort columns are columns of the table
  that are not partition keys, and `BucketColumns` needs a positive `NumberOfBuckets`. The setting is off by default
  because DuckDB does not write to such a table (see below).
- `INSERT INTO` and `CREATE TABLE ... AS` write files in the table's format into the table location (one file per partition touched,
  partition columns are not stored in the files; text files use the table's delimiter and NULL
  string, `\N` for LazySimpleSerDe unless `serialization.null.format` says otherwise, and at most one header line) and register new partition directories in Glue with
  BatchCreatePartition. New partitions get `<key>=<value>` directories; rows of an existing partition are written to
  its registered location, which may be any directory below the table location (e.g. `<table>/2024/01`). Inserting
  into a partition whose location is not below the table location fails; the rows of other partitions can still be
  inserted. Glue errors on single partitions that may pass (throttling, internal errors) are retried; when Glue
  still refuses a partition, the write is rolled back by deleting the files it wrote (the partitions it registered
  stay, empty), so the statement can be run again without adding its
  rows twice. Because the partition keys are the last columns of the table, `INSERT ... VALUES` without a
  column list must list them last. `CREATE TABLE ... AS` creates the Glue table when the statement starts executing,
  before the query runs (planning it, e.g. with `EXPLAIN` or `PREPARE`, creates nothing); if the query fails the
  (empty) table stays. Writes to bucketed (clustered) tables, i.e. tables with `BucketColumns`, are refused;
  they can be read. `CREATE TABLE ... AS` with the bucketing options is refused before the table is created. Like
  Athena, JSON tables are written only with the Hive JsonSerDe (`org.apache.hive.hcatalog.data.JsonSerDe`, what
  `CREATE TABLE ... WITH (format = 'json')` creates): `INSERT` into a table with the OpenX JSON SerDe
  (`org.openx.data.jsonserde.JsonSerDe`) is refused, it can be read. `INSERT` also requires the table's InputFormat to
  be one Hive uses for its file format (`MapredParquetInputFormat`, `TextInputFormat` or `CombineTextInputFormat` for
  csv and json, `AvroContainerInputFormat`) or empty: another engine's InputFormat means a layout DuckDB does not
  know, and files written into it could corrupt the table.
- `ALTER TABLE ... ADD COLUMN` (appended last, no defaults or collations), `DROP COLUMN` (not the last data
  column, not a partition key, bucket or sort column) and `ALTER COLUMN ... TYPE` (no collations) update the Glue
  definition with UpdateTable.
  Existing data files keep their types, so only widening type changes are allowed: integer widening (TINYINT to
  BIGINT), FLOAT to DOUBLE, and anything to VARCHAR; partition keys can not be retyped. `ALTER COLUMN ... TYPE ...
  USING <expr>` is refused, since the data files can not be rewritten.
- Written files are compressed the way the table says: parquet with `parquet.compression` (and `compression_level`
  for zstd), csv and json with the codec the table records (gzip or zstd), named `.csv.gz` / `.json.zst`, and avro
  with `avro.output.codec`. A table without its format's codec parameter is written with `write_compression`, if it
  records one. Another codec is an error.
- `ALTER TABLE ... RENAME COLUMN` is supported for csv tables, whose files are read by position. Parquet, json and
  avro files are matched by name, so a rename would make the existing data read as NULL and is refused (replace the
  column with `glue_replace_columns` to do that deliberately). Partition keys, bucket and sort columns can not be
  renamed.
- `COMMENT ON COLUMN` sets the Glue column comment (`IS NULL` removes it); the comments of Glue columns show in
  `duckdb_columns()`. Comments on partition keys and on views are not supported yet.
- `ALTER TABLE ... SET (key = 'value', ...)` and `RESET (key, ...)` change the Glue table parameters (Hive's
  `TBLPROPERTIES`) with UpdateTable: `SET` adds or overwrites the listed keys, `RESET` removes them, and every other
  parameter and the rest of the definition stay as they are. Values are stored as strings (`compression_level = 4`
  becomes `'4'`); a key may be quoted (`'parquet.compression' = 'ZSTD'`). The parameters the table format is read
  from (`table_type`, `spark.sql.sources.provider`, `metadata_location`) can not be changed this way.
- `CALL glue_replace_columns('cat.db.t', {id: 'BIGINT', name: 'VARCHAR'}, comments := {id: '...'})` is Hive's
  `ALTER TABLE ... REPLACE COLUMNS`: it replaces all data columns of the table at once, which can also rename and
  reorder them. Types are DuckDB types, stored the way `CREATE TABLE` stores them; a column that stays (same name,
  compared case-insensitively) keeps its stored name and may only be widened, as with `ALTER COLUMN ... TYPE`. As in
  Hive, comments not given in `comments` are dropped; `keep_comments := true` keeps those of the columns that stay
  (a NULL in `comments` then removes one). The partition keys are kept
  and must not be listed; the bucketing and sort columns must be listed. It returns the columns as stored in Glue
  (Glue type names). The data files are not rewritten: parquet, json and avro files are matched by name (a renamed
  column reads as NULL), csv files by position, so a csv table keeps its number of columns, each may only be widened
  and giving it a new name renames it.
- `DROP TABLE` and `DROP SCHEMA` delete the Glue entries but leave the data files in S3. Glue deletes all tables of
  a database when the database is dropped, so `DROP SCHEMA` refuses a database that still has tables or views unless
  `CASCADE` is given.

Glue has no transactions: DDL takes effect immediately, files are visible as soon as they are written, and nothing
is rolled back on failure. `DELETE`, `UPDATE` and `MERGE INTO` are not supported. DDL (`CREATE`/`ALTER`/`DROP`
of schemas, tables and views, `CREATE TABLE ... AS`, the partition SQL, or `glue_add_partition`, `glue_alter_table`,
...) and `INSERT` inside an explicit `BEGIN` transaction are an error that aborts the transaction; end it with
`COMMIT`, `ABORT` or `ROLLBACK` and run the statement again.

Within a DuckDB transaction
(`BEGIN` … `COMMIT`) a table's definition and its list of partitions are fetched from Glue once and reused; the files
inside those partitions are listed at each scan. So a partition added outside the transaction becomes visible only after the transaction ends, while
a file added to an existing partition is seen by the next query (the scans of one query share a listing) — or, under
`glue_cache_listings`, after the next cache clear.

With `SET glue_metadata_cache_scope = 'connection'` the cache also serves the connection's later transactions. It is
per connection — two connections never share one — so another connection's write stays unseen until something clears
this connection's cache: a write through it, a rollback, or DETACH.

## Views

- `CREATE [OR REPLACE] VIEW [IF NOT EXISTS] db.v [(col, ...)] AS SELECT ...`, `CREATE SECURE VIEW` and
  `CREATE VIEW ... WITH (DEFER_BINDING)` create a Glue view (a table of type `VIRTUAL_VIEW`) marked as written by DuckDB.
  `OR REPLACE` updates it in place. Unqualified table names in the SELECT refer to the view's own database. The view's
  columns store their DuckDB types (e.g. `HUGEINT`, `INTEGER`), not Glue types.
- A column in the SELECT can not start with the name of another catalog the view reads (`cat.db.t.col`,
  `* EXCLUDE (cat.db.t.col)`), or with its own catalog's name as a row reference (`cat.t`), even when that name is a
  table, alias or column: use a table alias with another name.
- With `SET experimental_glue_view_enable_external_catalogs = true`, a view can also read tables of other kinds of
  catalogs (e.g. Iceberg, Postgres, a DuckDB file). Such a table is stored as `"external:<type>:<alias>".<schema>.<table>`,
  with the alias the catalog was attached under, so the view is tied to that alias: it reads wherever a catalog of that
  type is attached under that alias, whichever catalog that is, and fails where none is. Reading needs no setting.
  Catalogs in memory are refused.
- `SHOW [ALL] TABLES`, `duckdb_views()`, `information_schema.views` and `duckdb_columns()` list every view of a database,
  including views written by other engines. Only views written by DuckDB can be queried, replaced or dropped.
- `DROP VIEW [IF EXISTS] db.v` deletes the view.
- `ALTER VIEW ... RENAME` and `COMMENT ON VIEW` are not supported.

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
  `quote := '"'`, `escape := '\'` and `nullstr := '\N'` describe the files (an empty field is NULL unless `nullstr`
  says otherwise, as in `read_csv`); csv columns are matched by position, json keys by name. For json,
  `ignore_errors := true` reads a line that is not valid JSON as a row of NULLs.
- `partitions`: one struct per partition with a value for every partition key and an optional `location`; without
  a location the partition lives at `<root>/<key>=<value>/...`. The partition keys are the struct fields other than
  `location`, in that order, unless `partition_keys := [...]` names them. Without `partitions` the table is
  unpartitioned and all files below the root are read; `partition_keys` without `partitions` is an error, and
  `partitions := []` with `partition_keys` reads no rows.
- A root or partition without data files scans as zero rows.

## Partitions

DuckDB has no `ALTER TABLE ... PARTITION` syntax, so the Hive partition statements are table functions. The
partition is given as a struct naming every partition key; values are stored as strings in Glue, in partition key
order. The table name may be partially qualified (`'db.t'`, or `'t'` after `USE cat.db`); it is resolved like in a
query.

| function | Hive statement |
|----------|------------------|
| `glue_partitions('cat.db.t')` | `SHOW PARTITIONS`: one row per registered partition, a typed column per partition key plus `location` |
| `glue_get_partition_response('cat.db.t', {dt: '2016-05-14', country: 'IN'})` | `DESCRIBE FORMATTED ... PARTITION (...)`: the partition's location, SerDe, columns and parameters, and its complete Glue definition in `response` |
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

Listing the partitions of a table - `glue_partitions`, and planning a scan of a partitioned table - pages through
Glue's `GetPartitions`. A scan fetches them when planning first needs them (pruning, cardinality), not at bind, so
`DESCRIBE`, `CREATE VIEW` and `PREPARE` make no `GetPartitions` call; every scan of the table in a query shares one
fetch. The pages are asked for in parallel with Glue's Segment API:
`glue_get_partitions_segments` requests run at the same time, each over a segment of the partitions that does not
overlap with the others. `0`, the default, uses 8 requests against AWS and 1 against a Glue compatible server given
with `ENDPOINT` (moto ignores `Segment` and answers every segment with the whole table, so the partitions a segment
already returned are dropped); 10 is the maximum Glue accepts. The responses leave out the column schema every
partition would otherwise repeat, which is not used: only the partition values and the location are.

Against AWS, listing the 2526 partitions of a TPC-H SF1 `lineitem` took ~2.1s before and ~0.45s with the default of
8 segments; `SET glue_get_partitions_segments = 1` restores one request at a time.

## Inspecting tables

Every table entry carries its format in `duckdb_tables().tags['table_type']` (`HIVE`, `ICEBERG`, `DELTA`, `HUDI`,
`SYMLINK` or `UNKNOWN`). The full Glue definition of a table is available through a table function:

```sql
SELECT * FROM glue_get_table_response('my_datalake.default.some_table');
SELECT response.StorageDescriptor.Location FROM glue_get_table_response('my_datalake.default.some_table');
```

It returns one row with the classification, the Glue table type, location, SerDe, columns, partition keys and
parameters as columns, plus the complete Glue `Table` object as a VARIANT in `response`. A partially qualified name
(`'default.some_table'`, or `'some_table'` after `USE my_datalake.default`) is resolved like in a query, through the
table's catalog entry, so for a table DuckDB can not read (e.g. an unsupported column type) give the fully qualified
name.

`glue_get_partition_response('<table>', {dt: '2024-01-01', ...})` does the same for one partition of a Hive table:
its values, location, SerDe, columns and parameters as columns, plus the complete Glue `Partition` object in
`response`. The partition is given like for the other partition functions, as a struct naming every partition key.

```sql
SELECT location, response.StorageDescriptor.InputFormat
FROM glue_get_partition_response('my_datalake.default.some_table', {dt: '2024-01-01'});
```

`glue_get_database_response('<catalog>.<database>')` does the same for a Glue database (a DuckDB schema): its
description, location and parameters as columns and the complete Glue `Database` object in `response`. An unqualified
`'<database>'` is resolved like in a query, through the search path.

## HTTP transport and logging

The AWS SDK's Glue calls are routed through DuckDB's HTTP layer (httpfs), so they honor DuckDB's proxy and
certificate settings and appear in the HTTP log:

```sql
SET redact_http_logs = false; -- header values (incl. x-amz-target) are redacted by default
CALL enable_logging('HTTP', storage='memory');
-- ... run queries ...
SELECT request.type, request.url, request.headers['x-amz-target'], response.status FROM duckdb_logs_parsed('HTTP');
```

`SET glue_network_calls_via_duckdb = false` switches back to the SDK's own HTTP client.

## Testing

The tests are written against two `--test-config` files, which decide where the catalog and the storage are:

- `test/configs/local_glue.json`: [moto](https://github.com/getmoto/moto) serving the Glue API and
  [SeaweedFS](https://github.com/seaweedfs/seaweedfs) serving S3, both from `scripts/docker-compose.yml`, which also
  creates the bucket, the Glue database `default` and the bucketed table `default.fixture_bucketed_multi` (bucket
  columns without a NumberOfBuckets, which the extension does not create).
- `test/configs/cloud_glue.json`: a live AWS Glue Data Catalog, with credentials from the AWS credential chain.

A config creates the S3 secret (`on_init`) and sets `GLUE_CATALOG_ID`, `GLUE_ENDPOINT` and `DEFAULT_S3_LOCATION`,
which the tests use in their ATTACH; tests are skipped without a config (`require-env GLUE_CATALOG_ID`). Tests under
`test/sql/cloud/` read tables of the live account that the tests do not create and only run with the cloud config.
The local config also sets `GLUE_FAULT_ENDPOINT`: `scripts/glue_fault_proxy.py` in front of moto, which answers
GetTables on a database named `fault_*` with a Glue error; tests that need it are skipped without it.

```sh
make glue-fixture        # docker compose up (creates the bucket and the 'default' database)
make test-local          # unittest --test-config test/configs/local_glue.json 'test/sql/*', with retries
make glue-fixture-down

AWS_PROFILE=... AWS_CONFIG_FILE=~/.aws/config AWS_SHARED_CREDENTIALS_FILE=~/.aws/credentials make test-cloud
```

Both targets set `AWS_EC2_METADATA_DISABLED=true`: the test runner hides `~/.aws`, and without a region from the
environment or a profile the AWS SDK asks the EC2 instance metadata service for one, which off EC2 hangs for
minutes per client. A test config can not export process environment variables, so this stays on the command.

`make test-local` runs the tests through DuckDB's `duckdb/scripts/ci/run_tests.py` (Python 3.10+; pick the
interpreter with `PYTHON=python3.14`), one at a time in batches of `TEST_BATCH_SIZE` (10) tests per process, and reruns
a failing batch up to twice: against the local servers a read right after a write occasionally comes back with no rows.
Every retry is reported in the output. `TEST_BUILD=release` runs the `release` build instead of `relassert`. Every
process first installs the loadable extensions from `build/<type>/repository`; on Linux the debug info of `relassert`
makes them ~1.5 GB each, so CI tests a `make release EXT_RELEASE_FLAGS=-DFORCE_ASSERT=1` build (assertions and
sanitizers, no debug info).

A maintainer can run the whole suite of a pull request or a branch, on a release build, against a live catalog with
the `Cloud Glue tests` workflow (`.github/workflows/CloudGlueTests.yml`,
`gh workflow run CloudGlueTests.yml -f pr_number=<n>`, or `gh workflow run CloudGlueTests.yml --ref main` without a
PR). It is never triggered by a PR, refuses to run for anyone below the `maintain` role, tests the PR's head commit
(or the branch's) as it was when the run started, and reports the result as a `Cloud Glue tests` status on that
commit. The catalog, region and AWS credentials come from the variables and secrets of the `cloud-glue` environment,
listed at the top of the workflow. The tests write to the bucket `duckdb-glue-ci-<account id>-<region>`, which the
workflow creates if needed, under `duckdb-aws-glue-cloud-tests/ci-<run id>-<attempt>/`, which it deletes when the job
ends, whether the tests pass or not; a lifecycle rule on `duckdb-aws-glue-cloud-tests/` expires anything a failed run
leaves behind after 2 days.

Every test creates the tables it needs and writes under its own `{TEST_DIR}` prefix, so runs do not interfere with
each other; `make glue-fixture-down` throws the containers and their data away.

The benchmarks under `benchmark/` read from the same local servers. They build their tables in the `load` step and
use `debug_fs_delay_mean_ms` to add latency to every file open and read, standing in for the S3 round trip the local
SeaweedFS does not have (`make glue-fixture` first; the benchmark runner needs a build with `BUILD_BENCHMARK=1`):

```sh
AWS_EC2_METADATA_DISABLED=true ./build/relassert/benchmark/benchmark_runner benchmark/heavily_partitioned_table.benchmark
```

`benchmark/tpch/<sf>/<format>/` runs the 22 TPC-H queries against Hive tables of that format in the Glue database
`bench_tpch_<sf>_<format>` (`lineitem` and `orders` partitioned by 10-day buckets of `l_shipdate` and `o_orderdate`)
and checks the answers: `sf1/parquet/` at SF1, `sf0.1/csv/`, `sf0.1/json/` and `sf0.1/avro/` at SF0.1 (the Glue
database names spell the scale factor `sf1`, `sf0_1`, ...). The queries in `benchmark/tpch/queries/` are DuckDB's with
filters on the bucket columns added next to the date filters. The first run generates the data with `dbgen` and
writes it with CTAS, which takes a while; later runs reuse `duckdb_benchmark_data/glue_tpch_<sf>_<format>.duckdb`,
which `make glue-fixture` removes:

```sh
AWS_EC2_METADATA_DISABLED=true ./build/relassert/benchmark/benchmark_runner 'benchmark/tpch/sf1/parquet/.*'
```

`benchmark/tpcds/<sf>/<format>/` does the same for the 99 TPC-DS queries, `sf1/parquet/` at SF1 and the other formats
at SF0.01 (`sf0.01/<format>/`), with the Glue database `bench_tpcds_<sf>_<format>`: the fact tables are partitioned by
10-day buckets of their date key (`ss_sold_date_bucket`, `sr_returned_date_bucket`, `cs_sold_date_bucket`,
`cr_returned_date_bucket`, `ws_sold_date_bucket`, `wr_returned_date_bucket`, `inv_date_bucket`, each
`(date_sk - 2440588) // 10`, the same buckets as the TPC-H tables), the dimension tables are not. The queries in
`benchmark/tpcds/queries/` are DuckDB's with filters on the bucket columns added where a fact table's date key is
joined to a filtered `date_dim`. Its cache is `duckdb_benchmark_data/glue_tpcds_<sf>_<format>.duckdb`:

```sh
AWS_EC2_METADATA_DISABLED=true ./build/release/benchmark/benchmark_runner 'benchmark/tpcds/sf1/parquet/.*'
```

Both suites share a template per suite (`benchmark/tpch/tpch.benchmark.in`, `benchmark/tpcds/tpcds.benchmark.in`)
that takes `FORMAT`, `SF` and `SF_NAME`. The TPC-DS load needs a build without assertions
(`BUILD_BENCHMARK=1 make release`): its fact tables have rows with a NULL date key, and writing such a partition trips
a `D_ASSERT` in DuckDB's partitioned copy (see the header of `benchmark/tpcds/tpcds.benchmark.in` for a repro without
Glue).

`benchmark/{tpch,tpcds}/sf10/parquet/` run the same queries at SF10 (the TPC-DS load writes ~4 GB to S3), and
`benchmark/{tpch,tpcds}/sf0.01/parquet/` at SF0.01, which has as many partitions and files as SF1 but almost no rows,
so its timings are mostly the Glue calls, the S3 listings and the file opens. Neither runs in CI. (TPC-H q17 fails at
SF0.01: DuckDB's answer file for it is empty, the query returns one NULL.)

Both loads create their Glue tables with `CREATE TABLE IF NOT EXISTS ... AS`, so changing a load has no effect while
the tables are in Glue: rebuild the fixture with `make glue-fixture-down && make glue-fixture` first.

`benchmark_runner` runs the benchmarks below its own root (the repository), not the working directory. DuckDB's own
TPC-H and TPC-DS benchmarks on native tables, the reference for these, run with
`--root-dir duckdb` (`'benchmark/tpcds/sf1/.*' --sf 10` for TPC-DS at SF10).

`.github/workflows/Regression.yml` builds the benchmark runner for a PR and for its merge base once, then compares
their timings (5 runs each) in two jobs of about three minutes, so it runs only a few join-heavy TPC-H and TPC-DS
queries. The parquet job runs `benchmark/*.benchmark`, `benchmark/pushdown/`, the parquet benchmarks of
`benchmark/optimizer/` and TPC-DS q64 at SF1. The csv/json job runs the csv and json benchmarks of
`benchmark/optimizer/`, TPC-H q09, q18 and q21 at SF0.1 and TPC-DS q14, q64, q72 and q78 at SF0.01 on csv tables. The
other queries and the json and avro TPC benchmarks only run locally.

## Building

```sh
VCPKG_TOOLCHAIN_PATH='<path to vcpkg>/scripts/buildsystems/vcpkg.cmake' GEN=ninja make relassert
ASAN_OPTIONS=detect_container_overflow=0 ./build/relassert/test/unittest test/sql/test_glue_attach.test
```
