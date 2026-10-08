# AGENTS.md

This file provides guidance to coding agents when working with code in this repository. For DuckDB internals
(parser/planner/optimizer/execution, Vector API, generated files) see `duckdb/AGENTS.md`.

## What this is

A DuckDB extension (`glue`, attached with `ATTACH '<account_id>' AS cat (TYPE GLUE)`) that exposes an AWS Glue Data
Catalog as a DuckDB catalog through the AWS SDK Glue client. Hive (Glue native) tables are readable/writable
(parquet, csv, json, avro SerDes); Iceberg tables are served by a hidden duckdb-iceberg catalog over Glue's Iceberg
REST endpoint; Delta/Hudi tables are listed (`duckdb_tables().tags['table_type']`) but scans and DML throw. The top-level `README.md` is empty — `docs/README.md` is the real user-facing spec (attach
options, read/write semantics, partition functions, testing, benchmarks). Keep it in sync when behavior changes.

## Build

Extension template build (`extension-ci-tools`); the `duckdb` and `extension-ci-tools` submodules must be checked out.
vcpkg is required (AWS SDK, openssl, curl, ...):

```sh
VCPKG_TOOLCHAIN_PATH='<vcpkg>/scripts/buildsystems/vcpkg.cmake' GEN=ninja make relassert   # what tests use
BUILD_BENCHMARK=1 ... make relassert                                                    # also builds benchmark_runner
```

`relassert` is ASAN-instrumented while the vcpkg AWS SDK is not, so always run with
`ASAN_OPTIONS=detect_container_overflow=0` (otherwise false-positive container-overflow on secret/credential code).
Each `src/` subdirectory has its own `CMakeLists.txt` (an `add_library_unity` object library appended to
`GLUE_EXTENSION_FILES`, like DuckDB's own source tree): add new `.cpp` files there, and a new directory with
`add_subdirectory` in `src/CMakeLists.txt`. `extension_config.cmake` also links `tpch`, `tpcds` and `avro` (avro is
loaded on demand for AvroSerDe tables) and `iceberg` (pinned with `APPLY_PATCHES` to the hash the duckdb submodule
pins; needs `roaring` from vcpkg).

Run `make format-fix` (clang-format over `src` and `test`, via the duckdb submodule) before committing.

## Tests

SQLLogicTests under `test/sql/`. They need a `--test-config` that sets `GLUE_CATALOG_ID`, `GLUE_ENDPOINT`,
`DEFAULT_S3_LOCATION` and creates the S3 secret; without one every test is skipped (`require-env GLUE_CATALOG_ID`).

- `test/configs/local_glue.json`: moto (Glue, port 5555) + SeaweedFS (S3, port 9100) from `scripts/docker-compose.yml`.
  `make glue-fixture` / `make glue-fixture-down`; `make test-local` runs everything through DuckDB's
  `scripts/ci/run_tests.py` (Python 3.10+, `PYTHON=...`), serially, retrying a failing test twice: against the local
  servers a read right after a write occasionally comes back empty.
- `.github/workflows/CloudGlueTests.yml`: maintainer-only `workflow_dispatch` (input `pr_number`) running all tests
  of a PR against the account configured in the `cloud-glue` environment (`make test-cloud CLOUD_TEST_CONFIG=...`).
- `test/configs/cloud_glue.json`: live AWS (credential chain, eu-north-1). `test/sql/cloud/` only runs here
  (`require-env GLUE_TEST_CONFIG cloud`) and reads pre-existing tables in the account.

Single test:

```sh
AWS_EC2_METADATA_DISABLED=true ASAN_OPTIONS=detect_container_overflow=0 \
  ./build/relassert/test/unittest --test-config test/configs/local_glue.json test/sql/hive/dml/select.test
```

- `AWS_EC2_METADATA_DISABLED=true` is required: the test runner replaces HOME, so the SDK can't find a region and
  otherwise hangs for minutes on the EC2 metadata service. For cloud runs also export
  `AWS_PROFILE`, `AWS_CONFIG_FILE=$HOME/.aws/config` and `AWS_SHARED_CREDENTIALS_FILE=$HOME/.aws/credentials`.
- Tests write under `{DEFAULT_S3_LOCATION}/{TEST_DIR}` so runs don't collide. `DROP TABLE`/`DROP SCHEMA` leave S3
  files behind, and Glue has no transactions — nothing rolls back.
- **Only run the tests the user asks for.** Several tests drop tables or whole Glue databases (e.g. `default`); against
  the cloud config that destroys real data. Check what a test drops before running it.
- `test/configs/*.json` own the init SQL, env and skip policy. When something fails for one config only, skip the
  narrowest path there with a concrete reason instead of weakening the test; remove the skip when support lands.
  Validate config edits with `jq empty test/configs/*.json`.
- Test targets don't build first: rebuild before running tests.

Tests that need Iceberg tables (`test/sql/iceberg/`, `test/sql/combined_table_types/`) have
`require-env ICEBERG_SUPPORTED`, which only the cloud config sets (moto has no Iceberg REST endpoint), and
`set ignore_error_messages`, so REST errors (which mention `HTTP`) fail the test instead of skipping it. New tests
that create or read Iceberg tables need both.

Test format is DuckDB's sqllogictest (`statement ok|error`, `query I...`, `----`, `<REGEX>:` for error patterns,
`require-env`). Slow tests use `.test_slow`. Do not add `PRAGMA enable_verification`. Test error paths, not just the
happy path.

Benchmarks (`benchmark/`, incl. TPC-H/TPC-DS SF1 against local Glue) run with
`./build/relassert/benchmark/benchmark_runner <path>`; see `docs/README.md` for data caching and caveats
(TPC-DS load needs a non-assert `release` build).

## Architecture

- **Entry point** `src/glue_extension.cpp`: `Aws::InitAPI`, registers the `glue` StorageExtension, the table functions,
  the `glue_hive_ddl` grammar extension, and settings (`glue_network_calls_via_duckdb`, `glue_get_partitions_segments`,
  `hive_partition_listing_threshold`).
- **Catalog layer** `src/catalog/`: the usual DuckDB custom-catalog shape — `GlueCatalog` → `GlueSchemaSet` →
  `GlueSchemaEntry` (a Glue database) → `GlueTableSet` → `GlueTable`. `GlueAttach` parses ATTACH options. DDL
  (CREATE/DROP schema/table, ALTER column) is implemented in `GlueSchemaEntry`; Glue types are mapped in
  `src/core/glue_types.cpp`. Listing skips tables whose definition can't be converted (logged), direct lookup throws.
- **All Glue calls** go through static methods on `GlueAPI` (`src/api/`, one file per resource: databases, tables,
  partitions), which convert between SDK objects and plain structs (`GlueTableInfo`, `GluePartitionInfo`, ...). Updates
  copy the full `Table` into a `TableInput` because Glue's UpdateTable replaces the whole definition.
- **HTTP transport** `src/api/glue_http_client.cpp`: a global AWS `HttpClientFactory` sends SDK traffic through DuckDB's
  `HTTPUtil` (so it's logged and honors DuckDB proxy settings). The calling `ClientContext` reaches it via a
  thread_local scope (`GlueHttpClientContextScope`) set around each `GlueAPI` call.
- **Reading** `src/planning/hive_multi_file_reader.cpp`: `GlueTable::GetScanFunction` builds a `HiveScanInfo` (Glue
  schema, partitions from `GetPartitions`, format) and scans with the format's reader
  (`read_parquet`/`read_csv`/`read_json`/ `read_avro`) using `HiveMultiFileReader` + a lazy `HiveMultiFileList`:
  partition filters are pushed down to Glue's partition values before any S3 listing. The `hive_scan` table function
  (`src/functions/hive_scan_function.cpp`) reuses the same reader with schema/partitions given as arguments.
- **Writing** `src/planning/glue_hive_insert.cpp`: INSERT and CTAS plan a `PhysicalCopyToFile` in the table's format
  with hive partition directories, then register new partitions with `BatchCreatePartition` in Finalize. CTAS creates
  the Glue table at plan time.
- **Partition DDL**: DuckDB has no `ALTER TABLE ... PARTITION` syntax, so `src/functions/glue_partition_functions.cpp`
  provides `glue_partitions`, `glue_add_partition`, ... and `glue_alter_table`; `src/grammar/glue_grammar.cpp` (grammar
  extension `glue_hive_ddl`, enabled with `SET active_grammar_extensions = ['glue_hive_ddl']`) rewrites Hive partition
  SQL into `CALL glue_alter_table(...)`.
- **Iceberg** (`GlueCatalog::GetIcebergCatalog`): ATTACH also attaches a hidden (`AttachVisibility::HIDDEN`)
  `TYPE iceberg, ENDPOINT_TYPE glue` catalog `__glue_iceberg_<name>_<n>` (a failure is only logged; the next Iceberg
  lookup attaches again, as after a rollback of the attaching transaction).
  `GlueSchemaEntry::LookupEntry` returns that catalog's table entry, so duckdb-iceberg binds and plans scans, DML,
  DROP and ALTER (its planners cast `op.table` to their own entry type); listing stays on Glue. CREATE / CTAS with
  `table_type = 'iceberg'` reach the Glue schema and are rebound to the Iceberg schema. Cached Glue entries of
  Iceberg tables a transaction touched are invalidated when it ends (`GlueTransaction`), never during binding:
  listings hold references to them. Iceberg tables are refused in explicit transactions.
- `glue_get_table_response` (`src/functions/glue_functions.cpp`) returns the raw Glue `Table` as VARIANT — handy for
  asserting Glue state in tests.

## Coding guidelines

Follow DuckDB's style (`duckdb/AGENTS.md`):

- **Comments are kept to a minimum.** Code should be self-descriptive; a comment is one short line, and only where the
  code can't say it (e.g. a Glue/AWS quirk). No comments describing the change, the bug that was fixed, or the history
  of the code — that belongs in the commit message or PR. Don't add doc comments restating a function's name.
- Tabs for indentation, 120 columns. Always use braces for `if` and loops. Range-based for loops where possible.
- `[u]int(8|16|32|64)_t` instead of `int`/`long`; `idx_t` for offsets, indices and counts.
- `unique_ptr` for ownership, `shared_ptr` only when necessary, `optional_ptr`/`reference` instead of raw pointers.
  Never `const_cast`. Pass non-trivial objects by `const &`.
- Naming: files `snake_case`, types and functions `PascalCase`, variables `snake_case`.
- Class layout: `public:` section(s) before `private:`, methods and member variables in separate sections.
- Exceptions for query-terminating errors (`BinderException`, `NotImplementedException`, `IOException`, ...);
  `D_ASSERT` only for programmer errors, never for anything user input or Glue responses can trigger.
- When reading or writing vectors, use `Vector::Values<T>()` / `FlatVector::Writer<T>()` rather than `ToUnifiedFormat` loops.

## Change checklist

- C++ change: `make format-check`, build `relassert`, run the focused sqllogictests (locally against moto/SeaweedFS).
- Build change: read both the root `Makefile` and `extension-ci-tools/makefiles/duckdb_extension.Makefile`.
- Documentation-only change: check commands statically; don't start containers or run tests.

## Repo notes

- `duckdb/` and `extension-ci-tools/` are submodules; don't edit or bump them unless the task requires it.
- Don't commit generated/runtime directories (`build/`, `.cache/`, `duckdb_unittest_tempdir/`,
  `duckdb_benchmark_data/`).

- Watch the shell cwd: running `make` inside `duckdb/` starts a separate, huge build of the submodule.
