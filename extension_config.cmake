# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(glue
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)
# Linked into unittest, which the test extension under test/extension/glue_test_credentials must share with it
duckdb_extension_statically_link(glue)

# Avro backed Hive tables (AvroSerDe) are read with read_avro from the avro extension, which the glue extension loads
# when Glue reports such a table.
# dbgen / dsdgen for the TPC-H and TPC-DS benchmarks under benchmark/tpch and benchmark/tpcds
duckdb_extension_load(tpch)
duckdb_extension_load(tpcds)

# httpfs with S3 requests signed with the secret a file names (duckdb/duckdb-httpfs#521). Takes precedence over the
# duckdb submodule's .github/config/extensions/httpfs.cmake; without its patches, which this commit already contains.
# On Windows, httpfs's spilling tests fail to create the per-test temp directory under the deep CI checkout path
if (WIN32)
    set(HTTPFS_LOAD_TESTS "")
else()
    set(HTTPFS_LOAD_TESTS LOAD_TESTS)
endif()
duckdb_extension_load(httpfs
    ${HTTPFS_LOAD_TESTS}
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 5b1a46218dd31e811f1eeb0cac539e755cbb79c7
)

if (NOT MINGW)
    duckdb_extension_load(avro
        GIT_URL https://github.com/duckdb/duckdb-avro
        GIT_TAG beb8f4f7881f90de3d7a6eb5923ac820d06f6f67
        SUBMODULES "third_party/avro-c"
    )
endif()

# Iceberg tables in Glue views are read through an attached Iceberg catalog (ICEBERG_CATALOG); test/sql/cloud_glue
if (NOT EMSCRIPTEN AND NOT MINGW)
    duckdb_extension_load(iceberg
        GIT_URL https://github.com/duckdb/duckdb-iceberg
        GIT_TAG f979b5b28a044c49c38a26636ac01bc1f7047fa5
    )
endif()
