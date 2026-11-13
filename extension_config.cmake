# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(glue
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)
# Linked into unittest, which the test extension under test/extension/glue_test_credentials must share with it
duckdb_extension_statically_link(glue)

# Avro backed Hive tables (AvroSerDe) are read with read_avro from the avro extension, which the glue extension loads
# when Glue reports such a table. Pinned to the hash the duckdb submodule pins in
# .github/config/extensions/avro.cmake, so it is known to build against this duckdb commit.
# dbgen / dsdgen for the TPC-H and TPC-DS benchmarks under benchmark/tpch and benchmark/tpcds
duckdb_extension_load(tpch)
duckdb_extension_load(tpcds)

# httpfs with S3 requests signed with the secret a file names (duckdb/duckdb-httpfs#521). Takes precedence over the
# duckdb submodule's .github/config/extensions/httpfs.cmake; without its patches, which this commit already contains.
duckdb_extension_load(httpfs
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 5b1a46218dd31e811f1eeb0cac539e755cbb79c7
)

if (NOT MINGW)
    duckdb_extension_load(avro
        GIT_URL https://github.com/duckdb/duckdb-avro
        GIT_TAG 4d95356ce9433a14165156dbc6f59dcc91bd4889
        SUBMODULES "third_party/avro-c"
    )
endif()

# Iceberg tables in Glue views are read through an attached Iceberg catalog (ICEBERG_CATALOG); test/sql/cloud_glue
if (NOT EMSCRIPTEN AND NOT MINGW)
    duckdb_extension_load(iceberg
        GIT_URL https://github.com/duckdb/duckdb-iceberg
        GIT_TAG 7246b00586ad00f1c2a6c2c36490734a326056e9
    )
endif()
