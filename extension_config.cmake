# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(glue
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)
# Linked into unittest, which the test extension under test/extension/glue_test_credentials must share with it
duckdb_extension_statically_link(glue)

# dbgen / dsdgen for the TPC-H and TPC-DS benchmarks under benchmark/tpch and benchmark/tpcds
duckdb_extension_load(tpch)
duckdb_extension_load(tpcds)

# Pinned and patched as the duckdb submodule's .github/config/extensions/httpfs.cmake, until duckdb-httpfs main
# (with duckdb/duckdb-httpfs#521) builds against duckdb main
duckdb_extension_load(httpfs
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG a2485c0fd240cbddc0d467019f30201c9a46cf06
    APPLY_PATCHES
)

# Avro backed Hive tables (AvroSerDe) are read with read_avro from the avro extension, which the glue extension loads
# when Glue reports such a table. Pinned ahead of the duckdb submodule's 4d95356 (.github/config/extensions/avro.cmake)
# for the Wasm vcpkg triplet linking, the decimal fixed-size sign bit and the avro-c Windows checkout fixes, so without
# the submodule's .github/patches/extensions/avro, which target 4d95356.
if (NOT MINGW)
    duckdb_extension_load(avro
        GIT_URL https://github.com/duckdb/duckdb-avro
        GIT_TAG 5ef585162ca04de48f2fbfce939dac3322091836
        SUBMODULES "third_party/avro-c"
    )
endif()

# Iceberg tables in Glue views are read through an attached Iceberg catalog (ICEBERG_CATALOG); test/sql/cloud_glue.
# Pinned and patched as the duckdb submodule's .github/config/extensions/iceberg.cmake, until duckdb-iceberg main
# builds against duckdb main
if (NOT EMSCRIPTEN AND NOT MINGW)
    duckdb_extension_load(iceberg
        GIT_URL https://github.com/duckdb/duckdb-iceberg
        GIT_TAG 4c0c0874c48ec82a3e29910554891d04bcc0e988
        APPLY_PATCHES
    )
endif()
