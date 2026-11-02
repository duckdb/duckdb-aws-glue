# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(glue
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Avro backed Hive tables (AvroSerDe) are read with read_avro from the avro extension, which the glue extension loads
# when Glue reports such a table. Pinned to the hash the duckdb submodule pins in
# .github/config/extensions/avro.cmake, so it is known to build against this duckdb commit.
# dbgen / dsdgen for the TPC-H and TPC-DS benchmarks under benchmark/tpch and benchmark/tpcds
duckdb_extension_load(tpch)
duckdb_extension_load(tpcds)

if (NOT MINGW)
    duckdb_extension_load(avro
        GIT_URL https://github.com/duckdb/duckdb-avro
        GIT_TAG 345c3f4e821940233b0b889e815582e5a5a33dfe
        SUBMODULES "third_party/avro-c"
    )

    # Iceberg tables in Glue are served by Glue's Iceberg REST endpoint through an internal iceberg catalog. Pinned
    # to the hash the duckdb submodule pins in .github/config/extensions/iceberg.cmake.
    duckdb_extension_load(iceberg
        GIT_URL https://github.com/duckdb/duckdb-iceberg
        GIT_TAG 4c0c0874c48ec82a3e29910554891d04bcc0e988
    )
endif()
