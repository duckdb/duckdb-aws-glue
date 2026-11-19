cmake_minimum_required(VERSION 3.5...3.29)

project(GlueTestCredentialsExtension)

# A GlueFileCredentialsCallback for the sqllogictests under
# test/sql/file_credentials
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../src/include)
add_library(glue_test_credentials OBJECT glue_test_credentials_extension.cpp)
target_include_directories(glue_test_credentials PRIVATE ${AWSSDK_INCLUDE_DIRS})

# DuckDB adds test/, and with it unittest, after the extensions, and decides
# which extensions it links (DUCKDB_EXTENSION_GLUE_SHOULD_LINK) after it has
# added their directories. So the link is deferred to the end of DuckDB's
# top-level directory, and made only when glue is linked into the same binary.
function(glue_test_credentials_link_unittest)
  if(DUCKDB_EXTENSION_GLUE_SHOULD_LINK)
    target_link_libraries(unittest glue_test_credentials)
  endif()
endfunction()
cmake_language(DEFER DIRECTORY ${DuckDB_SOURCE_DIR} CALL
               glue_test_credentials_link_unittest)
