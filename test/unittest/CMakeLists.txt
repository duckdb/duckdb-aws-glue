# Catch tests of functions/hive_scan_call.hpp, which no SQL reaches; built only
# on request (make test-cpp)
add_executable(unittest_glue EXCLUDE_FROM_ALL main.cpp hive_scan_call_test.cpp)
target_include_directories(unittest_glue
                           PRIVATE ${CMAKE_SOURCE_DIR}/third_party/catch)
set_target_properties(unittest_glue PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                               ${CMAKE_BINARY_DIR}/test)
target_link_libraries(unittest_glue duckdb_static)
link_threads(unittest_glue "")

# Linked and loaded in this order (glue looks up read_parquet when it loads),
# once the build has added the extensions that come after glue
function(link_unittest_glue_extensions)
  set(EXTENSIONS core_functions parquet json httpfs avro glue)
  duckdb_link_extensions(unittest_glue ${EXTENSIONS})
endfunction()
cmake_language(DEFER DIRECTORY ${CMAKE_SOURCE_DIR} CALL
               link_unittest_glue_extensions)
