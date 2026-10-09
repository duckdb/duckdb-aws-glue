add_subdirectory(stats)

add_library_unity(glue_planning OBJECT glue_hive_insert.cpp
                  hive_multi_file_reader.cpp hive_query_cache.cpp)
set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_planning>
    PARENT_SCOPE)
