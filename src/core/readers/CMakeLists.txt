add_library_unity(glue_core_readers OBJECT hive_csv_options.cpp
                  hive_json_options.cpp opencsv_line.cpp)
set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_core_readers>
    PARENT_SCOPE)
