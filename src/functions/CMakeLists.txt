add_library_unity(
  glue_functions OBJECT glue_functions.cpp glue_partition_functions.cpp
  glue_show_columns_function.cpp hive_scan_function.cpp)
set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_functions>
    PARENT_SCOPE)
