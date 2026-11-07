add_library_unity(glue_execution OBJECT hive_copy.cpp hive_write_undo.cpp)
set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_execution>
    PARENT_SCOPE)
