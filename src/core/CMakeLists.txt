add_library_unity(glue_core OBJECT glue_format.cpp glue_info.cpp glue_types.cpp)
set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_core>
    PARENT_SCOPE)
