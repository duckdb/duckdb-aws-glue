add_subdirectory(readers)

add_library_unity(
  glue_core
  OBJECT
  glue_format.cpp
  glue_info.cpp
  glue_log_types.cpp
  glue_types.cpp
  helpers.cpp)
set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_core>
    PARENT_SCOPE)
