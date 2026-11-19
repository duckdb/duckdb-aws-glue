add_library_unity(
  glue_catalog
  OBJECT
  glue_attach.cpp
  glue_catalog.cpp
  glue_file_credentials.cpp
  glue_schema_entry.cpp
  glue_schema_set.cpp
  glue_table.cpp
  glue_table_set.cpp
  glue_transaction.cpp
  glue_view.cpp
  glue_view_names.cpp
  glue_transaction_manager.cpp
  scan_filter_equality.cpp)
set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_catalog>
    PARENT_SCOPE)
