add_library_unity(
  glue_api
  OBJECT
  glue_api_util.cpp
  glue_client.cpp
  glue_database_api.cpp
  glue_http_client.cpp
  glue_partition_api.cpp
  glue_retry_strategy.cpp
  glue_table_api.cpp)
target_link_libraries(glue_api ${AWSSDK_LINK_LIBRARIES})

set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_api>
    PARENT_SCOPE)
