cmake_minimum_required(VERSION 3.5...3.29)

# Set extension name here
set(TARGET_NAME glue)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD
    "17"
    CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# The AWS SDK (installed through vcpkg) provides the Glue Data Catalog client
find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)
find_package(AWSSDK REQUIRED COMPONENTS core glue sts sso)

# The AWS find_package can override TARGET_NAME, reset it
set(TARGET_NAME glue)

include_directories(src/include)
include_directories(${CURL_INCLUDE_DIRS})
include_directories(${AWSSDK_INCLUDE_DIRS})

set(GLUE_EXTENSION_FILES)
add_subdirectory(src)

build_static_extension(${TARGET_NAME} ${GLUE_EXTENSION_FILES})
build_loadable_extension(${TARGET_NAME} " " ${GLUE_EXTENSION_FILES})

# Link the AWS SDK, CURL and OpenSSL in both the static library and the loadable
# extension
target_include_directories(${EXTENSION_NAME}
                           PUBLIC $<BUILD_INTERFACE:${AWSSDK_INCLUDE_DIRS}>)
target_link_libraries(${EXTENSION_NAME} ${AWSSDK_LINK_LIBRARIES}
                      ${CURL_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto)
target_include_directories(${LOADABLE_EXTENSION_NAME}
                           PRIVATE $<BUILD_INTERFACE:${AWSSDK_INCLUDE_DIRS}>)
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${AWSSDK_LINK_LIBRARIES}
                      ${CURL_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

# The C++ tests (make test-cpp); they need cmake_language(DEFER)
if(BUILD_UNITTESTS
   AND NOT EMSCRIPTEN
   AND CMAKE_VERSION VERSION_GREATER_EQUAL 3.19)
  add_subdirectory(test/unittest)
endif()
