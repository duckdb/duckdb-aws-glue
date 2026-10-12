cmake_minimum_required(VERSION 3.5...3.29)

project(GlueTestLoaderExtension)

# A GovernedGlueTableLoader for the sqllogictests of catalogs attached with
# GOVERNANCE true
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../src/include)

add_library(glue_test_loader OBJECT glue_test_loader_extension.cpp)
# DuckDB adds test/, and with it unittest, after the extensions
cmake_language(
  DEFER
  DIRECTORY
  ${DuckDB_SOURCE_DIR}
  CALL
  target_link_libraries
  unittest
  glue_test_loader)
