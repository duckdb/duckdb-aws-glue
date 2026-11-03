add_library_unity(glue_planning_stats OBJECT hive_stats.cpp
                  hive_footer_statistics.cpp)
set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_planning_stats>
    PARENT_SCOPE)
