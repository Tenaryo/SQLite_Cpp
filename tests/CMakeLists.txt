file(GLOB TEST_SOURCES *.cpp)

foreach(test_source ${TEST_SOURCES})
    get_filename_component(test_name ${test_source} NAME_WE)
    add_executable(${test_name} ${test_source})
    target_link_libraries(${test_name} PRIVATE sqlite_core)
    target_compile_options(${test_name} PRIVATE -UNDEBUG)
endforeach()
