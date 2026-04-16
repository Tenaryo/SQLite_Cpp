cmake_minimum_required(VERSION 3.21)

project(SQLite_Cpp VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(sqlite_core INTERFACE)
target_include_directories(sqlite_core INTERFACE ${CMAKE_SOURCE_DIR}/src)
target_compile_features(sqlite_core INTERFACE cxx_std_23)
target_compile_options(sqlite_core INTERFACE
    -Wall -Wextra -Wpedantic -Werror -Wshadow -Wconversion
    $<$<CONFIG:Debug>:-g -O0>
    $<$<CONFIG:Release>:-O3>
)

add_executable(sqlite src/main.cpp)
target_link_libraries(sqlite PRIVATE sqlite_core)

option(BUILD_TESTS "Build tests" ON)
if(BUILD_TESTS)
    add_subdirectory(tests)
endif()
