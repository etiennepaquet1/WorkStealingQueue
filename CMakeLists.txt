cmake_minimum_required(VERSION 3.28)
project(WorkStealingQueue LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)


add_library(ProjectHeaders INTERFACE)
target_include_directories(ProjectHeaders INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)

include(FetchContent)

# Fetch doctest from GitHub
FetchContent_Declare(
    doctest
    GIT_REPOSITORY https://github.com/doctest/doctest.git
    GIT_TAG v2.4.11
)

FetchContent_MakeAvailable(doctest)

add_executable(WSQBench
        bench/bench.cpp
)
target_link_libraries(WSQBench PRIVATE ProjectHeaders)
target_link_libraries(WSQBench PRIVATE doctest::doctest)
target_compile_features(WSQBench PRIVATE cxx_std_23)


# Test executable
add_executable(WSQTests
    test/tests.cpp
)
target_link_libraries(WSQTests PRIVATE ProjectHeaders)
target_link_libraries(WSQTests PRIVATE doctest::doctest)
target_compile_features(WSQTests PRIVATE cxx_std_23)

# Optional: add test to CTest
include(CTest)
add_test(NAME WSQTests COMMAND WSQTests)
