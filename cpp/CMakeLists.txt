cmake_minimum_required(VERSION 3.10)
project(MyCppProject C CXX)

enable_testing()
set(CMAKE_CXX_STANDARD 17)

include_directories(include)
file(GLOB_RECURSE SOURCES src/*.cpp src/*.c)
file(GLOB_RECURSE TEST_SOURCES test/*.cpp)

find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

add_executable(runTests ${SOURCES} ${TEST_SOURCES})
target_link_libraries(runTests ${GTEST_LIBRARIES} pthread)

add_test(NAME AllTests COMMAND runTests)
