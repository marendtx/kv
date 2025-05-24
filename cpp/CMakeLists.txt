cmake_minimum_required(VERSION 3.10)
project(MyCppProject C CXX)

set(CMAKE_CXX_STANDARD 17)
enable_testing()

include_directories(include)


add_executable(myapp
    src/main.cpp
    src/mylib.cpp
)
target_include_directories(myapp PRIVATE include)

find_package(GTest REQUIRED)

add_executable(runTests
    test/test_main.cpp
    test/test_bplustree.cpp
    src/mylib.cpp
)
target_include_directories(runTests PRIVATE include)
target_link_libraries(runTests GTest::GTest GTest::Main pthread)

add_test(NAME AllTests COMMAND runTests)