cmake_minimum_required(VERSION 3.10)
project(MyCppProject C CXX)

set(CMAKE_CXX_STANDARD 17)
enable_testing()

include_directories(
    include
    api
)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

include_directories($ENV{HOME}/.local/include)
include_directories($ENV{HOME}/.local/include/libnuraft)
link_directories($ENV{HOME}/.local/lib)

add_executable(client
    src/client.cpp
    api/kvstore.pb.cc
    api/kvstore.grpc.pb.cc
)
target_include_directories(client PRIVATE include api)
target_link_libraries(client
    gRPC::grpc++
    protobuf::libprotobuf
)

add_executable(myapp
    src/main.cpp
    src/server.cpp
    src/bptree.cpp
    src/grpc_server.cpp
    src/raft_inmem_log_store.cpp
    src/raft_server.cpp
    src/raft_state_machine.cpp
    src/raft_state_mgr.cpp
    api/kvstore.pb.cc
    api/kvstore.grpc.pb.cc
)
target_include_directories(myapp PRIVATE include api)
target_link_libraries(myapp
    gRPC::grpc++
    gRPC::grpc++_reflection
    protobuf::libprotobuf
    nuraft
)

find_package(GTest REQUIRED)
add_executable(runTests
    test/test_bptree.cpp
    src/bptree.cpp
)
target_include_directories(runTests PRIVATE include)
target_link_libraries(runTests GTest::GTest GTest::Main pthread)

add_test(NAME AllTests COMMAND runTests)