cmake_minimum_required(VERSION 3.10)
project(MyCppProject C CXX)

set(CMAKE_CXX_STANDARD 17)
enable_testing()

# gRPC, Protobuf
# find_package(Protobuf REQUIRED)
# find_package(gRPC REQUIRED)

include_directories(
    include
    api
)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
# find_package(NuRaft CONFIG REQUIRED)

# NuRaft
include_directories($ENV{HOME}/.local/include)
include_directories($ENV{HOME}/.local/include/libnuraft)
link_directories($ENV{HOME}/.local/lib)

# # --- gRPCサーバー ---
# add_executable(grpc_server
#     src/server.cpp
#     api/hello.pb.cc
#     api/hello.grpc.pb.cc
# )
# target_include_directories(grpc_server PRIVATE include api)
# target_link_libraries(grpc_server
#     gRPC::grpc++
#     protobuf::libprotobuf
# )

# --- gRPCクライアント ---
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

# add_executable(server
#     src/server.cpp
#     api/kvstore.pb.cc
#     api/kvstore.grpc.pb.cc
# )
# target_include_directories(server PRIVATE include api)
# target_link_libraries(server
#     gRPC::grpc++
#     protobuf::libprotobuf
# )

# --- 通常アプリ ---
add_executable(myapp
    src/main.cpp
    src/server.cpp
    src/b_plus_tree.cpp
    api/kvstore.pb.cc
    api/kvstore.grpc.pb.cc
)
target_include_directories(myapp PRIVATE include api)
target_link_libraries(myapp
    gRPC::grpc++
    protobuf::libprotobuf
    nuraft
)

# --- テスト ---
find_package(GTest REQUIRED)
add_executable(runTests
    test/test_main.cpp
    test/test_bplustree.cpp
    src/mylib.cpp
    src/b_plus_tree.cpp
)
target_include_directories(runTests PRIVATE include)
target_link_libraries(runTests GTest::GTest GTest::Main pthread)

add_test(NAME AllTests COMMAND runTests)