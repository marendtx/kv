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

# --- gRPCサーバー ---
add_executable(grpc_server
    src/server.cpp
    api/hello.pb.cc
    api/hello.grpc.pb.cc
    src/mylib.cpp     # 必要に応じて
)
target_include_directories(grpc_server PRIVATE include api)
target_link_libraries(grpc_server
    gRPC::grpc++
    protobuf::libprotobuf
)

# --- gRPCクライアント ---
add_executable(grpc_client
    src/client.cpp
    api/hello.pb.cc
    api/hello.grpc.pb.cc
    src/mylib.cpp     # 必要に応じて
)
target_include_directories(grpc_client PRIVATE include api)
target_link_libraries(grpc_client
    gRPC::grpc++
    protobuf::libprotobuf
)

# --- 通常アプリ ---
add_executable(myapp
    src/main.cpp
    src/mylib.cpp
)
target_include_directories(myapp PRIVATE include)

# --- テスト ---
find_package(GTest REQUIRED)
add_executable(runTests
    test/test_main.cpp
    test/test_bplustree.cpp
    src/mylib.cpp
)
target_include_directories(runTests PRIVATE include)
target_link_libraries(runTests GTest::GTest GTest::Main pthread)

add_test(NAME AllTests COMMAND runTests)