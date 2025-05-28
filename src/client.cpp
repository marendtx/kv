#include "kvstore.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

struct Args {
    std::string host = "localhost";
    int port = 50051;
};
Args parse_args(int argc, char **argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        std::string s = argv[i];
        if (s == "--host" && i + 1 < argc)
            args.host = argv[++i];
        else if (s == "--port" && i + 1 < argc)
            args.port = std::stoi(argv[++i]);
    }
    return args;
}
std::vector<std::string> split(const std::string &s) {
    std::istringstream iss(s);
    std::vector<std::string> v;
    for (std::string t; iss >> t;)
        v.push_back(t);
    return v;
}

// Usage:
// > ./build/client --host localhost --port 50051
// > put foo bar
// [Put] OK
// > get foo
// [Get] foo → bar
// > del foo
// [Delete] OK
// > join 2 localhost:12346
// [Join] OK
// > leave 2
// [Leave] OK
// > quit

int main(int argc, char **argv) {
    auto args = parse_args(argc, argv);
    std::string address = args.host + ":" + std::to_string(args.port);
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = kvstore::KVStore::NewStub(channel);

    std::cout << "接続先: " << address << std::endl;
    std::cout << "コマンド: put/get/del/join/leave/quit" << std::endl;

    std::string line;
    while (std::cout << "> ", std::getline(std::cin, line)) {
        auto v = split(line);
        if (v.empty())
            continue;
        std::string cmd = v[0];

        if (cmd == "put" && v.size() == 3) {
            kvstore::PutRequest req;
            kvstore::PutResponse res;
            req.set_key(v[1]);
            req.set_value(v[2]);
            grpc::ClientContext ctx;
            auto status = stub->Put(&ctx, req, &res);
            std::cout << (status.ok() && res.success() ? "[Put] OK" : "[Put] Failed: " + res.error()) << std::endl;
        } else if (cmd == "get" && v.size() == 2) {
            kvstore::GetRequest req;
            kvstore::GetResponse res;
            req.set_key(v[1]);
            grpc::ClientContext ctx;
            auto status = stub->Get(&ctx, req, &res);
            if (status.ok() && res.found())
                std::cout << "[Get] " << req.key() << " → " << res.value() << std::endl;
            else
                std::cout << "[Get] Not found or error: " << res.error() << std::endl;
        } else if ((cmd == "del" || cmd == "delete") && v.size() == 2) {
            kvstore::DeleteRequest req;
            kvstore::DeleteResponse res;
            req.set_key(v[1]);
            grpc::ClientContext ctx;
            auto status = stub->Delete(&ctx, req, &res);
            std::cout << (status.ok() && res.success() ? "[Delete] OK" : "[Delete] Failed: " + res.error()) << std::endl;
        } else if (cmd == "join" && v.size() == 3) {
            kvstore::JoinRequest req;
            kvstore::JoinResponse res;
            req.set_node_id(v[1]);
            req.set_raft_endpoint(v[2]);
            grpc::ClientContext ctx;
            auto status = stub->Join(&ctx, req, &res);
            std::cout << (status.ok() && res.success() ? "[Join] OK" : "[Join] Failed: " + res.error()) << std::endl;
        } else if (cmd == "leave" && v.size() == 2) {
            kvstore::LeaveRequest req;
            kvstore::LeaveResponse res;
            req.set_node_id(v[1]);
            grpc::ClientContext ctx;
            auto status = stub->Leave(&ctx, req, &res);
            std::cout << (status.ok() && res.success() ? "[Leave] OK" : "[Leave] Failed: " + res.error()) << std::endl;
        } else if (cmd == "quit" || cmd == "exit") {
            break;
        } else if (cmd == "help") {
            std::cout << "put <key> <val>\nget <key>\ndel <key>\njoin <node_id> <raft_endpoint>\nleave <node_id>\nquit" << std::endl;
        } else {
            std::cout << "コマンドが不正です（helpで一覧表示）" << std::endl;
        }
    }
    return 0;
}
