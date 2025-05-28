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
// > status
// ... (Raftクラスタ状態表示)
// > members
// ... (クラスタ全メンバー一覧表示)
// > quit

int main(int argc, char **argv) {
    auto args = parse_args(argc, argv);
    std::string address = args.host + ":" + std::to_string(args.port);
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = kvstore::KVStore::NewStub(channel);

    std::cout << "接続先: " << address << std::endl;
    std::cout << "コマンド: put/get/del/join/leave/status/members/quit" << std::endl;

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
        } else if (cmd == "status") {
            kvstore::StatusRequest req;
            kvstore::StatusResponse res;
            grpc::ClientContext ctx;
            auto status = stub->Status(&ctx, req, &res);
            if (status.ok()) {
                std::cout << "[Status]\n"
                          << "  node_id: " << res.node_id() << "\n"
                          << "  leader_id: " << res.leader_id() << "\n"
                          << "  commit_index: " << res.commit_index() << "\n"
                          << "  raft_state: " << res.raft_state() << "\n";
            } else {
                std::cout << "[Status] 取得失敗" << std::endl;
            }
        } else if (cmd == "members") {
            kvstore::ListMembersRequest req;
            kvstore::ListMembersResponse res;
            grpc::ClientContext ctx;
            auto status = stub->ListMembers(&ctx, req, &res);
            if (status.ok()) {
                std::cout << "[Members]" << std::endl;
                for (const auto &m : res.members()) {
                    std::cout << "  id: " << m.id()
                              << "  endpoint: " << m.endpoint()
                              << (m.is_leader() ? " (LEADER)" : "") << std::endl;
                }
            } else {
                std::cout << "[Members] 取得失敗" << std::endl;
            }
        } else if (cmd == "quit" || cmd == "exit") {
            break;
        } else if (cmd == "help") {
            std::cout << "put <key> <val>\nget <key>\ndel <key>\njoin <node_id> <raft_endpoint>\nleave <node_id>\nstatus\nmembers\nquit" << std::endl;
        } else {
            std::cout << "コマンドが不正です（helpで一覧表示）" << std::endl;
        }
    }
    return 0;
}
