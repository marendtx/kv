#include <iostream>
#include <optional>
#include <string>
#include <vector>

// 必要なヘッダー
#include "kvstore.grpc.pb.h"
#include <grpcpp/grpcpp.h>

void RunServer(int node_id, const std::string &grpc_host, int grpc_port, const std::string &raft_host, int raft_port);

// ユーティリティ: コマンドライン引数パース
struct Args {
    int node_id;
    std::string grpc_host;
    int grpc_port;
    std::string raft_host;
    int raft_port;
    std::optional<std::pair<std::string, int>> join_target; // (host, port)
};

Args parse_args(int argc, char **argv) {
    if (argc != 6 && argc != 9) {
        std::cerr << "Usage:\n";
        std::cerr << "  " << argv[0] << " <node_id> <grpc_host> <grpc_port> <raft_host> <raft_port> [--join <leader_host> <leader_port>]\n";
        std::exit(1);
    }
    Args a;
    a.node_id = std::stoi(argv[1]);
    a.grpc_host = argv[2];
    a.grpc_port = std::stoi(argv[3]);
    a.raft_host = argv[4];
    a.raft_port = std::stoi(argv[5]);
    if (argc == 9) {
        std::string join_flag = argv[6];
        if (join_flag != "--join") {
            std::cerr << "Expected --join\n";
            std::exit(1);
        }
        a.join_target = {{argv[7], std::stoi(argv[8])}};
    }
    return a;
}

// --- gRPCでJoinリクエストを送る ---
bool send_join_req(const std::string &target_host, int target_port, int node_id, const std::string &raft_endpoint) {
    std::string address = target_host + ":" + std::to_string(target_port);
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = kvstore::KVStore::NewStub(channel);

    kvstore::JoinRequest req;
    req.set_node_id(std::to_string(node_id));
    req.set_raft_endpoint(raft_endpoint);
    kvstore::JoinResponse res;
    grpc::ClientContext ctx;
    auto status = stub->Join(&ctx, req, &res);
    if (status.ok() && res.success()) {
        std::cout << "[Join] 成功: クラスタへ参加しました" << std::endl;
        return true;
    } else {
        std::cerr << "[Join] 失敗: " << res.error() << std::endl;
        return false;
    }
}

int main(int argc, char **argv) {
    auto args = parse_args(argc, argv);

    // Join要求送信（--join指定時のみ）
    if (args.join_target) {
        std::string raft_endpoint = args.raft_host + ":" + std::to_string(args.raft_port);
        if (!send_join_req(
                args.join_target->first,
                args.join_target->second,
                args.node_id,
                raft_endpoint)) {
            std::cerr << "Join failed. Exiting..." << std::endl;
            return 1;
        }
    }

    RunServer(args.node_id, args.grpc_host, args.grpc_port, args.raft_host, args.raft_port);
    return 0;
}
