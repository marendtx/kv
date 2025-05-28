#include "b_bplus_tree.hpp"
#include "kvstore.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

// Raft系も必要ならinclude
#include "libnuraft/buffer_serializer.hxx"
#include "libnuraft/in_memory_log_store.hxx"
#include "libnuraft/nuraft.hxx"

// -- B+treeとRaftをまとめたシングルトン --
struct KVSingletons {
    BPlusTree bptree;
    // Raft関連（今は省略、あとで組み込む）
    // std::shared_ptr<nuraft::raft_server> raft;
    // ...
    std::mutex mutex; // 排他制御
};
KVSingletons &get_kv_singleton() {
    static KVSingletons s;
    return s;
}

// ---- KVStoreサービス実装 ----
class KVStoreServiceImpl final : public kvstore::KVStore::Service {
public:
    // Put
    grpc::Status Put(grpc::ServerContext *context, const kvstore::PutRequest *request, kvstore::PutResponse *response) override {
        std::lock_guard<std::mutex> lock(get_kv_singleton().mutex);

        // 今はRaftを経由せず直接B+tree
        get_kv_singleton().bptree.insert(toBytes(request->key()), toBytes(request->value()));
        response->set_success(true);
        response->set_error("");
        return grpc::Status::OK;
    }

    // Get
    grpc::Status Get(grpc::ServerContext *context, const kvstore::GetRequest *request, kvstore::GetResponse *response) override {
        std::lock_guard<std::mutex> lock(get_kv_singleton().mutex);

        ByteArray val = get_kv_singleton().bptree.search(toBytes(request->key()));
        if (!val.empty()) {
            response->set_found(true);
            response->set_value(fromBytes(val));
            response->set_error("");
        } else {
            response->set_found(false);
            response->set_error("not found");
        }
        return grpc::Status::OK;
    }

    // Delete
    grpc::Status Delete(grpc::ServerContext *context, const kvstore::DeleteRequest *request, kvstore::DeleteResponse *response) override {
        std::lock_guard<std::mutex> lock(get_kv_singleton().mutex);

        ByteArray val = get_kv_singleton().bptree.search(toBytes(request->key()));
        if (!val.empty()) {
            get_kv_singleton().bptree.remove(toBytes(request->key()));
            response->set_success(true);
            response->set_error("");
        } else {
            response->set_success(false);
            response->set_error("not found");
        }
        return grpc::Status::OK;
    }

    // 他のAPIは今は未実装
};

// サーバ起動
void RunServer(const std::string &address) {
    KVStoreServiceImpl service;
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "gRPCサーバ起動: " << address << std::endl;
    server->Wait();
}
