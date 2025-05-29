#include "bptree.hpp"
#include "kvstore.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

// NuRaft
#include "libnuraft/buffer_serializer.hxx"
#include "libnuraft/in_memory_log_store.hxx"
#include "libnuraft/nuraft.hxx"

using namespace nuraft;

// ------------- シングルトン ------------- //
struct KVSingletons {
    BPTree bptree;
    ptr<raft_server> raft;
    ptr<state_machine> raft_state_machine;
    std::mutex mutex;
};
KVSingletons &get_kv_singleton() {
    static KVSingletons s;
    return s;
}

// ------------- StateMachine ------------- //
class BPTreeStateMachine : public state_machine {
public:
    BPTreeStateMachine(BPTree &tree) : tree_(tree), last_commit_idx_(0) {}

    ptr<buffer> commit(const ulong log_idx, buffer &data) override {
        std::cout << "state_machine: commit" << std::endl;

        buffer_serializer bs(data);
        // ここではPut/Delete両対応（op: 0=put, 1=del）
        int op = bs.get_u8();
        std::string key = bs.get_str();

        if (op == 0) { // Put
            std::string value = bs.get_str();
            tree_.insert(toBytes(key), toBytes(value));
            std::cout << "[commit/put] " << key << " → " << value << std::endl;
        } else if (op == 1) { // Delete
            tree_.remove(toBytes(key));
            std::cout << "[commit/del] " << key << std::endl;
        }

        last_commit_idx_ = log_idx;
        return nullptr;
    }

    ptr<buffer> commit_ext(const ext_op_params &params) {
        std::cout << "state_machine: commit_ext" << std::endl;
        return commit(params.log_idx, *params.data);
    }
    void commit_config(const ulong log_idx, ptr<cluster_config> &new_conf) {
        std::cout << "state_machine: commit_config" << std::endl;
    }
    ptr<buffer> pre_commit(const ulong log_idx, buffer &data) {
        std::cout << "state_machine: pre_commit" << std::endl;
        return nullptr;
    }
    ptr<buffer> pre_commit_ext(const ext_op_params &params) {
        std::cout << "state_machine: pre_commit_ext" << std::endl;
        return pre_commit(params.log_idx, *params.data);
    }
    void rollback(const ulong log_idx, buffer &data) {
        std::cout << "state_machine: rollback" << std::endl;
    }
    void rollback_config(const ulong log_idx, ptr<cluster_config> &conf) {
        std::cout << "state_machine: rollback_config" << std::endl;
    }
    void rollback_ext(const ext_op_params &params) {
        std::cout << "state_machine: rollback_ext" << std::endl;
        rollback(params.log_idx, *params.data);
    }
    int64 get_next_batch_size_hint_in_bytes() {
        std::cout << "state_machine: get_next_batch_size_hint_in_bytes" << std::endl;
        return 0;
    }
    void save_snapshot_data(snapshot &s, const ulong offset, buffer &data) {
        std::cout << "state_machine: save_snapshot_data" << std::endl;
    }
    void save_logical_snp_obj(snapshot &s, ulong &obj_id, buffer &data, bool is_first_obj, bool is_last_obj) {
        std::cout << "state_machine: save_logical_snp_obj" << std::endl;
    }

    bool apply_snapshot(snapshot &) override {
        std::cout << "state_machine: apply_snapshot" << std::endl;
        return true;
    }
    int read_snapshot_data(snapshot &s, const ulong offset, buffer &data) {
        std::cout << "state_machine: read_snapshot_data" << std::endl;
        return 0;
    }
    int read_logical_snp_obj(snapshot &s, void *&user_snp_ctx, ulong obj_id, ptr<buffer> &data_out, bool &is_last_obj) {
        std::cout << "state_machine: read_logical_snp_obj" << std::endl;
        data_out = buffer::alloc(4); // A dummy buffer.
        is_last_obj = true;
        return 0;
    }
    void free_user_snp_ctx(void *&user_snp_ctx) {
        std::cout << "state_machine: free_user_snp_ctx" << std::endl;
    }

    ptr<snapshot> last_snapshot() override {
        std::cout << "state_machine: last_snapshot" << std::endl;
        return nullptr;
    }
    ulong last_commit_index() override {
        std::cout << "state_machine: last_commit_index" << std::endl;
        return last_commit_idx_;
    }
    void create_snapshot(snapshot &, async_result<bool>::handler_type &) override {
        std::cout << "state_machine: create_snapshot" << std::endl;
    }

    bool chk_create_snapshot() {
        std::cout << "state_machine: chk_create_snapshot" << std::endl;
        return true;
    }

    bool allow_leadership_transfer() {
        std::cout << "state_machine: allow_leadership_transfer" << std::endl;
        return true;
    }

    uint64_t adjust_commit_index(const adjust_commit_index_params &params) {
        std::cout << "state_machine: adjust_commit_index" << std::endl;
        return params.expected_commit_index_;
    }

private:
    BPTree &tree_;
    ulong last_commit_idx_;
};

// ------------- KVStoreサービス実装 ------------- //
class KVStoreServiceImpl final : public kvstore::KVStore::Service {
public:
    // Put（Raft経由）
    grpc::Status Put(grpc::ServerContext *context, const kvstore::PutRequest *request, kvstore::PutResponse *response) override {
        std::lock_guard<std::mutex> lock(get_kv_singleton().mutex);

        // 1byte: op(0=put), string: key, string: value
        std::string key = request->key();
        std::string value = request->value();
        auto log = buffer::alloc(1 + sizeof(int) * 2 + key.size() + value.size());
        buffer_serializer bs(log);
        bs.put_u8(0); // 0=put
        bs.put_str(key);
        bs.put_str(value);

        auto raft = get_kv_singleton().raft;
        if (!raft->is_leader()) {
            response->set_success(false);
            response->set_error("not leader");
            return grpc::Status::OK;
        }
        auto result = raft->append_entries({log});
        try {
            result->get(); // commit待ち
            response->set_success(true);
            response->set_error("");
        } catch (std::exception &e) {
            response->set_success(false);
            response->set_error(e.what());
        }
        return grpc::Status::OK;
    }

    // Get（B+tree直読み、分散Read時は工夫が必要）
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

    // Delete（Raft経由）
    grpc::Status Delete(grpc::ServerContext *context, const kvstore::DeleteRequest *request, kvstore::DeleteResponse *response) override {
        std::lock_guard<std::mutex> lock(get_kv_singleton().mutex);

        std::string key = request->key();
        auto log = buffer::alloc(1 + sizeof(int) + key.size());
        buffer_serializer bs(log);
        bs.put_u8(1); // 1=delete
        bs.put_str(key);

        auto raft = get_kv_singleton().raft;
        if (!raft->is_leader()) {
            response->set_success(false);
            response->set_error("not leader");
            return grpc::Status::OK;
        }
        auto result = raft->append_entries({log});
        try {
            result->get();
            response->set_success(true);
            response->set_error("");
        } catch (std::exception &e) {
            response->set_success(false);
            response->set_error(e.what());
        }
        return grpc::Status::OK;
    }

    // --- Join（ノード追加: リーダーが処理） ---
    grpc::Status Join(grpc::ServerContext *context, const kvstore::JoinRequest *request, kvstore::JoinResponse *response) override {
        std::cout << "server joined: " << request->node_id() << std::endl;

        auto raft = get_kv_singleton().raft;
        if (!raft || !raft->is_leader()) {
            response->set_success(false);
            response->set_error("not leader");
            return grpc::Status::OK;
        }

        int new_node_id;
        try {
            new_node_id = std::stoi(request->node_id());
        } catch (...) {
            response->set_success(false);
            response->set_error("invalid node_id");
            return grpc::Status::OK;
        }
        std::string endpoint = request->raft_endpoint();

        // すでにクラスタに存在しないかチェック
        std::vector<ptr<nuraft::srv_config>> configs;
        raft->get_srv_config_all(configs);
        for (auto &s : configs) {
            if (s->get_id() == new_node_id) {
                response->set_success(false);
                response->set_error("already exists");
                return grpc::Status::OK;
            }
        }

        nuraft::srv_config srv_conf_to_add(new_node_id, endpoint);
        // ★ここが公式サンプル流
        auto ret = raft->add_srv(srv_conf_to_add);
        if (!ret || !ret->get_accepted()) {
            response->set_success(false);
            response->set_error("add_srv failed: " + std::to_string(ret ? ret->get_result_code() : -1));
            return grpc::Status::OK;
        }
        response->set_success(true);
        response->set_error("");
        return grpc::Status::OK;
    }

    // --- Leave（ノード除去: リーダーが処理） ---
    grpc::Status Leave(grpc::ServerContext *context, const kvstore::LeaveRequest *request, kvstore::LeaveResponse *response) override {
        auto raft = get_kv_singleton().raft;
        if (!raft || !raft->is_leader()) {
            response->set_success(false);
            response->set_error("not leader");
            return grpc::Status::OK;
        }

        int node_id;
        try {
            node_id = std::stoi(request->node_id());
        } catch (...) {
            response->set_success(false);
            response->set_error("invalid node_id");
            return grpc::Status::OK;
        }

        // 存在確認
        std::vector<ptr<nuraft::srv_config>> configs;
        raft->get_srv_config_all(configs);
        bool found = false;
        for (auto &s : configs) {
            if (s->get_id() == node_id)
                found = true;
        }
        if (!found) {
            response->set_success(false);
            response->set_error("node not found");
            return grpc::Status::OK;
        }

        // 公式サンプル流 remove_srv
        auto ret = raft->remove_srv(node_id);
        if (!ret || !ret->get_accepted()) {
            response->set_success(false);
            response->set_error("remove_srv failed: " + std::to_string(ret ? ret->get_result_code() : -1));
            return grpc::Status::OK;
        }
        response->set_success(true);
        response->set_error("");
        return grpc::Status::OK;
    }

    // クラスタ状態取得
    grpc::Status Status(grpc::ServerContext *context, const kvstore::StatusRequest *request, kvstore::StatusResponse *response) override {
        auto &kv = get_kv_singleton();
        auto raft = kv.raft;
        response->set_node_id(raft->get_id());
        response->set_leader_id(raft->get_leader());
        response->set_commit_index(raft->get_committed_log_idx());

        // Raft状態をテキストで
        std::string raft_state = "maybe follower";
        if (raft->is_leader())
            raft_state = "leader";
        response->set_raft_state(raft_state);

        return grpc::Status::OK;
    }

    // クラスタメンバー一覧
    grpc::Status ListMembers(grpc::ServerContext *context, const kvstore::ListMembersRequest *request, kvstore::ListMembersResponse *response) override {
        auto &kv = get_kv_singleton();
        auto raft = kv.raft;

        std::vector<ptr<nuraft::srv_config>> configs;
        raft->get_srv_config_all(configs);
        int leader_id = raft->get_leader();

        for (const auto &cfg : configs) {
            auto *member = response->add_members();
            member->set_id(cfg->get_id());
            member->set_endpoint(cfg->get_endpoint());
            member->set_is_leader(cfg->get_id() == leader_id);
        }
        return grpc::Status::OK;
    }
};

// サーバメンバー一覧表示
void server_list(const ptr<raft_server> &raft) {
    std::vector<ptr<srv_config>> configs;
    raft->get_srv_config_all(configs);
    int leader_id = raft->get_leader();
    std::cout << "[Members]\n";
    for (auto &srv : configs) {
        std::cout << "  id: " << srv->get_id()
                  << "  endpoint: " << srv->get_endpoint();
        if (srv->get_id() == leader_id)
            std::cout << " (LEADER)";
        std::cout << std::endl;
    }
}
void server_status(const ptr<raft_server> &raft) {
    std::cout << "[Status]\n"
              << "  node_id: " << raft->get_id() << "\n"
              << "  leader_id: " << raft->get_leader() << "\n"
              << "  commit_index: " << raft->get_committed_log_idx() << "\n";
}

void server_console(const ptr<raft_server> &raft) {
    std::string cmd;
    std::cout << "serverコマンド: list/status/quit\n";
    while (std::cout << "server> ", std::getline(std::cin, cmd)) {
        if (cmd == "list")
            server_list(raft);
        else if (cmd == "status")
            server_status(raft);
        else if (cmd == "quit" || cmd == "exit")
            break;
        else
            std::cout << "コマンド: list/status/quit\n";
    }
}

void RunServer(int node_id, const std::string &grpc_host, const int grpc_port, const std::string &raft_host, int raft_port) {
    // auto raft_endpoint = raft_host + ":" + std::to_string(raft_port);
    // auto grpc_endpoint = grpc_host + ":" + std::to_string(grpc_port);

    // auto &kv = get_kv_singleton();
    // kv.raft_state_machine = cs_new<BPTreeStateMachine>(kv.bptree);
    // auto state_mgr = cs_new<inmem_state_mgr>(node_id, raft_endpoint);
    // raft_launcher launcher;
    // asio_service::options asio_opt;
    // raft_params params;
    // ptr<logger> my_logger = nullptr;

    // kv.raft = launcher.init(kv.raft_state_machine, state_mgr, my_logger, raft_port, asio_opt, params);
    // if (!kv.raft) {
    //     std::cerr << "Raft init failed! launcher.init() returned nullptr." << std::endl;
    //     std::exit(1);
    // }
    // while (!kv.raft->is_initialized()) {
    //     std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // }
    // std::cout << "Raft initialized. NodeID=" << node_id << ", endpoint=" << raft_endpoint << std::endl;

    // // gRPCサーバ起動（別スレッドで）
    // KVStoreServiceImpl service;
    // grpc::ServerBuilder builder;
    // builder.AddListeningPort(grpc_endpoint, grpc::InsecureServerCredentials());
    // builder.RegisterService(&service);
    // std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    // std::cout << "gRPCサーバ起動: " << grpc_endpoint << std::endl;

    // // インタラクティブコマンドループ（メインスレッド）
    // server_console(kv.raft);

    // // サーバ停止
    // server->Shutdown();
    // launcher.shutdown();
}