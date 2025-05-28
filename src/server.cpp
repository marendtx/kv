#include "b_bplus_tree.hpp"
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

namespace nuraft {

    inmem_log_store::inmem_log_store()
        : start_idx_(1), raft_server_bwd_pointer_(nullptr), disk_emul_delay(0), disk_emul_thread_(nullptr), disk_emul_thread_stop_signal_(false), disk_emul_last_durable_index_(0) {
        // Dummy entry for index 0.
        ptr<buffer> buf = buffer::alloc(sz_ulong);
        logs_[0] = cs_new<log_entry>(0, buf);
    }

    inmem_log_store::~inmem_log_store() {
        if (disk_emul_thread_) {
            disk_emul_thread_stop_signal_ = true;
            disk_emul_ea_.invoke();
            if (disk_emul_thread_->joinable()) {
                disk_emul_thread_->join();
            }
        }
    }

    ptr<log_entry> inmem_log_store::make_clone(const ptr<log_entry> &entry) {
        // NOTE:
        //   Timestamp is used only when `replicate_log_timestamp_` option is on.
        //   Otherwise, log store does not need to store or load it.
        ptr<log_entry> clone = cs_new<log_entry>(entry->get_term(),
                                                 buffer::clone(entry->get_buf()),
                                                 entry->get_val_type(),
                                                 entry->get_timestamp(),
                                                 entry->has_crc32(),
                                                 entry->get_crc32(),
                                                 false);
        return clone;
    }

    ulong inmem_log_store::next_slot() const {
        std::lock_guard<std::mutex> l(logs_lock_);
        // Exclude the dummy entry.
        return start_idx_ + logs_.size() - 1;
    }

    ulong inmem_log_store::start_index() const {
        return start_idx_;
    }

    ptr<log_entry> inmem_log_store::last_entry() const {
        ulong next_idx = next_slot();
        std::lock_guard<std::mutex> l(logs_lock_);
        auto entry = logs_.find(next_idx - 1);
        if (entry == logs_.end()) {
            entry = logs_.find(0);
        }

        return make_clone(entry->second);
    }

    ulong inmem_log_store::append(ptr<log_entry> &entry) {
        ptr<log_entry> clone = make_clone(entry);

        std::lock_guard<std::mutex> l(logs_lock_);
        size_t idx = start_idx_ + logs_.size() - 1;
        logs_[idx] = clone;

        if (disk_emul_delay) {
            uint64_t cur_time = timer_helper::get_timeofday_us();
            disk_emul_logs_being_written_[cur_time + disk_emul_delay * 1000] = idx;
            disk_emul_ea_.invoke();
        }

        return idx;
    }

    void inmem_log_store::write_at(ulong index, ptr<log_entry> &entry) {
        ptr<log_entry> clone = make_clone(entry);

        // Discard all logs equal to or greater than `index.
        std::lock_guard<std::mutex> l(logs_lock_);
        auto itr = logs_.lower_bound(index);
        while (itr != logs_.end()) {
            itr = logs_.erase(itr);
        }
        logs_[index] = clone;

        if (disk_emul_delay) {
            uint64_t cur_time = timer_helper::get_timeofday_us();
            disk_emul_logs_being_written_[cur_time + disk_emul_delay * 1000] = index;

            // Remove entries greater than `index`.
            auto entry = disk_emul_logs_being_written_.begin();
            while (entry != disk_emul_logs_being_written_.end()) {
                if (entry->second > index) {
                    entry = disk_emul_logs_being_written_.erase(entry);
                } else {
                    entry++;
                }
            }
            disk_emul_ea_.invoke();
        }
    }

    ptr<std::vector<ptr<log_entry>>>
    inmem_log_store::log_entries(ulong start, ulong end) {
        ptr<std::vector<ptr<log_entry>>> ret =
            cs_new<std::vector<ptr<log_entry>>>();

        ret->resize(end - start);
        ulong cc = 0;
        for (ulong ii = start; ii < end; ++ii) {
            ptr<log_entry> src = nullptr;
            {
                std::lock_guard<std::mutex> l(logs_lock_);
                auto entry = logs_.find(ii);
                if (entry == logs_.end()) {
                    entry = logs_.find(0);
                    assert(0);
                }
                src = entry->second;
            }
            (*ret)[cc++] = make_clone(src);
        }
        return ret;
    }

    ptr<std::vector<ptr<log_entry>>>
    inmem_log_store::log_entries_ext(ulong start,
                                     ulong end,
                                     int64 batch_size_hint_in_bytes) {
        ptr<std::vector<ptr<log_entry>>> ret =
            cs_new<std::vector<ptr<log_entry>>>();

        if (batch_size_hint_in_bytes < 0) {
            return ret;
        }

        size_t accum_size = 0;
        for (ulong ii = start; ii < end; ++ii) {
            ptr<log_entry> src = nullptr;
            {
                std::lock_guard<std::mutex> l(logs_lock_);
                auto entry = logs_.find(ii);
                if (entry == logs_.end()) {
                    entry = logs_.find(0);
                    assert(0);
                }
                src = entry->second;
            }
            ret->push_back(make_clone(src));
            accum_size += src->get_buf().size();
            if (batch_size_hint_in_bytes &&
                accum_size >= (ulong)batch_size_hint_in_bytes)
                break;
        }
        return ret;
    }

    ptr<log_entry> inmem_log_store::entry_at(ulong index) {
        ptr<log_entry> src = nullptr;
        {
            std::lock_guard<std::mutex> l(logs_lock_);
            auto entry = logs_.find(index);
            if (entry == logs_.end()) {
                entry = logs_.find(0);
            }
            src = entry->second;
        }
        return make_clone(src);
    }

    ulong inmem_log_store::term_at(ulong index) {
        ulong term = 0;
        {
            std::lock_guard<std::mutex> l(logs_lock_);
            auto entry = logs_.find(index);
            if (entry == logs_.end()) {
                entry = logs_.find(0);
            }
            term = entry->second->get_term();
        }
        return term;
    }

    ptr<buffer> inmem_log_store::pack(ulong index, int32 cnt) {
        std::vector<ptr<buffer>> logs;

        size_t size_total = 0;
        for (ulong ii = index; ii < index + cnt; ++ii) {
            ptr<log_entry> le = nullptr;
            {
                std::lock_guard<std::mutex> l(logs_lock_);
                le = logs_[ii];
            }
            assert(le.get());
            ptr<buffer> buf = le->serialize();
            size_total += buf->size();
            logs.push_back(buf);
        }

        ptr<buffer> buf_out = buffer::alloc(sizeof(int32) +
                                            cnt * sizeof(int32) +
                                            size_total);
        buf_out->pos(0);
        buf_out->put((int32)cnt);

        for (auto &entry : logs) {
            ptr<buffer> &bb = entry;
            buf_out->put((int32)bb->size());
            buf_out->put(*bb);
        }
        return buf_out;
    }

    void inmem_log_store::apply_pack(ulong index, buffer &pack) {
        pack.pos(0);
        int32 num_logs = pack.get_int();

        for (int32 ii = 0; ii < num_logs; ++ii) {
            ulong cur_idx = index + ii;
            int32 buf_size = pack.get_int();

            ptr<buffer> buf_local = buffer::alloc(buf_size);
            pack.get(buf_local);

            ptr<log_entry> le = log_entry::deserialize(*buf_local);
            {
                std::lock_guard<std::mutex> l(logs_lock_);
                logs_[cur_idx] = le;
            }
        }

        {
            std::lock_guard<std::mutex> l(logs_lock_);
            auto entry = logs_.upper_bound(0);
            if (entry != logs_.end()) {
                start_idx_ = entry->first;
            } else {
                start_idx_ = 1;
            }
        }
    }

    bool inmem_log_store::compact(ulong last_log_index) {
        std::lock_guard<std::mutex> l(logs_lock_);
        for (ulong ii = start_idx_; ii <= last_log_index; ++ii) {
            auto entry = logs_.find(ii);
            if (entry != logs_.end()) {
                logs_.erase(entry);
            }
        }

        // WARNING:
        //   Even though nothing has been erased,
        //   we should set `start_idx_` to new index.
        if (start_idx_ <= last_log_index) {
            start_idx_ = last_log_index + 1;
        }
        return true;
    }

    bool inmem_log_store::flush() {
        disk_emul_last_durable_index_ = next_slot() - 1;
        return true;
    }

    void inmem_log_store::close() {}

    void inmem_log_store::set_disk_delay(raft_server *raft, size_t delay_ms) {
        disk_emul_delay = delay_ms;
        raft_server_bwd_pointer_ = raft;

        if (!disk_emul_thread_) {
            disk_emul_thread_ =
                std::unique_ptr<std::thread>(
                    new std::thread(&inmem_log_store::disk_emul_loop, this));
        }
    }

    ulong inmem_log_store::last_durable_index() {
        uint64_t last_log = next_slot() - 1;
        if (!disk_emul_delay) {
            return last_log;
        }

        return disk_emul_last_durable_index_;
    }

    void inmem_log_store::disk_emul_loop() {
        // This thread mimics async disk writes.

        size_t next_sleep_us = 100 * 1000;
        while (!disk_emul_thread_stop_signal_) {
            disk_emul_ea_.wait_us(next_sleep_us);
            disk_emul_ea_.reset();
            if (disk_emul_thread_stop_signal_)
                break;

            uint64_t cur_time = timer_helper::get_timeofday_us();
            next_sleep_us = 100 * 1000;

            bool call_notification = false;
            {
                std::lock_guard<std::mutex> l(logs_lock_);
                // Remove all timestamps equal to or smaller than `cur_time`,
                // and pick the greatest one among them.
                auto entry = disk_emul_logs_being_written_.begin();
                while (entry != disk_emul_logs_being_written_.end()) {
                    if (entry->first <= cur_time) {
                        disk_emul_last_durable_index_ = entry->second;
                        entry = disk_emul_logs_being_written_.erase(entry);
                        call_notification = true;
                    } else {
                        break;
                    }
                }

                entry = disk_emul_logs_being_written_.begin();
                if (entry != disk_emul_logs_being_written_.end()) {
                    next_sleep_us = entry->first - cur_time;
                }
            }

            if (call_notification) {
                raft_server_bwd_pointer_->notify_log_append_completion(true);
            }
        }
    }

    class inmem_state_mgr : public state_mgr {
    public:
        inmem_state_mgr(int srv_id,
                        const std::string &endpoint)
            : my_id_(srv_id), my_endpoint_(endpoint), cur_log_store_(cs_new<inmem_log_store>()) {
            my_srv_config_ = cs_new<srv_config>(srv_id, endpoint);

            // Initial cluster config: contains only one server (myself).
            saved_config_ = cs_new<cluster_config>();
            saved_config_->get_servers().push_back(my_srv_config_);
        }

        ~inmem_state_mgr() {}

        ptr<cluster_config> load_config() {
            std::cout << "state_mgr: load_config" << std::endl;
            return saved_config_;
        }

        void save_config(const cluster_config &config) {
            std::cout << "state_mgr: save_config" << std::endl;
            ptr<buffer> buf = config.serialize();
            saved_config_ = cluster_config::deserialize(*buf);
        }

        void save_state(const srv_state &state) {
            std::cout << "state_mgr: save_state" << std::endl;
            ptr<buffer> buf = state.serialize();
            saved_state_ = srv_state::deserialize(*buf);
        }

        ptr<srv_state> read_state() {
            std::cout << "state_mgr: read_state" << std::endl;
            return saved_state_;
        }

        ptr<log_store> load_log_store() {
            std::cout << "state_mgr: load_log_store" << std::endl;
            return cur_log_store_;
        }

        int32 server_id() {
            std::cout << "state_mgr: server_id" << std::endl;
            return my_id_;
        }

        void system_exit(const int exit_code) {
            std::cout << "state_mgr: system_exit" << std::endl;
        }

        ptr<srv_config> get_srv_config() const {
            std::cout << "state_mgr: get_src_config" << std::endl;
            return my_srv_config_;
        }

    private:
        int my_id_;
        std::string my_endpoint_;
        ptr<inmem_log_store> cur_log_store_;
        ptr<srv_config> my_srv_config_;
        ptr<cluster_config> saved_config_;
        ptr<srv_state> saved_state_;
    };
}

using namespace nuraft;

// ------------- シングルトン ------------- //
struct KVSingletons {
    BPlusTree bptree;
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
    BPTreeStateMachine(BPlusTree &tree) : tree_(tree), last_commit_idx_(0) {}

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
    BPlusTree &tree_;
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

// ------------- サーバ起動 ------------- //
void RunServer(int node_id, const std::string &grpc_host, const int grpc_port, const std::string &raft_host, int raft_port) {
    auto raft_endpoint = raft_host + ":" + std::to_string(raft_port);
    auto grpc_endpoint = grpc_host + ":" + std::to_string(grpc_port);
    // Raftセットアップ（1ノード用。複数ノード時はクラスタ設定要調整）
    auto &kv = get_kv_singleton();
    kv.raft_state_machine = cs_new<BPTreeStateMachine>(kv.bptree);
    auto state_mgr = cs_new<inmem_state_mgr>(node_id, raft_endpoint);
    raft_launcher launcher;
    asio_service::options asio_opt;
    raft_params params;
    ptr<logger> my_logger = nullptr;

    kv.raft = launcher.init(kv.raft_state_machine, state_mgr, my_logger, raft_port, asio_opt, params);
    if (!kv.raft) {
        std::cerr << "Raft init failed! launcher.init() returned nullptr." << std::endl;
        std::exit(1);
    }
    while (!kv.raft->is_initialized()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "Raft initialized. NodeID=" << node_id << ", endpoint=" << raft_endpoint << std::endl;

    // gRPCサーバ起動
    KVStoreServiceImpl service;
    grpc::ServerBuilder builder;
    builder.AddListeningPort(grpc_endpoint, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "gRPCサーバ起動: " << grpc_endpoint << std::endl;
    server->Wait();

    // サーバ停止時はRaftをシャットダウン
    launcher.shutdown();
}
