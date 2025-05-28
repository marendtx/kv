#include "b_bplus_tree.hpp"
#include "libnuraft/buffer_serializer.hxx"
#include "libnuraft/in_memory_log_store.hxx"
#include "libnuraft/nuraft.hxx"
#include <cassert>
#include <cstddef>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

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
            // Just return in-memory data in this example.
            // May require reading from disk here, if it has been written to disk.
            return saved_config_;
        }

        void save_config(const cluster_config &config) {
            // Just keep in memory in this example.
            // Need to write to disk here, if want to make it durable.
            ptr<buffer> buf = config.serialize();
            saved_config_ = cluster_config::deserialize(*buf);
        }

        void save_state(const srv_state &state) {
            // Just keep in memory in this example.
            // Need to write to disk here, if want to make it durable.
            ptr<buffer> buf = state.serialize();
            saved_state_ = srv_state::deserialize(*buf);
        }

        ptr<srv_state> read_state() {
            // Just return in-memory data in this example.
            // May require reading from disk here, if it has been written to disk.
            return saved_state_;
        }

        ptr<log_store> load_log_store() {
            return cur_log_store_;
        }

        int32 server_id() {
            return my_id_;
        }

        void system_exit(const int exit_code) {
        }

        ptr<srv_config> get_srv_config() const { return my_srv_config_; }

    private:
        int my_id_;
        std::string my_endpoint_;
        ptr<inmem_log_store> cur_log_store_;
        ptr<srv_config> my_srv_config_;
        ptr<cluster_config> saved_config_;
        ptr<srv_state> saved_state_;
    };
}

class BPTreeStateMachine : public nuraft::state_machine {
public:
    BPTreeStateMachine(BPlusTree &tree) : tree_(tree), last_commit_idx_(0) {}

    // NuRaftがcommit時に呼ぶ
    nuraft::ptr<nuraft::buffer> commit(const ulong log_idx, nuraft::buffer &data) override {
        nuraft::buffer_serializer bs(data);
        std::string key = bs.get_str();
        std::string value = bs.get_str();

        ByteArray k = toBytes(key);
        ByteArray v = toBytes(value);
        tree_.insert(k, v);
        last_commit_idx_ = log_idx;
        std::cout << "[commit] " << key << " → " << value << std::endl;
        return nullptr;
    }

    // 必須API最低限
    bool apply_snapshot(nuraft::snapshot &) override { return true; }
    nuraft::ptr<nuraft::snapshot> last_snapshot() override { return nullptr; }
    ulong last_commit_index() override { return last_commit_idx_; }
    void create_snapshot(nuraft::snapshot &, nuraft::async_result<bool>::handler_type &) override {}

private:
    BPlusTree &tree_;
    ulong last_commit_idx_;
};

void RunServer(const std::string &address);

int main() {
    BPlusTree tree;

    // データ挿入
    tree.insert(toBytes("apple"), toBytes("red"));
    tree.insert(toBytes("banana"), toBytes("yellow"));
    tree.insert(toBytes("cherry"), toBytes("dark red"));
    tree.insert(toBytes("date"), toBytes("brown"));
    tree.insert(toBytes("fig"), toBytes("purple"));
    tree.insert(toBytes("grape"), toBytes("green"));

    std::cout << "Inserted records:\n";
    tree.traverse();

    // ツリー構造表示
    std::cout << "\nVisualizing B+ Tree:\n";
    tree.visualize();

    // 検索テスト
    auto val = tree.search(toBytes("cherry"));
    std::cout << "\nSearch 'cherry': ";
    if (!val.empty()) {
        std::cout << fromBytes(val) << "\n";
    } else {
        std::cout << "Not found\n";
    }

    // 削除テスト
    tree.remove(toBytes("date"));
    std::cout << "\nAfter removing 'date':\n";
    tree.traverse();

    // 範囲検索テスト
    auto range = tree.rangeSearch(toBytes("banana"), toBytes("grape"));
    std::cout << "\nRange search from 'banana' to 'grape':\n";
    for (auto &[k, v] : range) {
        std::cout << fromBytes(k) << " → " << fromBytes(v) << "\n";
    }

    // 永続化テスト
    std::string dir = "tree_data";
    tree.saveTree(dir);
    std::cout << "\nTree saved to disk.\n";

    BPlusTree reloaded;
    reloaded.loadTree(dir);
    std::cout << "\nReloaded tree from disk:\n";
    reloaded.traverse();
    reloaded.visualize();

    std::cout << std::endl
              << "///////////////////////alright!! B+tree seems to be fine!! Then, let's check out Raft!!/////////////////"
              << std::endl
              << std::endl;

    using namespace nuraft;

    // -- 1. B+treeインスタンス作成 --
    BPlusTree bptree;

    // -- 2. StateMachine, StateMgr, etc --
    auto state_machine = cs_new<BPTreeStateMachine>(bptree);
    auto state_mgr = cs_new<inmem_state_mgr>(1, "localhost:12345");
    raft_launcher launcher;
    asio_service::options asio_opt;
    raft_params params;
    int port = 12345;
    ptr<logger> my_logger = nullptr; // logger無しでOK

    // -- 3. Raft起動 --
    auto server = launcher.init(state_machine, state_mgr, my_logger, port, asio_opt, params);
    while (!server->is_initialized()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // -- 4. RaftでInsert: key="foo", value="bar" --
    {
        std::string key = "foo";
        std::string value = "bar";
        ptr<buffer> log = buffer::alloc(sizeof(int) * 2 + key.size() + value.size());
        buffer_serializer bs(log);
        bs.put_str(key);
        bs.put_str(value);
        auto result = server->append_entries({log});
        result->get();
    }

    // -- 5. B+treeに反映されているか確認 --
    ByteArray v = bptree.search(toBytes("foo"));
    std::cout << "[B+tree get] foo → " << fromBytes(v) << std::endl;

    // -- 6. いくつか書き込んで全部get --
    for (int i = 0; i < 3; ++i) {
        std::string key = "k" + std::to_string(i);
        std::string value = "v" + std::to_string(i);
        ptr<buffer> log = buffer::alloc(sizeof(int) * 2 + key.size() + value.size());
        buffer_serializer bs(log);
        bs.put_str(key);
        bs.put_str(value);
        auto result = server->append_entries({log});
        result->get();
    }
    for (int i = 0; i < 3; ++i) {
        std::string key = "k" + std::to_string(i);
        ByteArray v = bptree.search(toBytes(key));
        std::cout << "[B+tree get] " << key << " → " << fromBytes(v) << std::endl;
    }

    // -- 7. シャットダウン --
    launcher.shutdown();

    std::cout << std::endl
              << "///////////////////////alright!! Raft seems to be fine!! Then, let's check out gRPC!!/////////////////"
              << std::endl
              << std::endl;

    ///////////////////////////////////////////////////////

    RunServer("0.0.0.0:12345");

    return 0;
}
