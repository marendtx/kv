#pragma once
#include "bptree.hpp"
#include "libnuraft/nuraft.hxx"
#include <cstring>
#include <string>

class RaftStateMachine : public nuraft::state_machine {
private:
    BPTree tree_;
    std::atomic<uint64_t> last_committed_idx_ = 0;

public:
    struct op_payload {
        ByteArray key;
        ByteArray value;
    };

    ByteArray getKey(const ByteArray &key);
    std::vector<std::pair<ByteArray, ByteArray>> scan(const ByteArray &min, const ByteArray &max);

    nuraft::ptr<nuraft::buffer> enc_log(const op_payload &payload);

    void dec_log(nuraft::buffer &log, op_payload &payload_out);

    void applyPayload(op_payload payload);

    nuraft::ptr<nuraft::buffer> commit(const ulong log_idx, nuraft::buffer &data);

    void commit_config(const ulong log_idx, nuraft::ptr<nuraft::cluster_config> &new_conf);

    bool apply_snapshot(nuraft::snapshot &s);

    nuraft::ptr<nuraft::snapshot> last_snapshot();

    ulong last_commit_index();

    void create_snapshot(nuraft::snapshot &s, nuraft::async_result<bool>::handler_type &when_done);

    nuraft::ptr<nuraft::buffer> pre_commit(const ulong log_idx, nuraft::buffer &data);
};
