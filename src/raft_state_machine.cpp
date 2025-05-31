#include "raft_state_machine.hpp"
#include "libnuraft/nuraft.hxx"
#include <cstring>
#include <string>

ByteArray RaftStateMachine::getKey(const ByteArray &key) {
    return tree_.search(key);
};

std::vector<std::pair<ByteArray, ByteArray>> RaftStateMachine::scan(const ByteArray &min, const ByteArray &max) {
    return tree_.rangeSearch(min, max);
};

nuraft::ptr<nuraft::buffer> RaftStateMachine::enc_log(const op_payload &payload) {
    size_t total_size = sizeof(uint32_t) + payload.key.size() + sizeof(uint32_t) + payload.value.size();
    nuraft::ptr<nuraft::buffer> ret = nuraft::buffer::alloc(total_size);
    nuraft::buffer_serializer bs(ret);

    bs.put_u32((uint32_t)payload.key.size());
    bs.put_raw(payload.key.data(), payload.key.size());

    bs.put_u32((uint32_t)payload.value.size());
    bs.put_raw(payload.value.data(), payload.value.size());

    return ret;
}

void RaftStateMachine::dec_log(nuraft::buffer &log, op_payload &payload_out) {
    nuraft::buffer_serializer bs(log);

    uint32_t key_size = bs.get_u32();
    const char *key_data = reinterpret_cast<const char *>(bs.get_raw(key_size));
    payload_out.key.assign(key_data, key_size);

    uint32_t val_size = bs.get_u32();
    const char *val_data = reinterpret_cast<const char *>(bs.get_raw(val_size));
    payload_out.value.assign(val_data, val_size);
}

void RaftStateMachine::applyPayload(op_payload payload) {
    if (payload.value.empty()) {
        tree_.remove(toBytes(payload.key));
    } else {
        tree_.insert(toBytes(payload.key), toBytes(payload.value));
    }
};

nuraft::ptr<nuraft::buffer> RaftStateMachine::commit(const ulong log_idx, nuraft::buffer &data) {
    op_payload payload;
    dec_log(data, payload);

    applyPayload(payload);

    last_committed_idx_ = log_idx;

    nuraft::ptr<nuraft::buffer> ret = nuraft::buffer::alloc(sizeof(log_idx));
    nuraft::buffer_serializer bs(ret);
    bs.put_u64(log_idx);
    return ret;
}

void RaftStateMachine::commit_config(const ulong log_idx, nuraft::ptr<nuraft::cluster_config> &new_conf) {
    last_committed_idx_ = log_idx;
}

bool RaftStateMachine::apply_snapshot(nuraft::snapshot &s) {
    return true;
}

nuraft::ptr<nuraft::snapshot> RaftStateMachine::last_snapshot() {
    return nullptr;
}

ulong RaftStateMachine::last_commit_index() {
    return last_committed_idx_;
}

void RaftStateMachine::create_snapshot(nuraft::snapshot &s, nuraft::async_result<bool>::handler_type &when_done) {}

nuraft::ptr<nuraft::buffer> RaftStateMachine::pre_commit(const ulong log_idx, nuraft::buffer &data) {
    return nullptr;
}
