#include "raft_state_mgr.hpp"

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

nuraft::ptr<nuraft::cluster_config> RaftStateMgr::load_config() {
    std::cout << "state_mgr: load_config" << std::endl;
    return saved_config_;
}

void RaftStateMgr::save_config(const nuraft::cluster_config &config) {
    std::cout << "state_mgr: save_config" << std::endl;
    nuraft::ptr<nuraft::buffer> buf = config.serialize();
    saved_config_ = nuraft::cluster_config::deserialize(*buf);
}

void RaftStateMgr::save_state(const nuraft::srv_state &state) {
    std::cout << "state_mgr: save_state" << std::endl;
    nuraft::ptr<nuraft::buffer> buf = state.serialize();
    saved_state_ = nuraft::srv_state::deserialize(*buf);
}

nuraft::ptr<nuraft::srv_state> RaftStateMgr::read_state() {
    std::cout << "state_mgr: read_state" << std::endl;
    return saved_state_;
}

nuraft::ptr<nuraft::log_store> RaftStateMgr::load_log_store() {
    std::cout << "state_mgr: load_log_store" << std::endl;
    return cur_log_store_;
}

nuraft::int32 RaftStateMgr::server_id() {
    std::cout << "state_mgr: server_id" << std::endl;
    return my_id_;
}

void RaftStateMgr::system_exit(const int exit_code) {
    std::cout << "state_mgr: system_exit" << std::endl;
}

nuraft::ptr<nuraft::srv_config> RaftStateMgr::get_srv_config() const {
    std::cout << "state_mgr: get_src_config" << std::endl;
    return my_srv_config_;
}
