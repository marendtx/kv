#pragma once
#include "libnuraft/buffer_serializer.hxx"
#include "libnuraft/in_memory_log_store.hxx"
#include "libnuraft/nuraft.hxx"

class RaftStateMgr : public nuraft::state_mgr {
public:
    RaftStateMgr(int srv_id, const std::string &endpoint)
        : my_id_(srv_id), my_endpoint_(endpoint), cur_log_store_(nuraft::cs_new<nuraft::inmem_log_store>()) {
        my_srv_config_ = nuraft::cs_new<nuraft::srv_config>(srv_id, endpoint);

        // Initial cluster config: contains only one server (myself).
        saved_config_ = nuraft::cs_new<nuraft::cluster_config>();
        saved_config_->get_servers().push_back(my_srv_config_);
    }

    ~RaftStateMgr() {};

    nuraft::ptr<nuraft::cluster_config> load_config();

    void save_config(const nuraft::cluster_config &config);
    void save_state(const nuraft::srv_state &state);

    nuraft::ptr<nuraft::srv_state> read_state();

    nuraft::ptr<nuraft::log_store> load_log_store();

    nuraft::int32 server_id();

    void system_exit(const int exit_code);

    nuraft::ptr<nuraft::srv_config> get_srv_config() const;

private:
    int my_id_;
    std::string my_endpoint_;
    nuraft::ptr<nuraft::inmem_log_store> cur_log_store_;
    nuraft::ptr<nuraft::srv_config> my_srv_config_;
    nuraft::ptr<nuraft::cluster_config> saved_config_;
    nuraft::ptr<nuraft::srv_state> saved_state_;
};