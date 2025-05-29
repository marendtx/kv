#include "raft_server.hpp"
#include "libnuraft/nuraft.hxx"
#include "raft_state_machine.hpp"
#include "raft_state_mgr.hpp"

#include <string>

RaftServer::RaftServer(int id, int raftPort, nuraft::ptr<RaftStateMachine> stateMachine) {
    nuraft::ptr<nuraft::logger> my_logger = nullptr;
    state_machine_ = stateMachine;
    nuraft::ptr<nuraft::state_mgr> my_state_manager = nuraft::cs_new<RaftStateMgr>(id, "localhost:" + std::to_string(raftPort));
    nuraft::asio_service::options asio_opt;
    nuraft::raft_params params;

    launcher_ = nuraft::cs_new<nuraft::raft_launcher>();
    server_ = launcher_->init(state_machine_, my_state_manager, my_logger, raftPort, asio_opt, params);

    while (!server_->is_initialized()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

RaftServer::~RaftServer() {
    launcher_->shutdown();
}

ByteArray RaftServer::getKey(const ByteArray &key) {
    return state_machine_->getKey(key);
}
int RaftServer::putKey(const ByteArray &key, const ByteArray &value) {
    nuraft::ptr<nuraft::buffer> new_log = state_machine_->enc_log({key, value});
    auto result = server_->append_entries({new_log});
    return result->get_result_code();
}

int RaftServer::deleteKey(const ByteArray &key) {
    nuraft::ptr<nuraft::buffer> new_log = state_machine_->enc_log({key, {}});
    auto result = server_->append_entries({new_log});
    return result->get_result_code();
}

std::vector<std::pair<ByteArray, ByteArray>> RaftServer::scanKey(const ByteArray &min, const ByteArray &max) {
    return state_machine_->scan(min, max);
}

int RaftServer::addServer(std::string id, std::string address) {
    nuraft::srv_config srv_conf_to_add(std::stoi(id), address);
    auto result = server_->add_srv(srv_conf_to_add);
    return result->get_result_code();
}

std::vector<RaftServer::Server> RaftServer::listServers() {
    std::vector<nuraft::ptr<nuraft::srv_config>> configs;
    server_->get_srv_config_all(configs);

    int leader_id = server_->get_leader();

    std::vector<RaftServer::Server> servers;
    for (auto &entry : configs) {
        nuraft::ptr<nuraft::srv_config> &srv = entry;
        RaftServer::Server server;
        server.id = srv->get_id();
        server.address = srv->get_endpoint();
        server.isLeader = leader_id == srv->get_id();
        servers.push_back(server);
    }
    return servers;
}
