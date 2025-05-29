#pragma once
#include "libnuraft/nuraft.hxx"
#include "raft_state_machine.hpp"
#include "raft_state_mgr.hpp"

#include <string>

class RaftServer {
private:
    nuraft::ptr<nuraft::raft_server> server_;
    nuraft::ptr<nuraft::raft_launcher> launcher_;
    nuraft::ptr<RaftStateMachine> state_machine_;

public:
    struct Server {
        std::string id;
        std::string address;
        bool isLeader;
    };
    RaftServer(int id, int raftPort, nuraft::ptr<RaftStateMachine> stateMachine);
    ~RaftServer();
    ByteArray getKey(const ByteArray &key);
    int putKey(const ByteArray &key, const ByteArray &value);
    int deleteKey(const ByteArray &key);

    std::vector<std::pair<ByteArray, ByteArray>> scanKey(const ByteArray &min, const ByteArray &max);
    int addServer(std::string id, std::string address);

    std::vector<RaftServer::Server> listServers();
};
