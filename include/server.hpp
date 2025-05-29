#pragma once
#include "grpc_server.hpp"
#include "libnuraft/nuraft.hxx"
#include "raft_server.hpp"
#include "raft_state_machine.hpp"

void RunServer(int grpcPort, int raftPort, int raftId);