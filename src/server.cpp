#include "grpc_server.hpp"
#include "libnuraft/nuraft.hxx"
#include "raft_server.hpp"
#include "raft_state_machine.hpp"

std::unique_ptr<grpc::Server> server;

void RunServer(int grpcPort, int raftPort, int raftId) {
    std::string server_address("0.0.0.0:" + std::to_string(grpcPort));
    nuraft::ptr<RaftStateMachine> sm = nuraft::cs_new<RaftStateMachine>();
    nuraft::ptr<RaftServer> rs = nuraft::cs_new<RaftServer>(raftId, raftPort, sm);

    GrpcServer::Service *service = new GrpcServer(rs);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service);

    grpc::reflection::InitProtoReflectionServerBuilderPlugin();

    server = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    server->Wait();
    delete service;
}
