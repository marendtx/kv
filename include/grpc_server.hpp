#pragma once
#include "bptree.hpp"
#include "kvstore.grpc.pb.h"
#include "libnuraft/nuraft.hxx"
#include "raft_server.hpp"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include <string>

class GrpcServer final : public kvstore::KVStore::Service {
private:
    nuraft::ptr<RaftServer> raft_server_;

public:
    GrpcServer(nuraft::ptr<RaftServer> rs) : raft_server_(rs) {}
    grpc::Status Put(grpc::ServerContext *context, const kvstore::PutRequest *request, kvstore::PutResponse *response) override;
    grpc::Status Get(grpc::ServerContext *context, const kvstore::GetRequest *request, kvstore::GetResponse *response) override;
    grpc::Status Delete(grpc::ServerContext *context, const kvstore::DeleteRequest *request, kvstore::DeleteResponse *response) override;

    grpc::Status Scan(grpc::ServerContext *context, const kvstore::ScanRequest *request, kvstore::ScanResponse *response) override;

    grpc::Status AddServer(grpc::ServerContext *context, const kvstore::AddServerRequest *request, kvstore::AddServerResponse *response) override;

    grpc::Status ListServers(grpc::ServerContext *context, const kvstore::ListServersRequest *request, kvstore::ListServersResponse *response) override;
};
