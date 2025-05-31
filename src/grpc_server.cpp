#include "grpc_server.hpp"
#include "bptree.hpp"
#include "kvstore.grpc.pb.h"
#include "libnuraft/nuraft.hxx"
#include "raft_server.hpp"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include <string>

grpc::Status GrpcServer::Put(grpc::ServerContext *context, const kvstore::PutRequest *request, kvstore::PutResponse *response) {
    auto key = request->key();
    auto value = request->value();
    if (value.empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "Request failed. Value cannot be empty.");
    }
    int ret = raft_server_->putKey(toBytes(key), toBytes(value));
    if (!ret) {
        return grpc::Status::OK;
    } else {
        return grpc::Status(grpc::ABORTED, "Request failed. Return code: " + std::to_string(ret));
    }
}

grpc::Status GrpcServer::Get(grpc::ServerContext *context, const kvstore::GetRequest *request, kvstore::GetResponse *response) {
    auto key = request->key();
    ByteArray ret = raft_server_->getKey(toBytes(key));
    if (!ret.empty()) {
        response->set_value(fromBytes(ret));
        return grpc::Status::OK;
    }
    return grpc::Status(grpc::ABORTED, "Request failed.");
}

grpc::Status GrpcServer::Delete(grpc::ServerContext *context, const kvstore::DeleteRequest *request, kvstore::DeleteResponse *response) {
    auto key = request->key();
    int ret = raft_server_->deleteKey(toBytes(key));
    if (!ret) {
        return grpc::Status::OK;
    } else {
        return grpc::Status(grpc::ABORTED, "Request failed. Return code: " + std::to_string(ret));
    }
}

grpc::Status GrpcServer::Scan(grpc::ServerContext *context, const kvstore::ScanRequest *request, kvstore::ScanResponse *response) {
    auto startKey = request->start_key();
    auto endKey = request->end_key();
    auto ret = raft_server_->scanKey(toBytes(startKey), toBytes(endKey));
    if (!ret.empty()) {
        for (auto &pair : ret) {
            auto *kv_pair = response->add_results();
            kv_pair->set_key(fromBytes(pair.first));
            kv_pair->set_value(fromBytes(pair.second));
        }
        return grpc::Status::OK;
    } else {
        return grpc::Status(grpc::ABORTED, "Request failed.");
    }
}

grpc::Status GrpcServer::AddServer(grpc::ServerContext *context, const kvstore::AddServerRequest *request, kvstore::AddServerResponse *response) {
    int ret = raft_server_->addServer(request->server_id(), request->server_address());
    if (!ret) {
        return grpc::Status::OK;
    } else {
        return grpc::Status(grpc::ABORTED, "Request failed. Return code: " + std::to_string(ret));
    }
}

grpc::Status GrpcServer::ListServers(grpc::ServerContext *context, const kvstore::ListServersRequest *request, kvstore::ListServersResponse *response) {
    auto ret = raft_server_->listServers();
    if (!ret.empty()) {
        for (auto &server : ret) {
            kvstore::ServerInfo *info = response->add_servers();
            info->set_server_id(server.id);
            info->set_server_address(server.address);
            info->set_is_leader(server.isLeader);
        }
        return grpc::Status::OK;
    } else {
        return grpc::Status(grpc::ABORTED, "Request failed.");
    }
}
