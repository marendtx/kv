#include "kvstore.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>

int main(int argc, char **argv) {
    std::string address = "localhost:12345";
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = kvstore::KVStore::NewStub(channel);

    // --- Put ---
    {
        kvstore::PutRequest req;
        kvstore::PutResponse res;
        req.set_key("hello");
        req.set_value("world");
        grpc::ClientContext ctx;
        auto status = stub->Put(&ctx, req, &res);
        if (status.ok() && res.success()) {
            std::cout << "[Put] OK" << std::endl;
        } else {
            std::cout << "[Put] Failed: " << res.error() << std::endl;
        }
    }

    // --- Get ---
    {
        kvstore::GetRequest req;
        kvstore::GetResponse res;
        req.set_key("hello");
        grpc::ClientContext ctx;
        auto status = stub->Get(&ctx, req, &res);
        if (status.ok() && res.found()) {
            std::cout << "[Get] " << req.key() << " → " << res.value() << std::endl;
        } else {
            std::cout << "[Get] Not found or error: " << res.error() << std::endl;
        }
    }

    // --- Delete ---
    {
        kvstore::DeleteRequest req;
        kvstore::DeleteResponse res;
        req.set_key("hello");
        grpc::ClientContext ctx;
        auto status = stub->Delete(&ctx, req, &res);
        if (status.ok() && res.success()) {
            std::cout << "[Delete] OK" << std::endl;
        } else {
            std::cout << "[Delete] Failed: " << res.error() << std::endl;
        }
    }

    // --- Get again ---
    {
        kvstore::GetRequest req;
        kvstore::GetResponse res;
        req.set_key("hello");
        grpc::ClientContext ctx;
        auto status = stub->Get(&ctx, req, &res);
        if (status.ok() && res.found()) {
            std::cout << "[Get] " << req.key() << " → " << res.value() << std::endl;
        } else {
            std::cout << "[Get after delete] Not found or error: " << res.error() << std::endl;
        }
    }

    return 0;
}
