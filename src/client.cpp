// gRPC C++ client for kvstore.proto
// Assumes that gRPC and Protobuf are properly compiled and set up

#include <iostream>
#include <memory>
#include <string>

#include "kvstore.grpc.pb.h"
#include <grpcpp/grpcpp.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using kvstore::AddServerRequest;
using kvstore::AddServerResponse;
using kvstore::DeleteRequest;
using kvstore::DeleteResponse;
using kvstore::GetRequest;
using kvstore::GetResponse;
using kvstore::KeyValuePair;
using kvstore::KVStore;
using kvstore::ListServersRequest;
using kvstore::ListServersResponse;
using kvstore::PutRequest;
using kvstore::PutResponse;
using kvstore::ScanRequest;
using kvstore::ScanResponse;

class KVStoreClient {
public:
    KVStoreClient(std::shared_ptr<Channel> channel) : stub_(KVStore::NewStub(channel)) {}

    void Put(const std::string &key, const std::string &value) {
        PutRequest request;
        request.set_key(key);
        request.set_value(value);

        PutResponse response;
        ClientContext context;

        Status status = stub_->Put(&context, request, &response);
        if (status.ok()) {
            std::cout << "Put succeeded." << std::endl;
        } else {
            std::cerr << "Put failed: " << status.error_message() << std::endl;
        }
    }

    void Get(const std::string &key) {
        GetRequest request;
        request.set_key(key);

        GetResponse response;
        ClientContext context;

        Status status = stub_->Get(&context, request, &response);
        if (status.ok()) {
            std::cout << "Value: " << response.value() << std::endl;
        } else {
            std::cerr << "Get failed: " << status.error_message() << std::endl;
        }
    }

    void Delete(const std::string &key) {
        DeleteRequest request;
        request.set_key(key);

        DeleteResponse response;
        ClientContext context;

        Status status = stub_->Delete(&context, request, &response);
        if (status.ok()) {
            std::cout << "Delete succeeded." << std::endl;
        } else {
            std::cerr << "Delete failed: " << status.error_message() << std::endl;
        }
    }

    void Scan(const std::string &start_key, const std::string &end_key) {
        ScanRequest request;
        request.set_start_key(start_key);
        request.set_end_key(end_key);

        ScanResponse response;
        ClientContext context;

        Status status = stub_->Scan(&context, request, &response);
        if (status.ok()) {
            for (const auto &pair : response.results()) {
                std::cout << pair.key() << ": " << pair.value() << std::endl;
            }
        } else {
            std::cerr << "Scan failed: " << status.error_message() << std::endl;
        }
    }

    void AddServer(const std::string &id, const std::string &address) {
        AddServerRequest request;
        request.set_server_id(id);
        request.set_server_address(address);

        AddServerResponse response;
        ClientContext context;

        Status status = stub_->AddServer(&context, request, &response);
        if (status.ok()) {
            std::cout << "AddServer succeeded." << std::endl;
        } else {
            std::cerr << "AddServer failed: " << status.error_message() << std::endl;
        }
    }

    void ListServers() {
        ListServersRequest request;
        ListServersResponse response;
        ClientContext context;

        Status status = stub_->ListServers(&context, request, &response);
        if (status.ok()) {
            for (const auto &server : response.servers()) {
                std::cout << server.server_id() << " @ " << server.server_address()
                          << (server.is_leader() ? " [Leader]" : "") << std::endl;
            }
        } else {
            std::cerr << "ListServers failed: " << status.error_message() << std::endl;
        }
    }

private:
    std::unique_ptr<KVStore::Stub> stub_;
};

int main(int argc, char **argv) {
    std::string grpcPortStr = argv[1];
    KVStoreClient client(grpc::CreateChannel("localhost:" + grpcPortStr, grpc::InsecureChannelCredentials()));

    std::string command;
    while (true) {
        std::cout << "> ";
        std::cin >> command;
        if (command == "put") {
            std::string key, value;
            std::cin >> key >> value;
            client.Put(key, value);
        } else if (command == "get") {
            std::string key;
            std::cin >> key;
            client.Get(key);
        } else if (command == "delete") {
            std::string key;
            std::cin >> key;
            client.Delete(key);
        } else if (command == "scan") {
            std::string start, end;
            std::cin >> start >> end;
            client.Scan(start, end);
        } else if (command == "addserver") {
            std::string id, addr;
            std::cin >> id >> addr;
            client.AddServer(id, addr);
        } else if (command == "listservers") {
            client.ListServers();
        } else if (command == "exit") {
            break;
        } else {
            std::cout << "Unknown command." << std::endl;
        }
    }
    return 0;
}
