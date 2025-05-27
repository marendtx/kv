#include <iostream>
#include <memory>
#include <string>

#include "hello.grpc.pb.h"
#include <grpcpp/grpcpp.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using helloworld::HelloReply;
using helloworld::HelloRequest;
using helloworld::HelloService;

class HelloClient {
public:
    HelloClient(std::shared_ptr<Channel> channel)
        : stub_(HelloService::NewStub(channel)) {}

    std::string SayHello(const std::string &user) {
        HelloRequest request;
        request.set_name(user);

        HelloReply reply;
        ClientContext context;

        Status status = stub_->SayHello(&context, request, &reply);

        if (status.ok()) {
            return reply.message();
        } else {
            return "RPC failed";
        }
    }

private:
    std::unique_ptr<HelloService::Stub> stub_;
};

int main(int argc, char **argv) {
    HelloClient client(grpc::CreateChannel(
        "localhost:50051", grpc::InsecureChannelCredentials()));

    std::string user("world");
    if (argc > 1) {
        user = argv[1];
    }

    std::string reply = client.SayHello(user);
    std::cout << "Received: " << reply << std::endl;

    return 0;
}
