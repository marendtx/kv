#include "server.hpp"
#include <iostream>

int main(int argc, char **argv) {
    if (argc < 4) {
        std::cout << "invalid argument" << std::endl;
        return 1;
    }

    std::string grpcPortStr = argv[1];
    std::string raftPortStr = argv[2];
    std::string nodeIdStr = argv[3];

    // Usage: ./build/myapp 50001 10001 1
    RunServer(std::stoi(grpcPortStr), std::stoi(raftPortStr), std::stoi(nodeIdStr));

    return 0;
}