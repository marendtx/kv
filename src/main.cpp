#include "server.hpp"
#include <iostream>

int main(int argc, char **argv) {

    if (argc < 4) {
        std::cout << "invalid argument" << std::endl;
        return 1;
    }

    std::string grpcPortStr = argv[1];
    std::string nodeIdStr = argv[2];
    std::string raftPortStr = argv[3];

    RunServer(std::stoi(grpcPortStr), std::stoi(nodeIdStr), std::stoi(raftPortStr));

    return 0;
}