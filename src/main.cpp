#include <string>

void RunServer(int node_id, const std::string &grpc_host, const int grpc_port, const std::string &raft_host, int raft_port);

int main() {
    // 今回はNodeID=1、raft endpoint "localhost:12345"
    RunServer(1, "0.0.0.0", 50051, "localhost", 12345);
    return 0;
}