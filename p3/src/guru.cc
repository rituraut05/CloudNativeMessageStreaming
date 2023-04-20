#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <string>

#include "dps.grpc.pb.h"

using grpc::Channel;
using grpc::Status;
using std::string;
using std::shared_ptr;
using std::unique_ptr;
using dps::BrokerServer;
using dps::GuruServer;

class GuruToBrokerClient {
  public:
    GuruToBrokerClient(shared_ptr<Channel> channel);
    int StartElection(int topicid);

  private:
    unique_ptr<GuruServer::Stub> stub_;
};

class GuruRaftServer final : public GuruServer::Service {
  public:
    explicit GuruRaftServer() {}
};

int main(int argc, char* argv[]) {
  return 0;
}