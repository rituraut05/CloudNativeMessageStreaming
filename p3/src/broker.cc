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

class BrokerToGuruClient {
  public:
    BrokerToGuruClient(shared_ptr<Channel> guruchannel);
    int SetLeader();
    int SendHeartbeat();

  private:
    unique_ptr<GuruServer::Stub> gurustub_;
};

class BrokerClient {
  public:
    BrokerClient(shared_ptr<Channel> channel);
    int AppendEntries(int nextIndex, int lastIndex);
    int RequestVote(int lastLogTerm, int lastLogIndex, int followerID);

  private:
    unique_ptr<BrokerServer::Stub> stub_;
};

class BrokerRaftServer final : public BrokerServer::Service {
  public:
    explicit BrokerRaftServer() {}
};

int main(int argc, char* argv[]) {
  return 0;
}