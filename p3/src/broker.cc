#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <string>
#include <thread>

#include "timer.hh"
#include "utils.hh"
#include "dps.grpc.pb.h"

using grpc::Channel;
using grpc::Status;
using grpc::ClientContext;
using std::string;
using std::shared_ptr;
using std::unique_ptr;
using std::thread;
using dps::BrokerServer;
using dps::GuruServer;
using dps::HeartbeatRequest;
using dps::HeartbeatResponse;
using util::Timer;

#define HEARTBEAT_TIMEOUT       1000
#define HEART   "\xE2\x99\xA5"

// *************************** Class Definitions ******************************
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

// *************************** Volatile Variables *****************************
uint clusterID;
uint serverID;
Timer heartbeatTimer(1, HEARTBEAT_TIMEOUT);
BrokerToGuruClient* bgClient;

// *************************** Functions *************************************

void runHeartbeatTimer() {
  heartbeatTimer.start(HEARTBEAT_TIMEOUT);
  while(heartbeatTimer.get_tick() < heartbeatTimer._timeout) ; // spin
  int ret = bgClient->SendHeartbeat();
  runHeartbeatTimer();
}




BrokerToGuruClient::BrokerToGuruClient(shared_ptr<Channel> guruchannel)
  : gurustub_(GuruServer::NewStub(guruchannel)) {}

int BrokerToGuruClient::SendHeartbeat() {
  HeartbeatRequest request;
  HeartbeatResponse response;
  Status status;
  ClientContext context;

  request.set_serverid(serverID);
  request.set_clusterid(clusterID);
  response.Clear();
  status = gurustub_->SendHeartbeat(&context, request, &response);

  if(status.ok()) {
    printf("[SendHeartbeat RPC] %s %s %s Successfully sent heartbeat to Guru %s %s %s\n", HEART, HEART, HEART, HEART, HEART, HEART);
    return 0;
  } else {
    printf("[SendHeartbeat RPC] Failure.\n");
    return -1;
  }
}



class BrokerGrpcServer final : public BrokerServer::Service {
  public:
    explicit BrokerGrpcServer() {}
};

int main(int argc, char* argv[]) {
  if(argc != 4) {
    printf("Usage: ./broker <serverid> <clusterid> <guruAddr>\n");
    return 0;
  }
  serverID = atoi(argv[1]);
  clusterID = atoi(argv[2]);
  bgClient = new BrokerToGuruClient(grpc::CreateChannel(argv[3], grpc::InsecureChannelCredentials()));

  thread heartbeat(runHeartbeatTimer);

  if(heartbeat.joinable()) heartbeat.join();
  return 0;
}