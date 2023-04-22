#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <string>
#include <vector>
#include <thread>

#include "timer.hh"
#include "dps.grpc.pb.h"

using grpc::Channel;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::Status;
using grpc::ServerContext;
using std::string;
using std::shared_ptr;
using std::unique_ptr;
using std::vector;
using std::thread;
using dps::BrokerServer;
using dps::GuruServer;
using dps::HeartbeatRequest;
using dps::HeartbeatResponse;
using util::Timer;

#define BROKER_ALIVE_TIMEOUT    5000
#define HEART   "\xE2\x99\xA5"

class BrokerTimer {
  public: 
    uint brokerid;
    Timer timer;

    BrokerTimer(int bid, int tick, int timeout) 
      : timer(tick, timeout) {
      brokerid = bid;
    }
};

int CLUSTER_CNT = 1;
vector<BrokerTimer*> brokerAliveTimers;

class GuruToBrokerClient {
  public:
    GuruToBrokerClient(shared_ptr<Channel> channel);
    int StartElection(int topicid);

  private:
    unique_ptr<GuruServer::Stub> stub_;
};

class GuruGrpcServer final : public GuruServer::Service {
  public:
    explicit GuruGrpcServer() {}

    Status SendHeartbeat(ServerContext *context, const HeartbeatRequest *request, HeartbeatResponse *response) override
    {
      bool rpcSuccess = false;
      int brokerid = request->serverid();
      int clusterid = request->clusterid();

      printf("Received %s from broker %d in cluster %d\n", HEART, brokerid, clusterid);

      for(BrokerTimer* bt : brokerAliveTimers) {
        if(bt->brokerid == brokerid) {
          bt->timer.reset(BROKER_ALIVE_TIMEOUT);
        } else {
          if (bt->timer.get_tick() > BROKER_ALIVE_TIMEOUT) {
            printf("Brokerid: %d down.\n", bt->brokerid);
            // Start election for topics for brokerid
          }
        }
      }
      return Status::OK;
    }
};

void RunGrpcServer(string server_address) {
  GuruGrpcServer service;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server = builder.BuildAndStart();
  std::cout << "[RunGrpcServer] Server listening on " << server_address << std::endl;
  server->Wait();
}

int main(int argc, char* argv[]) {
  // For testing 
  for(int j = 0; j<3; j++) {
    brokerAliveTimers.push_back(new BrokerTimer(j, 1, BROKER_ALIVE_TIMEOUT));
  }

  RunGrpcServer("0.0.0.0:50050");
  return 0;
}