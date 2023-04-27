#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <string>
#include <vector>
#include <thread>

#include "timer.hh"
#include "common.hh"
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
using std::unordered_map;
using dps::BrokerServer;
using dps::GuruServer;
using dps::HeartbeatRequest;
using dps::HeartbeatResponse;
using util::Timer;

#define BROKER_ALIVE_TIMEOUT    5000
#define HEART   "\xE2\x99\xA5"

// ****************************** Class Definitions ***********************
class GuruToBrokerClient {
  public:
    GuruToBrokerClient(shared_ptr<Channel> channel);
    int StartElection(int topicid);

  private:
    unique_ptr<GuruServer::Stub> stub_;
};

// ****************************** Variables ******************************
// Persist information 
vector<Cluster> clusters;
unordered_map<int, ServerInfo> brokers;
// --------------------

int cluster_cnt = 0;
unordered_map<int, Timer> brokerAliveTimers;


// **************************** Guru Grpc Server **************************
class GuruGrpcServer final : public GuruServer::Service {
  public:
    explicit GuruGrpcServer() {}

    Status SendHeartbeat(ServerContext *context, const HeartbeatRequest *request, HeartbeatResponse *response) override
    {
      int brokerid = request->serverid();
      int clusterid = request->clusterid();

      printf("Received %s from broker %d in cluster %d\n", HEART, brokerid, clusterid);

      brokerAliveTimers[brokerid].reset(BROKER_ALIVE_TIMEOUT);
      if(!brokers[brokerid].alive) brokers[brokerid].alive = true;

      for(Cluster c: clusters) {
        for(uint servid: c.brokers) {
          if(servid != brokerid) {
            if(brokers[servid].alive && 
              brokerAliveTimers[servid].get_tick() > BROKER_ALIVE_TIMEOUT) {
              printf("[SendHeartbeat] Brokerid: %d in cluster %d down.\n", servid, brokers[servid].clusterid);
              brokers[servid].alive = false;
              // Trigger election for topics of broker servid
            }
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

// **************************** Main **********************************
int main(int argc, char* argv[]) {
  // For testing, remove later ---------------------------------
  cluster_cnt++;
  Cluster c0(0);
  ServerInfo s0(0, 0, "0.0.0.0:50051");
  ServerInfo s1(1, 0, "0.0.0.0:50052");
  c0.addBroker(s0.serverid);
  c0.addBroker(s1.serverid);
  clusters.push_back(c0);
  brokers[s0.serverid] = s0;
  brokers[s1.serverid] = s1;
  for(int j = 0; j<2; j++) {
    brokerAliveTimers[j] = Timer(1, BROKER_ALIVE_TIMEOUT);
  }
  c0.print();
  // ---------------------------------------------

  RunGrpcServer("0.0.0.0:50050");
  return 0;
}