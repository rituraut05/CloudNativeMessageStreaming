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
using grpc::ClientContext;
using grpc::StatusCode;
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
using dps::StartElectionRequest;
using dps::StartElectionResponse;
using dps::ClusterConfigRequest;
using dps::ClusterConfigResponse;
using dps::ServerConfig;
using util::Timer;

#define BROKER_ALIVE_TIMEOUT    5000
#define HEART   "\xE2\x99\xA5"

// ****************************** Variables ******************************

int cluster_cnt = 0;
unordered_map<int, Timer> brokerAliveTimers;

vector<Cluster> clusters; // persist
unordered_map<int, ServerInfo> brokers; // persist

unordered_map<int, int> topicToClusterMap; // persist
unordered_map<int, int> topicToLeaderMap; // persist
unordered_map<int, vector<int>> leaderToTopicsMap;
unordered_map<int, vector<int>> publisherToTopicsMap; // required?
unordered_map<int, vector<int>> subscriberToTopicsMap;

// **************************** Functions ********************************

void invokeStartElection(int bid, int topicID) {
  int ret = brokers[bid].gbClient->StartElection(topicID);
  if(ret == 0) {
    printf("Election started successfully for topic %d at broker %s %d\n", topicID, brokers[bid].server_name, bid);
  } else {
    printf("Failure in starting election for topic %d at broker %s %d\n", topicID, brokers[bid].server_name, bid);
  }
}

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
      if(!brokers[brokerid].alive) {
        brokers[brokerid].alive = true;
        // TODO: send BrokerUp calls to other brokers in cluster 
      }

      for(Cluster c: clusters) {
        for(uint servid: c.brokers) {
          if(servid != brokerid) {
            if(brokers[servid].alive && 
              brokerAliveTimers[servid].get_tick() > BROKER_ALIVE_TIMEOUT) {
              printf("[SendHeartbeat] Brokerid: %d in cluster %d down.\n", servid, c.clusterid);
              brokers[servid].alive = false;
              for(int topicid: leaderToTopicsMap[servid]) {
                printf("[SendHeartbeat] Triggering election for topic %d in cluster %d.\n", topicid, c.clusterid);
                vector<thread> startElectionThreads;
                for(uint bid: c.brokers) {
                  if(bid != servid) {
                    startElectionThreads.push_back(thread(invokeStartElection, bid, topicid));
                  }
                }
                for(int i=0; i<startElectionThreads.size(); i++) {
                  startElectionThreads[i].join();
                }
                startElectionThreads.clear();
                // TODO: add this topic to temporary set of topics for which election is started.
                // TODO: update topicToLeaderMap[topicID] = -1
              }
              // TODO: remove the servid from leaderToTopicsMap
            }
          }
        }
      }
      return Status::OK;
    }

    Status RequestConfig(ServerContext *context, const ClusterConfigRequest *request, ClusterConfigResponse *response) override
    {
      uint brokerid = request->serverid();
      uint clusterid = brokers[brokerid].clusterid;

      printf("[RequestConfig] Sending ClusterConfig for cluster %d to broker %d.\n", clusterid, brokerid);

      Cluster config;
      for(Cluster c: clusters) {
        if (c.clusterid == clusterid) {
          config = c;
          break;
        }
      }

      response->set_serverid(brokerid);
      response->set_clusterid(clusterid);
      response->set_clustersize(config.size);
      for(uint bid: config.brokers) {
        ServerConfig *brokerConfig = response->add_brokers();
        brokerConfig->set_serverid(bid);
        brokerConfig->set_servaddr(brokers[bid].server_addr);
      }
      for(uint tpcid: config.topics) {
        response->add_topics(tpcid);
      }
      return Status::OK;
    }
};

GuruToBrokerClient::GuruToBrokerClient(shared_ptr<Channel> channel)
  : stub_(BrokerServer::NewStub(channel)) {}

int GuruToBrokerClient::StartElection(int topicID) {
  StartElectionRequest request;
  StartElectionResponse reply;
  Status status;
  ClientContext context;

  request.set_topicid(topicID);
  reply.Clear();

  status = stub_->StartElection(&context, request, &reply);

  if(status.ok()) {
    printf("[StartElection]: GuruToBrokerClient - RPC Success\n");
    return 0;
  } else {
    if(status.error_code() == StatusCode::UNAVAILABLE){
      printf("[StartElection]: GuruToBrokerClient - Unavailable server\n");
    }
    printf("[StartElection]: RPC Failure\n");
    return -1;
  }
}

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
  s0.initGuruToBrokerClient();
  s1.initGuruToBrokerClient();
  c0.addBroker(s0.serverid);
  c0.addBroker(s1.serverid);
  c0.addTopic(1);
  c0.addTopic(2);
  clusters.push_back(c0);
  brokers[s0.serverid] = s0;
  brokers[s1.serverid] = s1;
  for(int j = 0; j<2; j++) {
    brokerAliveTimers[j] = Timer(1, BROKER_ALIVE_TIMEOUT);
  }
  c0.print();
  vector<int> topicsOfLeader1;
  topicsOfLeader1.push_back(1);
  topicsOfLeader1.push_back(2);
  leaderToTopicsMap[1] = topicsOfLeader1;
  // ---------------------------------------------

  RunGrpcServer("0.0.0.0:50050");
  return 0;
}