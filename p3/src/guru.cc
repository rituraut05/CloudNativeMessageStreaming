#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <string>
#include <vector>
#include <thread>
#include <shared_mutex>

#include "utils.hh"
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
using std::shared_mutex;
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
#define WAIT_FOR_LEADER_TIMEOUT 1.5*MAX_ELECTION_TIMEOUT
#define HEART   "\xE2\x99\xA5"

// ****************************** Variables ******************************

int cluster_cnt = 0;

// brokerid - Timer with BROKER_ALIVE_TIMEOUT
unordered_map<int, Timer> brokerAliveTimers;

// topicid - Timer with WAIT_FOR_LEADER_TIMEOUT
unordered_map<int, Timer> waitForElectionTimers;

vector<Cluster> clusters; // persist
// brokerid - ServerInfo
unordered_map<int, ServerInfo> brokers; // persist

// topicid - clusterid
unordered_map<int, int> topicToClusterMap; // persist
// topicid - leaderid (brokerid)
unordered_map<int, int> topicToLeaderMap; // persist
// leaderid (brokerid) - list of topic ids under its leadership
unordered_map<int, vector<int>> leaderToTopicsMap;
// publisherid - list of topics it sends messages to
unordered_map<int, vector<int>> publisherToTopicsMap; // required?
// subscriberid - list of topics it consumes messages from
unordered_map<int, vector<int>> subscriberToTopicsMap;

shared_mutex mutex_tlm; // lock for topicToLeaderMap

// **************************** Functions ********************************

void invokeStartElection(int bid, int topicID) {
  int ret = brokers[bid].gbClient->StartElection(topicID);
  if(ret == 0) {
    printf("[invokeStartElection] Election started successfully for topic %d at broker %s %d\n", topicID, brokers[bid].server_name.c_str(), bid);
  } else {
    printf("[invokeStartElection] Failure in starting election for topic %d at broker %s %d\n", topicID, brokers[bid].server_name.c_str(), bid);
  }
}

void checkLeaderElections() {
  while(true) {
    mutex_tlm.lock();
    for(auto tl: topicToLeaderMap) {
      uint topicid = tl.first;
      uint leaderid = tl.second;
      if(leaderid == -1 && 
        waitForElectionTimers.find(topicid) != waitForElectionTimers.end() &&
        waitForElectionTimers[topicid].get_tick() > WAIT_FOR_LEADER_TIMEOUT) { // retrigger election
        waitForElectionTimers[topicid].reset(WAIT_FOR_LEADER_TIMEOUT);
        Cluster clusterOwningTopic;
        for(Cluster c: clusters) {
          if(c.clusterid == topicToClusterMap[topicid]) {
            clusterOwningTopic = c;
            break;
          }
        }
        for(uint brokeridInCluster: clusterOwningTopic.brokers) {
          if(brokers[brokeridInCluster].alive) {
            printf("[CheckLeaderElections] Re-running election for topic %d in broker %d in cluster %d\n", topicid, brokeridInCluster, clusterOwningTopic.clusterid);
            thread tmpthread(invokeStartElection, brokeridInCluster, topicid);
            tmpthread.detach();
          }
        }
      }
    }
    mutex_tlm.unlock();
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

      // printf("Received %s from broker %d in cluster %d\n", HEART, brokerid, clusterid);

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
                for(uint bid: c.brokers) {
                  if(bid != servid) {
                    thread tmpthread(invokeStartElection, bid, topicid);
                    tmpthread.detach();
                    if(waitForElectionTimers.find(topicid) == waitForElectionTimers.end())
                      waitForElectionTimers[topicid] = Timer(1, WAIT_FOR_LEADER_TIMEOUT);
                    else
                      waitForElectionTimers[topicid].reset(WAIT_FOR_LEADER_TIMEOUT);
                  }
                }
                mutex_tlm.lock();
                topicToLeaderMap[topicid] = -1;
                mutex_tlm.unlock();
              }
              leaderToTopicsMap[servid].clear();
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
  ServerInfo s2(2, 0, "0.0.0.0:50053");
  s0.initGuruToBrokerClient();
  s1.initGuruToBrokerClient();
  s2.initGuruToBrokerClient();
  c0.addBroker(s0.serverid);
  c0.addBroker(s1.serverid);
  c0.addBroker(s2.serverid);
  c0.addTopic(1);
  c0.addTopic(2);
  clusters.push_back(c0);
  brokers[s0.serverid] = s0;
  brokers[s1.serverid] = s1;
  brokers[s2.serverid] = s2;
  for(int j = 0; j<3; j++) {
    brokerAliveTimers[j] = Timer(1, BROKER_ALIVE_TIMEOUT);
  }
  c0.print();
  vector<int> topicsOfLeader1;
  topicsOfLeader1.push_back(1);
  topicsOfLeader1.push_back(2);
  leaderToTopicsMap[1] = topicsOfLeader1;
  // ---------------------------------------------

  thread checkLeaderElectionsThread(checkLeaderElections);
  RunGrpcServer(GURU_ADDRESS);
  if(checkLeaderElectionsThread.joinable()) checkLeaderElectionsThread.join();
  return 0;
}