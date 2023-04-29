#ifndef COMMON_HPP
#define COMMON_HPP

#include <string>
#include <vector>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include "dps.grpc.pb.h"

#define GURU_ADDRESS "0.0.0.0:50050"

using std::string;
using std::vector;
using std::remove;
using std::to_string;
using std::unique_ptr;
using std::shared_ptr;
using grpc::CreateChannel;
using grpc::InsecureChannelCredentials;
using grpc::Channel;
using dps::BrokerServer;

class GuruToBrokerClient {
  public:
    GuruToBrokerClient(shared_ptr<Channel> channel);
    int StartElection(int topicid);

  private:
    unique_ptr<BrokerServer::Stub> stub_;
};

class BrokerClient {
  public:
    BrokerClient(shared_ptr<Channel> channel);
    int AppendEntries(int nextIndex, int lastIndex);
    int RequestVote(int lastLogTerm, int lastLogIndex, int followerID, int topicID);

  private:
    unique_ptr<BrokerServer::Stub> stub_;
};

class ServerInfo {
  public:
    uint serverid;
    string server_addr;
    string server_name;
    uint clusterid;
    bool alive;
    GuruToBrokerClient *gbClient;
    BrokerClient *client;

    ServerInfo() {}
    ServerInfo(uint sid, uint cid, string servaddr) {
      this->serverid = sid;
      this->clusterid = cid;
      this->server_addr = servaddr;
      this->server_name = "C" + to_string(cid) + "S" + to_string(sid);
      this->alive = true;
    }

    void initGuruToBrokerClient() {
      this->gbClient = new GuruToBrokerClient(CreateChannel(this->server_addr, InsecureChannelCredentials()));
    }

    void initBrokerClient() {
      this->client = new BrokerClient(CreateChannel(this->server_addr, InsecureChannelCredentials()));
    }
};

class Cluster {
  public:
    uint clusterid;
    uint size;
    vector<uint> brokers;
    vector<uint> topics;

    Cluster() {}
    Cluster(uint cid) {
      this->clusterid = cid;
      this->size = 0;
    }

    void addTopic(uint topicid) {
      this->topics.push_back(topicid);
    }

    void removeTopic(uint topicid) {
      remove(this->topics.begin(), this->topics.end(), topicid);
    }

    void addBroker(uint sid) {
      this->brokers.push_back(sid);
      this->size++;
    }

    void removeBroker(uint sid) {
      int i = 0;
      for(uint servid: brokers) {
        if(servid == sid) break;
        i++;
      }
      brokers.erase(brokers.begin() + i);
      this->size--;
    }

    void print() {
      printf("-- Cluster: %d\n", clusterid);
      for(uint servid: brokers) {
        printf("Broker: %d\n", servid);
      }
      printf("Topics: ");
      for(uint topicid: topics) {
        printf("%d ", topicid);
      }
      printf("\n\n");
    }
};

#endif