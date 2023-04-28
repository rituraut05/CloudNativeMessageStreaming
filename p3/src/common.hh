#ifndef COMMON_HPP
#define COMMON_HPP

#include <string>
#include <vector>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include "dps.grpc.pb.h"

using std::string;
using std::vector;
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

class ServerInfo {
  public:
    uint serverid;
    string server_addr;
    string server_name;
    uint clusterid;
    bool alive;
    GuruToBrokerClient *gbClient;

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
};

class Cluster {
  public:
    uint clusterid;
    uint size;
    vector<uint> brokers;

    Cluster() {}
    Cluster(uint cid) {
      this->clusterid = cid;
      this->size = 0;
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
      printf("\n");
    }
};

#endif