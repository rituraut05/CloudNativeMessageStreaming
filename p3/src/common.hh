#ifndef COMMON_HPP
#define COMMON_HPP

#include <string>
#include <vector>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include "dps.grpc.pb.h"

#define GURU_ADDRESS "0.0.0.0:50050"

#define TOPICS_PER_CLUSTER_THRESHOLD 10


using std::string;
using std::vector;
using std::remove;
using std::to_string;
using std::unique_ptr;
using std::shared_ptr;
using std::stoi;
using grpc::CreateChannel;
using grpc::InsecureChannelCredentials;
using grpc::Channel;
using dps::BrokerServer;

string C0_ADDRESSES[3] = {"0.0.0.0:50051",  "0.0.0.0:50052", "0.0.0.0:50053"};

class GuruToBrokerClient {
  public:
    GuruToBrokerClient(shared_ptr<Channel> channel);
    int StartElection(int topicid);
    int BrokerUp(int brokerid);

  private:
    unique_ptr<BrokerServer::Stub> stub_;
};

class BrokerClient {
  public:
    BrokerClient(shared_ptr<Channel> channel);
    int AppendEntries(int topicId, int nextIndex, int lastIndex);
    int RequestVote(int lastLogTerm, int lastLogIndex, int followerID, int topicID);

  private:
    unique_ptr<BrokerServer::Stub> stub_;
};

namespace common {
  vector<string> split(string str, char delim) {
    vector<string> strs;
    string temp = "";

    for(int i=0; i<str.length(); i++) {
      if(str[i] == delim) {
        strs.push_back(temp);
        temp = "";
      } else {
        temp = temp + (str.c_str())[i];
      }
    }
    strs.push_back(temp);
    return strs;
  }
}


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

    ServerInfo(string sstr) : 
      ServerInfo(stoi(common::split(sstr, ';')[0]), stoi(common::split(sstr, ';')[1]), common::split(sstr, ';')[2]) {}

    void initGuruToBrokerClient() {
      this->gbClient = new GuruToBrokerClient(CreateChannel(this->server_addr, InsecureChannelCredentials()));
    }

    void initBrokerClient() {
      this->client = new BrokerClient(CreateChannel(this->server_addr, InsecureChannelCredentials()));
    }

    // format: serverid;clusterid;server_addr;
    string toString() {
      string ret = "";
      ret += to_string(this->serverid) + ";";
      ret += to_string(this->clusterid) + ";";
      ret += this->server_addr;
      return ret;
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

    Cluster(string cstr) {
      vector<string> cstrParts = common::split(cstr, ';');
      this->clusterid = stoi(cstrParts[0]);
      this->size = stoi(cstrParts[1]);
      for(int i=2; i<this->size+2; i++) {
        this->brokers.push_back(stoi(cstrParts[i]));
      }
      for(int i = this->size+3; i<stoi(cstrParts[this->size+2]) + this->size+3; i++) {
        this->topics.push_back(stoi(cstrParts[i]));
      }
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

    // format: clusterid;no_of_brokers;brokerids;no_of_topics;topicids;
    string toString() {
      string ret = "";
      ret += to_string(this->clusterid) + ";";
      ret += to_string(this->size) + ";";
      for(uint bid: this->brokers) {
        ret += to_string(bid) + ";";
      }
      ret += to_string(this->topics.size()) + ";";
      for(uint tid: this->topics) {
        ret += to_string(tid) + ";";
      }
      ret.pop_back();
      return ret;
    }
};

#endif