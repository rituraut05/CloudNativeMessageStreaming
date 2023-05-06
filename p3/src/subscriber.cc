#include <grpcpp/grpcpp.h>
#include <string>

#include "utils.hh"
#include "dps.grpc.pb.h"

using grpc::Channel;
using std::string;
using std::shared_ptr;
using std::unique_ptr;
using dps::BrokerServer;
using dps::GuruServer;


leveldbPtr subDB;
vector<int> topics;
unordered_map<int, string> topicLeaderMap;

void openOrCreateDBs() {
  leveldb::Options options;
  options.create_if_missing = true;

  leveldb::Status sub_status = leveldb::DB::Open(options, "/tmp/sub/topics" , &subDB);
  if (!sub_status.ok()) std::cerr << sub_status.ToString() << endl;
  assert(sub_status.ok());
  printf("[openOrCreateDBs] Successfully opened sub DB.\n");
  
}



void initialize() {
  string value;
  leveldb::Status topics_Status = subDB->Get(leveldb::ReadOptions(), "topics", &value);
  if (!topics_Status.ok()) {
    std::cerr << "[initializePersistedValues] topics" << ": Error: " << topics_Status.ToString() << endl;
  } else {
    cout<<value<<endl;
    // topics = stoi(value);
    printf("[initializePersistedValues] topics = %s\n", value.c_str());
    
  }
}
typedef unique_ptr<BrokerServer::Stub> BrokerStub;

class Subscriber {
  public:
    Subscriber(shared_ptr<Channel> guruchannel);
    BrokerStub GetBrokerForRead(int topicid);
    int Subscribe(int topicid);
    int ReadMessageStream(BrokerStub brokerstub, int topicid, int index);

  private:
    unique_ptr<GuruServer::Stub> gurustub_;
};

Subscriber::Subscriber(shared_ptr<Channel> guruchannel) 
  : gurustub_(GuruServer::NewStub(guruchannel)) 
{
  printf("------------ Opened channel to Guru -------------\n");
}

// Subscriber::ReadMessageStream()

int main(int argc, char* argv[]) {
  openOrCreateDBs();
  subDB->Put(leveldb::WriteOptions(), "topics", "1 2 3");
  initialize();
  return 0;
}
