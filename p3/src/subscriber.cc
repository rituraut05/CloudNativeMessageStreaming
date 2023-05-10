#include <grpcpp/grpcpp.h>
#include <string>

#include "utils.hh"
#include "common.hh"
#include "dps.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::Channel;
using grpc::ClientReader;
using grpc::ClientReaderWriter;

using std::cout;
using std::endl;
using std::stoi;
using std::string;
using std::shared_ptr;
using std::unique_ptr;

using dps::BrokerServer;
using dps::GuruServer;
using dps::GetBrokerRequest;
using dps::GetBrokerResponse;
using dps::ReadMessageRequest;
using dps::ReadMessageResponse;

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
typedef unique_ptr<GuruServer::Stub> GuruStub;

// *************************** Volatile Variables *****************************
uint brokerId;
string brokerAddr;


class Subscriber {
  public:
    Subscriber(shared_ptr<Channel> guruchannel);
    int GetBrokerForRead(int topicid);
    int Subscribe(int topicid);
    int ReadMessageStream(int topicid, int index);
    BrokerStub brokerstub_;
  private:
    GuruStub gurustub_;
};

Subscriber* subClient;

Subscriber::Subscriber(shared_ptr<Channel> guruchannel) 
  : gurustub_(GuruServer::NewStub(guruchannel)) 
{
  printf("------------ Opened channel to Guru -------------\n");
}

int Subscriber::GetBrokerForRead(int topicId){
  GetBrokerRequest request;
  GetBrokerResponse response;
  Status status;
  ClientContext context;

  request.set_topicid(topicId);
  response.Clear();
  status = gurustub_->GetBrokerForRead(&context, request, &response);

  if(status.ok()){
    printf("[GetBrokerForRead] Read messages from Broker : %d at address: %s.\n", response.brokerid(), response.brokeraddr().c_str());
    brokerId = response.brokerid();
    brokerAddr = response.brokeraddr();
    this->brokerstub_ = BrokerServer::NewStub(grpc::CreateChannel(brokerAddr, grpc::InsecureChannelCredentials()));
    return 0;
  } else {
    printf("[GetBrokerForRead] Unable to fetch broker for this topic, please retry.\n");
    return -1;
  }
}

int Subscriber::ReadMessageStream(int topicId, int index) 
{
  ReadMessageRequest request;
  ReadMessageResponse response;
  Status status;
  ClientContext context;

  request.set_topicid(topicId);
  request.set_index(index);
  response.Clear();

  std::unique_ptr<ClientReader<ReadMessageResponse> > reader(
            this->brokerstub_->ReadMessageStream(&context, request));

  int readInd = 0;
  while (reader->Read(&response)) {
      printf("Message: %s %d\n", response.message().c_str(), response.readind());
      readInd = response.readind();
  }
  printf("Read till %d index\n", readInd);
  status = reader->Finish();
  if(status.ok()){
    printf("[ReadMessageStream] Read message!\n");
    return readInd;
  } else {
    printf("[ReadMessageStream] Unable to reach broker for this topic, please retry.\n");
    return -1;
  }
  return readInd;
}

int main(int argc, char* argv[]) {
  char c = '\0';
  int topicId = -1;
  int brokerId = 0;
  int index = 0;

  // Get command line args
  while ((c = getopt(argc, argv, "t:b:i:")) != -1) {
    switch (c){
      case 't':
        topicId = stoi(optarg);
        printf("TopicID: %d\n", topicId);
        break;
      case 'b':
        brokerId = stoi(optarg);
        printf("BrokerID: %d\n", brokerId);
        break;
      case 'i':
        index = stoi(optarg);
        printf("Message index: %d\n", index);
        break;
      default:
        cout << "Invalid arg" << endl;
        return -1;
    }
  }

  if(topicId == -1){
    printf("TopicID is a required argument! Add using -t <topicId>\n");
    return 0;
  }

  subClient = new Subscriber(grpc::CreateChannel(GURU_ADDRESS, grpc::InsecureChannelCredentials()));

  int getBroker = subClient->GetBrokerForRead(topicId);
  assert(getBroker == 0);

  int read = subClient->ReadMessageStream(topicId, index);
  return read;
}
