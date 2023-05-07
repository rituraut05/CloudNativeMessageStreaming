#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <ctime>
#include <unistd.h>
#include <grpcpp/grpcpp.h>
#include <shared_mutex>

#include "dps.grpc.pb.h"
#include "common.hh"

using std::cout;
using std::endl;
using std::stoi;
using std::string;
using std::shared_ptr;
using std::unique_ptr;
using std::shared_mutex;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using dps::BrokerServer;
using dps::GuruServer;
using dps::GetBrokerRequest;
using dps::GetBrokerResponse;
using dps::PublishMessageRequest;
using dps::PublishMessageResponse;

typedef unique_ptr<BrokerServer::Stub> BrokerStub;
typedef unique_ptr<GuruServer::Stub> GuruStub;

// *************************** Volatile Variables *****************************
uint brokerId = 0;
string brokerAddr;
string message;
int retry = 5;
shared_mutex mutex_brokerId; 

class Publisher {
  public:
    Publisher(shared_ptr<Channel> guruchannel);
    int GetBrokerForWrite(int topicid); 
    int AddTopic(int topicid);
    int PublishMessage(int topicid, string message);
    BrokerStub brokerstub_;

  private:
    GuruStub gurustub_;
};

Publisher* publishClient;

Publisher::Publisher(shared_ptr<Channel> guruchannel) 
  : gurustub_(GuruServer::NewStub(guruchannel)) 
{
  printf("------------ Opened channel to Guru -------------\n");
}

int Publisher::GetBrokerForWrite(int topicId){
  GetBrokerRequest request;
  GetBrokerResponse response;
  Status status;
  ClientContext context;

  request.set_topicid(topicId);
  response.Clear();
  status = gurustub_->GetBrokerForWrite(&context, request, &response);

  if(status.ok()){
    printf("[GetBrokerForWrite] Publish messages to Broker : %d at address: %s.\n", response.brokerid(), response.brokeraddr().c_str());
    mutex_brokerId.lock();
    brokerId = response.brokerid();
    mutex_brokerId.unlock();
    brokerAddr = response.brokeraddr();
    this->brokerstub_ = BrokerServer::NewStub(grpc::CreateChannel(brokerAddr, grpc::InsecureChannelCredentials()));
    return 0;
  } else {
    printf("[GetBrokerForWrite] Unable to fetch broker for this topic, please retry.\n");
    return -1;
  }
}

int Publisher::PublishMessage(int topicId, string message){
  PublishMessageRequest request;
  PublishMessageResponse response;
  Status status;
  ClientContext context;

  request.set_topicid(topicId);
  request.set_message(message);
  response.Clear();

  status = this->brokerstub_->PublishMessage(&context, request, &response);

  if(status.ok()){
    if(response.db_errno() == EPERM){
      printf("[PublishMessage] Broker has changed. Contact GURU!\n");
      this->GetBrokerForWrite(topicId);
      return -1;
    }
    printf("[PublishMessage] Published message!\n");
    return 0;
  } else {
    printf("[PublishMessage] Unable to reach broker for this topic, please retry.\n");
    return -1;
  }
  return 0;
}

std::string gen_random(const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::string tmp_s;
    tmp_s.reserve(len);
    srand((unsigned)time(NULL) * getpid());
    for (int i = 0; i < len; ++i) {
        tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    return tmp_s;
}

int main(int argc, char* argv[]) {
  char c = '\0';
  int topicId = -1;
  int msgLength = 8;

  // Get command line args
  while ((c = getopt(argc, argv, "t:b:l:")) != -1) {
    switch (c){
      case 't':
        topicId = stoi(optarg);
        printf("TopicID: %d\n", topicId);
        break;
      case 'b':
        mutex_brokerId.lock();
        brokerId = stoi(optarg);
        mutex_brokerId.unlock();
        printf("BrokerID: %d\n", brokerId);
        break;
      case 'l':
        msgLength = stoi(optarg);
        printf("Message length: %d\n", msgLength);
        break;
      default:
        cout << "Invalid arg" << endl;
        return -1;
    }
  }

  if(topicId == -1){
    printf("TopicID is a required argument! Add using -t <topicId>\n");
    return -1;
  }

  //getBrokerForWrite(topicId)
  publishClient = new Publisher(grpc::CreateChannel(GURU_ADDRESS, grpc::InsecureChannelCredentials()));

  if(brokerId > 2){
    int getBroker = publishClient->GetBrokerForWrite(topicId);
    assert(getBroker == 0);
  } else {
    publishClient->brokerstub_ = BrokerServer::NewStub(grpc::CreateChannel(C0_ADDRESSES[brokerId], grpc::InsecureChannelCredentials()));
  }

  message = gen_random(msgLength);
  cout<<message<<endl;
  int published = publishClient->PublishMessage(topicId, message);
  while(published != 0 && retry > 0){
    published = publishClient->PublishMessage(topicId, message);
    retry--;
  }
  mutex_brokerId.lock();
  printf("BrokerID: %d", brokerId);
  mutex_brokerId.unlock();
  // if(published == -1) return -1;
  return brokerId;
}
