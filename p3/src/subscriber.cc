#include <grpcpp/grpcpp.h>
#include <string>

#include "dps.grpc.pb.h"

using grpc::Channel;
using std::string;
using std::shared_ptr;
using std::unique_ptr;
using dps::BrokerServer;
using dps::GuruServer;

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

int main(int argc, char* argv[]) {
  return 0;
}
