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

class Publisher {
  public:
    Publisher(shared_ptr<Channel> guruchannel);
    BrokerStub GetBrokerForWrite(int topicid); 
    int AddTopic(int topicid);
    int SendMessage(BrokerStub brokerstub, int topicid, string message);

  private:
    unique_ptr<GuruServer::Stub> gurustub_;
};

Publisher::Publisher(shared_ptr<Channel> guruchannel) 
  : gurustub_(GuruServer::NewStub(guruchannel)) 
{
  printf("------------ Opened channel to Guru -------------\n");
}

int main(int argc, char* argv[]) {
  return 0;
}
