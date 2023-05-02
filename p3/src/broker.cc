#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <string>

#include "utils.hh"
#include "common.hh"
#include "dps.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::StatusCode;

using std::string;
using std::shared_ptr;
using std::unique_ptr;
using std::thread;
using std::endl;
using dps::BrokerServer;
using dps::GuruServer;
using dps::HeartbeatRequest;
using dps::HeartbeatResponse;
using dps::RequestVoteRequest;
using dps::RequestVoteResponse;
using dps::StartElectionRequest;
using dps::StartElectionResponse;
using dps::ClusterConfigRequest;
using dps::ClusterConfigResponse;
using dps::ServerConfig;
using util::Timer;


// *************************** Class Definitions ******************************
class BrokerToGuruClient {
  public:
    BrokerToGuruClient(shared_ptr<Channel> guruchannel);
    int SetLeader();
    int SendHeartbeat();
    int RequestConfig(int brokerid);

  private:
    unique_ptr<GuruServer::Stub> gurustub_;
};

// *************************** Volatile Variables *****************************
uint clusterID;
uint serverID;
Timer heartbeatTimer(1, HEARTBEAT_TIMEOUT);
unordered_map<int, ServerInfo> brokersInCluster;
BrokerToGuruClient* bgClient;

// *************************** Functions *************************************

void runHeartbeatTimer() {
  heartbeatTimer.start(HEARTBEAT_TIMEOUT);
  while(heartbeatTimer.get_tick() < heartbeatTimer._timeout) ; // spin
  int ret = bgClient->SendHeartbeat();
  runHeartbeatTimer();
}

void setCurrState(State cs, int topicID)
{
  mutex_cs.lock();
  currStateMap[topicID] = cs;
  mutex_cs.unlock();
  if(cs == LEADER) {
    // add topicid to your topics list.
    topicsUnderLeadership.push_back(topicID);
    // TODO: call setLeaderId for guru.
    printf("%s %s %s %s %s %s %s %s %s %s %s %s \n", SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE);
  }
  printf("Server %d = %s for term = %d\n", serverID, stateNames[cs].c_str(), currentTerm[topicID]);
}


BrokerToGuruClient::BrokerToGuruClient(shared_ptr<Channel> guruchannel)
  : gurustub_(GuruServer::NewStub(guruchannel)) {}

int BrokerToGuruClient::SendHeartbeat() {
  HeartbeatRequest request;
  HeartbeatResponse response;
  Status status;
  ClientContext context;

  request.set_serverid(serverID);
  request.set_clusterid(clusterID);
  response.Clear();
  status = gurustub_->SendHeartbeat(&context, request, &response);

  if(status.ok()) {
    printf("[SendHeartbeat RPC] %s %s %s Successfully sent heartbeat to Guru %s %s %s\n", HEART, HEART, HEART, HEART, HEART, HEART);
    return 0;
  } else {
    printf("[SendHeartbeat RPC] Failure.\n");
    return -1;
  }
}

int BrokerToGuruClient::RequestConfig(int brokerid) {
  ClusterConfigRequest request;
  ClusterConfigResponse response;
  Status status;
  ClientContext context;

  request.set_serverid(brokerid);
  response.Clear();
  status = gurustub_->RequestConfig(&context, request, &response);

  if(status.ok()) {
    printf("[RequestConfig] Setting clusterID = %d.\n", response.clusterid());
    clusterID = response.clusterid();
    brokersInCluster.clear();
    for(ServerConfig sc: response.brokers()) {
      ServerInfo si(sc.serverid(), clusterID, sc.servaddr());
      if(si.serverid != serverID) si.initBrokerClient();
      brokersInCluster[si.serverid] = si;
    }
    topicsInCluster.clear();
    for(uint topicid: response.topics()) {
      topicsInCluster.push_back(topicid);
    }
    return 0;
  } else {
    printf("[RequestConfig] Unable to fetch cluster config, please retry.\n");
    return -1;
  }
}


BrokerClient::BrokerClient(shared_ptr<Channel> channel)
  : stub_(BrokerServer::NewStub(channel)) {}

int BrokerClient::RequestVote(int lastLogTerm, int candLastLogIndex, int followerID, int topicID){
  printf("[RequestVote]: RaftClient invoked\n");

  RequestVoteRequest request;
  RequestVoteResponse reply;
  Status status;
  ClientContext context;

  request.set_term(currentTerm[topicID]);
  request.set_candidateid(serverID);
  request.set_lastlogterm(lastLogTerm);
  request.set_lastlogindex(candLastLogIndex);
  request.set_topicid(topicID);

  reply.Clear();

  status = stub_->RequestVote(&context, request, &reply);

  if(status.ok()) {
    printf("[RequestVote]: BrokerClient - RPC Success\n");
    if(reply.term() > currentTerm[topicID]) {
      printf("[RequestVote]: BrokerClient - Term of the server %d is higher than %d candidate\n", followerID, serverID);
    }
    if(reply.votegranted()){
      printf("[RequestVote]: BrokerClient - Server %d granted vote for %d\n",followerID, serverID);
      return 1;
    }else{
      printf("[RequestVote]: BrokerClient - Server %d did not vote for %d\n",followerID, serverID);
    }
  } else {

      if(status.error_code() == StatusCode::UNAVAILABLE){
        printf("[RequestVote]: BrokerClient - Unavailable server\n");
      }
      printf("[RequestVote]: BrokerClient - RPC Failure\n");
      return -1; // failure
  }

  return 0;
}



/************************ Helper Functions for BrokerGrpcServer **************************************/
int getRandomTimeout() {
  unsigned seed = system_clock::now().time_since_epoch().count();
  default_random_engine generator(seed);
  uniform_int_distribution<int> distribution(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT);
  return distribution(generator);
}

void invokeRequestVote(BrokerClient* followerClient, int followerID, int topicID){
  // RequestVote, gather votes
  // should implement retries of RequestVote on unsuccessful returns
  int lastLogTerm = 0;
  if(logs[topicID].size()>0) {
    lastLogIndex[topicID] = logs[topicID].back().index;
    lastLogTerm = logs[topicID].back().term;
  }
  int ret = followerClient->RequestVote(lastLogTerm, lastLogIndex[topicID], followerID, topicID);
  if (ret == 1) {
    mutex_votes.lock();
    votesReceived[topicID]++;
    mutex_votes.unlock();
  }
  return;
}

void runElection(int topicID) {
  printf("Hi I have started the runElection async function\n");

  // start election timer
  beginElectionTimer.start(getRandomTimeout());
  while(beginElectionTimer.running() && 
    beginElectionTimer.get_tick() < beginElectionTimer._timeout) ; // spin
  printf("[runElection] Spun for %d ms before timing out in state %d for term %d\n", beginElectionTimer.get_tick(), currStateMap[topicID], currentTerm[topicID]);

  // invoke requestVote on other alive brokers.
  mutex_votes.lock();
  votesReceived[topicID] = 0;
  mutex_votes.unlock();

  mutex_ct.lock();
  currentTerm[topicID]++;
  pmetadata[topicID]->Put(leveldb::WriteOptions(), "currentTerm", to_string(currentTerm[topicID]));
  mutex_ct.unlock();

  mutex_vf.lock();
  votedFor[topicID] = serverID;
  pmetadata[topicID]->Put(leveldb::WriteOptions(), "votedFor", to_string(serverID));
  mutex_vf.unlock();

  mutex_votes.lock();
  votesReceived[topicID]++;
  mutex_votes.unlock();

  printf("[runElection] Running Election for topic %d, term=%d\n", topicID, currentTerm[topicID]);

  // invoke RequestVote threads
  vector<thread> RequestVoteThreads;
  for(auto si: brokersInCluster) {
    if(si.second.serverid != serverID) {

      RequestVoteThreads.push_back(thread(invokeRequestVote, si.second.client, si.second.serverid, topicID));
    }
  }

 // wait until all request votes threads have completed.
  for(int i=0; i<RequestVoteThreads.size(); i++){
    if(RequestVoteThreads[i].joinable()) {
      RequestVoteThreads[i].join();
    }
  }
  RequestVoteThreads.clear();

  // call setLeader if majority votes were received.
  int majority = (BROKER_COUNT+1)/2;
  printf("votesReceived = %d, Majority = %d for topic %d\n", votesReceived[topicID], majority, topicID);
  if(votesReceived[topicID] >= majority) {
    printf("Candidate %d received majority of votes from available servers for topic %d\n", serverID, topicID);
    setCurrState(LEADER, topicID);
  }
}


/****************************************** BrokerGrpcServer *****************************************/
class BrokerGrpcServer final : public BrokerServer::Service {
  public:
    explicit BrokerGrpcServer() {}

    Status RequestVote(ServerContext *context, const RequestVoteRequest *req, RequestVoteResponse *resp) override
    {
      int term = req->term();
      int candidateID = req->candidateid();
      int lli = req->lastlogindex();
      int llt = req->lastlogterm();
      int topicID = req->topicid();

      mutex_cs.lock();
      printf("[RequestVote] invoked on %s %d by candidate %d for term %d for topic %d with lli %d, llt %d\n", stateNames[currStateMap[topicID]].c_str(), serverID, candidateID, term, topicID, lli, llt);
      mutex_cs.unlock();

      mutex_ct.lock();
      int ctLocal = currentTerm[topicID];
      mutex_ct.unlock();


      if (term < ctLocal){ // curr server has a greater term than candidate so it will not vote
        resp->set_term(ctLocal);
        resp->set_votegranted(false);
        printf("NOT voting: term %d < currentTerm %d\n", term, ctLocal);

        return Status::OK;
      }
      if(term > ctLocal){
        // JUST UPDATE CURRENTTERM AND DON"T VOTE
        // TODO: Discuss reasons
        /* just update currentTerm and don't vote.
        Reason 1: if the current leader which is alive and has same currentTerm can receive 
        this candidate's term on next appendEntries response becomes a follower.
        Reason 2: incase of no leader in this candidate's term, this vote should 
        */
        mutex_ct.lock();
        currentTerm[topicID] = term;
        pmetadata[topicID]->Put(leveldb::WriteOptions(), "currentTerm", to_string(currentTerm[topicID]));
        mutex_ct.unlock();

        mutex_vf.lock();
        votedFor[topicID] = -1;
        pmetadata[topicID]->Put(leveldb::WriteOptions(), "votedFor", to_string(votedFor[topicID]));
        mutex_vf.unlock();
        // IMP: whenever currentTerm is increased we should also update votedFor to -1, should check AppendEntries also for such scenarios.

        setCurrState(FOLLOWER, topicID);

        // electionTimer.reset(getRandomTimeout());
        resp->set_term(ctLocal); 
        resp->set_votegranted(false);
        printf("[RequestVote]Candidate %d has higher term than me, updating current term.\n", candidateID);
      }
      
      // else if(term == ctLocal){ // that means someBody has already sent the requestVote as it has already seen this term     
      mutex_vf.lock();
      if(votedFor[topicID] == -1 || votedFor[topicID] == candidateID) {
        int voter_lli = 0;
        int voter_llt = 0;
        if(logs[topicID].size()>0){
          voter_lli = logs[topicID].back().index;
          voter_llt = logs[topicID].back().term;
        }

        if(llt > voter_llt || (llt == voter_llt && lli >= voter_lli)) { // candidate has longer log than voter or ..
          resp->set_term(ctLocal); 
          resp->set_votegranted(true);
          // electionTimer.reset(getRandomTimeout());
          printf("llt = %d \nvoter_llt = %d \nlli = %d \nvoter_lli = %d\n", llt, voter_llt, lli, voter_lli);
          printf("VOTED!: Candidate has longer log than me\n");

          mutex_ct.lock();
          currentTerm[topicID] = term;
          pmetadata[topicID]->Put(leveldb::WriteOptions(), "currentTerm", to_string(currentTerm[topicID]));
          mutex_ct.unlock();

          votedFor[topicID] = candidateID;
          pmetadata[topicID]->Put(leveldb::WriteOptions(), "votedFor", to_string(candidateID));
        } else {
          resp->set_term(currentTerm[topicID]); 
          resp->set_votegranted(false);
          printf("llt = %d \nvoter_llt = %d \nlli = %d \nvoter_lli = %d\n", llt, voter_llt, lli, voter_lli);
          printf("NOT voting: I have most recent log or longer log\n");
        }
        mutex_vf.unlock();
        return Status::OK;
      } else {
        resp->set_term(ctLocal);
        resp->set_votegranted(false);
        printf("NOT voting: votedFor %d\n", votedFor[topicID]);
        mutex_vf.unlock();
        return Status::OK;
      }
      mutex_vf.unlock();
      // }

      // anything that doesn't follow the above condition don't vote!
      return Status::OK;
    }

    Status StartElection(ServerContext *context, const StartElectionRequest *req, StartElectionResponse *resp) override
    {
      int topicID = req->topicid();
      // start election which will trigger requestVote
      std::async(std::launch::async, runElection, topicID);
      /*
      // test the above otherwise replace it with the below
      // runElectionThread = thread { runElection, topicID}; 
      // TODO: Join this thread appropriately.
      */
      return Status::OK;
    }
};

void openOrCreateDBs() {
  leveldb::Options options;
  options.create_if_missing = true;

  for(auto id: topicsInCluster){
    leveldbPtr plogsPtr;
    leveldb::Status plogs_status = leveldb::DB::Open(options, "/tmp/plogs" + to_string(serverID) + "-" + to_string(id), &plogsPtr);
    if (!plogs_status.ok()) std::cerr << plogs_status.ToString() << endl;
    assert(plogs_status.ok());
    plogs[id] = plogsPtr;
    printf("[openOrCreateDBs] Successfully opened plogs DB.\n");

    leveldbPtr pmetadataPtr;
    leveldb::Status pmetadata_status = leveldb::DB::Open(options, "/tmp/pmetadata" + to_string(serverID) + "-" + to_string(id), &pmetadataPtr);
    if(!pmetadata_status.ok()) std::cerr << pmetadata_status.ToString() << endl;
    assert(pmetadata_status.ok());
    pmetadata[id] = pmetadataPtr;
    printf("[openOrCreateDBs] Successfully opened pmetadata DB.\n");

    leveldbPtr replicateddbPtr;
    leveldb::Status replicateddb_status = leveldb::DB::Open(options, "/tmp/replicateddb" + to_string(serverID) + "-" + to_string(id), &replicateddbPtr);
    if(!replicateddb_status.ok()) std::cerr << replicateddb_status.ToString() << endl;
    assert(replicateddb_status.ok());
    replicateddb[id] = replicateddbPtr;
    printf("[openOrCreateDBs] Successfully opened replicateddb DB.\n");
  }
  
}

void RunGrpcServer(string server_address) {
  BrokerGrpcServer service;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server = builder.BuildAndStart();
  std::cout << "[RunGrpcServer] Server listening on " << server_address << std::endl;
  server->Wait();
}

int main(int argc, char* argv[]) {
  if(argc != 2) {
    printf("Usage: ./broker <serverid>\n");
    return 0;
  }
  serverID = atoi(argv[1]);
  bgClient = new BrokerToGuruClient(grpc::CreateChannel(GURU_ADDRESS, grpc::InsecureChannelCredentials()));

  int rc_ret = bgClient->RequestConfig(serverID);
  assert(rc_ret == 0);
  for(auto si: brokersInCluster) {
    printf("Broker: %d-%s in Cluster: %d\n", si.first, si.second.server_name.c_str(), clusterID);
  }
  for(uint tpcid: topicsInCluster) {
    printf("Topic added to cluster: %d\n", tpcid);
  }

  /*
  * Please use brokersInCluster.client to call BrokerClient's functions like AppendEntries and RequestVote
  * which will contact BrokerGrpcServer of respective server_addr 
  */

  openOrCreateDBs();
  thread heartbeat(runHeartbeatTimer);
  RunGrpcServer(brokersInCluster[serverID].server_addr);

  if(heartbeat.joinable()) heartbeat.join();
  return 0;
}