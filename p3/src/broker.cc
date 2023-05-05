#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <string>

#include "utils.hh"
#include "common.hh"
#include "timer.hh"
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
using dps::SetLeaderRequest;
using dps::SetLeaderResponse;
using dps::StartElectionRequest;
using dps::StartElectionResponse;
using dps::ClusterConfigRequest;
using dps::ClusterConfigResponse;
using dps::ServerConfig;
using dps::AppendEntriesRequest;
using dps::AppendEntriesResponse;
using dps::LogEntry;
using dps::BrokerUpRequest;
using dps::BrokerUpResponse;
using util::Timer;


// *************************** Class Definitions ******************************
class BrokerToGuruClient {
  public:
    BrokerToGuruClient(shared_ptr<Channel> guruchannel);
    int SetLeader(int topicid);
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

bool greaterThanMajority(unordered_map<int, int> map, int N) {
  int majcnt = (int)((BROKER_COUNT+1)/2);
  for(auto& [key, value] : map) {
    if(value >= N) majcnt--;
  }
  if(majcnt > 0) return false;
  return true;
}

void checkAndUpdateCommitIndex() {
  // check and update commit index 
  while(true){
    mutex_tul.lock();
    vector<int> tulLocal = topicsUnderLeadership;
    mutex_tul.unlock();
    for(int topicId : tulLocal) {
      mutex_lli.lock();
      int lliLocal = lastLogIndex[topicId];
      mutex_lli.unlock();
      mutex_mi.lock();
      matchIndex[topicId][serverID] = lliLocal;
      unordered_map<int, int> matchIndexLocal = matchIndex[topicId];
      mutex_mi.unlock();
      
      mutex_ci.lock();
      mutex_ucif.lock();
      for(int N = lliLocal; N>commitIndex[topicId]; N--) {
        auto NLogIndexIt = logs[topicId].end();
        for(; NLogIndexIt != logs[topicId].begin(); NLogIndexIt--) {
          if(NLogIndexIt->index == N) break;
        }
        if(greaterThanMajority(matchIndexLocal, N) && NLogIndexIt->term == currentTerm[topicId]) {
          printf("[runRaftServer] LEADER: Commiting index = %d\n", N);
          commitIndex[topicId] = N;
          updateCommitIndexFlag[topicId] = true;
          break;
        }
      }
      mutex_ucif.unlock();
      mutex_ci.unlock();
    }
  }
}

void executeLog() {
  while(true) {
    mutex_ci.lock();
    unordered_map<int, int> commitIndexLocal = commitIndex;
    mutex_ci.unlock();
    mutex_la.lock();
    unordered_map<int, int> lastAppliedLocal = lastApplied;
    mutex_la.unlock();
    for(int topicId : topicsInCluster){
      vector<string> new_messages;
      if(lastAppliedLocal[topicId] < commitIndexLocal[topicId]) {
        printf("[ExecuteLog]: Executing log (size = %lu) for TOPIC %d from index: %d\n", logs[topicId].size(), topicId, lastAppliedLocal[topicId]+1); 
        for(int i = lastAppliedLocal[topicId]+1; i < logs[topicId].size(); i++){
          if(i <= commitIndexLocal[topicId]){
            new_messages.push_back(logs[topicId][i].msg);
            lastAppliedLocal[topicId]++;
          }
        }
      }
      // update
      if(new_messages.size() > 0){
        cout << new_messages.size() << endl;
        mutex_messageQ.lock();
        messageQ[topicId].insert(messageQ[topicId].end(), new_messages.begin(), new_messages.end());
        mutex_messageQ.unlock();
        mutex_la.lock();
        lastApplied[topicId] = lastAppliedLocal[topicId];
        pmetadata[topicId]->Put(leveldb::WriteOptions(), "lastApplied", to_string(lastApplied[topicId]));
        mutex_la.unlock();
      }
    }
  }
}

void setCurrState(int topicId, State cs)
{
  mutex_cs.lock();
  currStateMap[topicId] = cs;
  mutex_cs.unlock();
  if(cs == LEADER) {
    mutex_ni.lock();
    mutex_mi.lock();
    mutex_lli.lock();
    for(auto& [brokerId, si] : brokersInCluster) {
      nextIndex[topicId][brokerId] = lastLogIndex[topicId] + 1;
      if(brokerId == serverID) {
        matchIndex[topicId][brokerId] = lastLogIndex[topicId];
      } else {
        matchIndex[topicId][brokerId] = -1;
      }
    }
    mutex_lli.unlock();
    mutex_mi.unlock();
    mutex_ni.unlock();
    mutex_tul.lock();
    if(std::find(topicsUnderLeadership.begin(), topicsUnderLeadership.end(), topicId) == topicsUnderLeadership.end())
      topicsUnderLeadership.push_back(topicId);
    mutex_tul.unlock();
    printf("%s %s %s %s %s %s %s %s %s %s %s %s \n", SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE);
  }
  printf("Server %d = %s for term = %d and topic = %d\n", serverID, stateNames[cs].c_str(), currentTerm[topicId], topicId);
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
    // printf("[SendHeartbeat RPC] %s %s %s Successfully sent heartbeat to Guru %s %s %s\n", HEART, HEART, HEART, HEART, HEART, HEART);
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
    mutex_tul.lock();
    topicsUnderLeadership.clear();
    for(uint topicid: response.leadingtopics()) {
      topicsUnderLeadership.push_back(topicid);
    }
    mutex_tul.unlock();
    return 0;
  } else {
    printf("[RequestConfig] Unable to fetch cluster config, please retry.\n");
    return -1;
  }
}

int BrokerToGuruClient::SetLeader(int topicID) {
  SetLeaderRequest request;
  SetLeaderResponse response;
  Status status;
  ClientContext context;

  request.set_leaderid(serverID);
  request.set_topicid(topicID);
  response.Clear();
  status = gurustub_->SetLeader(&context, request, &response);

  if(status.ok()) {
    printf("[SetLeader RPC]: Successfully set leaderID %d at Guru for topic %d\n", serverID, topicID);
    return 0;
  } else {
    printf("[SetLeader RPC] Failure.\n");
    return -1;
  }
}

int sendAppendEntriesRpc(int followerid, int topicId, int nextIndexLocal, int lastidx){
  printf("Calling sendAppendEntriesRpc for follower %d\n", followerid);  
  int ret = brokersInCluster[followerid].client->AppendEntries(topicId, nextIndexLocal, lastidx);
  mutex_mi.lock();
  mutex_ni.lock();
  if(ret == 0) { // success
    sendLogEntries[followerid] = true;
    printf("[sendAppendEntriesRpc] AppendEntries successful for followerid = %d for topic = %d, startidx = %d, endidx = %d\n", followerid, topicId, nextIndex[topicId][followerid], lastidx);
    nextIndex[topicId][followerid] = lastidx + 1;
    matchIndex[topicId][followerid] = lastidx;
    
  } else if(ret == -1) { // RPC Failure
    sendLogEntries[followerid] = false;
  } else if(ret == -2) { // log inconsistency
    printf("[sendAppendEntriesRpc] AppendEntries failure; Log inconsistency for followerid = %d for topic = %d, new nextIndex = %d\n", followerid, topicId, nextIndex[topicId][followerid]);
    sendLogEntries[followerid] = true;
    nextIndex[topicId][followerid]--;
  } else if(ret == -3) { // term of follower bigger, convert to follower
    printf("[sendAppendEntriesRpc] AppendEntries failure; Follower (%d) has bigger term (new term = %d) for topic %d, converting to follower.\n", followerid, currentTerm.at(topicId), topicId);
    sendLogEntries[followerid] = true;
    setCurrState(topicId, FOLLOWER);
    return -1;
  }
  mutex_ni.unlock();
  mutex_mi.unlock();
  return 0;
}

void invokeAppendEntries(int followerid) {
  printf("[invokeAppendEntries] Starting thread for brokerID: %d\n", followerid);
  while(true) {
    int status = 0;
    mutex_tul.lock();
    vector<int> tulLocal = topicsUnderLeadership;
    mutex_tul.unlock();
    for(int topicId: tulLocal){
      // printf("[invokeAppendEntries] Entering for topicID: %d and followerId: %d!\n", topicId, followerid);
      mutex_lli.lock();
      mutex_ni.lock();
      int lli_local = lastLogIndex[topicId];
      int ni_local = nextIndex[topicId][followerid];
      mutex_ni.unlock();
      mutex_lli.unlock();
      // printf("[invokeAppendEntries] nextIndex[%d][%d]: %d\n", topicId, followerid, nextIndex[topicId][followerid]);
      // printf("[invokeAppendEntries] lastLogIndex[%d]: %d\n", topicId, lastLogIndex[topicId]);
      mutex_sle.lock();
      if(ni_local <= lli_local && sendLogEntries[followerid]) {
        mutex_sle.unlock();
        printf("[invokeAppendEntries] followerid != serverID for topic: %d\n", topicId);
        int lastidx = lli_local;
        status = sendAppendEntriesRpc(followerid, topicId, ni_local, lastidx);
      }
      mutex_sle.unlock();
      mutex_ucif.lock();
      if(updateCommitIndexFlag[topicId]) {
        printf("[invokeAppendEntries]sending rpc for updatecommitindex: %d\n", topicId);
        int lastidx = -1;
        status = sendAppendEntriesRpc(followerid, topicId, ni_local, lastidx);
        updateCommitIndexFlag[topicId] = false;
      }
      mutex_ucif.unlock();
      if(status == -1) break;
    }
  }
}

void updateLog(int topicId, std::vector<LogEntry> logEntries, int logIndex, int leaderCommitIndex){
  printf("[Broker(Raft)Server:AppendEntries]logs need update--int logIndex, int leaderCommitIndex: %d, %d\n", logIndex, leaderCommitIndex);
  Log logEntry;
  logs[topicId].erase(logs[topicId].begin()+logIndex, logs[topicId].end());
  // delete from DB
  for(auto itr = logEntries.begin(); itr != logEntries.end(); itr++){
    // printf("[Broker(Raft)Server:AppendEntries]adding entry\n");
    logEntry = Log(itr->index(), itr->term(), itr->topicid(), itr->messageindex(), itr->message());
    logs[topicId].push_back(logEntry);
    plogs[topicId]->Put(leveldb::WriteOptions(), to_string(logEntry.index), logEntry.toString());
    mutex_lli.lock();
    lastLogIndex[topicId] = itr->index();
    mutex_lli.unlock();
  }
  printRaftLog();
}

// ***************************** Broker(Raft)Client Code *****************************

BrokerClient::BrokerClient(std::shared_ptr<Channel> channel)
  : stub_(BrokerServer::NewStub(channel)){}

  int BrokerClient::AppendEntries(int topicId, int logIndex, int lastIndex) {
  printf("[BrokerRaftClient::AppendEntries]Entering\n");
  AppendEntriesRequest request;
  AppendEntriesResponse response;
  Status status;
  ClientContext context;

  int prevLogIndex = logIndex-1;
  // TODO : What if the log is empty
  request.set_term(currentTerm[topicId]);
  request.set_topicid(topicId);
  // request.set_leaderid(leaderID);
  request.set_prevlogindex(prevLogIndex);
  prevLogIndex == -1 ? request.set_prevlogterm(0) : request.set_prevlogterm(logs[topicId][prevLogIndex].term);
  request.set_leadercommitindex(commitIndex[topicId]);

  // creating log entries to store
  // printf("[BrokerRaftClient::AppendEntries]int topicId, int logIndex, int lastIndex: %d, %d, %d\n", topicId, logIndex, lastIndex);
  mutex_logs.lock();
  for (int nextIdx = logIndex; nextIdx <= lastIndex; nextIdx++) {
    // printf("[BrokerRaftClient::AppendEntries]add log entries to request\n");
    dps::LogEntry *reqEntry = request.add_entries();
    Log logEntry = logs[topicId][nextIdx];
    reqEntry->set_index(logEntry.index);
    reqEntry->set_term(logEntry.term);
    reqEntry->set_message(logEntry.msg);
  }
  mutex_logs.unlock();

  response.Clear();
  status = stub_->AppendEntries(&context, request, &response);

  if (status.ok()) {
    printf("[RaftClient::AppendEntries] RPC Success\n");
    if(response.success() == false){
      mutex_ct.lock();
      if(response.currterm() > currentTerm[topicId]){
        printf("[RaftClient::AppendEntries] Higher Term in Response\n");
        currentTerm[topicId] = response.currterm();
        pmetadata[topicId]->Put(leveldb::WriteOptions(), "currentTerm", to_string(currentTerm[topicId]));
        pmetadata[topicId]->Put(leveldb::WriteOptions(), "votedFor", to_string(-1));     
        return -3; // leader should convert to follower
      } else {
        printf("[RaftClient::AppendEntries] Term mismatch at prevLogIndex. Try with a lower nextIndex.\n");
        return -2; // decrement nextIndex
      }
      mutex_ct.unlock();
    }
  } else {
    printf("[RaftClient::AppendEntries] RPC Failure\n");
    return -1;
  }
  return 0;
}

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
  printf("Hi I have started the runElection async function for topic %d\n", topicID);

  // start election timer
  // TODO: Add locks for beginElectionTimer
  beginElectionTimer[topicID] = Timer(1, MAX_ELECTION_TIMEOUT);
  beginElectionTimer[topicID].set_running(true);
  beginElectionTimer[topicID].start(getRandomTimeout());
  printf("Should spin for topicid %d for %d ms\n", topicID, beginElectionTimer[topicID]._timeout);

  while(beginElectionTimer[topicID].running() && beginElectionTimer[topicID].get_tick() < beginElectionTimer[topicID]._timeout) ; // spin

  if(!beginElectionTimer[topicID].running()) return;
  
  printf("[runElection] Spun for %d ms before timing out in state %s for term %d\n", beginElectionTimer[topicID].get_tick(), stateNames[currStateMap[topicID]].c_str(), currentTerm[topicID]);

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

  beginElectionTimer[topicID].set_running(false);
  // call setLeader if majority votes were received.
  int majority = (int)((BROKER_COUNT+1)/2);
  printf("votesReceived = %d, Majority = %d for topic %d\n", votesReceived[topicID], majority, topicID);
  if(votesReceived[topicID] >= majority) {
    printf("Candidate %d received majority of votes from available servers for topic %d\n", serverID, topicID);
    setCurrState(topicID, LEADER);
    bgClient->SetLeader(topicID);
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

        setCurrState(topicID, FOLLOWER);

        // electionTimer.reset(getRandomTimeout());
        resp->set_term(ctLocal); 
        resp->set_votegranted(false);
        printf("[RequestVote]Candidate %d has higher term than me, updating current term.\n", candidateID);
      }
      
      // else if(term == ctLocal){ // that means someBody has already sent the requestVote as it has already seen this term     
      mutex_vf.lock();
      if(votedFor[topicID] == -1 || votedFor[topicID] == candidateID) {
        int voter_lli = -1;
        int voter_llt = 0;
        if(logs[topicID].size()>0){
          voter_lli = logs[topicID].back().index;
          voter_llt = logs[topicID].back().term;
        }

        if(llt > voter_llt || (llt == voter_llt && lli >= voter_lli)) { // candidate has longer log than voter or ..
          resp->set_term(ctLocal); 
          resp->set_votegranted(true);
          beginElectionTimer[topicID].set_running(false);
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
      using namespace std::chrono;
      auto start = high_resolution_clock::now();
      thread tmpthread(runElection, topicID);
      tmpthread.detach();
      auto stop = high_resolution_clock::now();
      auto duration = duration_cast<microseconds>(stop - start);
      cout << "Time required to start election : " << duration.count() << endl;

      return Status::OK;
    }
        
  Status AppendEntries(ServerContext *context, const AppendEntriesRequest *request, AppendEntriesResponse *response) override
  {
    printf("[Broker(Raft)Server:AppendEntries]Received RPC!\n");
    //Process Append Entries RPC
    bool rpcSuccess = false;
    int topicId = request->topicid();
    if(request->term() >= currentTerm[topicId]){
      printf("[Broker(Raft)Server:AppendEntries]Condn satisfied: request->term() >= currentTerm[topicId]\n");

      mutex_ct.lock();
      currentTerm[topicId] = (int)request->term(); // updating current term
      // pmetadata->Put(leveldb::WriteOptions(), "currentTerm", to_string(currentTerm));
      mutex_ct.unlock();
      
      mutex_cs.lock();
      State csLocal = currStateMap[topicId];
      mutex_cs.unlock();
      // if(csLocal != FOLLOWER) // candidates become followers
      //   setCurrState(FOLLOWER); 

      int leaderCommitIndex = request->leadercommitindex();
      int prevLogIndex = request->prevlogindex();
      
      // printf("[Broker(Raft)Server:AppendEntries]Gonna go and check if logs needs update\n");
      printf("[Broker(Raft)Server:AppendEntries]prevLogIndex: %d\n", prevLogIndex);
      printf("[Broker(Raft)Server:AppendEntries]leaderCommitIndex: %d\n", leaderCommitIndex);
      if((prevLogIndex == -1) || (logs[topicId][prevLogIndex].term == request->prevlogterm()))  {
          //append and change commit index
          printf("[Broker(Raft)Server:AppendEntries]request->entries().size(): %d\n", request->entries().size());
          if(request->entries().size() > 0) {
            std::vector<dps::LogEntry> logEntries(request->entries().begin(), request->entries().end());
            updateLog(topicId, logEntries, prevLogIndex+1, request->leadercommitindex());
          }
          // updateLog should handle db update
          rpcSuccess = true;
      }
      // }
      // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
      mutex_ci.lock();
      mutex_lli.lock();
      if(leaderCommitIndex > commitIndex[topicId]) {
        commitIndex[topicId] = std::min(leaderCommitIndex, lastLogIndex[topicId]);
        rpcSuccess = true;
        printRaftLog();
      }
      mutex_lli.unlock();
      mutex_ci.unlock();
    } 
    response->set_currterm(currentTerm[topicId]);
    response->set_success(rpcSuccess);
    return Status::OK;
  }

  Status BrokerUp(ServerContext *context, const BrokerUpRequest *request, BrokerUpResponse *reponse) override 
  {
    uint brokerupid = request->brokerid();
    printf("[BrokerUp] Broker %d now alive!\n", brokerupid);
    mutex_sle.lock();
    sendLogEntries[brokerupid] = true;
    mutex_sle.unlock();
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

void initializePersistedValues() {
  string value = "";
  for(auto id: topicsInCluster) {
    leveldb::Status currentTermStatus = pmetadata[id]->Get(leveldb::ReadOptions(), "currentTerm", &value);
    mutex_ct.lock();
    if (!currentTermStatus.ok()) {
      std::cerr << "[initializePersistedValues] currentTerm[" << id << "]: Error: " << currentTermStatus.ToString() << endl;
      currentTerm[id] = 0;
    } else {
      currentTerm[id] = stoi(value);
      printf("[initializePersistedValues] currentTerm[%d] = %d\n", id, currentTerm[id]);
    }
    mutex_ct.unlock();

    value = "";
    leveldb::Status lastAppliedStatus = pmetadata[id]->Get(leveldb::ReadOptions(), "lastApplied", &value);
    mutex_la.lock();
    if(!lastAppliedStatus.ok()) {
      std::cerr << "[initializePersistedValues] lastApplied[" << id << "]: Error: " << lastAppliedStatus.ToString() << endl;
      lastApplied[id] = -1;
    } else {
      lastApplied[id] = stoi(value);
      printf("[initializePersistedValues] lastApplied[%d] = %d\n", id, lastApplied[id]);
    }
    mutex_la.unlock();

    value = "";
    leveldb::Status votedForStatus = pmetadata[id]->Get(leveldb::ReadOptions(), "votedFor", &value);
    mutex_vf.lock();
    if(!votedForStatus.ok()) {
      std::cerr << "[initializePersistedValues] votedFor[" << id << "]: Error: " << votedForStatus.ToString() << endl;
      votedFor[id] = -1;
    } else {
      votedFor[id] = stoi(value);
      printf("[initializePersistedValues] votedFor[%d] = %d\n", id, votedFor[id]);
    }
    mutex_vf.unlock();

    int logidx = 0;
    while(true) {
      string logString = "";
      leveldb::Status logstatus = plogs[id]->Get(leveldb::ReadOptions(), to_string(logidx), &logString);
      if(logstatus.ok()) {
        Log l(logString);
        logs[id].push_back(l);
        if(logidx <= lastApplied[id])
          messageQ[id].push_back(l.msg);
        logidx++;
      } else {
        std::cerr << "[initializePersistedValues] logidx[" << id <<"] = " << logidx << ": Error: " << logstatus.ToString() << endl;
        break;
      }
    }
    mutex_lli.lock();
    lastLogIndex[id] = logidx - 1;
    mutex_lli.unlock();
    printf("[initializePersistedValues] Loaded logs into memory for topic %d till index = %d\n", id, logidx-1);
    printf("[initializePersistedValues] Loaded messageQ into memory for topic %d till index = %d\n", id, ((lastApplied[id] <= logidx-1) ? lastApplied[id] : logidx-1));
  }
}

void initializeVolatileValues() {
  for(auto id: topicsInCluster) {
    beginElectionTimer[id] = Timer(1, MAX_ELECTION_TIMEOUT);
    setCurrState(id, FOLLOWER);
    votesReceived[id] = 0;
    commitIndex[id] = -1;
    updateCommitIndexFlag[id] = false;
    for(auto& [brokerId, si] : brokersInCluster) {
      nextIndex[id].insert(std::make_pair(brokerId, 0));
      matchIndex[id].insert(std::make_pair(brokerId, -1));
    }
  }

  mutex_tul.lock();
  vector<int> tulLocal = topicsUnderLeadership;
  mutex_tul.unlock();
  for(uint id: tulLocal) {
    setCurrState(id, LEADER);
    commitIndex[id] = lastApplied[id];
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
  openOrCreateDBs();
  initializePersistedValues();
  initializeVolatileValues();
  for(auto si: brokersInCluster) {
    printf("Broker: %d-%s in Cluster: %d\n", si.first, si.second.server_name.c_str(), clusterID);
  }
  for(uint tpcid: topicsInCluster) {
    printf("Topic added to cluster: %d\n", tpcid);
  }

  for(auto& brokerId : brokersInCluster) {
    if(brokerId.first == serverID) continue;
    sendLogEntries[brokerId.first] = true;
    appendEntriesThreads[brokerId.first] = thread { invokeAppendEntries, brokerId.first }; 
  }

  thread heartbeat(runHeartbeatTimer);
  thread updateCommitIndex(checkAndUpdateCommitIndex);
  thread addToMessageQ(executeLog);
  RunGrpcServer(brokersInCluster[serverID].server_addr);
  if(heartbeat.joinable()) heartbeat.join();
  if(updateCommitIndex.joinable()) updateCommitIndex.join();
  if(addToMessageQ.joinable()) addToMessageQ.join();
  return 0;
}