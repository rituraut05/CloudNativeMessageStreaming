#ifndef UTILS_HPP
#define UTILS_HPP
#include <shared_mutex>
#include <unordered_map>
#include <string>
#include <vector>
#include "leveldb/db.h"

using std::vector;
using std::unordered_map;
using std::string;
using std::to_string;


#define HEARTBEAT_TIMEOUT       1000
#define HEART   "\xE2\x99\xA5"
#define SPADE   "\xE2\x99\xA0"


//// Added for testing, remove after getConfig API is written ////

// #define BROKER_CNT              3
// #define SERVER1 "0.0.0.0:50052" // node1
// #define SERVER2 "0.0.0.0:50053" // node2
// #define SERVER3 "0.0.0.0:50054" // node3
// string serverIPs[BROKER_CNT] = {SERVER1, SERVER2, SERVER3};

////////////////////////////////////////////////////////////

typedef leveldb::DB *leveldbPtr;

enum State {FOLLOWER, CANDIDATE, LEADER};
string stateNames[3] = {"FOLLOWER", "CANDIDATE", "LEADER"};

// ***************************** Volatile variables *****************************

unordered_map<int, State> currStateMap;

std::shared_mutex mutex_ci; // for commitIndex
std::shared_mutex mutex_lli; // for lastLogIndex
std::shared_mutex mutex_votes; // for votesReceived
std::shared_mutex mutex_leader; // for leaderID
std::shared_mutex mutex_cs; // for currState
std::shared_mutex mutex_er; // for electionRunning
std::shared_mutex mutex_aer; // for appendEntriesRunning
std::shared_mutex mutex_ct; // for currentTerm
std::shared_mutex mutex_la; // for lastApplied
std::shared_mutex mutex_vf; // for votedFor


/*
* logs are stored as key - value pairs in plogs with
* key = index and value = log.toString()
*/
unordered_map<int, leveldbPtr> plogs;

/*
* Stores other persistent variables ie
* currentTerm, lastApplied, votedFor 
*/
unordered_map<int, leveldbPtr> pmetadata;
/*
* Actual db service 
*/
unordered_map<int, leveldbPtr> replicateddb;

// ******************************** Log class *********************************

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

class Log {
  public:
    int index;
    int term;
    int topic;
    int msgindex;
    string msg;

    Log() {}
    Log(int index, int term, int topic, int msgindex, string msg){
      this->index = index;
      this->term = term;
      this->topic = topic;
      this->msgindex = msgindex;
      this->msg = msg;
    }

    // log string is of the format- index;term;topic;msgindex;msg
    Log(string logString) {
      vector<string> logParts = split(logString, ';');
      index = stoi(logParts[0]);
      term = stoi(logParts[1]);
      topic = stoi(logParts[2]);
      msgindex = stoi(logParts[3]);
      msg = logParts[4];
    }

    string toString() {
      string logString = "";
      logString += to_string(index) + ";";
      logString += to_string(term) + ";";
      logString += to_string(topic) + ";";
      logString += to_string(msgindex) + ";";
      logString += msg;
      return logString;
    }
};

// *************************** Persistent Variables **************************

unordered_map<int, int> currentTerm;
unordered_map<int, int> lastApplied;
unordered_map<int, int> votedFor;
unordered_map<int, vector<Log>> logs;

// ************************** DPS variables ************************************
vector<int> topicsInCluster;
vector<int> topicsUnderLeadership;

#endif