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


typedef leveldb::DB *leveldbPtr;

enum State {FOLLOWER, CANDIDATE, LEADER};
string stateNames[3] = {"FOLLOWER", "CANDIDATE", "LEADER"};

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
    string key;
    string value;

    Log() {}
    Log(int index, int term, string key, string value){
      this->index = index;
      this->term = term;
      this->key = key;
      this->value = value;
    }

    // log string is of the format- index;term;key;value
    Log(string logString) {
      vector<string> logParts = split(logString, ';');
      index = stoi(logParts[0]);
      term = stoi(logParts[1]);
      key = logParts[2];
      value = logParts[3];
    }

    string toString() {
      string logString = "";
      logString += to_string(index) + ";";
      logString += to_string(term) + ";";
      logString += key + ";";
      logString += value;
      return logString;
    }
};

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


// *************************** Persistent Variables **************************
unordered_map<int, int> currentTerm;
unordered_map<int, int> lastApplied;
unordered_map<int, int> votedFor;
unordered_map<int, vector<Log>> logs;



#endif