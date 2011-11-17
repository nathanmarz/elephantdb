#!/usr/local/bin/thrift --gen py:utf8strings --gen java:beans,nocamel,hashcode

namespace java elephantdb.generated

struct Value {
  1: optional binary data;
}

struct LoadingStatus {  
}

struct ReadyStatus {
  1: optional LoadingStatus update_status;
}

struct FailedStatus {
  1: string error_message;
}

struct ShutdownStatus {
}

union DomainStatus {
  1: ReadyStatus ready;
  2: LoadingStatus loading;
  3: FailedStatus failed;
  4: ShutdownStatus shutdown;
}

struct Status {
  1: required map<string, DomainStatus> domain_statuses;
}

exception DomainNotFoundException {
  1: required string domain;
}

exception DomainNotLoadedException {
  1: required string domain;
}

exception HostsDownException {
  1: required list<string> hosts;
}

exception InvalidConfigurationException {
  1: required list<string> mismatched_domains; // can happen if domain location changes or if num shards changes
  2: required bool port_changed;
  3: required bool hosts_changed;
}

exception WrongHostException {
}

service ElephantDBShared {
  DomainStatus getDomainStatus(1: string domain);
  list<string> getDomains();
  Status getStatus();
  bool isFullyLoaded();
  bool isUpdating();
  
/*
   This method will re-download the global configuration file and add any new domains
   */
  void updateAll() throws (1: InvalidConfigurationException ice);
  bool update(1: string domain); // is the supplied domain updating?
}

service ElephantDB extends ElephantDBShared {
  Value get(1: string domain, 2: binary key)
    throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);
  Value getString(1: string domain, 2: string key)
    throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);
  Value getInt(1: string domain, 2: i32 key)
    throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);
  Value getLong(1: string domain, 2: i64 key)
    throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);

  list<Value> multiGet(1: string domain, 2: list<binary> key)
    throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);
  list<Value> multiGetString(1: string domain, 2: list<string> key)
    throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);
  list<Value> multiGetInt(1: string domain, 2: list<i32> key)
    throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);
  list<Value> multiGetLong(1: string domain, 2: list<i64> key)
    throws (1: DomainNotFoundException dnfe, 2: HostsDownException hde, 3: DomainNotLoadedException dnle);

  list<Value> directMultiGet(1: string domain, 2: list<binary> key)
    throws (1: DomainNotFoundException dnfe, 2: DomainNotLoadedException dnle, 3: WrongHostException whe);
}


service ElephantDBSet extends ElephantDBShared {
  // Required kv pairs:
  // kv == (setKey, member) -> null
  // (setKey + "SIZE") -> i64
  // (setKey) -> list<string>

  bool member(1: string domain, 2: string setKey, 3: string member); // member?
  bool members(1: string domain, 2: string setKey); // returns all members
  list<string> diff(1: string domain, 2: string keyOne, 3: string keyTwo); // take variable args
  list<string> union(1: string domain, 2: string keyOne, 3: string keyTwo); // take variable args
  list<string> intersection(1: string domain, 2: string keyOne, 3: string keyTwo); // take variable args
  i64 size(1: string domain, 2: string key);

  list<Value> multiMember(1: string domain, 2: string setKey, 3: list<string> setVals);
}

service ElephantDBList extends ElephantDBShared {
  // Required kv pairs:
  
  // kv == (setKey + "TOTALSIZE") -> i64
  // kv == (setKey + "CHUNKS") -> i32
  // (setKey, chunkIdx) -> list<Value>  

  i32 length(1: string domain, 2: string key);
  i32 numChunks(1: string domain, 2: string key)  
  list<Value> getChunk(1: string domain, 2: string key, i32 chunkIdx);
  Value index(1: string domain, 2: string key, 3: i32 idx); // get item at index
  list<Value> range(1: string domain, 2: string key, 3: i32 startIdx, 4: i32 endIdx);
  list<Value> take(1: string domain, 2: string key, 3: i32 elems); // redundant with range.
  list<Value> takeAll(1: string domain, 2: string key); // redundant? we can use range(0, length + 1);

  list<Value> multiIndex(1: string domain, 2: string setKey, 3: list<i32> indices);
}

service ElephantDBSearch extends ElephantDBShared {
  // Thinking a bit more on this one. Lucene on the back end!
}
