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

service ElephantDB {
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

  DomainStatus getDomainStatus(1: string domain);
  list<string> getDomains();
  Status getStatus();
  bool isFullyLoaded();
  bool isUpdating();

  /*
    This method will re-download the global configuration file and add any new domains
  */
  void updateAll() throws (1: InvalidConfigurationException ice);
  //returns whether it's updating or not
  bool update(1: string domain);
}
