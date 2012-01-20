#!/usr/local/bin/thrift --gen py:utf8strings --gen java:beans,nocamel,hashcode

namespace java elephantdb.generated.keyval

include "core.thrift"

service ElephantDB extends core.ElephantDBShared {  
  core.Value get(1: string domain, 2: binary key)
    throws (1: core.DomainNotFoundException dnfe,
  2: core.HostsDownException hde,
  3: core.DomainNotLoadedException dnle);
  core.Value getString(1: string domain, 2: string key)
    throws (1: core.DomainNotFoundException dnfe,
  2: core.HostsDownException hde,
  3: core.DomainNotLoadedException dnle);
  
  core.Value getInt(1: string domain, 2: i32 key)
    throws (1: core.DomainNotFoundException dnfe,
  2: core.HostsDownException hde,
  3: core.DomainNotLoadedException dnle);

  core.Value getLong(1: string domain, 2: i64 key)
    throws (1: core.DomainNotFoundException dnfe,
  2: core.HostsDownException hde,
  3: core.DomainNotLoadedException dnle);
  
  list<core.Value> multiGet(1: string domain, 2: list<binary> key)
    throws (1: core.DomainNotFoundException dnfe,
  2: core.HostsDownException hde,
  3: core.DomainNotLoadedException dnle);
  
  list<core.Value> multiGetString(1: string domain, 2: list<string> key)
    throws (1: core.DomainNotFoundException dnfe,
  2: core.HostsDownException hde,
  3: core.DomainNotLoadedException dnle);
  
  list<core.Value> multiGetInt(1: string domain, 2: list<i32> key)
    throws (1: core.DomainNotFoundException dnfe,
  2: core.HostsDownException hde,
  3: core.DomainNotLoadedException dnle);
  
  list<core.Value> multiGetLong(1: string domain, 2: list<i64> key)
    throws (1: core.DomainNotFoundException dnfe,
  2: core.HostsDownException hde,
  3: core.DomainNotLoadedException dnle);

  list<core.Value> directMultiGet(1: string domain, 2: list<binary> key)
    throws (1: core.DomainNotFoundException dnfe,
  2: core.HostsDownException hde,
  3: core.DomainNotLoadedException dnle);
  
}
