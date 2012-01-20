#!/usr/local/bin/thrift --gen py:utf8strings --gen java:beans,nocamel,hashcode

namespace java elephantdb.keyval.generated

include "core.thrift"

service ElephantDB extends ElephantDBShared {
  // This interface will allow java clients to send kryo-serialized
  // keys over the wire.
  list<KryoRegistration> getRegistrations(1: string domain);
  Value kryoGet(1: string domain, 2: binary key);
  
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
