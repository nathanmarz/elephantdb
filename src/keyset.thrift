#!/usr/local/bin/thrift --gen py:utf8strings --gen java:beans,nocamel,hashcode

namespace java elephantdb.keyset.generated

include "core.thrift"

service ElephantDBSet extends ElephantDBShared {
  // Required kv pairs:
  // kv == (setKey, member) -> null
  // (setKey + "SIZE") -> i64
  // (setKey) -> list<string>

  bool member(1: string domain, 2: string setKey, 3: string member); // member?
  bool members(1: string domain, 2: string setKey); // returns all members
  list<string> setDiff(1: string domain, 2: string keyOne, 3: string keyTwo); // take variable args
  list<string> setUnion(1: string domain, 2: string keyOne, 3: string keyTwo); // take variable args
  list<string> setIntersection(1: string domain, 2: string keyOne, 3: string keyTwo); // take variable args
  i64 size(1: string domain, 2: string key);

  list<Value> multiMember(1: string domain, 2: string setKey, 3: list<string> setVals);
}


