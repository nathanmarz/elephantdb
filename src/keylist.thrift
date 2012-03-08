#!/usr/local/bin/thrift --gen py:utf8strings --gen java:beans,nocamel,hashcode

namespace java elephantdb.generated.keylist

include "core.thrift"

service ElephantDBList extends core.ElephantDBShared {
  // Required kv pairs:
  
  // kv == (setKey + "TOTALSIZE") -> i64
  // kv == (setKey + "CHUNKS") -> i32
  // (setKey, chunkIdx) -> list<Value>  

  i32 length(1: string domain, 2: string key);
  i32 numChunks(1: string domain, 2: string key)  
  list<core.Value> getChunk(1: string domain, 2: string key, 3: i32 chunkIdx);
  core.Value index(1: string domain, 2: string key, 3: i32 idx); // get item at index
  list<core.Value> range(1: string domain, 2: string key, 3: i32 startIdx, 4: i32 endIdx);
  list<core.Value> take(1: string domain, 2: string key, 3: i32 elems); // redundant with range.
  list<core.Value> takeAll(1: string domain, 2: string key); // redundant? we can use range(0, length + 1);
}
