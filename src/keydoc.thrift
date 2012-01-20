#!/usr/local/bin/thrift --gen py:utf8strings --gen java:beans,nocamel,hashcode

namespace java elephantdb.generated.keydoc

include "core.thrift"

service ElephantDBDoc extends core.ElephantDBShared {
  // Required kv pairs:
  // key -> Document
  
  // go from Value to Document or something.
  core.Value get(1: string domain, 2: string key);
  core.Value getField(1: string domain, 2: string key, 3: string field);
  core.Value getFields(1: string domain, 2: string key, 3: list<string> fields);
}
