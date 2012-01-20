#!/usr/local/bin/thrift --gen py:utf8strings --gen java:beans,nocamel,hashcode

namespace java elephantdb.keydoc.generated

include "core.thrift"

service ElephantDBDoc extends ElephantDBShared {
  // Required kv pairs:
  // key -> Document
  
  // go from Value to Document or something.
  Value get(1: string domain, 2: string key);
  Value getField(1: string domain, 2: string key, 3: string field);
  Value getFields(1: string domain, 2: string key, 3: list<string> fields);
}
