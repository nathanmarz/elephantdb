#!/usr/local/bin/thrift --gen py:utf8strings --gen java:beans,nocamel,hashcode

namespace java elephantdb.search.generated

include "core.thrift"

service ElephantDBSearch extends ElephantDBShared {
  // Thinking a bit more on this one. Lucene on the back end!
}
