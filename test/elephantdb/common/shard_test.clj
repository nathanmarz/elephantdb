(ns elephantdb.common.shard-test
  (:use elephantdb.common.shard
        midje.sweet))

(fact
  (compute-host->shards ["a" "b"] 5 1)
  => {"b" #{1 3}, "a" #{0 2 4}})
