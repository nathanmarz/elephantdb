(ns elephantdb.common.shard-test
  (:use elephantdb.common.shard
        midje.sweet))

(fact
  "The function should make make sure that each shard appears at least
   as much as specified by the replication count."
  (compute-host->shards ["a" "b"] 5 1) => {"a" #{0 2 4}
                                           "b" #{1 3}}
  (compute-host->shards ["a" "b" "c"] 5 2) => {"a" #{0 1 3 4}
                                               "b" #{1 2 4}
                                               "c" #{0 2 3}}

  "Can't specify a replication larger than the shard-count."
  (compute-host->shards ["a" "b"] 3 3) => (throws AssertionError))


(fact "shard-set tests"
  (let [index (generate-index ["a" "b" "c"] 5 2)]
    "Shard Sets should match the above tests of compute-host->shards."
    (shard-set index "a") => #{0 1 3 4}
    (shard-set index "b") => #{1 2 4}
    (shard-set index "c") => #{0 2 3}

    (host-set index 0) => #{"a" "c"}
    (host-set index 1) => #{"a" "b"}
    (host-set index 2) => #{"b" "c"}
    (host-set index 3) => #{"a" "c"}
    (host-set index 4) => #{"a" "b"}))

(tabular
 (fact "host prioritization testing. If the priority predicate is a
 set with a single host, we should see that host at the front of every
 sequence returned by prioritize-hosts."
   (let [index (generate-index ["a" "b" "c" "d" "e"] 10 5)]
     (prioritize-hosts index 5  #{?host}) => (has-prefix [?host])))
 ?host
 "a"
 "b"
 "c"
 "d"
 "e")
