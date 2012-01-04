(ns elephantdb.persistence.shardset-test
  (:use midje.sweet
        elephantdb.common.testing)
  (:import [elephantdb DomainSpec]
           [elephantdb.persistence ShardSet ShardSetImpl]))

(defn mk-spec
  "Returns a spec configured for the supplied number of shards."
  [shard-count]
  (DomainSpec. (elephantdb.persistence.JavaBerkDB.)
               (elephantdb.partition.HashModScheme.)
               shard-count))

(defn shard-set [path spec]
  (ShardSetImpl. path spec))

(defn shard-count [num-shards]
  (with-fs-tmp [_ tmp]
    (let [set (shard-set tmp (mk-spec num-shards))]
      (.getNumShards set))))

(fact
  (shard-count 10)  => 10
  (shard-count -10) => (throws AssertionError))


