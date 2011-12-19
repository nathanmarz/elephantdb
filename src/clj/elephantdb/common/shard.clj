(ns elephantdb.common.shard
  "Functions for dealing with sharding; the goal of this namespace is
   to generate the host->shard and shard->host map and provide various
   lookup functions inside of it."
  (:require [elephantdb.common.util :as u]))

(defn- host->shard-assigner
  [[hosts hosts-to-shards] shard]
  (let [[host & hosts] (drop-while #(get-in hosts-to-shards
                                            [% shard])
                                   hosts)
        existing (get hosts-to-shards host #{})]
    [hosts (->> (conj existing shard)
                (assoc hosts-to-shards host))]))

(defn compute-host->shards
  "Returns a map of host-> shard set. For example:

  (compute-host-to-shards 5 [\"a\" \"b\"] 1)
  ;=> {\"b\" #{1 3}, \"a\" #{0 2 4}}"
  [hosts shard-count replication]
  (u/safe-assert (>= (count hosts) replication)
                 "Replication greater than number of servers")
  (->> (u/repeat-seq replication (range shard-count))
       (reduce host->shard-assigner [(cycle hosts) {}])
       (second)))

;; `generate-index` is important; every domain will have one of these
;; maps, and will use it to discover where keys are located.

(defn generate-index
  "Generates a shard-index for use by a single domain."
  [hosts shard-count replication]
  (let [hosts-to-shards (compute-host-to-shards hosts
                                                shard-count
                                                replication)]
    {::hosts->shards hosts-to-shards
     ::shards->hosts (->> (u/reverse-multimap hosts-to-shards)
                            (u/val-map set))}))

(defn shard-set
  "Returns the set of shards located on the supplied host."
  [shard-index host]
  (-> (::hosts->shards shard-index)
      (get host)))

(defn host-set
  "Returns the set of hosts responsible for the supplied shard."
  [shard-index shard]
  (-> (::shards->hosts shard-index)
      (get shard)))

(defn prioritize-hosts
  "Accepts a domain, a key and some predicate by which to prioritize
  the keys. #{localhost} would be a nice example."
  [shard-index shard pred]
  (->> (host-set shard-index shard)
       (shuffle)
       (u/prioritize pred)))
