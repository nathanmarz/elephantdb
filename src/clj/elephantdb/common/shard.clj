(ns elephantdb.common.shard
  "Functions for dealing with sharding; the goal of this namespace is
   to generate the host->shard and shard->host map and provide various
   lookup functions inside of it."
  (:require [jackknife.core :as u]
            [jackknife.seq :as  seq]))

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

  (compute-host->shards 5 [\"a\" \"b\"] 1)
  ;=> {\"b\" #{1 3}, \"a\" #{0 2 4}}"
  [hosts shard-count replication]
  (u/safe-assert (>= (count hosts) replication)
                 "Replication greater than number of servers")
  (->> (seq/repeat-seq replication (range shard-count))
       (reduce host->shard-assigner [(cycle hosts) {}])
       (second)))

;; `generate-index` is important; every domain will have use one of
;; these maps to determine the host to which a particular shard key
;; has been sent.

(defn generate-index
  "Generates a shard-index for use by a single domain."
  [hosts shard-count replication]
  (let [hosts->shards (compute-host->shards hosts
                                            shard-count
                                            replication)]
    {::hosts->shards hosts->shards
     ::shards->hosts (->> (u/reverse-multimap hosts->shards)
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
  the hosts. For example, setting `pred` to `#{localhost}` would force
  the hostname bound to `localhost` to the front of the sequence."
  [shard-index shard pred]
  (->> (host-set shard-index shard)
       (shuffle)
       (seq/prioritize pred)))
